-- ============================================================
-- UTILITY FUNCTIONS
-- ============================================================

/* TEST DATABASE CONNECTION */
CREATE OR REPLACE FUNCTION watchdog.check_connection()
    RETURNS TABLE(okay BOOLEAN)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY SELECT TRUE;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;


-- ============================================================
-- PIPELINE FUNCTIONS
-- ============================================================

/* GET PRICE DATES */
CREATE OR REPLACE FUNCTION watchdog.get_dates()
    RETURNS TABLE("date" TEXT)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT TO_CHAR(effective_date, 'YYYYMMDD')
    FROM prices;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;


/* GET ITEM SKUS */
CREATE OR REPLACE FUNCTION watchdog.get_skus()
    RETURNS TABLE(sku VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT i.sku
    FROM items i;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;


/* GET SUPERMARKET CODES */
CREATE OR REPLACE FUNCTION watchdog.get_supermarkets()
    RETURNS TABLE(supermarket VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT s.supermarket
    FROM supermarkets s;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;


/* ETL OF BEST DEALS */
CREATE OR REPLACE FUNCTION watchdog.update_deals()
    RETURNS VOID
    SET search_path = 'watchdog'
AS $$
DECLARE
    latest_date DATE;
BEGIN
    SELECT MAX(effective_date)
    INTO latest_date
    FROM prices;

    IF latest_date IS NULL THEN
        RETURN;
    END IF;

    TRUNCATE deals;

    WITH
        daily_min_prices AS (
            SELECT
                sku,
                effective_date,
                MIN(unit_price) AS unit_price
            FROM prices
            GROUP BY sku, effective_date
        ),
        summary_statistics AS (
            SELECT
                sku,
                COUNT(1) AS frequency,
                AVG(unit_price) AS average_price,
                COALESCE(STDDEV(unit_price), 0) AS std_price,
                MIN(unit_price) AS q0_price,
                MAX(unit_price) AS q4_price
            FROM daily_min_prices
            GROUP BY sku
        ),
        latest_deals AS (
            SELECT
                p.sku,
                p.supermarket,
                p.promotion_en,
                p.promotion_zh,
                p.original_price,
                p.unit_price,
                s.preference
            FROM prices p
            LEFT JOIN supermarkets s ON p.supermarket = s.supermarket
            WHERE p.effective_date = latest_date
        ),
        preferred_deals AS (
            SELECT
                sku,
                supermarket,
                promotion_en,
                promotion_zh,
                original_price,
                unit_price,
                RANK() OVER (PARTITION BY sku ORDER BY unit_price, preference) AS rnk
            FROM latest_deals
        ),
        price_summary AS (
            SELECT
                d.sku,
                d.supermarket,
                d.promotion_en,
                d.promotion_zh,
                d.original_price,
                d.unit_price,
                s.frequency,
                s.average_price,
                s.std_price,
                s.q0_price,
                -1 * (s.std_price + 0.0001) + s.average_price AS unadjusted_bid,
                s.q4_price
            FROM preferred_deals d
            INNER JOIN summary_statistics s ON d.sku = s.sku
            WHERE d.rnk = 1
        ),
        price_adjustment AS (
            SELECT
                sku,
                supermarket,
                promotion_en,
                promotion_zh,
                original_price,
                unit_price,
                frequency,
                average_price,
                std_price,
                q0_price,
                CASE WHEN unadjusted_bid < q0_price THEN q0_price ELSE unadjusted_bid END AS bid_price,
                q4_price
            FROM price_summary
        )
    INSERT INTO deals
    SELECT
        sku,
        supermarket,
        promotion_en,
        promotion_zh,
        original_price,
        unit_price,
        frequency,
        average_price,
        std_price,
        q0_price,
        bid_price,
        q4_price,
        unit_price <= bid_price AS is_deal
    FROM price_adjustment;
END;
$$ LANGUAGE plpgsql VOLATILE;


/* GET IDS OF USERS WITH PRICE ALERT */
CREATE OR REPLACE FUNCTION watchdog.get_users()
    RETURNS TABLE(id TEXT)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    WITH
        users_with_deals AS (
            SELECT w.user_id
            FROM watchlists w
            WHERE EXISTS (SELECT 1 FROM deals d WHERE w.sku = d.sku AND d.is_deal = TRUE)
        )
    SELECT u.user_id
    FROM users u
    WHERE u.is_subscribed = TRUE
        AND (u.snoozed_until IS NULL OR u.snoozed_until <= (now() AT TIME ZONE 'Asia/Hong_Kong')::DATE)
        AND EXISTS (SELECT 1 FROM users_with_deals i WHERE u.user_id = i.user_id);
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;


/* LOG OMISSION DATE */
CREATE OR REPLACE FUNCTION watchdog.log_omission()
    RETURNS VOID
    SET search_path = 'watchdog'
AS $$
BEGIN
    INSERT INTO omissions (omission_date)
        VALUES (CURRENT_DATE)
    ON CONFLICT (omission_date) DO NOTHING;
END;
$$ LANGUAGE plpgsql VOLATILE;


/* REMOVE LEGACY PRICES */
CREATE OR REPLACE FUNCTION watchdog.remove_legacy_prices(p_interval INT)
    RETURNS VOID
    SET search_path = 'watchdog'
AS $$
BEGIN
    DELETE FROM prices
    WHERE effective_date < CURRENT_DATE - p_interval - 1;
END;
$$ LANGUAGE plpgsql VOLATILE;


-- ============================================================
-- BOT FUNCTIONS
-- ============================================================

/* REGISTER A USER */
CREATE OR REPLACE FUNCTION watchdog.register_user(
    p_user_id TEXT,
    p_language VARCHAR DEFAULT 'en'
)
    RETURNS TABLE("language" VARCHAR, "status" TEXT)
    SET search_path = 'watchdog'
AS $$
DECLARE
    user_exists BOOLEAN;
    language_code VARCHAR;
BEGIN
    SELECT EXISTS (SELECT 1 FROM users WHERE user_id = p_user_id)
    INTO user_exists;

    IF user_exists THEN
        RETURN QUERY
        SELECT
            display_language,
            'repeat'
        FROM users
        WHERE user_id = p_user_id;
    ELSE
        language_code := CASE
            WHEN p_language = 'zh' THEN 'zh'
            ELSE 'en'
        END;

        INSERT INTO users (user_id, display_language, is_subscribed)
            VALUES (p_user_id, language_code, FALSE);

        RETURN QUERY
        SELECT
            language_code,
            'new';
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


/* GET USER LANGUAGE */
CREATE OR REPLACE FUNCTION watchdog.get_language(p_user_id TEXT)
    RETURNS TABLE("language" VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT COALESCE(
        (SELECT display_language FROM users WHERE user_id = p_user_id),
        'na'
    );
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE SECURITY DEFINER;


/* EDIT A WATCHLIST ITEM */
CREATE OR REPLACE FUNCTION watchdog.edit_watchlist(
    p_user_id TEXT,
    p_sku VARCHAR
)
    RETURNS TABLE("language" VARCHAR, "status" TEXT, valid BOOLEAN)
    SET search_path = 'watchdog'
AS $$
DECLARE
    language_code VARCHAR;
    sku_exists BOOLEAN;
BEGIN
    SELECT
        display_language,
        EXISTS (SELECT 1 FROM items WHERE sku = p_sku)
    INTO language_code, sku_exists
    FROM users
    WHERE user_id = p_user_id;

    IF language_code IS NULL THEN
        RETURN QUERY SELECT 'na', 'user_not_found', FALSE;
        RETURN;
    END IF;

    IF NOT sku_exists THEN
        RETURN QUERY SELECT language_code, 'invalid_sku', FALSE;
        RETURN;
    END IF;

    INSERT INTO watchlists (user_id, sku)
        VALUES (p_user_id, p_sku)
    ON CONFLICT (user_id, sku) DO NOTHING;

    IF FOUND THEN
        RETURN QUERY SELECT language_code, 'add', TRUE;
    ELSE
        DELETE FROM watchlists
        WHERE user_id = p_user_id AND sku = p_sku;
        RETURN QUERY SELECT language_code, 'remove', TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


/* GET WATCHLIST ITEMS */
CREATE OR REPLACE FUNCTION watchdog.get_watchlist(p_user_id TEXT)
    RETURNS TABLE(sku VARCHAR, brand TEXT, "name" TEXT)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT
        w.sku,
        CASE WHEN u.display_language = 'en' THEN i.brand_en ELSE i.brand_zh END,
        CASE WHEN u.display_language = 'en' THEN i.name_en ELSE i.name_zh END
    FROM watchlists w
    LEFT JOIN items i ON w.sku = i.sku
    LEFT JOIN users u ON u.user_id = p_user_id
    WHERE w.user_id = p_user_id
    ORDER BY
        i.department_en,
        i.category_en,
        i.subcategory_en,
        w.sku;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE SECURITY DEFINER;


/* GET N RANDOM DEAL ITEMS */
CREATE OR REPLACE FUNCTION watchdog.draw_deals(
    p_user_id TEXT,
    p_limit INT DEFAULT 5
)
    RETURNS TABLE(
        sku VARCHAR,
        supermarket VARCHAR,
        promotion TEXT,
        original_price NUMERIC,
        unit_price NUMERIC,
        frequency INT,
        average_price NUMERIC,
        std_price NUMERIC,
        q0_price NUMERIC,
        q4_price NUMERIC,
        brand TEXT,
        "name" TEXT
    )
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.sku,
        d.supermarket,
        CASE WHEN u.display_language = 'en' THEN d.promotion_en ELSE d.promotion_zh END,
        d.original_price,
        d.unit_price,
        d.frequency,
        d.average_price,
        d.std_price,
        d.q0_price,
        d.q4_price,
        CASE WHEN u.display_language = 'en' THEN i.brand_en ELSE i.brand_zh END,
        CASE WHEN u.display_language = 'en' THEN i.name_en ELSE i.name_zh END
    FROM deals d
    INNER JOIN items i ON d.sku = i.sku
    LEFT JOIN users u ON u.user_id = p_user_id
    WHERE d.is_deal = TRUE
    ORDER BY RANDOM()
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER;


/* GET TIME SERIES OF ITEM PRICES */
CREATE OR REPLACE FUNCTION watchdog.get_prices(p_sku VARCHAR)
    RETURNS TABLE("date" TEXT, price NUMERIC)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT
        TO_CHAR(effective_date, 'YYYYMMDD'),
        MIN(unit_price)
    FROM prices
    WHERE sku = p_sku
    GROUP BY effective_date
    ORDER BY effective_date;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE SECURITY DEFINER;


/* GET ITEM INFORMATION */
CREATE OR REPLACE FUNCTION watchdog.get_item(
    p_user_id TEXT,
    p_sku VARCHAR
)
    RETURNS TABLE(frequency INT, bid_price NUMERIC, brand TEXT, "name" TEXT)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.frequency,
        d.bid_price,
        CASE WHEN u.display_language = 'en' THEN i.brand_en ELSE i.brand_zh END,
        CASE WHEN u.display_language = 'en' THEN i.name_en ELSE i.name_zh END
    FROM deals d
    INNER JOIN items i ON d.sku = i.sku
    LEFT JOIN users u ON u.user_id = p_user_id
    WHERE d.sku = p_sku;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE SECURITY DEFINER;


/* CHANGE SUBSCRIPTION STATUS */
CREATE OR REPLACE FUNCTION watchdog.change_subscription(p_user_id TEXT)
    RETURNS TABLE("language" VARCHAR, is_subscribed BOOLEAN)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    UPDATE users u
    SET is_subscribed = NOT u.is_subscribed, snoozed_until = NULL
    WHERE u.user_id = p_user_id
    RETURNING u.display_language, u.is_subscribed;

    IF NOT FOUND THEN
        RETURN QUERY SELECT 'na', FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


/* SNOOZE ALERTS UNTIL A GIVEN DATE */
CREATE OR REPLACE FUNCTION watchdog.snooze_user(p_user_id TEXT, p_until DATE)
    RETURNS TABLE("language" VARCHAR, snoozed_until DATE)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    UPDATE users u
    SET snoozed_until = GREATEST(u.snoozed_until, p_until)
    WHERE u.user_id = p_user_id
    RETURNING u.display_language, u.snoozed_until;

    IF NOT FOUND THEN
        RETURN QUERY SELECT 'na', p_until;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


/* CHANGE USER LANGUAGE */
CREATE OR REPLACE FUNCTION watchdog.change_language(p_user_id TEXT)
    RETURNS TABLE("language" VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    UPDATE users
    SET display_language = CASE WHEN display_language = 'en' THEN 'zh' ELSE 'en' END
    WHERE user_id = p_user_id
    RETURNING display_language;

    IF NOT FOUND THEN
        RETURN QUERY SELECT 'na';
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


/* GET ALERT */
CREATE OR REPLACE FUNCTION watchdog.get_alert(p_user_id TEXT)
    RETURNS TABLE(
        sku VARCHAR,
        supermarket VARCHAR,
        promotion TEXT,
        original_price NUMERIC,
        unit_price NUMERIC,
        brand TEXT,
        "name" TEXT
    )
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.sku,
        d.supermarket,
        CASE WHEN u.display_language = 'en' THEN d.promotion_en ELSE d.promotion_zh END,
        d.original_price,
        d.unit_price,
        CASE WHEN u.display_language = 'en' THEN i.brand_en ELSE i.brand_zh END,
        CASE WHEN u.display_language = 'en' THEN i.name_en ELSE i.name_zh END
    FROM deals d
    INNER JOIN items i ON d.sku = i.sku
    LEFT JOIN users u ON u.user_id = p_user_id
    WHERE d.is_deal = TRUE
        AND EXISTS (SELECT 1 FROM watchlists w WHERE d.sku = w.sku AND w.user_id = p_user_id)
    ORDER BY d.supermarket;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE SECURITY DEFINER;


/* REMOVE A USER */
CREATE OR REPLACE FUNCTION watchdog.remove_user(p_user_id TEXT)
    RETURNS TABLE("language" VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    DELETE FROM users
    WHERE user_id = p_user_id
    RETURNING display_language;

    IF NOT FOUND THEN
        RETURN QUERY SELECT 'na';
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
