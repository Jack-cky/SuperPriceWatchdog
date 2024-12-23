/* REGISTER AN USER */
CREATE OR REPLACE FUNCTION watchdog.register_user(usr_id TEXT, usr_lang VARCHAR DEFAULT 'en')
    RETURNS TABLE(_language VARCHAR, _status TEXT)
    SET search_path = 'watchdog'
AS $$
DECLARE
    is_exist BOOLEAN;
    language_code VARCHAR;
BEGIN
    SELECT EXISTS (SELECT 1 FROM users WHERE user_id = usr_id)
    INTO is_exist;
    
    IF is_exist THEN
        RETURN QUERY
        SELECT
            display_language
            , 'repeat'
        FROM users
        WHERE user_id = usr_id;
    ELSE
        SELECT CASE WHEN usr_lang = 'zh' THEN 'zh' ELSE 'en' END::VARCHAR
        INTO language_code;
        
        INSERT INTO users (user_id, display_language, is_subscribed)
            VALUES (usr_id, language_code, 'n');
        
        RETURN QUERY
        SELECT
            language_code
            , 'new';
    END IF;
END;
$$ LANGUAGE plpgsql;


/* GET USER LANGUAGE */
CREATE OR REPLACE FUNCTION watchdog.get_language(usr_id TEXT)
    RETURNS TABLE(_language VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT display_language
    FROM users
    WHERE user_id = usr_id
    UNION ALL
    SELECT 'na'
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;


/* EDIT A WATCHLIST ITEM */
CREATE OR REPLACE FUNCTION watchdog.edit_watchlist(usr_id TEXT, code VARCHAR)
    RETURNS TABLE(_language VARCHAR, _status TEXT, _valid BOOLEAN)
    SET search_path = 'watchdog'
AS $$
DECLARE
    language_code VARCHAR;
    is_exist BOOLEAN;
    is_valid BOOLEAN;
BEGIN
    SELECT 
        display_language
        , EXISTS (SELECT 1 FROM watchlists WHERE user_id = usr_id AND sku = code)
        , EXISTS (SELECT 1 FROM items WHERE sku = code)
    INTO language_code, is_exist, is_valid
    FROM users
    WHERE user_id = usr_id;
    
    IF is_valid AND is_exist THEN
        DELETE FROM watchlists
        WHERE 1 = 1
            AND user_id = usr_id
            AND sku = code;
        
        RETURN QUERY
        SELECT
            language_code
            , 'remove'
            , is_valid;
    ELSEIF is_valid AND NOT is_exist THEN
        INSERT INTO watchlists (user_id, sku)
            VALUES (usr_id, code);
        
        RETURN QUERY
        SELECT
            language_code
            , 'add'
            , is_valid;
    ELSE
        RETURN QUERY
        SELECT
            COALESCE(language_code, 'na')
            , 'na'
            , COALESCE(is_valid, False);
    END IF;
END;
$$ LANGUAGE plpgsql;


/* GET WATCHLIST ITEMS */
CREATE OR REPLACE FUNCTION watchdog.get_watchlist(usr_id TEXT)
    RETURNS TABLE(_sku VARCHAR, _brand TEXT, _name TEXT)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT
        w.sku
        , CASE WHEN u.display_language = 'en' THEN i.brand_en ELSE i.brand_zh END
        , CASE WHEN u.display_language = 'en' THEN i.name_en ELSE i.name_zh END
    FROM watchlists w
    LEFT JOIN items i ON w.sku = i.sku
    LEFT JOIN users u ON u.user_id = usr_id
    WHERE w.user_id = usr_id
    ORDER BY
        i.department_en
        , i.category_en
        , i.subcategory_en
        , w.sku;
END;
$$ LANGUAGE plpgsql;


/* GET N RANDOM DEAL ITEMS */
CREATE OR REPLACE FUNCTION watchdog.get_random_deals(usr_id TEXT, n INT DEFAULT 5)
    RETURNS TABLE(
        _sku VARCHAR
        , _supermarket VARCHAR
        , _promotion TEXT
        , _price NUMERIC
        , _frequency INT
        , _average NUMERIC
        , _std NUMERIC
        , _q0 NUMERIC
        , _q4 NUMERIC
        , _brand TEXT
        , _name TEXT
    )
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    WITH
        t_language AS (
            SELECT display_language
            FROM users
            WHERE user_id = usr_id
        )
    SELECT
        d.sku
        , d.supermarket
        , CASE WHEN l.display_language = 'en' THEN d.promotion_en ELSE d.promotion_zh END
        , d.unit_price
        , d.frequency
        , d.average_price
        , d.std_price
        , d.q0_price
        , d.q4_price
        , CASE WHEN l.display_language = 'en' THEN i.brand_en ELSE i.brand_zh END
        , CASE WHEN l.display_language = 'en' THEN i.name_en ELSE i.name_zh END
    FROM deals d
    INNER JOIN items i ON d.sku = i.sku
    CROSS JOIN t_language l
    WHERE d.is_deal = 'y'
    ORDER BY RANDOM()
    LIMIT n;
END;
$$ LANGUAGE plpgsql;


/* GET TIME SERIES OF ITEM PRICES */
CREATE OR REPLACE FUNCTION watchdog.get_prices(code VARCHAR)
    RETURNS TABLE(_date VARCHAR, _price NUMERIC)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT
        effective_date
        , MIN(unit_price)
    FROM prices
    WHERE sku = code
    GROUP BY effective_date
    ORDER BY effective_date;
END;
$$ LANGUAGE plpgsql;


/* GET ITEM INFORMATION */
CREATE OR REPLACE FUNCTION watchdog.get_item(usr_id TEXT, code VARCHAR)
    RETURNS TABLE(_frequency INT, _q1 NUMERIC, _brand TEXT, _name TEXT)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    WITH
        t_language AS (
            SELECT display_language
            FROM users
            WHERE user_id = usr_id
        )
        , t_item AS (
            SELECT
                i.sku
                , CASE WHEN l.display_language = 'en' THEN i.brand_en ELSE i.brand_zh END AS item_brand
                , CASE WHEN l.display_language = 'en' THEN i.name_en ELSE i.name_zh END AS item_name
            FROM items i
            CROSS JOIN t_language l
            WHERE sku = code
        )
    SELECT
        d.frequency
        , d.q1_price
        , i.item_brand
        , i.item_name
    FROM deals d
    INNER JOIN t_item i ON d.sku = i.sku;
END;
$$ LANGUAGE plpgsql;


/* CHANGE SUBSCRIPTION STATUS */
CREATE OR REPLACE FUNCTION watchdog.change_subscription(usr_id TEXT)
    RETURNS TABLE(_language VARCHAR, _status VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    WITH
        t_information AS (
            UPDATE users
            SET is_subscribed = CASE WHEN is_subscribed = 'n' THEN 'y' ELSE 'n' END
            WHERE user_id = usr_id
            RETURNING display_language, is_subscribed
        )
    SELECT
        COALESCE((SELECT display_language FROM t_information), 'na')
        , COALESCE((SELECT is_subscribed FROM t_information), 'na');
END;
$$ LANGUAGE plpgsql;


/* CHANGE USER LANGUAGE */
CREATE OR REPLACE FUNCTION watchdog.change_language(usr_id TEXT)
    RETURNS TABLE(_language VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    WITH
        t_information AS (
            UPDATE users
            SET display_language = CASE WHEN display_language = 'en' THEN 'zh' ELSE 'en' END
            WHERE user_id = usr_id
            RETURNING display_language
        )
    SELECT COALESCE((SELECT display_language FROM t_information), 'na');
END;
$$ LANGUAGE plpgsql;


/* GET ALERT */
CREATE OR REPLACE FUNCTION watchdog.get_alert(usr_id TEXT)
    RETURNS TABLE(
        _sku VARCHAR
        , _supermarket VARCHAR
        , _promotion TEXT
        , _fix NUMERIC
        , _price NUMERIC
        , _brand TEXT
        , _name TEXT
    )
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    WITH
        t_language AS (
            SELECT display_language
            FROM users
            WHERE user_id = usr_id
        )
    SELECT
        d.sku
        , d.supermarket
        , CASE WHEN l.display_language = 'en' THEN d.promotion_en ELSE d.promotion_zh END
        , d.original_price
        , d.unit_price
        , CASE WHEN l.display_language = 'en' THEN i.brand_en ELSE i.brand_zh END
        , CASE WHEN l.display_language = 'en' THEN i.name_en ELSE i.name_zh END
    FROM deals d
    INNER JOIN items i ON d.sku = i.sku
    CROSS JOIN t_language l
    WHERE 1 = 1
        AND d.is_deal = 'y'
        AND EXISTS (SELECT 1 FROM watchlists w WHERE d.sku = w.sku AND w.user_id = usr_id)
    ORDER BY d.supermarket;
END;
$$ LANGUAGE plpgsql;


/* REMOVE AN USER */
CREATE OR REPLACE FUNCTION watchdog.remove_user(usr_id TEXT)
    RETURNS TABLE(_language VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    DELETE FROM watchlists
    WHERE user_id = usr_id;
    
    RETURN QUERY
    WITH
        t_information AS (
            DELETE FROM users
            WHERE user_id = usr_id
            RETURNING display_language
        )
    SELECT COALESCE((SELECT display_language FROM t_information), 'na');
END;
$$ LANGUAGE plpgsql;
