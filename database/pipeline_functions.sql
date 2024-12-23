/* TEST DATABASE CONNECTION */
CREATE OR REPLACE FUNCTION watchdog.check_connection()
    RETURNS TABLE(okay BOOLEAN)
    SET search_path = ''
AS $$
BEGIN
    RETURN QUERY
    SELECT TRUE;
END;
$$ LANGUAGE plpgsql;


/* GET PRICE DATES */
CREATE OR REPLACE FUNCTION watchdog.get_dates()
    RETURNS TABLE(_date VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT effective_date
    FROM prices
    GROUP BY effective_date;
END;
$$ LANGUAGE plpgsql;


/* GET ITEM SKUS */
CREATE OR REPLACE FUNCTION watchdog.get_skus()
    RETURNS TABLE(_sku VARCHAR)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    SELECT sku
    FROM items
    GROUP BY sku;
END;
$$ LANGUAGE plpgsql;


/* ETL OF BEST DEALS */
CREATE OR REPLACE FUNCTION watchdog.update_deals()
    RETURNS VOID
    SET search_path = 'watchdog'
AS $$
DECLARE
    latest_date VARCHAR(8);
BEGIN
    SELECT MAX(effective_date)
    INTO latest_date
    FROM prices;
    
    TRUNCATE TABLE deals;
    
    WITH
        t_summary_statistic AS (
            SELECT
                sku
                , COUNT(DISTINCT effective_date) AS frequency
                , AVG(unit_price) AS average_price
                , STDDEV(unit_price) AS std_price
                , MIN(unit_price) AS q0_price
                , PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY unit_price) AS q1_price
                , MAX(unit_price) AS q4_price
            FROM prices
            GROUP BY sku
        )
        , t_latest_deal AS (
            SELECT
                p.sku
                , p.supermarket
                , p.promotion_en
                , p.promotion_zh
                , p.original_price
                , p.unit_price
                , s.preference
            FROM prices p
            
            LEFT JOIN supermarkets s ON p.supermarket = s.supermarket
            WHERE p.effective_date = latest_date
        )
        , t_preferred_deal AS (
            SELECT
                sku
                , supermarket
                , promotion_en
                , promotion_zh
                , original_price
                , unit_price
                , RANK() OVER (PARTITION BY sku ORDER BY unit_price, preference) AS rnk
            FROM t_latest_deal
        )
    INSERT INTO deals
    SELECT
        d.sku
        , d.supermarket
        , d.promotion_en
        , d.promotion_zh
        , d.original_price
        , d.unit_price
        , s.frequency
        , s.average_price
        , s.std_price
        , s.q0_price
        , s.q1_price
        , s.q4_price
        , CASE WHEN d.unit_price + 0.5 < s.q1_price THEN 'y' ELSE 'n' END
    FROM t_preferred_deal d
    INNER JOIN t_summary_statistic s ON d.sku = s.sku
    WHERE d.rnk = 1;
END;
$$ LANGUAGE plpgsql;


/* GET IDS OF USER WITH PRICE ALERT */
CREATE OR REPLACE FUNCTION watchdog.get_users()
    RETURNS TABLE(_id TEXT)
    SET search_path = 'watchdog'
AS $$
BEGIN
    RETURN QUERY
    WITH
        t_identity AS (
            SELECT w.user_id
            FROM watchlists w
            WHERE EXISTS (SELECT 1 FROM deals d WHERE w.sku = d.sku AND d.is_deal = 'y')
        )
    SELECT u.user_id
    FROM users u
    WHERE 1 = 1
        AND u.is_subscribed = 'y'
        AND EXISTS (SELECT 1 FROM t_identity i WHERE u.user_id = i.user_id);
END;
$$ LANGUAGE plpgsql;
