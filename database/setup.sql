/* INITIALISE DATABASE SCHEMA */
CREATE OR REPLACE FUNCTION public.create_schema()
    RETURNS VOID
    SET search_path = ''
AS $$
BEGIN
    CREATE SCHEMA IF NOT EXISTS watchdog;
    
    GRANT USAGE ON SCHEMA watchdog TO anon, authenticated, service_role;
    GRANT ALL ON ALL TABLES IN SCHEMA watchdog TO anon, authenticated, service_role;
    GRANT ALL ON ALL ROUTINES IN SCHEMA watchdog TO anon, authenticated, service_role;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA watchdog TO anon, authenticated, service_role;
    
    ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA watchdog
        GRANT ALL ON TABLES TO anon, authenticated, service_role;
    ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA watchdog
        GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
    ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA watchdog
        GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;
END;
$$ LANGUAGE plpgsql;


/* INITIALISE DATABASE TABLE */
CREATE OR REPLACE FUNCTION watchdog.create_tables()
    RETURNS VOID
    SET search_path = 'watchdog'
AS $$
BEGIN
    CREATE TABLE IF NOT EXISTS watchdog.items (
        sku VARCHAR(20)
        , department_en TEXT
        , department_zh TEXT
        , category_en TEXT
        , category_zh TEXT
        , subcategory_en TEXT
        , subcategory_zh TEXT
        , brand_en TEXT
        , brand_zh TEXT
        , name_en TEXT
        , name_zh TEXT
        , created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        , CONSTRAINT item_pk PRIMARY KEY(sku)
    );
    
    CREATE TABLE IF NOT EXISTS watchdog.supermarkets (
        supermarket VARCHAR(10)
        , preference FLOAT
        , created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        , CONSTRAINT supermarket_pk PRIMARY KEY(supermarket)
    );
    
    CREATE TABLE IF NOT EXISTS watchdog.users (
        user_id TEXT
        , display_language VARCHAR(2)
        , is_subscribed VARCHAR(1)
        , created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        , CONSTRAINT user_pk PRIMARY KEY(user_id)
    );
    
    CREATE TABLE IF NOT EXISTS watchdog.deals (
        sku VARCHAR(20)
        , supermarket VARCHAR(10)
        , promotion_en TEXT
        , promotion_zh TEXT
        , original_price NUMERIC
        , unit_price NUMERIC
        , frequency INT
        , average_price NUMERIC
        , std_price NUMERIC
        , q0_price NUMERIC
        , q1_price NUMERIC
        , q4_price NUMERIC
        , is_deal VARCHAR(1)
        , created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        , CONSTRAINT deal_pk PRIMARY KEY(sku)
        , CONSTRAINT deal_item_fk FOREIGN KEY(sku) REFERENCES items(sku)
        , CONSTRAINT deal_supermarket_fk FOREIGN KEY(supermarket) REFERENCES supermarkets(supermarket)
    );
    
    CREATE TABLE IF NOT EXISTS watchdog.prices (
        sku VARCHAR(20)
        , effective_date VARCHAR(8)
        , supermarket VARCHAR(10)
        , promotion_en TEXT
        , promotion_zh TEXT
        , original_price NUMERIC
        , unit_price NUMERIC
        , created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        , CONSTRAINT price_item_fk FOREIGN KEY(sku) REFERENCES items(sku)
        , CONSTRAINT price_supermarket_fk FOREIGN KEY(supermarket) REFERENCES supermarkets(supermarket)
    );
    
    CREATE TABLE IF NOT EXISTS watchdog.watchlists (
        user_id TEXT
        , sku VARCHAR(20)
        , created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        , CONSTRAINT watchlist_user_fk FOREIGN KEY(user_id) REFERENCES users(user_id)
        , CONSTRAINT watchlist_item_fk FOREIGN KEY(sku) REFERENCES items(sku)
    );
    
    CREATE INDEX IF NOT EXISTS price_sku_date_idx ON prices(sku, effective_date);
    
    INSERT INTO supermarkets (supermarket, preference)
        SELECT * FROM (VALUES
            ('WELLCOME', 1),
            ('JASONS', 2),
            ('MANNINGS', 3),
            ('PARKNSHOP', 4),
            ('WATSONS', 5),
            ('AEON', 6),
            ('DCHFOOD', 7)
        ) AS v(supermarket, preference)
    ON CONFLICT (supermarket) DO NOTHING;
    
    ALTER TABLE items ENABLE ROW LEVEL SECURITY;
    ALTER TABLE prices ENABLE ROW LEVEL SECURITY;
    ALTER TABLE supermarkets ENABLE ROW LEVEL SECURITY;
    ALTER TABLE users ENABLE ROW LEVEL SECURITY;
    ALTER TABLE deals ENABLE ROW LEVEL SECURITY;
    ALTER TABLE watchlists ENABLE ROW LEVEL SECURITY;
END;
$$ LANGUAGE plpgsql;
