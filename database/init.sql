-- Create the watchdog schema and grant permissions
SELECT public.create_schema();

-- Verify database connection in watchdog schema
SELECT okay FROM watchdog.check_connection();

-- Create all tables, indexes, triggers, and enable RLS
SELECT watchdog.create_tables();

-- Insert seed data for supermarkets
INSERT INTO watchdog.supermarkets (supermarket, preference)
    SELECT * FROM (VALUES
        ('WELLCOME', 1),
        ('JASONS', 2),
        ('MANNINGS', 3),
        ('PARKNSHOP', 4),
        ('WATSONS', 5),
        ('AEON', 6)
    ) AS v(supermarket, preference)
ON CONFLICT (supermarket) DO NOTHING;

-- Reset the identity sequence to account for manually inserted preference values
SELECT setval(
    pg_get_serial_sequence('watchdog.supermarkets', 'preference'),
    (SELECT MAX(preference) FROM watchdog.supermarkets)
);
