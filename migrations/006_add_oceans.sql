-- ============================================================
-- 006: Add oceans as continent-level entries
-- ============================================================

INSERT INTO frl.frl_location_continents (name) VALUES
    ('Atlantic Ocean'),
    ('Pacific Ocean'),
    ('Indian Ocean'),
    ('Arctic Ocean'),
    ('Southern Ocean')
ON CONFLICT (name) DO NOTHING;
