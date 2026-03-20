-- ============================================================
-- EU Pesticides & MRL Database – Neon Postgres Schema
-- Project: eu-pesticides  (Neon free-tier, 0.5 GB)
-- Source: EURL Pesticides / EU Reg 396/2005
-- ============================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- fast text search
CREATE EXTENSION IF NOT EXISTS unaccent;  -- accent-insensitive search

-- ── Core MRL table ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pesticides_mrl (
    id              SERIAL PRIMARY KEY,
    substance       TEXT        NOT NULL,          -- e.g. "Glyphosate"
    substance_group TEXT,                          -- e.g. "Organophosphates"
    cas_number      TEXT,                          -- CAS registry number
    product         TEXT        NOT NULL,          -- e.g. "Apples"
    product_code    TEXT,                          -- EC product code
    product_group   TEXT,                          -- e.g. "Pome fruit"
    mrl_mg_kg       NUMERIC(10,4),                 -- Maximum Residue Level
    mrl_unit        TEXT DEFAULT 'mg/kg',
    is_default      BOOLEAN DEFAULT FALSE,         -- EU default MRL (0.01 mg/kg)
    regulation      TEXT,                          -- e.g. "EU 2023/124"
    country         TEXT DEFAULT 'EU',
    valid_from      DATE,
    valid_to        DATE,                          -- NULL = currently valid
    notes           TEXT,
    source_url      TEXT,
    row_hash        TEXT UNIQUE NOT NULL,          -- dedup key
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Active substance registry ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS substances (
    id              SERIAL PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    name_de         TEXT,                          -- German name
    cas_number      TEXT,
    substance_group TEXT,
    eu_approved     BOOLEAN DEFAULT TRUE,
    approval_expiry DATE,
    carcinogen      BOOLEAN DEFAULT FALSE,
    endocrine_disruptor BOOLEAN DEFAULT FALSE,
    notes           TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── AI-generated insights log ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS insights_log (
    id              SERIAL PRIMARY KEY,
    niche           TEXT NOT NULL DEFAULT 'pesticides',
    summary         TEXT,
    key_findings    JSONB,
    trend           TEXT,
    statistics      JSONB,
    action_items    JSONB,
    model           TEXT,
    generated_at    TIMESTAMPTZ DEFAULT NOW(),
    is_current      BOOLEAN DEFAULT TRUE
);

-- Mark old insights as not current before inserting new
CREATE OR REPLACE FUNCTION set_insights_not_current()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE insights_log SET is_current = FALSE
    WHERE niche = NEW.niche AND is_current = TRUE;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_insert_insights
    BEFORE INSERT ON insights_log
    FOR EACH ROW EXECUTE FUNCTION set_insights_not_current();

-- ── Indexes for performance ─────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_mrl_substance     ON pesticides_mrl (substance);
CREATE INDEX IF NOT EXISTS idx_mrl_product       ON pesticides_mrl (product);
CREATE INDEX IF NOT EXISTS idx_mrl_substance_gin ON pesticides_mrl USING gin(substance gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_mrl_product_gin   ON pesticides_mrl USING gin(product gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_mrl_valid_from    ON pesticides_mrl (valid_from);
CREATE INDEX IF NOT EXISTS idx_mrl_country       ON pesticides_mrl (country);
CREATE INDEX IF NOT EXISTS idx_insights_current  ON insights_log (niche, is_current);

-- ── Helpful views ───────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_high_mrl AS
    SELECT substance, product, mrl_mg_kg, regulation, valid_from
    FROM pesticides_mrl
    WHERE mrl_mg_kg > 1.0 AND valid_to IS NULL
    ORDER BY mrl_mg_kg DESC;

CREATE OR REPLACE VIEW v_banned_substances AS
    SELECT m.substance, m.product, m.mrl_mg_kg, m.regulation, m.notes
    FROM pesticides_mrl m
    JOIN substances s ON lower(m.substance) = lower(s.name)
    WHERE s.eu_approved = FALSE;

CREATE OR REPLACE VIEW v_summary_stats AS
    SELECT
        COUNT(DISTINCT substance)     AS total_substances,
        COUNT(DISTINCT product)       AS total_products,
        COUNT(*)                      AS total_mrl_records,
        AVG(mrl_mg_kg)               AS avg_mrl,
        MAX(mrl_mg_kg)               AS max_mrl,
        MIN(mrl_mg_kg)               AS min_mrl,
        COUNT(*) FILTER (WHERE is_default) AS default_mrl_count,
        MAX(updated_at)              AS last_updated
    FROM pesticides_mrl
    WHERE valid_to IS NULL;

-- ── Sample data for development / testing ───────────────────────────────────
INSERT INTO pesticides_mrl
    (substance, substance_group, product, product_group, mrl_mg_kg, regulation, valid_from, row_hash)
VALUES
    ('Glyphosate',    'Organophosphonates', 'Wheat',        'Cereals',      10.0,  'EU 2023/1157', '2023-07-01', md5('glyphosate-wheat-10.0')),
    ('Glyphosate',    'Organophosphonates', 'Sunflower seed','Oil seeds',   20.0,  'EU 2023/1157', '2023-07-01', md5('glyphosate-sunflower-20.0')),
    ('Chlorpyrifos',  'Organophosphates',   'Apples',       'Pome fruit',   0.01,  'EU 2020/1085', '2020-09-13', md5('chlorpyrifos-apples-0.01')),
    ('Imidacloprid',  'Neonicotinoids',     'Tomatoes',     'Fruiting veg', 1.0,   'EU 2022/744',  '2022-06-01', md5('imidacloprid-tomatoes-1.0')),
    ('Thiamethoxam',  'Neonicotinoids',     'Carrots',      'Root veg',     0.05,  'EU 2021/808',  '2021-05-20', md5('thiamethoxam-carrots-0.05')),
    ('Deltamethrin',  'Pyrethroids',        'Strawberries', 'Small fruit',  0.2,   'EU 2023/456',  '2023-03-15', md5('deltamethrin-strawberries-0.2')),
    ('Tebuconazole',  'Triazoles',          'Grapes',       'Small fruit',  3.0,   'EU 2022/1109', '2022-07-01', md5('tebuconazole-grapes-3.0')),
    ('Azoxystrobin',  'Strobilurins',       'Barley',       'Cereals',      0.5,   'EU 2021/1950', '2021-11-15', md5('azoxystrobin-barley-0.5')),
    ('Cypermethrin',  'Pyrethroids',        'Spinach',      'Leafy veg',    0.05,  'EU 2020/749',  '2020-06-01', md5('cypermethrin-spinach-0.05')),
    ('Dimethoate',    'Organophosphates',   'Cherries',     'Stone fruit',  0.01,  'EU 2019/1015', '2019-07-10', md5('dimethoate-cherries-0.01'))
ON CONFLICT (row_hash) DO NOTHING;

-- ── Grant read-only role for public API exposure (optional) ─────────────────
-- CREATE ROLE eu_data_reader WITH LOGIN PASSWORD 'change_me';
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO eu_data_reader;
-- GRANT USAGE ON SCHEMA public TO eu_data_reader;

-- Done
SELECT 'Schema created successfully' AS status,
       (SELECT count(*) FROM pesticides_mrl) AS sample_rows;
