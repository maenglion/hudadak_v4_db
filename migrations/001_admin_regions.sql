BEGIN;

CREATE TABLE IF NOT EXISTS air.admin_regions (
    code text PRIMARY KEY,
    level text NOT NULL CHECK (level IN ('sido', 'sigungu')),
    name text NOT NULL,
    full_name text NOT NULL,
    parent_code text,
    geom geometry(MultiPolygon, 4326) NOT NULL,
    source_name text NOT NULL,
    source_date date,
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS admin_regions_geom_gix
    ON air.admin_regions USING GIST (geom);
CREATE INDEX IF NOT EXISTS admin_regions_level_parent_idx
    ON air.admin_regions(level, parent_code);

ALTER TABLE air.stations
    ADD COLUMN IF NOT EXISTS sido_code text,
    ADD COLUMN IF NOT EXISTS sigungu_code text;

CREATE INDEX IF NOT EXISTS stations_sido_code_idx
    ON air.stations(sido_code);
CREATE INDEX IF NOT EXISTS stations_sigungu_code_idx
    ON air.stations(sigungu_code);

COMMIT;
