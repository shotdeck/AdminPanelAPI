-- Create frl_images_location table to store parsed filming location data
CREATE TABLE IF NOT EXISTS frl.frl_images_location (
    id              BIGSERIAL       PRIMARY KEY,
    image_id        INTEGER         NOT NULL,
    raw_location    TEXT,
    planet          VARCHAR(100),
    continent       VARCHAR(100),
    country         VARCHAR(200),
    state_region    VARCHAR(200),
    city            VARCHAR(200),
    specific_location TEXT,
    coordinates     POINT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_frl_images_location_image
        FOREIGN KEY (image_id) REFERENCES frl.frl_images(idnum)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_frl_images_location_image_id
    ON frl.frl_images_location(image_id);

CREATE INDEX IF NOT EXISTS idx_frl_images_location_country
    ON frl.frl_images_location(country);

CREATE INDEX IF NOT EXISTS idx_frl_images_location_city
    ON frl.frl_images_location(city);

CREATE INDEX IF NOT EXISTS idx_frl_images_location_coordinates
    ON frl.frl_images_location USING GIST (coordinates);
