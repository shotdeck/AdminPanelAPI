-- Multi-user Camera Movement QC.
--
-- Two tables:
--  1. frl_camera_movement_users - the fixed roster reviewers pick from at
--     login. No real auth; is_admin flags who may add/edit/remove users
--     (MacK). Seeded with the initial team if the table is empty.
--  2. frl_camera_movement_image_owner - one row per image assigning it to the
--     reviewer who fetched it, so each person can see just their own work.
--     Existing already-analyzed images are backfilled to 'MacK' once.

CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_users (
    id          SERIAL       PRIMARY KEY,
    name        VARCHAR(120) NOT NULL UNIQUE,
    is_admin    BOOLEAN      NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS frl.frl_camera_movement_image_owner (
    imageid      INTEGER      PRIMARY KEY,
    owner        VARCHAR(120) NOT NULL,
    assigned_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cmio_owner ON frl.frl_camera_movement_image_owner (owner);

-- Seed the initial roster only if empty (MacK is admin).
INSERT INTO frl.frl_camera_movement_users (name, is_admin)
SELECT v.name, v.is_admin
FROM (VALUES
    ('MacK', true),
    ('Sam', false),
    ('Ethan', false),
    ('Ajai', false),
    ('Noah', false)
) AS v(name, is_admin)
WHERE NOT EXISTS (SELECT 1 FROM frl.frl_camera_movement_users);

-- One-time backfill: assign every already-analyzed image to MacK if we have
-- no ownership records yet.
INSERT INTO frl.frl_camera_movement_image_owner (imageid, owner)
SELECT DISTINCT cm.imageid, 'MacK'
FROM frl.frl_join_image_camera_movements cm
WHERE NOT EXISTS (SELECT 1 FROM frl.frl_camera_movement_image_owner)
ON CONFLICT (imageid) DO NOTHING;
