-- BTS (customer behind-the-scenes media spaces): schema and tables.
-- Run by hand as the same database user AdminPanelAPI connects with, so the
-- API owns the tables. Safe to re-run.
-- Files and folders live in the R2 bucket "bts"; these tables only hold the
-- spaces, their link tokens (hashed / sealed), the activity log and notes.

BEGIN;

CREATE SCHEMA IF NOT EXISTS bts;

CREATE TABLE IF NOT EXISTS bts.spaces (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          TEXT        NOT NULL,
    token_hash    BYTEA       NOT NULL UNIQUE,
    token_sealed  BYTEA       NOT NULL,
    notes         TEXT,
    quota_bytes   BIGINT,
    expires_at    TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ,
    created_by    TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bts.activity (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    space_id  BIGINT      NOT NULL REFERENCES bts.spaces (id),
    actor     TEXT        NOT NULL,
    action    TEXT        NOT NULL,
    path      TEXT,
    new_path  TEXT,
    bytes     BIGINT,
    ip        TEXT,
    at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS activity_space_at_idx ON bts.activity (space_id, at DESC);

-- Notes on files and folders. path is relative to the space; folder paths end
-- with '/' (folders are only prefixes in R2).
CREATE TABLE IF NOT EXISTS bts.notes (
    space_id    BIGINT      NOT NULL REFERENCES bts.spaces (id),
    path        TEXT        NOT NULL,
    note        TEXT        NOT NULL,
    updated_by  TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (space_id, path)
);

COMMIT;
