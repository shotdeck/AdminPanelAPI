-- BTS: a work status on each space (new -> processing -> complete), set by admins.
-- Run by hand as the same database user AdminPanelAPI connects with. Safe to re-run.

BEGIN;

ALTER TABLE bts.spaces
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'processing', 'complete'));

COMMIT;
