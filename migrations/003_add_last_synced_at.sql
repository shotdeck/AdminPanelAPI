-- Add last_synced_at column to track which rules have been synced.
-- Rules with updated_at > last_synced_at (or last_synced_at IS NULL) are considered unsynced.

ALTER TABLE frl.frl_popularity_tag_rules
ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMP;
