-- Add category column to frl_popularity_tag_rules
ALTER TABLE frl.frl_popularity_tag_rules
ADD COLUMN IF NOT EXISTS category VARCHAR(255);
