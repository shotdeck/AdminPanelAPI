-- Change unique constraint from tag-only to (tag, category) so the same tag
-- can exist with different categories (e.g. "Martin Scorsese" as Director AND as Actors).

-- Drop the existing unique index/constraint on tag alone.
-- The constraint name may vary; try the most common auto-generated names.
DO $$
BEGIN
    -- Try dropping a unique index named after the column
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'frl'
          AND tablename = 'frl_popularity_tag_rules'
          AND indexdef ILIKE '%unique%'
          AND indexdef ILIKE '%tag%'
          AND indexdef NOT ILIKE '%category%'
    ) THEN
        -- Find and drop the exact constraint
        EXECUTE (
            SELECT format('ALTER TABLE frl.frl_popularity_tag_rules DROP CONSTRAINT %I',
                          conname)
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = 'frl'
              AND c.conrelid = 'frl.frl_popularity_tag_rules'::regclass
              AND c.contype = 'u'
              AND array_length(c.conkey, 1) = 1
        );
    END IF;
END
$$;

-- Create the new unique constraint on (tag, category).
-- COALESCE handles NULL category so two rows with the same tag and NULL category
-- are still considered duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS uq_frl_popularity_tag_rules_tag_category
ON frl.frl_popularity_tag_rules (tag, COALESCE(category, ''));
