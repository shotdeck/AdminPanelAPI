-- Relocate-clips jobs can recut every clip of a movie, not only the flagged ones.
-- ClipRelocateService.EnsureTablesAsync also applies this on startup.
ALTER TABLE frl.frl_clip_relocate_jobs ADD COLUMN IF NOT EXISTS recut_all BOOLEAN NOT NULL DEFAULT FALSE;
