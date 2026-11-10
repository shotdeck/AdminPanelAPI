-- Parallel Qdrant rebuild: per-slice checkpoints on the job row (AdminPanelAPI QdrantSync).
-- The API also adds this on startup; safe to re-run.
ALTER TABLE frl.frl_qdrant_sync_jobs ADD COLUMN IF NOT EXISTS slices JSONB;
