-- Per-reviewer passwords for Camera Movement QC login.
-- Admins authenticate against the CAMERAMOVEMENTPASSWORD app setting;
-- everyone else against this salted PBKDF2 hash (never plaintext).
ALTER TABLE frl.frl_camera_movement_users
    ADD COLUMN IF NOT EXISTS password_hash TEXT;
