ALTER TABLE sporos_injection ADD COLUMN verification_attempts INTEGER NOT NULL DEFAULT 0
    CHECK (verification_attempts >= 0);
