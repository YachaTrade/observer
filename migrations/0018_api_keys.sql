-- API Keys table for external API access
-- Using pgactive snowflake ID for distributed systems
CREATE SEQUENCE IF NOT EXISTS api_keys_snowflake_seq;

CREATE TABLE api_keys (
    id BIGINT PRIMARY KEY DEFAULT pgactive.pgactive_snowflake_id_nextval('api_keys_snowflake_seq'),
    key_hash VARCHAR(64) NOT NULL UNIQUE,  -- SHA256 hash of API key
    key_prefix VARCHAR(12) NOT NULL,        -- First 12 chars (nadfun_xxxxxxxx)
    name VARCHAR(255) NOT NULL,
    description TEXT,
    owner_address VARCHAR(42),              -- Optional: link to account
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,    -- NULL = never expires
    last_used_at TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE,
    request_count BIGINT DEFAULT 0          -- Total request count for analytics
);

-- Index for fast key lookup
CREATE INDEX idx_api_keys_key_hash ON api_keys(key_hash);
-- Index for active keys only
CREATE INDEX idx_api_keys_active ON api_keys(is_active) WHERE is_active = TRUE;
