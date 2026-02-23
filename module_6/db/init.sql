CREATE TABLE IF NOT EXISTS applicants (
    id SERIAL PRIMARY KEY,
    program_name TEXT,
    university TEXT,
    comments TEXT,
    date_added TEXT,
    entry_url TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_watermarks (
    source TEXT PRIMARY KEY,
    last_seen TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);