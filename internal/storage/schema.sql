CREATE TABLE IF NOT EXISTS logs (
    id TEXT PRIMARY KEY NOT NULL,
    timestamp TEXT NOT NULL,
    service TEXT NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('INFO', 'WARN', 'ERROR', 'DEBUG')),
    message TEXT NOT NULL,
    received_at TEXT NOT NULL,
    source_ip TEXT NOT NULL,
    env TEXT NOT NULL
);
PRAGMA user_version = 1;
