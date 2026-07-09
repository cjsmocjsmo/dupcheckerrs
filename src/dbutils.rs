use rusqlite::{Connection, Result};

pub fn open_database(dry_run: bool, db_path: &str) -> Result<Option<Connection>> {
    if dry_run {
        return Ok(None);
    }

    let conn = Connection::open(db_path)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS hashes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            original_path TEXT NOT NULL,
            stored_path TEXT NOT NULL,
            detected_format TEXT NOT NULL,
            original_extension TEXT NOT NULL,
            action_taken TEXT NOT NULL,
            quarantine_path TEXT,
            transcode_path TEXT,
            ingest_ts TEXT NOT NULL
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS movie_hashes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            original_path TEXT NOT NULL,
            detected_format TEXT NOT NULL,
            extension TEXT NOT NULL,
            ingest_ts TEXT NOT NULL
        )",
        [],
    )?;

    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -20000;",
    )?;

    Ok(Some(conn))
}
