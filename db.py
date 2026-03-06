import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

#   Connect to my postgres instance
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def create_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS discogs_collection (
            release_id    INTEGER PRIMARY KEY,
            master_id     INTEGER,
            artist        TEXT,
            title         TEXT,
            date_added    TEXT,
            variant       TEXT,
            format        TEXT,
            release_date  TEXT,
            country       TEXT,
            label         TEXT,
            catno         TEXT,
            genres        TEXT,
            styles        TEXT,
            loaded_at     TIMESTAMP
        );
    """)

def upsert_row(cur, row):
    cur.execute("""
        INSERT INTO discogs_collection (
            release_id, master_id, artist, title, date_added,
            variant, format, release_date, country, label,
            catno, genres, styles, loaded_at
        ) VALUES (
            %(release_id)s, %(master_id)s, %(artist)s, %(title)s, %(date_added)s,
            %(variant)s, %(format)s, %(release_date)s, %(country)s, %(label)s,
            %(catno)s, %(genres)s, %(styles)s, %(loaded_at)s
        )
        ON CONFLICT (release_id) DO UPDATE SET
            master_id    = EXCLUDED.master_id,
            artist       = EXCLUDED.artist,
            title        = EXCLUDED.title,
            date_added   = EXCLUDED.date_added,
            variant      = EXCLUDED.variant,
            format       = EXCLUDED.format,
            release_date = EXCLUDED.release_date,
            country      = EXCLUDED.country,
            label        = EXCLUDED.label,
            catno        = EXCLUDED.catno,
            genres       = EXCLUDED.genres,
            styles       = EXCLUDED.styles,
            loaded_at    = EXCLUDED.loaded_at;
    """, row)

def load_rows(rows: list[dict]):
    conn = get_connection()
    cur = conn.cursor()
    create_table(cur)
    for row in rows:
        upsert_row(cur, row)
    conn.commit()
    cur.close()
    conn.close()

    print(f'Loaded {len(rows)} into discogs_collection')