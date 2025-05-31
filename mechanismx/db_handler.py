import psycopg2
import json
import os

# Load DB config
with open('config/db_config.json') as f:
    db_config = json.load(f)

def ensure_table():
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transaction_pointer (
            id SERIAL PRIMARY KEY,
            last_index INT DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM transaction_pointer")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO transaction_pointer (last_index) VALUES (0)")
        conn.commit()
    cur.close()
    conn.close()

def get_last_index():
    ensure_table()
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute("SELECT last_index FROM transaction_pointer ORDER BY id DESC LIMIT 1")
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result

def update_last_index(new_index):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute("""
        UPDATE transaction_pointer 
        SET last_index = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = (SELECT id FROM transaction_pointer ORDER BY id DESC LIMIT 1)
    """, (new_index,))
    conn.commit()
    cur.close()
    conn.close()
