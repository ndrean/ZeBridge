import asyncio
import sqlite3
import msgpack
import json
from flask import Flask, jsonify
from nats.aio.client import Client as NATS

app = Flask(__name__)
nc = NATS()

DB_FILE = "cache.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    # Enable Write-Ahead Logging (WAL) mode for SQLite to allow concurrent reads while writing!
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kv_store (
            table_name TEXT, 
            id TEXT, 
            data TEXT, 
            PRIMARY KEY (table_name, id)
        )
    """)
    conn.close()

init_db()

async def nats_worker():
    await nc.connect("nats://localhost:4222")
    print("✅ Connected to NATS Stream")
    
    sub = await nc.subscribe("cdc.>")
    
    # Open ONE persistent connection for the write worker
    conn = sqlite3.connect(DB_FILE)
    
    try:
        async for msg in sub:
            try:
                event = msgpack.unpackb(msg.data)
                table = event['table']
                op = event['operation']
                data = event['data']
                row_id = str(data.get('id'))
                
                if op in ('INSERT', 'UPDATE'):
                    conn.execute(
                        "INSERT OR REPLACE INTO kv_store (table_name, id, data) VALUES (?, ?, ?)",
                        (table, row_id, json.dumps(data))
                    )
                elif op == 'DELETE':
                    conn.execute("DELETE FROM kv_store WHERE table_name = ? AND id = ?", (table, row_id))
                
                # Commit individual events or add a micro-batch timer here
                conn.commit()
                print(f"⚡ Local cache hydrated: {op} -> {table}:{row_id}")
                
            except Exception as e:
                print(f"❌ Worker iteration error: {e}")
                conn.rollback()
    finally:
        conn.close()

@app.before_serving
async def start_nats():
    asyncio.create_task(nats_worker())

@app.route('/users/<id>')
def get_user(id):
    # Reads are blindingly fast and do not block the write worker because of PRAGMA journal_mode=WAL;
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.execute("SELECT data FROM kv_store WHERE table_name = ? AND id = ?", ('users', id))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return row[0], 200, {'Content-Type': 'application/json'}
    return jsonify({"error": "User state not yet propagated to edge cache"}), 404