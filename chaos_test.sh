#!/bin/bash
set -e

# Configuration (Uses variables if set, otherwise defaults)
PG_HOST=${PG_HOST:-127.0.0.1}
PG_PORT=${PG_PORT:-55432}
PG_USER=${PG_USER:-postgres}
PG_PASSWORD=${PG_PASSWORD:-postgres_password}
PG_DB=${PG_DB:-postgres}

export PGPASSWORD=$PG_PASSWORD

echo "🌪️ Starting Chaos Testing on ZeBridge! 🌪️"
echo "-------------------------------------------"

# 1. Start a background process to simulate continuous load
echo "📈 Starting continuous data generation in the background..."
(
    for i in {1..50}; do
        psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c \
            "INSERT INTO test_types (is_active, small_int_val, int_val, text_val, timestamp_val) 
             VALUES (true, $i, $((i * 100)), 'chaos_load_$i', NOW());" > /dev/null 2>&1
        sleep 0.1
    done
    echo "✅ Data generation complete."
) &
LOAD_PID=$!

sleep 2

# 2. Perform a mid-stream DDL migration
echo "💥 Performing mid-stream DDL migration (Adding 'chaos_metric' column)..."
psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c \
    "ALTER TABLE test_types ADD COLUMN chaos_metric integer DEFAULT 42;"

echo "✅ DDL migration executed on Postgres."

# 3. Insert more records featuring the NEW schema structure
sleep 1
echo "📈 Inserting records using the newly added column..."
for i in {51..60}; do
    psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c \
        "INSERT INTO test_types (is_active, small_int_val, int_val, text_val, timestamp_val, chaos_metric) 
         VALUES (false, $i, $((i * 100)), 'post_migration_$i', NOW(), 999);" > /dev/null 2>&1
done

# Wait for background load to finish
wait $LOAD_PID

echo "-------------------------------------------"
echo "🎉 Chaos test execution complete!"
echo "👉 Check the ZeBridge logs to verify it gracefully handled the schema evolution and re-mapped the MsgPack payloads!"
