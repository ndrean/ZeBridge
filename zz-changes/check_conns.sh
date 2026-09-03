echo "wal_stream.zig:"
grep -A 2 -B 2 "PQconnectdb" src/wal_stream.zig
echo "\ngeneration_producer.zig:"
sed -n '150,180p' src/generation_producer.zig
echo "\nmutation_listener.zig:"
sed -n '430,460p' src/mutation_listener.zig
echo "\nwal_monitor.zig:"
grep -A 2 -B 2 "PQconnectdb" src/wal_monitor.zig
