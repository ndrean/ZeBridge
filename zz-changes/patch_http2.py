import re

with open("src/http_server.zig", "r") as f:
    text = f.read()

# Update handleStatus JSON output
status_template = """                \\\\  "refused_tables": {d},
                \\\\  "refused_events_dropped": {d},
                \\\\  "gc_total_reaped": {d},
                \\\\  "gc_last_sweep_time": {d}
                \\\\}}"""
text = text.replace('                \\\\  "refused_tables": {d},\n                \\\\  "refused_events_dropped": {d}\n                \\\\}}', status_template)

status_args = """                snap.queue_usage_percent,
                if (self.refused) |r| r.refused_count.load(.acquire) else 0,
                if (self.refused) |r| r.dropped_total.load(.acquire) else 0,
                snap.gc_total_reaped,
                snap.gc_last_sweep_time,
            });"""
text = text.replace('                snap.queue_usage_percent,\n                if (self.refused) |r| r.refused_count.load(.acquire) else 0,\n                if (self.refused) |r| r.dropped_total.load(.acquire) else 0,\n            });', status_args)

# Update handleMetrics Prometheus output
metrics_template = """                \\\\# HELP bridge_schema_events_published_total SCHEMA/KV events published to NATS (DDL schemas, suspensions, drop tombstones) - kept out of the CDC counter so that one stays equal to row events
                \\\\# TYPE bridge_schema_events_published_total counter
                \\\\bridge_schema_events_published_total {d}
                \\\\
                \\\\# HELP bridge_gc_total_reaped_total Total soft-deleted rows reaped by the sweeper
                \\\\# TYPE bridge_gc_total_reaped_total counter
                \\\\bridge_gc_total_reaped_total {d}
                \\\\
                \\\\# HELP bridge_gc_last_sweep_timestamp_seconds Unix timestamp of the last sweep
                \\\\# TYPE bridge_gc_last_sweep_timestamp_seconds gauge
                \\\\bridge_gc_last_sweep_timestamp_seconds {d}
                \\\\
                \\\\# HELP bridge_last_ack_lsn"""
text = text.replace('                \\\\# HELP bridge_schema_events_published_total SCHEMA/KV events published to NATS (DDL schemas, suspensions, drop tombstones) - kept out of the CDC counter so that one stays equal to row events\n                \\\\# TYPE bridge_schema_events_published_total counter\n                \\\\bridge_schema_events_published_total {d}\n                \\\\\n                \\\\# HELP bridge_last_ack_lsn', metrics_template)

metrics_args = """                snap.wal_messages_received,
                snap.cdc_events_published,
                snap.schema_events_published,
                snap.gc_total_reaped,
                snap.gc_last_sweep_time,
                snap.last_ack_lsn,"""
text = text.replace('                snap.wal_messages_received,\n                snap.cdc_events_published,\n                snap.schema_events_published,\n                snap.last_ack_lsn,', metrics_args)

with open("src/http_server.zig", "w") as f:
    f.write(text)
