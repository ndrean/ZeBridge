import re

with open("src/http_server.zig", "r") as f:
    text = f.read()

# Update handleStatus
status_template = """                \\\\  "schema_events_published": {d},
                \\\\  "gc_total_reaped": {d},
                \\\\  "gc_last_sweep_time": {d}
                \\\\}}
"""
text = re.sub(r'\\\\  "schema_events_published": \{d\}\n\s*\\\\\}\}\n', status_template, text)

status_args = """                snap.nats_publish_ack_ns,
                snap.nats_publishes,
                snap.schema_events_published,
                snap.gc_total_reaped,
                snap.gc_last_sweep_time,
            );"""
text = re.sub(r'snap\.nats_publish_ack_ns,\n\s*snap\.nats_publishes,\n\s*snap\.schema_events_published,\n\s*\);', status_args, text)

# Update handleMetrics
metrics_template = """                \\\\# HELP zebridge_schema_events_published Total schema/KV events published
                \\\\# TYPE zebridge_schema_events_published counter
                \\\\zebridge_schema_events_published {d}
                \\\\# HELP zebridge_gc_total_reaped Total soft-deleted rows reaped by the sweeper
                \\\\# TYPE zebridge_gc_total_reaped counter
                \\\\zebridge_gc_total_reaped {d}
                \\\\# HELP zebridge_gc_last_sweep_time Unix timestamp of the last sweep
                \\\\# TYPE zebridge_gc_last_sweep_time gauge
                \\\\zebridge_gc_last_sweep_time {d}
"""
text = re.sub(r'\\\\# HELP zebridge_schema_events_published Total schema/KV events published\n\s*\\\\# TYPE zebridge_schema_events_published counter\n\s*\\\\zebridge_schema_events_published \{d\}\n', metrics_template, text)

metrics_args = """                snap.nats_publish_ack_ns,
                snap.nats_publishes,
                snap.schema_events_published,
                snap.gc_total_reaped,
                snap.gc_last_sweep_time,
            );"""
text = re.sub(r'snap\.nats_publish_ack_ns,\n\s*snap\.nats_publishes,\n\s*snap\.schema_events_published\n\s*\);', metrics_args, text)

with open("src/http_server.zig", "w") as f:
    f.write(text)
