import re

with open("src/metrics.zig", "r") as f:
    text = f.read()

# 1. Add fields to Metrics struct
struct_fields = """    schema_events_published: std.atomic.Value(u64),

    // Sweeper Telemetry (intercepted from zebridge_gc_watermark WAL events)
    gc_total_reaped: std.atomic.Value(u64),
    gc_last_sweep_time: std.atomic.Value(i64),"""
text = text.replace("    schema_events_published: std.atomic.Value(u64),", struct_fields)

# 2. Add init
init_fields = """            .schema_events_published = std.atomic.Value(u64).init(0),
            .gc_total_reaped = std.atomic.Value(u64).init(0),
            .gc_last_sweep_time = std.atomic.Value(i64).init(0),"""
text = text.replace("            .schema_events_published = std.atomic.Value(u64).init(0),", init_fields)

# 3. Add method
methods = """    /// Lock-free update of GC stats intercepted from WAL
    pub fn updateGcStats(self: *Metrics, reaped: u64) void {
        _ = self.gc_total_reaped.fetchAdd(reaped, .monotonic);
        self.gc_last_sweep_time.store(@as(i64, @intCast(c.time(null))), .monotonic);
    }

    /// Lock-free increment of the SCHEMA/KV events counter"""
text = text.replace("    /// Lock-free increment of the SCHEMA/KV events counter", methods)

# 4. Add to Snapshot struct
snapshot_fields = """        schema_events_published: u64,
        gc_total_reaped: u64,
        gc_last_sweep_time: i64,"""
text = text.replace("        schema_events_published: u64,", snapshot_fields)

# 5. Add to snapshot method
snapshot_init = """            .schema_events_published = self.schema_events_published.load(.monotonic),
            .gc_total_reaped = self.gc_total_reaped.load(.monotonic),
            .gc_last_sweep_time = self.gc_last_sweep_time.load(.monotonic),"""
text = text.replace("            .schema_events_published = self.schema_events_published.load(.monotonic),", snapshot_init)

with open("src/metrics.zig", "w") as f:
    f.write(text)
