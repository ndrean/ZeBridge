import re

with open("src/event_processor.zig", "r") as f:
    text = f.read()

intercept_code = """        // Intercept GC telemetry from the sweeper
        if (operation[0] == 'U' and std.mem.eql(u8, rel.name, "zebridge_gc_watermark")) {
            var reaped_val: u64 = 0;
            for (all_columns.items) |col| {
                if (std.mem.eql(u8, col.name, "reaped")) {
                    if (col.value == .int64 and col.value.int64 > 0) {
                        reaped_val = @as(u64, @intCast(col.value.int64));
                    }
                    break;
                }
            }
            if (reaped_val > 0) {
                self.metrics.updateGcStats(reaped_val);
            }
        }

        // Extract ID value for logging (scan for "id" column in the combined list)"""

text = text.replace('        // Extract ID value for logging (scan for "id" column in the combined list)', intercept_code)

with open("src/event_processor.zig", "w") as f:
    f.write(text)
