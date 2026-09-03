import re

with open("src/event_processor.zig", "r") as f:
    text = f.read()

text = text.replace('self.metrics.updateGcStats(reaped_val);', 'if (self.metrics) |m| m.updateGcStats(reaped_val);')

with open("src/event_processor.zig", "w") as f:
    f.write(text)
