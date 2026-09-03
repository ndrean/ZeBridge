with open("src/bridge_sweeper.zig", "r") as f:
    text = f.read()

text = text.replace('std.debug.print("FATAL: failed to initialize connection: {any}\n", .{err});', 'std.debug.print("FATAL: failed to initialize connection: {any}\\n", .{err});')
text = text.replace('std.debug.print("ERROR: failed to initialize reconnected session: {any}\n", .{err});', 'std.debug.print("ERROR: failed to initialize reconnected session: {any}\\n", .{err});')

with open("src/bridge_sweeper.zig", "w") as f:
    f.write(text)
