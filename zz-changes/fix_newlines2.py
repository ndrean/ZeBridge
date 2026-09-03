with open("src/bridge_sweeper.zig", "r") as f:
    text = f.read()

text = text.replace('std.debug.print("Reconnected and initialized.\n", .{});', 'std.debug.print("Reconnected and initialized.\\n", .{});')

with open("src/bridge_sweeper.zig", "w") as f:
    f.write(text)
