with open("src/bridge_sweeper.zig", "r") as f:
    text = f.read()

text = text.replace('setup_connection(allocator, pg_conn, principal, sweeps.items, dry_run) catch |err| {', 'setup_connection(allocator, pg_conn.?, principal, sweeps.items, dry_run) catch |err| {')

with open("src/bridge_sweeper.zig", "w") as f:
    f.write(text)
