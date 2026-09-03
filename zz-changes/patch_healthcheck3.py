import re

with open("docker-compose.full.yml", "r") as f:
    text = f.read()

old = 'test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://127.0.0.1:9090/health"]'
new = 'test: ["CMD-SHELL", "wget --no-verbose --tries=1 --spider http://127.0.0.1:$${BRIDGE_PUBLISH_PORT:-9090}/health || exit 1"]'

text = text.replace(old, new)

with open("docker-compose.full.yml", "w") as f:
    f.write(text)
