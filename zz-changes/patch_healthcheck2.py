import re

with open("docker-compose.full.yml", "r") as f:
    text = f.read()

# Find the end of the bridge service (right before proxy or 7b)
bridge_end_pattern = r'(    networks:\n      - cdc-net\n    restart: unless-stopped)\n\n  # 7b'
healthcheck = r"""    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://127.0.0.1:9090/health"]
      interval: 10s
      timeout: 5s
      retries: 3
\1

  # 7b"""

text = re.sub(bridge_end_pattern, healthcheck, text)

with open("docker-compose.full.yml", "w") as f:
    f.write(text)
