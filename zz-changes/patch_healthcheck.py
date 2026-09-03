import re

with open("docker-compose.full.yml", "r") as f:
    text = f.read()

healthcheck = """    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://127.0.0.1:9090/health"]
      interval: 10s
      timeout: 5s
      retries: 3
    networks:"""

text = text.replace('    networks:', healthcheck)

with open("docker-compose.full.yml", "w") as f:
    f.write(text)
