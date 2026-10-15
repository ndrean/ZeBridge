FROM node:22-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl unzip file \
 && rm -rf /var/lib/apt/lists/*
COPY check.sh /check.sh
CMD ["sh", "/check.sh"]
