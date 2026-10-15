FROM node:22-slim
ARG ZB_VERSION
WORKDIR /smoke
RUN npm init -y >/dev/null && npm pkg set type=module \
 && npm install --no-audit --no-fund "@zebridge/client@${ZB_VERSION}" better-sqlite3 @nats-io/transport-node
COPY smoke.mjs ./
CMD ["node", "smoke.mjs"]
