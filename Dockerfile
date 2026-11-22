FROM node:24-alpine
LABEL org.opencontainers.image.source="https://github.com/karnotxyz/committed-data-proxy-service"
WORKDIR /app
COPY --chown=node:node package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY --chown=node:node server.js archive.js delivery.js ./
USER node
EXPOSE 9945
CMD ["node", "server.js"]
