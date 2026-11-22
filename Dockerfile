FROM node:22-alpine
LABEL org.opencontainers.image.source="https://github.com/karnotxyz/committed-data-proxy-service"
WORKDIR /app
COPY --chown=node:node package.json server.js ./
USER node
EXPOSE 9945
CMD ["node", "server.js"]
