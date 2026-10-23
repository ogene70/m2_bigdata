FROM node:20-alpine

WORKDIR /app

COPY server.js /app/server.js

ENV PORT=8090
ENV NODE_ENV=production

EXPOSE 8090

CMD ["node", "/app/server.js"]
