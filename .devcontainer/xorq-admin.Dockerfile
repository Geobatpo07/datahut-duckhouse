# syntax=docker/dockerfile:1.7
# XORQ admin dashboard (React/Vite) served by nginx. Build context = repo root.
# No secrets: the API URL is injected at build time via a non-sensitive ARG.

FROM node:20-alpine AS build
WORKDIR /app

COPY xorq-admin-dashboard/package*.json ./
RUN npm ci || npm install

COPY xorq-admin-dashboard/ ./
RUN npm run build


FROM nginx:1.27-alpine

# SPA fallback: serve index.html for client-side routes.
RUN printf 'server {\n  listen 80;\n  root /usr/share/nginx/html;\n  location / {\n    try_files $uri $uri/ /index.html;\n  }\n}\n' \
    > /etc/nginx/conf.d/default.conf

COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:80/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
