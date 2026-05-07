# SQL RAG Frontend — React + Vite
# =================================
#
# Two stages:
#   - dev: hot-reload Vite dev server (used by docker-compose default)
#   - prod: nginx serving the built static bundle
#
# Build:
#   docker build --target prod -t sqlrag/frontend:prod ./frontend
#
# Local dev (used by docker-compose.yml):
#   docker compose up frontend     # runs the dev stage

# =============================================================
# Stage: deps — install npm dependencies
# =============================================================
FROM node:22-bookworm-slim AS deps

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

# =============================================================
# Stage: dev — Vite dev server with HMR
# =============================================================
FROM node:22-bookworm-slim AS dev

WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Vite needs to be told to listen on 0.0.0.0 inside a container
ENV HOST=0.0.0.0

EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]

# =============================================================
# Stage: build — produce static bundle
# =============================================================
FROM node:22-bookworm-slim AS build

WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Build the production bundle (tsc + vite build per package.json)
RUN npm run build

# =============================================================
# Stage: prod — nginx serving the static bundle
# =============================================================
FROM nginx:1.27-alpine AS prod

# Health check
RUN apk add --no-cache curl

# Copy the built assets
COPY --from=build /app/dist /usr/share/nginx/html

# Site config
COPY <<'EOF' /etc/nginx/conf.d/default.conf
server {
    listen 80;
    server_name _;
    root /usr/share/nginx/html;
    index index.html;

    # SPA: all unknown routes fall back to index.html so client-side
    # routing works on direct page loads.
    location / {
        try_files $uri $uri/ /index.html;
    }

    location /healthz {
        return 200 "ok\n";
        add_header Content-Type text/plain;
    }
}
EOF

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -fsS http://localhost/healthz || exit 1
