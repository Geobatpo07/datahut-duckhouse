# Step 1: Build the React app
FROM node:20-alpine AS build
WORKDIR /app

# Install dependencies first to leverage Docker cache
COPY xorq-admin-dashboard/package*.json ./
RUN npm install

# Copy the rest of the app and build
COPY xorq-admin-dashboard/ ./
RUN npm run build

# Step 2: Serve using nginx
FROM nginx:alpine

# Vite outputs to `dist` by default. For CRA, use `build` instead.
COPY --from=build /app/dist /usr/share/nginx/html

# Optional: use custom nginx config for SPA fallback routing
# COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
