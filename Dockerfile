FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
# Reproducible install from the lockfile, without dev dependencies
RUN npm ci --omit=dev

COPY . .

# Don't run as root inside the container
USER node
