FROM node:20

WORKDIR /app

ENV NODE_OPTIONS=--max-old-space-size=8192

# Copy only package files first (better caching)
COPY package*.json ./

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    make \
    g++ \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install node modules
RUN npm install

# Install PM2 globally
RUN npm install -g pm2

# Copy full project
COPY . .

# Debug (optional, you can remove later)
RUN ls -la /app

# Use PM2 runtime (IMPORTANT FIX)
CMD ["pm2-runtime", "ecosystem.config.json"]
