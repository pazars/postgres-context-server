FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy source code
COPY . .

# Make the script executable
RUN chmod +x index.mjs

EXPOSE 3000

CMD ["node", "index.mjs"]