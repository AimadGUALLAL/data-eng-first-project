#!/bin/bash

# Vérifier si on est en production (sur EC2)
if [ -f "/home/ubuntu/.production" ]; then
    ENV_FILE=".env.production"
    echo "🚀 PRODUCTION MODE"
else
    ENV_FILE=".env"
    echo "🔧 DEVELOPMENT MODE"
fi

# Vérifier que le fichier .env existe
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ $ENV_FILE not found!"
    echo "📝 Create it with your credentials"
    exit 1
fi

echo "⏹️  Stopping containers..."
docker-compose down

echo "🔨 Building images..."
docker-compose build --no-cache

echo "▶️  Starting containers..."
docker-compose --env-file $ENV_FILE up -d

echo "⏳ Waiting 30 seconds..."
sleep 30

echo "✅ Checking status..."
docker-compose ps

echo ""
echo "🎉 Deployment complete!"