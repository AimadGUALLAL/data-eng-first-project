#!/bin/bash

# Script to generate security keys for Airflow

echo "🔐 Generating Airflow Security Keys"
echo "===================================="
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 is required but not installed."
    exit 1
fi

# Generate Fernet Key
echo "📝 Generating Fernet Key..."
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
echo "AIRFLOW_FERNET_KEY=$FERNET_KEY"
echo ""

# Generate Secret Key
echo "📝 Generating Secret Key..."
if command -v openssl &> /dev/null; then
    SECRET_KEY=$(openssl rand -hex 32)
    echo "AIRFLOW_SECRET_KEY=$SECRET_KEY"
else
    SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    echo "AIRFLOW_SECRET_KEY=$SECRET_KEY"
fi
echo ""

# Generate secure password
echo "📝 Generating secure password suggestion..."
SECURE_PASSWORD=$(python3 -c "import secrets, string; chars = string.ascii_letters + string.digits + '!@#$%^&*'; print(''.join(secrets.choice(chars) for _ in range(16)))")
echo "SUGGESTED_PASSWORD=$SECURE_PASSWORD"
echo ""

echo "✅ Copy these values to your .env.production file"
echo ""
echo "⚠️  IMPORTANT: Keep these keys secure and never commit them to git!"