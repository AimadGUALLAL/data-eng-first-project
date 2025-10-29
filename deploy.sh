#!/bin/bash

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_header() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
}

# Detect environment
if [ -f "/home/ubuntu/.production" ] || [ -f ".production" ]; then
    ENV_FILE=".env.production"
    COMPOSE_FILE="docker-compose.prod.yml"
    print_header "🚀 Crypto Data Pipeline - Production Deployment"
else
    ENV_FILE=".env"
    COMPOSE_FILE="docker-compose.yml"
    print_header "🔧 Crypto Data Pipeline - Development Deployment"
fi

print_info "Using environment file: $ENV_FILE"
print_info "Using compose file: $COMPOSE_FILE"

# Check if env file exists
if [ ! -f "$ENV_FILE" ]; then
    print_error "$ENV_FILE not found!"
    print_warning "Please create $ENV_FILE with required variables"
    exit 1
fi

# Check required variables in env file
print_info "Checking required environment variables..."
REQUIRED_VARS=(
    "AWS_ACCESS_KEY_ID"
    "AWS_SECRET_ACCESS_KEY"
    "AWS_DEFAULT_REGION"
    "AIRFLOW_ADMIN_USERNAME"
    "AIRFLOW_ADMIN_PASSWORD"
    "AIRFLOW_ADMIN_EMAIL"
    "POSTGRES_PASSWORD"
    "AIRFLOW_FERNET_KEY"
    "AIRFLOW_SECRET_KEY"
)

MISSING_VARS=()
for var in "${REQUIRED_VARS[@]}"; do
    if ! grep -q "^${var}=" "$ENV_FILE"; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -ne 0 ]; then
    print_error "Missing required variables in $ENV_FILE:"
    for var in "${MISSING_VARS[@]}"; do
        echo "  - $var"
    done
    exit 1
fi

print_success "Environment file contains required variables"

# Setup directories with correct permissions
print_info "Setting up directories..."
mkdir -p ./airflow/dags ./airflow/logs ./airflow/plugins

# Fix permissions for Airflow (UID 50000)
print_info "Setting correct permissions..."
if command -v sudo &> /dev/null; then
    sudo chown -R 50000:0 ./airflow/dags ./airflow/logs ./airflow/plugins 2>/dev/null || \
        chown -R 50000:0 ./airflow/dags ./airflow/logs ./airflow/plugins 2>/dev/null || \
        print_warning "Could not change ownership. Running as current user."
    
    sudo chmod -R 775 ./airflow/logs ./airflow/dags ./airflow/plugins 2>/dev/null || \
        chmod -R 775 ./airflow/logs ./airflow/dags ./airflow/plugins 2>/dev/null || \
        print_warning "Could not change permissions."
fi

# Stop old containers
print_info "Stopping old containers..."
docker-compose -f $COMPOSE_FILE down 2>/dev/null || true

# Clean up old images (optional, comment out if you want to keep them)
# print_info "Cleaning up old images..."
# docker image prune -f

# Build new images
print_info "Building Docker images..."
docker-compose -f $COMPOSE_FILE build --no-cache

# Start services
print_info "Starting services..."
docker-compose -f $COMPOSE_FILE --env-file $ENV_FILE up -d

# Wait for services to be ready
print_info "Waiting for services to initialize..."
sleep 10

# Check if postgres is ready
print_info "Checking PostgreSQL..."
for i in {1..30}; do
    if docker-compose -f $COMPOSE_FILE exec -T postgres pg_isready -U airflow &>/dev/null; then
        print_success "PostgreSQL is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        print_error "PostgreSQL failed to start"
        docker-compose -f $COMPOSE_FILE logs postgres
        exit 1
    fi
    echo -n "."
    sleep 2
done

# Wait for init to complete
print_info "Waiting for Airflow initialization..."
for i in {1..60}; do
    if docker-compose -f $COMPOSE_FILE ps | grep -q "airflow-init.*Exit 0"; then
        print_success "Airflow initialization completed"
        break
    fi
    if docker-compose -f $COMPOSE_FILE ps | grep -q "airflow-init.*Exit [1-9]"; then
        print_error "Airflow initialization failed"
        docker-compose -f $COMPOSE_FILE logs airflow-init
        exit 1
    fi
    if [ $i -eq 60 ]; then
        print_warning "Initialization timeout, checking logs..."
        docker-compose -f $COMPOSE_FILE logs airflow-init
    fi
    echo -n "."
    sleep 2
done

# Wait for webserver to be healthy
print_info "Waiting for Airflow Webserver..."
for i in {1..60}; do
    if curl -s http://localhost:8080/health | grep -q "healthy" 2>/dev/null; then
        print_success "Airflow Webserver is ready"
        break
    fi
    if [ $i -eq 60 ]; then
        print_warning "Webserver health check timeout"
    fi
    echo -n "."
    sleep 2
done

# Display status
echo ""
print_header "📊 Deployment Status"
docker-compose -f $COMPOSE_FILE ps

# Show resource usage
echo ""
print_info "Container resource usage:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"

# Get webserver URL
if [ -f "/home/ubuntu/.production" ] || [ -f ".production" ]; then
    INSTANCE_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "YOUR_EC2_IP")
    WEBSERVER_URL="http://${INSTANCE_IP}:8080"
else
    WEBSERVER_URL="http://localhost:8080"
fi

# Final messages
echo ""
print_header "🎉 Deployment Complete!"
echo ""
print_success "Airflow Webserver: $WEBSERVER_URL"
print_info "Username: $(grep AIRFLOW_ADMIN_USERNAME $ENV_FILE | cut -d '=' -f2)"
print_info "Password: (check $ENV_FILE)"
echo ""
print_info "Useful commands:"
echo "  📋 View logs:           docker-compose -f $COMPOSE_FILE logs -f"
echo "  📋 View specific logs:  docker-compose -f $COMPOSE_FILE logs -f airflow-webserver"
echo "  🔄 Restart services:    docker-compose -f $COMPOSE_FILE restart"
echo "  ⏹️  Stop services:       docker-compose -f $COMPOSE_FILE down"
echo "  📊 Check status:        docker-compose -f $COMPOSE_FILE ps"
echo ""