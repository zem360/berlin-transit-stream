#!/bin/bash

# Berlin Departure Board - Application Stop & Cleanup Script
# This script stops all services and cleans up Docker volumes

set -e  # Exit on any error

echo "🛑 Stopping Berlin Departure Board Application Stack..."
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if Docker is available
if ! command_exists docker; then
    print_error "Docker is not installed or not in PATH"
    exit 1
fi

if ! command_exists docker-compose; then
    print_error "Docker Compose is not installed or not in PATH"
    exit 1
fi

# Step 1: Stop any running background processes from start.sh
print_status "Stopping background application processes..."

if [ -f .poller.pid ]; then
    POLLER_PID=$(cat .poller.pid)
    if kill -0 $POLLER_PID 2>/dev/null; then
        print_status "Stopping poller (PID: $POLLER_PID)..."
        kill $POLLER_PID 2>/dev/null || true
        print_success "Poller stopped"
    else
        print_warning "Poller process not running"
    fi
    rm -f .poller.pid
else
    print_warning "No poller PID file found"
fi

if [ -f .spark.pid ]; then
    SPARK_PID=$(cat .spark.pid)
    if kill -0 $SPARK_PID 2>/dev/null; then
        print_status "Stopping Spark processor (PID: $SPARK_PID)..."
        kill $SPARK_PID 2>/dev/null || true
        print_success "Spark processor stopped"
    else
        print_warning "Spark processor not running"
    fi
    rm -f .spark.pid
else
    print_warning "No Spark PID file found"
fi

# Step 2: Stop any additional Python processes that might be running
print_status "Stopping any remaining application processes..."
pkill -f "berlin_departure_board" 2>/dev/null || print_warning "No additional application processes found"

# Step 3: Stop Streamlit processes
print_status "Stopping Streamlit processes..."
pkill -f "streamlit" 2>/dev/null || print_warning "No Streamlit processes found"

# Step 4: Stop Docker Compose services
print_status "Stopping Docker Compose services..."
if [ -f docker-compose.yml ] || [ -f docker-compose.yaml ]; then
    docker-compose down --timeout 30
    print_success "Docker Compose services stopped"
else
    print_warning "No docker-compose.yml file found"
fi

# Step 5: Remove Docker containers (if any are still running)
print_status "Removing any remaining containers..."
CONTAINERS=$(docker ps -a --filter "name=kafka" --filter "name=redis" --filter "name=zookeeper" -q)
if [ ! -z "$CONTAINERS" ]; then
    docker rm -f $CONTAINERS 2>/dev/null || true
    print_success "Remaining containers removed"
else
    print_warning "No matching containers found"
fi

# Step 6: Clean up Docker volumes
print_status "Cleaning up Docker volumes..."

# List all volumes related to the project
PROJECT_VOLUMES=$(docker volume ls --filter "name=berlin" -q)
KAFKA_VOLUMES=$(docker volume ls --filter "name=kafka" -q)
REDIS_VOLUMES=$(docker volume ls --filter "name=redis" -q)

# Combine all volumes
ALL_VOLUMES="$PROJECT_VOLUMES $KAFKA_VOLUMES $REDIS_VOLUMES"

if [ ! -z "$ALL_VOLUMES" ]; then
    print_status "Found volumes to clean: $ALL_VOLUMES"
    echo -n "Do you want to remove these volumes? This will delete all data! (y/N): "
    read -r CONFIRM

    if [[ $CONFIRM =~ ^[Yy]$ ]]; then
        for volume in $ALL_VOLUMES; do
            if [ ! -z "$volume" ]; then
                print_status "Removing volume: $volume"
                docker volume rm "$volume" 2>/dev/null || print_warning "Could not remove volume: $volume"
            fi
        done
        print_success "Volumes cleaned up"
    else
        print_warning "Volume cleanup skipped"
    fi
else
    print_warning "No project-related volumes found"
fi

# Step 7: Clean up Docker networks
print_status "Cleaning up Docker networks..."
PROJECT_NETWORKS=$(docker network ls --filter "name=berlin" -q)
if [ ! -z "$PROJECT_NETWORKS" ]; then
    for network in $PROJECT_NETWORKS; do
        if [ ! -z "$network" ]; then
            print_status "Removing network: $network"
            docker network rm "$network" 2>/dev/null || print_warning "Could not remove network: $network"
        fi
    done
    print_success "Networks cleaned up"
else
    print_warning "No project-related networks found"
fi

# Step 8: Optional Docker system cleanup
echo -n "Do you want to run Docker system prune to clean up unused resources? (y/N): "
read -r PRUNE_CONFIRM

if [[ $PRUNE_CONFIRM =~ ^[Yy]$ ]]; then
    print_status "Running Docker system prune..."
    docker system prune -f
    print_success "Docker system cleaned up"
else
    print_warning "Docker system prune skipped"
fi

# Step 9: Clean up any log files or temporary files
print_status "Cleaning up temporary files..."
rm -f .poller.pid .spark.pid
rm -f *.log 2>/dev/null || true
print_success "Temporary files cleaned up"

print_success "=============================================="
print_success "🎉 All services stopped and cleanup complete!"
print_success "=============================================="

echo ""
echo "Summary of actions performed:"
echo "✅ Stopped background application processes"
echo "✅ Stopped Docker Compose services"
echo "✅ Removed containers"
echo "✅ Cleaned up volumes (if confirmed)"
echo "✅ Cleaned up networks"
echo "✅ Removed temporary files"
echo ""
echo "You can now safely restart the application with ./start.sh"
