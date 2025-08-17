#!/bin/bash

# Berlin Departure Board - Application Startup Script
# This script starts the entire application stack in the correct order

set -e  # Exit on any error

echo "🚀 Starting Berlin Departure Board Application Stack..."
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

# Check prerequisites
print_status "Checking prerequisites..."

if ! command_exists docker; then
    print_error "Docker is not installed or not in PATH"
    exit 1
fi

if ! command_exists docker-compose; then
    print_error "Docker Compose is not installed or not in PATH"
    exit 1
fi

if ! command_exists poetry; then
    print_error "Poetry is not installed or not in PATH"
    exit 1
fi

print_success "All prerequisites found"

# Step 1: Start Docker Compose
print_status "Starting Docker Compose services (Kafka, Redis, etc.)..."
docker-compose up -d

# Wait for services to be ready
print_status "Waiting for services to start up..."
sleep 10

# Check if Kafka container is running
if ! docker ps | grep -q kafka; then
    print_error "Kafka container is not running"
    exit 1
fi

print_success "Docker services started"

# Step 2: Create Kafka topic
print_status "Creating Kafka topic 'station-departures'..."

# Wait a bit more for Kafka to be fully ready
sleep 5

# Create the topic (ignore error if it already exists)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic station-departures --partitions 10 --replication-factor 1 2>/dev/null || {
    print_warning "Topic 'station-departures' may already exist"
}

# Verify topic was created/exists
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -q "station-departures"; then
    print_success "Kafka topic 'station-departures' is ready"
else
    print_error "Failed to create/verify Kafka topic"
    exit 1
fi

# Step 3: Start the poller in background
print_status "Starting BVG data poller..."
poetry run python -m berlin_departure_board.poller.scheduler &
POLLER_PID=$!
print_success "Poller started (PID: $POLLER_PID)"

# Step 4: Start Spark processing in background
print_status "Starting Spark streaming processor..."
poetry run python -m berlin_departure_board.spark_processing.runner &
SPARK_PID=$!
print_success "Spark processor started (PID: $SPARK_PID)"

# Step 5: Wait a moment for data to start flowing
print_status "Waiting for data pipeline to initialize..."
sleep 15

# Step 6: Start Streamlit dashboard
print_status "Starting Streamlit dashboard..."
print_success "Dashboard will be available at: http://localhost:8501"

# Store PIDs for cleanup
echo $POLLER_PID > .poller.pid
echo $SPARK_PID > .spark.pid

# Function to cleanup background processes
cleanup() {
    print_status "Shutting down services..."

    if [ -f .poller.pid ]; then
        POLLER_PID=$(cat .poller.pid)
        if kill -0 $POLLER_PID 2>/dev/null; then
            print_status "Stopping poller (PID: $POLLER_PID)..."
            kill $POLLER_PID
        fi
        rm -f .poller.pid
    fi

    if [ -f .spark.pid ]; then
        SPARK_PID=$(cat .spark.pid)
        if kill -0 $SPARK_PID 2>/dev/null; then
            print_status "Stopping Spark processor (PID: $SPARK_PID)..."
            kill $SPARK_PID
        fi
        rm -f .spark.pid
    fi

    print_status "Stopping Docker Compose..."
    docker-compose down

    print_success "All services stopped"
    exit 0
}

# Set up trap to cleanup on script exit
trap cleanup SIGINT SIGTERM EXIT

# Start the dashboard (this will run in foreground)
poetry run streamlit run berlin_departure_board/dashboard/app.py --server.port 8501 --server.address 0.0.0.0

# This line should never be reached due to trap, but just in case
cleanup
