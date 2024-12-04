#!/bin/bash

# Function to start the services
start_services() {
    echo "Starting Docker containers..."
    docker-compose up -d

    # Wait for the Docker containers to fully start (optional)
    echo "Waiting for containers to initialize..."
    sleep 10

    # Start the Python server as 'pythonuser'
    echo "Starting the Python server..."
    sudo -u pythonuser ./MainServerEnv/bin/python3 ./Main.py
}

# Function to stop the services
stop_services() {
    echo "Stopping Python server..."

    # Kill the Python server process
    pkill -f 'python3 ./Main.py'

    echo "Stopping Docker containers..."
    docker-compose down
}

# Main logic to handle start or stop based on the argument
if [ "$1" == "start" ]; then
    start_services
elif [ "$1" == "stop" ]; then
    stop_services
else
    echo "Usage: $0 {start|stop}"
    exit 1
fi
