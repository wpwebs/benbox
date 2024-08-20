#!/bin/bash

# Check if service name is provided as an argument; if not, default to 'hotdeal'
service="${1:-hotdeal}"
# Convert the service to lowercase
service_name="${service,,}"

# Define the service file content
SERVICE_FILE_CONTENT="[Unit]
Description=$service Background Service
After=network.target

[Service]
ExecStart=/home/debian/$service_name/src/main.py
WorkingDirectory=/home/debian/$service_name
StandardOutput=inherit
StandardError=inherit
Restart=always
User=debian
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target"

# Create the service file with proper permissions
echo "$SERVICE_FILE_CONTENT" | sudo tee /etc/systemd/system/$service_name.service > /dev/null

# Reload systemd manager configuration
sudo systemctl daemon-reload

# Enable the service to start on boot
sudo systemctl enable $service_name.service

# Start (or restart if already running) the service
sudo systemctl restart $service_name.service

# Check if the service is active
if sudo systemctl is-active --quiet $service_name.service; then
    echo "$service_name service is running."
else
    echo "Failed to start $service_name service. Please check the status for more details."
    sudo systemctl status $service_name.service
fi
