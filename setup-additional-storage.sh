#!/bin/bash
# Setup Additional Storage for TrustMesh Nodes
# Run this script on each node with attached /dev/vdb volume

set -e

echo "=== TrustMesh Additional Storage Setup ==="
echo "Setting up /dev/vdb for Docker storage..."

# Check if volume exists
if [ ! -b "/dev/vdb" ]; then
    echo "Error: /dev/vdb volume not found!"
    exit 1
fi

# Check current disk usage
echo "Current disk usage:"
df -h

# Format volume if not already formatted
if ! blkid /dev/vdb > /dev/null 2>&1; then
    echo "Formatting /dev/vdb..."
    sudo mkfs.ext4 /dev/vdb
else
    echo "/dev/vdb already formatted"
fi

# Create mount point
echo "Creating mount point..."
sudo mkdir -p /var/lib/docker-new

# Mount the volume
echo "Mounting /dev/vdb..."
sudo mount /dev/vdb /var/lib/docker-new

# Add to fstab for permanent mount
if ! grep -q "/dev/vdb" /etc/fstab; then
    echo "Adding to /etc/fstab..."
    echo '/dev/vdb /var/lib/docker-new ext4 defaults 0 2' | sudo tee -a /etc/fstab
fi

# Stop services
echo "Stopping Docker and Kubernetes services..."
sudo systemctl stop docker || true
sudo systemctl stop kubelet || true

# Backup and migrate Docker data
if [ -d "/var/lib/docker" ] && [ "$(ls -A /var/lib/docker)" ]; then
    echo "Backing up existing Docker data..."
    sudo cp -a /var/lib/docker /var/lib/docker-backup-$(date +%Y%m%d_%H%M%S)
    
    echo "Migrating Docker data to new volume..."
    sudo rsync -avP /var/lib/docker/ /var/lib/docker-new/
    
    # Rename directories
    sudo mv /var/lib/docker /var/lib/docker-old
fi

sudo mv /var/lib/docker-new /var/lib/docker

# Configure Docker daemon
echo "Configuring Docker daemon..."
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json <<EOF
{
  "data-root": "/var/lib/docker",
  "storage-driver": "overlay2",
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
EOF

# Restart services
echo "Starting services..."
sudo systemctl start docker
sudo systemctl start kubelet || true

# Wait for Docker to start
sleep 5

# Verify setup
echo "=== Verification ==="
echo "New disk usage:"
df -h

echo "Docker root directory:"
docker info | grep "Docker Root Dir" || true

echo "Docker status:"
sudo systemctl status docker --no-pager -l

echo "=== Setup Complete ==="
echo "Docker is now using the 70GB volume for storage"