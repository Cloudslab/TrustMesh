#!/bin/bash
# Clean Storage Setup for TrustMesh K3s Nodes
# Handles failed previous attempts and sets up 70GB /dev/vdb for Docker storage

set -e

echo "=== TrustMesh Storage Setup - Clean Recovery ==="
echo "This script will fix any partial setup and configure /dev/vdb for Docker"

# Function to safely stop services
stop_services() {
    echo "Stopping all relevant services..."
    sudo systemctl stop docker 2>/dev/null || true
    sudo systemctl stop k3s 2>/dev/null || true  
    sudo systemctl stop k3s-agent 2>/dev/null || true
    sudo systemctl stop containerd 2>/dev/null || true
    
    sleep 5
    
    # Force kill any remaining processes
    sudo pkill -f docker 2>/dev/null || true
    sudo pkill -f containerd 2>/dev/null || true
    sudo pkill -f k3s 2>/dev/null || true
    
    sleep 10
    echo "Services stopped"
}

# Function to clean up previous attempts
cleanup_previous() {
    echo "Cleaning up any previous failed attempts..."
    
    # Unmount any existing mounts
    sudo umount /var/lib/docker-new 2>/dev/null || true
    sudo umount /var/lib/docker 2>/dev/null || true
    sudo umount /mnt/docker-storage 2>/dev/null || true
    
    # Remove temporary directories
    sudo rm -rf /var/lib/docker-new 2>/dev/null || true
    sudo rm -rf /mnt/docker-storage 2>/dev/null || true
    
    # Clean up fstab entries
    sudo sed -i '/docker-new/d' /etc/fstab 2>/dev/null || true
    
    echo "Cleanup complete"
}

# Check prerequisites
if [ ! -b "/dev/vdb" ]; then
    echo "ERROR: /dev/vdb volume not found!"
    echo "Make sure the 70GB volume is attached to this node"
    exit 1
fi

echo "Current disk usage BEFORE setup:"
df -h | grep -E "(Filesystem|/dev/root|/dev/vda|/dev/vdb)" || df -h

# Stop everything
stop_services

# Clean up any previous attempts
cleanup_previous

# Format volume if needed
echo "Checking /dev/vdb format..."
if ! blkid /dev/vdb > /dev/null 2>&1; then
    echo "Formatting /dev/vdb as ext4..."
    sudo mkfs.ext4 -F /dev/vdb
    echo "Format complete"
else
    echo "/dev/vdb already formatted"
    blkid /dev/vdb
fi

# Create temporary mount point
echo "Setting up temporary mount point..."
sudo mkdir -p /mnt/docker-storage
sudo mount /dev/vdb /mnt/docker-storage

# Backup existing Docker data if it exists
BACKUP_NEEDED=false
if [ -d "/var/lib/docker" ] && [ "$(ls -A /var/lib/docker 2>/dev/null)" ]; then
    echo "Found existing Docker data, backing up..."
    BACKUP_DIR="/var/lib/docker-backup-$(date +%Y%m%d_%H%M%S)"
    sudo cp -a /var/lib/docker "$BACKUP_DIR"
    echo "Backup created at: $BACKUP_DIR"
    
    echo "Copying Docker data to new volume..."
    sudo rsync -avP /var/lib/docker/ /mnt/docker-storage/
    BACKUP_NEEDED=true
    echo "Data migration complete"
else
    echo "No existing Docker data found"
fi

# Remove old Docker directory
if [ -d "/var/lib/docker" ]; then
    echo "Removing old Docker directory..."
    sudo rm -rf /var/lib/docker
fi

# Create new Docker directory
echo "Creating new Docker directory..."
sudo mkdir -p /var/lib/docker

# Unmount from temporary location and mount to final location
echo "Setting up final mount..."
sudo umount /mnt/docker-storage
sudo mount /dev/vdb /var/lib/docker

# Add permanent mount to fstab
echo "Adding permanent mount to /etc/fstab..."
# Remove any existing entries first
sudo sed -i '\|/var/lib/docker|d' /etc/fstab
echo '/dev/vdb /var/lib/docker ext4 defaults 0 2' | sudo tee -a /etc/fstab

# Configure Docker daemon
echo "Configuring Docker daemon..."
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json > /dev/null <<EOF
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

# Set proper permissions
sudo chown root:root /var/lib/docker
sudo chmod 755 /var/lib/docker

# Start services
echo "Starting services..."
sudo systemctl start docker

# Wait for Docker to initialize
echo "Waiting for Docker to start..."
sleep 15

# Start K3s services
if systemctl list-unit-files | grep -q "k3s.service"; then
    echo "Starting K3s server..."
    sudo systemctl start k3s
elif systemctl list-unit-files | grep -q "k3s-agent.service"; then
    echo "Starting K3s agent..."  
    sudo systemctl start k3s-agent
fi

# Wait for services to stabilize
sleep 10

# Verification
echo ""
echo "=== VERIFICATION ==="
echo ""

echo "1. Disk usage after setup:"
df -h | grep -E "(Filesystem|/dev/root|/dev/vda|/dev/vdb|docker)" || df -h

echo ""
echo "2. Mount verification:"
mount | grep vdb

echo ""
echo "3. Docker status:"
if sudo systemctl is-active --quiet docker; then
    echo "✓ Docker is running"
    docker info | grep "Docker Root Dir" 2>/dev/null || echo "Docker info not yet available"
else
    echo "✗ Docker is not running"
    echo "Docker logs:"
    sudo journalctl -u docker --no-pager -l | tail -10
fi

echo ""
echo "4. K3s status:"
if sudo systemctl is-active --quiet k3s; then
    echo "✓ K3s server is running"
elif sudo systemctl is-active --quiet k3s-agent; then
    echo "✓ K3s agent is running"
else
    echo "! K3s not running (may be normal for some nodes)"
fi

echo ""
echo "5. Available space in Docker directory:"
df -h /var/lib/docker

echo ""
if [ "$BACKUP_NEEDED" = true ]; then
    echo "📋 NOTE: Your original Docker data was backed up to:"
    ls -la /var/lib/docker-backup-* 2>/dev/null || echo "Backup directory not found"
fi

echo ""
echo "✅ SETUP COMPLETE!"
echo "Docker is now using the 70GB volume (/dev/vdb) for storage"
echo "You can safely delete old backup directories once you verify everything works"

# Cleanup temporary directory
sudo rmdir /mnt/docker-storage 2>/dev/null || true