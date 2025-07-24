#!/bin/bash
# Storage Setup for K3s Agent Nodes (Containerd)
# Sets up 70GB /dev/vdb for containerd storage on compute nodes

set -e

echo "=== K3s Agent Storage Setup - Containerd ==="
echo "This script will configure /dev/vdb for containerd storage on K3s agent nodes"

# Function to safely stop services
stop_services() {
    echo "Stopping K3s agent service..."
    sudo systemctl stop k3s-agent 2>/dev/null || true
    
    sleep 5
    
    # Force kill any remaining processes
    sudo pkill -f k3s 2>/dev/null || true
    sudo pkill -f containerd 2>/dev/null || true
    
    sleep 10
    echo "Services stopped"
}

# Function to clean up previous attempts
cleanup_previous() {
    echo "Cleaning up any previous failed attempts..."
    
    # Unmount any existing mounts
    sudo umount /var/lib/rancher-new 2>/dev/null || true
    sudo umount /var/lib/rancher 2>/dev/null || true
    sudo umount /mnt/containerd-storage 2>/dev/null || true
    
    # Remove temporary directories
    sudo rm -rf /var/lib/rancher-new 2>/dev/null || true
    sudo rm -rf /mnt/containerd-storage 2>/dev/null || true
    
    # Clean up fstab entries
    sudo sed -i '/rancher-new/d' /etc/fstab 2>/dev/null || true
    
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
sudo mkdir -p /mnt/containerd-storage
sudo mount /dev/vdb /mnt/containerd-storage

# K3s uses /var/lib/rancher for all its data including containerd
BACKUP_NEEDED=false
if [ -d "/var/lib/rancher" ] && [ "$(ls -A /var/lib/rancher 2>/dev/null)" ]; then
    echo "Found existing K3s/containerd data, backing up..."
    BACKUP_DIR="/var/lib/rancher-backup-$(date +%Y%m%d_%H%M%S)"
    sudo cp -a /var/lib/rancher "$BACKUP_DIR"
    echo "Backup created at: $BACKUP_DIR"
    
    echo "Copying K3s/containerd data to new volume..."
    sudo rsync -avP /var/lib/rancher/ /mnt/containerd-storage/
    BACKUP_NEEDED=true
    echo "Data migration complete"
else
    echo "No existing K3s/containerd data found"
fi

# Remove old rancher directory
if [ -d "/var/lib/rancher" ]; then
    echo "Removing old rancher directory..."
    sudo rm -rf /var/lib/rancher
fi

# Create new rancher directory
echo "Creating new rancher directory..."
sudo mkdir -p /var/lib/rancher

# Unmount from temporary location and mount to final location
echo "Setting up final mount..."
sudo umount /mnt/containerd-storage
sudo mount /dev/vdb /var/lib/rancher

# Add permanent mount to fstab
echo "Adding permanent mount to /etc/fstab..."
# Remove any existing entries first
sudo sed -i '\|/var/lib/rancher|d' /etc/fstab
echo '/dev/vdb /var/lib/rancher ext4 defaults 0 2' | sudo tee -a /etc/fstab

# Reload systemd to recognize fstab changes
sudo systemctl daemon-reload

# Set proper permissions
sudo chown root:root /var/lib/rancher
sudo chmod 755 /var/lib/rancher

# Start K3s agent
echo "Starting K3s agent..."
sudo systemctl start k3s-agent

# Wait for service to initialize
echo "Waiting for K3s agent to start..."
sleep 20

# Verification
echo ""
echo "=== VERIFICATION ==="
echo ""

echo "1. Disk usage after setup:"
df -h | grep -E "(Filesystem|/dev/root|/dev/vda|/dev/vdb|rancher)" || df -h

echo ""
echo "2. Mount verification:"
mount | grep vdb

echo ""
echo "3. K3s agent status:"
if sudo systemctl is-active --quiet k3s-agent; then
    echo "✓ K3s agent is running"
else
    echo "✗ K3s agent is not running"
    echo "K3s agent logs:"
    sudo journalctl -u k3s-agent --no-pager -l | tail -10
fi

echo ""
echo "4. Available space in rancher directory:"
df -h /var/lib/rancher

echo ""
echo "5. Containerd processes:"
ps aux | grep containerd | grep -v grep || echo "No containerd processes visible (normal for K3s)"

echo ""
if [ "$BACKUP_NEEDED" = true ]; then
    echo "📋 NOTE: Your original K3s data was backed up to:"
    ls -la /var/lib/rancher-backup-* 2>/dev/null || echo "Backup directory not found"
fi

echo ""
echo "✅ SETUP COMPLETE!"
echo "K3s agent is now using the 70GB volume (/dev/vdb) for containerd storage"
echo "Container images and data will be stored on the large volume"

# Cleanup temporary directory
sudo rmdir /mnt/containerd-storage 2>/dev/null || true

echo ""
echo "🔍 You can verify the node joined the cluster from the master with:"
echo "   kubectl get nodes"