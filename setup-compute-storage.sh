#!/bin/bash
# K3s Agent Storage Setup for Compute Nodes
# Sets up 70GB /dev/vdb for K3s agent (containerd) storage

set -e

echo "=== K3s Agent Storage Setup ==="
echo "This script will configure /dev/vdb for K3s agent storage on compute nodes"

# Check if volume exists
if [ ! -b "/dev/vdb" ]; then
    echo "ERROR: /dev/vdb volume not found!"
    echo "Make sure the 70GB volume is attached to this node"
    exit 1
fi

echo "Current disk usage BEFORE setup:"
df -h | grep -E "(Filesystem|/dev/root|/dev/vda|/dev/vdb)" || df -h

# Stop K3s agent and clean up processes
echo "Stopping K3s agent..."
sudo systemctl stop k3s-agent 2>/dev/null || true
sleep 5
sudo pkill -f k3s 2>/dev/null || true
sudo pkill -f containerd-shim 2>/dev/null || true
sleep 10
echo "K3s agent stopped"

# Clean up any previous mounts
echo "Cleaning up previous attempts..."
sudo umount /var/lib/rancher 2>/dev/null || true
sudo umount /mnt/k3s-storage 2>/dev/null || true
sudo rm -rf /mnt/k3s-storage 2>/dev/null || true
sudo sed -i '\|/var/lib/rancher|d' /etc/fstab 2>/dev/null || true

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

# Create temporary mount point and mount
echo "Setting up temporary mount..."
sudo mkdir -p /mnt/k3s-storage
sudo mount /dev/vdb /mnt/k3s-storage

# Backup existing rancher data if it exists
BACKUP_NEEDED=false
if [ -d "/var/lib/rancher" ] && [ "$(ls -A /var/lib/rancher 2>/dev/null)" ]; then
    echo "Found existing K3s agent data, backing up..."
    BACKUP_DIR="/mnt/k3s-storage/rancher-backup-$(date +%Y%m%d_%H%M%S)"
    sudo cp -a /var/lib/rancher "$BACKUP_DIR"
    echo "Backup created at: $BACKUP_DIR"
    
    echo "Moving K3s agent data to new volume..."
    sudo rsync -avP /var/lib/rancher/ /mnt/k3s-storage/
    BACKUP_NEEDED=true
    echo "Data migration complete"
    
    # Remove old directory
    sudo rm -rf /var/lib/rancher
else
    echo "No existing K3s agent data found"
fi

# Create new rancher directory
echo "Setting up K3s agent storage..."
sudo mkdir -p /var/lib/rancher

# Unmount from temporary location and mount to final location
echo "Mounting volume to /var/lib/rancher..."
sudo umount /mnt/k3s-storage
sudo mount /dev/vdb /var/lib/rancher

# Add permanent mount to fstab
echo "Adding permanent mount to /etc/fstab..."
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
df -h | grep -E "(Filesystem|/dev/root|/dev/vda|/dev/vdb)" || df -h

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
echo "4. K3s agent storage:"
df -h /var/lib/rancher

echo ""
echo "5. Storage usage breakdown:"
echo "   Rancher data: $(du -sh /var/lib/rancher 2>/dev/null | awk '{print $1}' || echo 'Initializing...')"

echo ""
if [ "$BACKUP_NEEDED" = true ]; then
    echo "📋 NOTE: Original K3s data backed up on the new volume"
fi

echo ""
echo "✅ SETUP COMPLETE!"
echo "K3s agent is now using the 70GB volume (/dev/vdb) for all storage"
echo "Container images and pod data will use the large volume"

# Cleanup temporary directory
sudo rmdir /mnt/k3s-storage 2>/dev/null || true

echo ""
echo "🔍 Verify this node joined the cluster from the server:"
echo "   kubectl get nodes"