#!/bin/bash
# Additional K3s Server Storage Setup for Network Management Console
# Sets up containerd storage on the same 70GB volume already used by Docker

set -e

echo "=== Network Console K3s Server Storage Setup ==="
echo "This script will configure K3s server (containerd) storage alongside Docker"

# Check if Docker volume is already mounted
if ! mountpoint -q /var/lib/docker; then
    echo "ERROR: /var/lib/docker is not mounted!"
    echo "Please run fix-storage-clean.sh first to set up Docker storage"
    exit 1
fi

if ! mount | grep -q "/dev/vdb on /var/lib/docker"; then
    echo "ERROR: /dev/vdb is not mounted to /var/lib/docker!"
    echo "Please run fix-storage-clean.sh first to set up Docker storage"
    exit 1
fi

echo "✓ Docker storage already configured on /dev/vdb"

# Function to safely stop K3s server
stop_k3s() {
    echo "Stopping K3s server..."
    sudo systemctl stop k3s 2>/dev/null || true
    
    sleep 5
    
    # Force kill any remaining processes
    sudo pkill -f k3s 2>/dev/null || true
    
    sleep 10
    echo "K3s server stopped"
}

echo "Current disk usage BEFORE K3s setup:"
df -h | grep -E "(Filesystem|/dev/root|/dev/vda|/dev/vdb)" || df -h

# Stop K3s server
stop_k3s

# Create rancher directory on the Docker volume
echo "Creating K3s data directory on Docker volume..."
sudo mkdir -p /var/lib/docker/k3s-server-data

# Backup existing rancher data if it exists
BACKUP_NEEDED=false
if [ -d "/var/lib/rancher" ] && [ "$(ls -A /var/lib/rancher 2>/dev/null)" ]; then
    echo "Found existing K3s server data, backing up..."
    BACKUP_DIR="/var/lib/docker/rancher-backup-$(date +%Y%m%d_%H%M%S)"
    sudo cp -a /var/lib/rancher "$BACKUP_DIR"
    echo "Backup created at: $BACKUP_DIR"
    
    echo "Moving K3s server data to shared volume..."
    sudo rsync -avP /var/lib/rancher/ /var/lib/docker/k3s-server-data/
    BACKUP_NEEDED=true
    echo "Data migration complete"
    
    # Remove old directory
    sudo rm -rf /var/lib/rancher
else
    echo "No existing K3s server data found"
fi

# Create new rancher directory and bind mount to the Docker volume
echo "Setting up K3s server storage..."
sudo mkdir -p /var/lib/rancher

# Create bind mount for rancher to use space on the Docker volume
echo "Creating bind mount for K3s server data..."
sudo mount --bind /var/lib/docker/k3s-server-data /var/lib/rancher

# Add bind mount to fstab for persistence
echo "Adding bind mount to /etc/fstab..."
# Remove any existing entries first
sudo sed -i '\|/var/lib/rancher|d' /etc/fstab
echo '/var/lib/docker/k3s-server-data /var/lib/rancher none bind 0 0' | sudo tee -a /etc/fstab

# Reload systemd to recognize fstab changes
sudo systemctl daemon-reload

# Set proper permissions
sudo chown root:root /var/lib/rancher
sudo chmod 755 /var/lib/rancher

# Start K3s server
echo "Starting K3s server..."
sudo systemctl start k3s

# Wait for service to initialize
echo "Waiting for K3s server to start..."
sleep 20

# Verification
echo ""
echo "=== VERIFICATION ==="
echo ""

echo "1. Disk usage after setup:"
df -h | grep -E "(Filesystem|/dev/root|/dev/vda|/dev/vdb)" || df -h

echo ""
echo "2. Mount verification:"
mount | grep -E "(vdb|rancher)"

echo ""
echo "3. Storage locations:"
echo "   Docker data: $(df -h /var/lib/docker | tail -1)"
echo "   K3s data:    $(df -h /var/lib/rancher | tail -1)"

echo ""
echo "4. K3s server status:"
if sudo systemctl is-active --quiet k3s; then
    echo "✓ K3s server is running"
else
    echo "✗ K3s server is not running"
    echo "K3s server logs:"
    sudo journalctl -u k3s --no-pager -l | tail -10
fi

echo ""
echo "5. Cluster status:"
sleep 5
if command -v kubectl >/dev/null 2>&1; then
    echo "Checking cluster nodes..."
    kubectl get nodes 2>/dev/null || echo "kubectl not available yet"
else
    echo "kubectl not found, checking k3s kubectl..."
    sudo k3s kubectl get nodes 2>/dev/null || echo "Cluster not ready yet"
fi

echo ""
echo "6. Available space breakdown:"
echo "   Total volume usage: $(df -h /var/lib/docker | tail -1 | awk '{print $3 "/" $2 " (" $5 ")"}')"
echo "   Docker data: $(du -sh /var/lib/docker/overlay2 2>/dev/null | awk '{print $1}' || echo 'No data')"
echo "   K3s data: $(du -sh /var/lib/docker/k3s-server-data 2>/dev/null | awk '{print $1}' || echo 'No data')"

echo ""
if [ "$BACKUP_NEEDED" = true ]; then
    echo "📋 NOTE: Your original K3s data was backed up to:"
    ls -la /var/lib/docker/rancher-backup-* 2>/dev/null || echo "Backup directory not found"
fi

echo ""
echo "✅ SETUP COMPLETE!"
echo "Both Docker and K3s server now use the 70GB volume (/dev/vdb)"
echo "  - Docker images/containers: /var/lib/docker"  
echo "  - K3s server/containerd: /var/lib/rancher (bind mounted to Docker volume)"
echo ""
echo "🔍 Verify cluster with: kubectl get nodes"