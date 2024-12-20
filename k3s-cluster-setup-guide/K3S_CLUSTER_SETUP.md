# K3S Cluster Setup Guide

## 1. K3S Server Setup

1. Locate the `setup-k3s-server.sh` script in the current directory.

2. Grant execution permission to the script:
   ```bash
   sudo chmod +x ./setup-k3s-server.sh
   ```

3. Execute the script:
   ```bash
   ./setup-k3s-server.sh
   ```
   
4. Apply the changes to your current session:
   ```bash
   source ~/.bashrc
   ```
   
5. Get the `K3S_TOKEN`:
   ```bash
   sudo cat /var/lib/rancher/k3s/server/node-token
   ```
6. Get the `K3S_URL`:
   ```bash
   echo https://$(kubectl get nodes -o jsonpath='{range .items[*]}{@.metadata.name}{"\t"}{@.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' | grep "client-console" | awk '{print $2}'):6443
   ```
   **Important:** Take note of the `K3S_TOKEN` and `K3S_URL` output. You'll need these for agent setup.

## 2. K3S Agent Setup

1. Locate the `setup-k3s-agent.sh` script in the current directory.
2. Update the following environment variables in the script:

   ```bash
   export K3S_TOKEN=<Token from server setup>
   export K3S_URL=<URL from server setup>
   export K3S_NODE_NAME=<Node name>
   ```

   **Naming Convention:**
    - Compute nodes: `compute-node-1`, `compute-node-2`, etc.
    - IoT nodes: `iot-node-1`, `iot-node-2`, etc.

   > **Note:** Ensure you follow the naming convention in a serialized manner.

3. Grant execution permission to the script:
   ```bash
   sudo chmod +x ./setup-k3s-agent.sh
   ```

4. Execute the script:
   ```bash
   ./setup-k3s-agent.sh
   ```

5. Verify the node addition on the server:
   ```bash
   kubectl get nodes
   ```

## Troubleshooting

If you encounter any issues during setup, please refer to the K3S documentation or contact your system administrator.