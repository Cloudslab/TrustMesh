# TrustMesh Installation Guide

This guide provides step-by-step instructions for installing and testing TrustMesh.

## System Requirements

### Minimum Cluster Configuration

A minimum of 6 nodes are required to deploy the framework. These may be virtual machines or physical devices.

- 1 Control Node: 4 CPU cores, 16GB RAM (recommended)
- 1 IoT Node: 1 CPU core, 4GB RAM
- 4 Compute Nodes: 1 CPU core, 4GB RAM each

### Software Prerequisites
- Ubuntu (recommended), macOS, or Windows
- Docker
- K3S Kubernetes Cluster
- Python 3.8

## Installation Steps

### 1. Clone Repository
```bash
git clone https://github.com/murtazahr/TrustMesh.git
cd trustmesh
```

### 2. Set Up K3S Cluster
1. Navigate to cluster setup guide:
   ```bash
   cd k3s-cluster-setup-guide
   ```
2. Follow instructions in `K3S_CLUSTER_SETUP.md` for:
    - Cluster initialization
    - Node configuration
    - Network setup

### 3. Deploy Framework
Perform all operations from this step onwards on the server node of the k3s cluster.

#### Option 1: Using Pre-built Images (Recommended for Quick Start)
1. Skip the build step (By default, the deployment script will pull images from the author's docker registry)
2. Deploy the network:
   ```bash
   chmod +x build-and-deploy-network.sh
   ./build-and-deploy-network.sh
   ```
   When prompted:
    - Specify 4 compute nodes
    - Specify 1 IoT node

> **Note:** The script may take a few minutes to complete the deployment.

#### Option 2: Building Custom Images
1. Update DOCKER_USERNAME in build-project.sh with your username
2. Login to Docker:
   ```bash
   docker login
   ```
3. Build images:
   ```bash
   chmod +x build-project.sh
   ./build-project.sh
   ```
4. Update the image address for images in the `build-and-deploy-network.sh` script
4. Follow deployment steps from Option 1

### 4. Verify Installation

1. Check pod status:
   ```bash
   kubectl get pods
   ```
   All pods should show "Running" status within 2-3 minutes.


## Basic Usage Example

1. Deploy the cold-chain use-case applications:
   ```bash
   kubectl exec -it network-management-console-xxxxx -c application-deployment-client bash
   python docker_image_client.py deploy_image process-sensor-data.tar app_requirements.json
   python docker_image_client.py deploy_image anomaly-detection.tar app_requirements.json
   python docker_image_client.py deploy_image generate-alerts.tar app_requirements.json
   ```

* A sample `app_requirements.json` is provided in the `sample-apps/sample-jsons` directory. You may use that.
* After each application's deployment, a unique application ID will be printed to the console. Save that for the later steps.

2. Create test workflow:
   ```bash
   kubectl exec -it network-management-console-xxxxx -c workflow-creation-client bash
   python workflow_creation_client.py test_graph.json
   ```
   
* The `test_graph.json` file specifies the workflow as a DAG where each node is an application and edges represent the flow of data or order of execution. For the cold-chain usecase, your json should be as follows:

```json
{
   "start": "Process Sensor Data ID",
   "nodes": {
      "Process Sensor Data ID": {"next": ["Detect Anomalies ID"]},
      "Detect Anomalies ID": {"next": ["Generate Alerts ID"]},
      "Generate Alerts ID": {"next": []}
   }
}
```
* A workflow ID will be printed to the console on successful creation of workflow. Record that for the next step.

3. Initiate Data Processing Requests from IoT node:
   ```bash
   kubectl exec -it iot-0-xxxxx -c iot-node bash
   python cold-chain-data-simulation.py <workflowID>
   ```

Expected output:
* You should see data processing requests being sent to the network.
* In 3-5 seconds you should see the result of the processed data being printed to the console.

## Clean-up

To delete all network components and clean-up the environment:
```bash
chmod +x clean-k8s-environment.sh && ./clean-k8s-environment.sh
```

## Troubleshooting

If pods aren't running:
1. Check node status:
   ```bash
   kubectl get nodes
   ```
2. Verify resource availability:
   ```bash
   kubectl describe nodes
   ```
3. Check pod logs:
   ```bash
   kubectl logs <pod-name>
   ```

For additional support, including technical issues, contact mrangwala@student.unimelb.edu.au