# Running MNIST Federated Learning on TrustMesh

This guide provides complete instructions for deploying and running the MNIST federated learning application on TrustMesh with **time-windowed aggregation** and **local validation**.

## Overview

The MNIST federated learning application demonstrates distributed machine learning across 5 IoT nodes using a **two-phase architecture**:

### **Phase 1: Local Training**
- Each IoT node trains on its local MNIST partition (80% train, 20% test)
- Submits training data → TrustMesh processes as standard workflow
- Receives locally trained model weights

### **Phase 2: Federated Aggregation** 
- IoT nodes submit trained weights → `aggregation-request-tp`
- Time-windowed collection (3-minute timeout or minimum 3 nodes)
- FedAvg aggregation with blockchain consensus validation
- Global model broadcast back to participating nodes via ZMQ

**Node Data Distribution:**
- **iot-0**: Digits 0, 1 
- **iot-1**: Digits 2, 3 
- **iot-2**: Digits 4, 5
- **iot-3**: Digits 6, 7
- **iot-4**: Digits 8, 9

Each node splits its data: **80% training, 20% local test** for convergence detection.

## Key Features

✅ **Time-Windowed Aggregation**: Nodes participate independently, aggregation happens with whoever submits within 3-minute windows

✅ **Local Validation**: Convergence detection based on each node's local test data (not blockchain validation)

✅ **Blockchain Consensus**: Deterministic validation using shared MNIST validation dataset for model integrity

✅ **Privacy Preserving**: Training data never leaves nodes, only model weights are shared

✅ **Offline-Ready**: MNIST dataset pre-downloaded in Docker images - no internet access required at runtime

## Prerequisites

### System Requirements

**Minimum Cluster Configuration:**
- **Network Management Console Node**: 4 CPU cores, 16GB RAM
- **Compute Nodes**: 1 CPU core, 4GB RAM each (minimum 4 nodes)
- **IoT Nodes**: 1 CPU core, 4GB RAM each (5 nodes required)

### Software Requirements
- Docker
- K3s Kubernetes Cluster
- Python 3.8+
- Git

## Step 1: Clone and Setup Repository

```bash
git clone https://github.com/murtazahr/TrustMesh.git
cd TrustMesh
```

## Step 2: Set Up K3s Cluster

### 2.1 Server Node Setup

On your designated server/control node:

```bash
cd k3s-cluster-setup-guide
chmod +x setup-k3s-server.sh
./setup-k3s-server.sh
```

Get the server token for agent nodes:
```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

### 2.2 Compute Node Setup

On each compute node (compute-node-1, compute-node-2, compute-node-3, compute-node-4):

```bash
cd k3s-cluster-setup-guide
chmod +x setup-k3s-agent.sh

# Export server details
export K3S_URL=https://[SERVER_IP]:6443
export K3S_TOKEN=[TOKEN_FROM_SERVER]

./setup-k3s-agent.sh
```

### 2.3 IoT Nodes Setup

Set up 5 IoT nodes with hostnames:
- `iot-node-1` (becomes iot-0 in Kubernetes)
- `iot-node-2` (becomes iot-1 in Kubernetes) 
- `iot-node-3` (becomes iot-2 in Kubernetes)
- `iot-node-4` (becomes iot-3 in Kubernetes)
- `iot-node-5` (becomes iot-4 in Kubernetes)

On each IoT node:
```bash
cd k3s-cluster-setup-guide
chmod +x setup-k3s-agent.sh

export K3S_URL=https://[SERVER_IP]:6443
export K3S_TOKEN=[TOKEN_FROM_SERVER]

./setup-k3s-agent.sh
```

### 2.4 Verify Cluster Setup

```bash
kubectl get nodes
```

Expected output showing all nodes in Ready state:
```
NAME                           STATUS   ROLES                  AGE   VERSION
network-management-console     Ready    control-plane,master   5m    v1.27.3+k3s1
compute-node-1                 Ready    <none>                 4m    v1.27.3+k3s1
compute-node-2                 Ready    <none>                 4m    v1.27.3+k3s1
compute-node-3                 Ready    <none>                 4m    v1.27.3+k3s1
compute-node-4                 Ready    <none>                 4m    v1.27.3+k3s1
iot-node-1                     Ready    <none>                 3m    v1.27.3+k3s1
iot-node-2                     Ready    <none>                 3m    v1.27.3+k3s1
iot-node-3                     Ready    <none>                 3m    v1.27.3+k3s1
iot-node-4                     Ready    <none>                 3m    v1.27.3+k3s1
iot-node-5                     Ready    <none>                 3m    v1.27.3+k3s1
```

## Step 3: Build and Deploy TrustMesh with Federated Learning

### 3.1 Update Docker Username

```bash
# Edit build-project.sh
nano build-project.sh

# Change line 6:
DOCKER_USERNAME=your_dockerhub_username
```

### 3.2 Build All Images (Including Federated Learning)

```bash
# Login to Docker Hub
docker login

# Build all images including federated learning TPs
chmod +x build-project.sh
./build-project.sh
```

The build script automatically:
- Builds all TrustMesh core components
- **Builds new federated learning TPs:**
  - `aggregation-request-tp` (collects model weights)
  - `aggregation-confirmation-tp` (performs FedAvg + validation)
  - `validation-dataset-distributor` (distributes MNIST validation data)
- Builds single `federated-training-task` application
- Pushes everything to Docker registry

### 3.3 Deploy TrustMesh Network with Federated Learning

```bash
chmod +x build-and-deploy-network.sh
./build-and-deploy-network.sh
```

Follow the prompts for:
- Number of compute nodes (minimum 4)
- Redis cluster configuration
- CouchDB setup
- SSL certificate generation

The deployment script automatically:
- Deploys all TrustMesh components
- **Deploys new federated learning TPs in each compute node**
- **Runs MNIST validation dataset distribution job**
- Sets up ZMQ communication for IoT ↔ Compute
- Configures Redis pub/sub for inter-compute coordination

### 3.4 Verify Deployment

```bash
kubectl get pods
```

Wait for all pods to be in `Running` state. You should see:
- All TrustMesh components running
- All compute nodes (pbft-0, pbft-1, pbft-2, pbft-3) with **aggregation TPs**
- All IoT nodes ready
- **MNIST validation dataset distribution job completed**

Check new federated learning components:
```bash
# Check aggregation TPs in compute nodes
kubectl logs pbft-0 -c aggregation-request-tp
kubectl logs pbft-0 -c aggregation-confirmation-tp

# Check validation dataset distribution
kubectl get jobs
kubectl logs job/mnist-validation-dataset-distributor
```

Expected logs:
```bash
# Aggregation Request TP:
Starting Aggregation Request Transaction Processor
Validator URL: tcp://sawtooth-validator:4004
Redis: redis-0.redis-service:6379
Aggregation Request TP started successfully

# Validation Dataset Distributor:
Successfully distributed validation dataset to Redis
Dataset key: mnist_validation_dataset
Total samples: 1000
Data integrity hash: a1b2c3d4...
```

## Step 4: Deploy MNIST Federated Learning Application

### 4.1 Access Network Management Console

```bash
kubectl exec -it deployment/network-management-console -c application-deployment-client -- bash
```

### 4.2 Deploy Single Training Application

**Important**: We now use a **single training task** instead of 3 separate tasks:

```bash
# Deploy the federated training application
python docker_image_client.py deploy_image federated-training-task.tar.gz app_requirements.json

# Wait for deployment confirmation
echo "Application deployed successfully"
```

### 4.3 Create Federated Learning Workflow

```bash
# Still in the network-management-console container
kubectl exec -it deployment/network-management-console -c workflow-creation-client -- bash

# Create the federated workflow
python workflow_creation_client.py federated_dependency_graph.json

# Note the returned workflow ID (e.g., workflow_12345)
```

The `federated_dependency_graph.json` now contains:
- **Single task**: `federated-training-task` 
- **Federated config**: 5 nodes, 3 minimum, FedAvg strategy
- **Two-phase architecture metadata**

## Step 5: Run Federated Learning

### 5.1 Start IoT Nodes (Each Node Independently)

On each IoT node, run the federated learning simulation. The script now **auto-detects the node ID** from the hostname by default and uses consistent parameter formatting:

**IoT Node 0 (iot-node-1):**
```bash
kubectl exec -it iot-0-xxxxx -- bash
cd /app
python mnist-federated-learning-simulation.py --workflow-id workflow_12345 --max-rounds 5
```

**IoT Node 1 (iot-node-2):**
```bash
kubectl exec -it iot-1-xxxxx -- bash
cd /app
python mnist-federated-learning-simulation.py --workflow-id workflow_12345 --max-rounds 5
```

**Continue for nodes iot-2, iot-3, iot-4...**

> **Note**: The script automatically detects the node ID from the hostname (iot-0, iot-1, etc.). You can override this with `--node-id` if needed:
> ```bash
> python mnist-federated-learning-simulation.py --workflow-id workflow_12345 --max-rounds 5 --node-id custom-node-name
> ```

### 5.2 Monitor Federated Learning Progress

Each node will show:

```bash
================================================================================
🤖 MNIST FEDERATED LEARNING NODE
================================================================================
🏷️  Node ID: iot-0
🔄 Workflow ID: workflow_12345
🔢 Max Rounds: 5
🔌 Federated Extension: Available
🕰️ Start Time: 2024-01-15 14:30:25
================================================================================

################################################################################
🎆 FEDERATED LEARNING SESSION STARTED
################################################################################
📱 Node Information:
   • Node ID: iot-0
   • Node Index: 0
   • Assigned Classes: [0, 1]
   • Training Samples: 800
   • Test Samples: 200
🔄 Session Configuration:
   • Workflow ID: workflow_12345
   • Maximum Rounds: 5
   • Federated Extension: Available
   • Start Time: 2024-01-15T14:30:25.123456
################################################################################

================================================================================
🔥 FEDERATED LEARNING ROUND 1/5 STARTED
================================================================================
🟦 PHASE 1: TRAINING PHASE
   • Objective: Submit training data to TrustMesh for processing
   • Expected outcome: Receive trained model weights

✅ TRAINING PHASE COMPLETED SUCCESSFULLY
   • Schedule ID: schedule_abc123
   • Training samples: 500
   • Assigned classes: [0, 1]
   • Total phase duration: 45.2s

🟨 PHASE 2: AGGREGATION PHASE
   • Objective: Submit trained weights for global aggregation
   • Expected outcome: Contribute to FedAvg aggregation process

✅ AGGREGATION SUBMISSION SUCCESSFUL
   • Weights submitted to aggregation-request-tp
   • Now waiting for global model aggregation...

⏳ WAITING FOR GLOBAL MODEL AGGREGATION
✅ GLOBAL MODEL RECEIVED SUCCESSFULLY
   • Aggregation wait duration: 180.1s
   • Aggregated weights: 8 layers
   • Model ready for local validation

📊 LOCAL VALIDATION PHASE
✅ LOCAL VALIDATION COMPLETED
   • Validation duration: 2.34s
   • Local accuracy: 0.8450 (84.50%)
   • Per-class accuracies:
     - class_0: 0.8200 (82.00%)
     - class_1: 0.8700 (87.00%)
...
```

### 5.3 Enhanced Logging Features

The updated script provides **comprehensive logging** with visual indicators and structured information:

**🎯 Event-Driven Logging**: Every major event is logged with timestamps, durations, and detailed context
- Training phase submission and completion
- Aggregation phase with weight analysis  
- Global model reception with participation details
- Local validation with per-class accuracy breakdowns

**📊 Performance Monitoring**: Detailed timing and performance metrics
- Phase durations and wait times
- Model parameter counts and layer information
- Network communication latency
- Training data distribution analysis

**🚨 Error Handling**: Clear error messages with troubleshooting context
- Timeout scenarios with possible causes
- Missing data or connection issues
- Validation failures with specific reasons

### 5.4 Time-Windowed Aggregation in Action

You'll observe:
- **Independent participation**: Nodes join rounds independently
- **3-minute time windows**: Aggregation happens with whoever submits in time
- **Selective participation**: Only nodes in each window get the global model
- **Local convergence**: Nodes stop based on their own test data performance

Example scenario:
- **Round 1**: iot-0, iot-1, iot-2 submit → Aggregation with 3 nodes
- **Round 2**: iot-0, iot-3, iot-4 submit → Aggregation with different 3 nodes  
- **Round 3**: iot-1 converged locally → Only iot-0, iot-2, iot-3, iot-4 continue

## Step 6: Script Parameters and Options

### 6.1 Available Parameters

The MNIST federated learning script supports the following parameters:

```bash
python mnist-federated-learning-simulation.py [OPTIONS]

Required:
  --workflow-id TEXT    Workflow ID for the federated learning experiment

Optional:
  --max-rounds INTEGER  Maximum number of federated rounds (default: 5)
  --node-id TEXT       Node ID override (by default auto-detects from hostname)
  --help               Show help message and exit
```

### 6.2 Auto-Detection Features

**Hostname Detection**: The script automatically detects the node ID from the Kubernetes pod hostname:
- `iot-0-xxxxx` → `iot-0`
- `iot-1-xxxxx` → `iot-1`
- etc.

**Environment Variables**: Falls back to checking `IOT_NODE_ID` environment variable if hostname detection fails.

**Manual Override**: Use `--node-id` parameter to specify a custom node identifier if needed.

### 6.3 Usage Examples

**Standard usage (recommended):**
```bash
python mnist-federated-learning-simulation.py --workflow-id workflow_12345 --max-rounds 5
```

**Custom node ID:**
```bash
python mnist-federated-learning-simulation.py --workflow-id workflow_12345 --node-id custom-node --max-rounds 3
```

**Quick test run:**
```bash
python mnist-federated-learning-simulation.py --workflow-id workflow_12345 --max-rounds 2
```

## Step 7: Monitor and Troubleshoot

### 7.1 Monitor Aggregation Process

```bash
# Check aggregation requests in blockchain
kubectl logs pbft-0 -c aggregation-request-tp -f

# Check FedAvg aggregation and validation
kubectl logs pbft-0 -c aggregation-confirmation-tp -f

# Monitor Redis coordination
kubectl exec -it redis-0 -- redis-cli monitor
```

### 7.2 Check Convergence Status

Each node logs its convergence independently:
```bash
📈 Final Convergence Status:
   • rounds_without_improvement: 3
   • best_accuracy: 0.892
   • current_accuracy: 0.892
   • should_continue: False

🏁 CONVERGENCE DETECTED AFTER ROUND 4
   • Based on local validation performance
   • Stopping federated learning session
```

### 7.3 Verify Blockchain Consensus

The blockchain performs deterministic validation:
```bash
kubectl logs pbft-0 -c aggregation-confirmation-tp | grep "MNIST validation"

# Expected output:
# MNIST validation - Accuracy: 0.8934, Loss: 0.3245, Passed: true
# Blockchain consensus validation score: 0.893
```

### 7.4 Common Issues and Solutions

**Issue**: MNIST dataset download fails with "Temporary failure in name resolution"
```bash
# This occurs when pods don't have internet access to download datasets
# Solution: The Docker images now pre-download MNIST during build
# If you encounter this error, rebuild the images:
cd TrustMesh
./build-project.sh  # This rebuilds all images with pre-downloaded MNIST
./build-and-deploy-network.sh  # Redeploy with updated images
```

**Issue**: Validation dataset not found
```bash
# Solution: Check if validation job completed
kubectl get jobs
kubectl logs job/mnist-validation-dataset-distributor

# If the job failed due to network issues, it's likely the same MNIST download problem
# The updated validation-dataset-distributor image also pre-downloads MNIST
```

**Issue**: Aggregation timeout
```bash
# Solution: Check if enough nodes are participating
# Minimum 3 nodes required within 3-minute window
```

**Issue**: ZMQ communication errors
```bash
# Solution: Verify IoT node network connectivity
kubectl exec -it iot-0-xxxxx -- netstat -tulpn | grep :5555
```

## Step 8: Results and Analysis

### 8.1 Expected Outcomes

**Successful federated learning will show:**
- Each node trains on its specific digit classes (0-1, 2-3, 4-5, 6-7, 8-9)
- Global model learns all 10 digits through federated aggregation
- Local accuracy improves over rounds on each node's test set
- Convergence detection stops nodes independently after 3 rounds without improvement

**Typical accuracy progression:**
```
Round 1: Local=0.72, Blockchain=0.65
Round 2: Local=0.84, Blockchain=0.79  
Round 3: Local=0.89, Blockchain=0.87
Round 4: Local=0.89, Blockchain=0.89 (no improvement)
Round 5: Convergence detected - stopping
```

### 8.2 Performance Characteristics

- **Time per round**: ~2-3 minutes (including 3-minute aggregation window)
- **Network overhead**: Only model weights transmitted (not data)
- **Convergence**: Typically 3-5 rounds for MNIST
- **Fault tolerance**: System continues with partial participation

## Cleanup

To clean up the entire deployment:

```bash
chmod +x clean-k8s-environment.sh
./clean-k8s-environment.sh
```

This removes:
- All TrustMesh components
- Federated learning TPs and jobs
- MNIST validation dataset
- All secrets and persistent volumes

## Architecture Summary

The updated TrustMesh federated learning implementation features:

1. **Two-Phase Architecture**: Clean separation of training and aggregation phases
2. **Time-Windowed Aggregation**: 3-minute collection windows for robust participation
3. **Local Validation**: Convergence based on each node's private test data
4. **Blockchain Consensus**: Deterministic validation for model integrity
5. **Privacy Preservation**: Only model weights leave nodes, never raw data
6. **Fault Tolerance**: System adapts to varying node participation

This design provides a production-ready federated learning framework with strong privacy guarantees and robust consensus mechanisms.