# Running MNIST Federated Learning on TrustMesh

This guide provides complete instructions for deploying and running the MNIST federated learning application on TrustMesh with **minimal manual setup**.

## Overview

The MNIST federated learning application demonstrates distributed machine learning across 5 IoT nodes, where each node:
- Keeps its data local (privacy-preserving)
- Trains on 2 specific MNIST digit classes
- Contributes model parameters to global aggregation
- Receives updated global model for next round

**Node Data Distribution:**
- **iot-0**: Digits 0, 1 (Coordinator)
- **iot-1**: Digits 2, 3 
- **iot-2**: Digits 4, 5
- **iot-3**: Digits 6, 7
- **iot-4**: Digits 8, 9

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
NAME            STATUS   ROLES                  AGE   VERSION
network-management-console    Ready    control-plane,master   5m    v1.27.3+k3s1
compute-node-1  Ready    <none>                 4m    v1.27.3+k3s1
compute-node-2  Ready    <none>                 4m    v1.27.3+k3s1
compute-node-3  Ready    <none>                 4m    v1.27.3+k3s1
compute-node-4  Ready    <none>                 4m    v1.27.3+k3s1
iot-node-1      Ready    <none>                 3m    v1.27.3+k3s1
iot-node-2      Ready    <none>                 3m    v1.27.3+k3s1
iot-node-3      Ready    <none>                 3m    v1.27.3+k3s1
iot-node-4      Ready    <none>                 3m    v1.27.3+k3s1
iot-node-5      Ready    <none>                 3m    v1.27.3+k3s1
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

# Build all images including federated learning applications
chmod +x build-project.sh
./build-project.sh
```

This script now automatically:
- Builds all TrustMesh core components
- Builds MNIST federated learning applications
- Builds federated learning coordinator service
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

The script now automatically:
- Deploys all TrustMesh components
- **Automatically deploys the federated learning coordinator**
- Sets up Redis connectivity for coordination

### 3.4 Verify Deployment

```bash
kubectl get pods
```

Wait for all pods to be in `Running` state. You should see:
- All TrustMesh components running
- **federated-schedule-tp-xxx** pod running
- All IoT nodes ready

Check federated schedule TP specifically:
```bash
kubectl get pods -l app=federated-schedule-tp
kubectl logs -l app=federated-schedule-tp
```

Expected federated schedule TP log:
```
Starting Federated Schedule Transaction Processor
Validator URL: tcp://sawtooth-validator:4004
Redis: redis-0.redis-service:6379
Federated Schedule TP started successfully
```

## Step 4: Deploy MNIST Federated Learning Applications

### 4.1 Connect to Application Deployment Client

```bash
kubectl exec -it network-management-console-xxxxx -c application-deployment-client bash
```

### 4.2 Deploy the Three Applications

The build script created deployment files. Deploy them:

**Deploy Local Training Application:**
```bash
python docker_image_client.py deploy_image mnist-fl-local-training.tar.gz app_requirements.json
```
**Note the returned Application ID** (e.g., `abc123-local-training`)

**Deploy Model Aggregation Application:**
```bash  
python docker_image_client.py deploy_image mnist-fl-aggregate-models.tar.gz app_requirements.json
```
**Note the returned Application ID** (e.g., `def456-aggregate-models`)

**Deploy Model Evaluation Application:**
```bash
python docker_image_client.py deploy_image mnist-fl-model-evaluation.tar.gz app_requirements.json
```
**Note the returned Application ID** (e.g., `ghi789-model-evaluation`)

### 4.3 Create Workflow

Update the dependency graph with your actual Application IDs:

```bash
# Edit the dependency graph
nano dependency_graph.json
```

Update with your Application IDs:
```json
{
  "start": "abc123-local-training",
  "nodes": {
    "abc123-local-training": {"next": ["def456-aggregate-models"]},
    "def456-aggregate-models": {"next": ["ghi789-model-evaluation"]},
    "ghi789-model-evaluation": {"next": []}
  }
}
```

**Connect to Workflow Creation Client:**
```bash
kubectl exec -it network-management-console-xxxxx -c workflow-creation-client bash
```

**Create the Workflow:**
```bash
python workflow_creation_client.py dependency_graph.json
```
**Note the returned Workflow ID** (e.g., `mnist-fl-workflow-xyz789`)

## Step 5: Run MNIST Federated Learning

### 5.1 Execute on All IoT Nodes Simultaneously

**Important**: All 5 IoT nodes must start within a few minutes of each other for proper coordination.

Open 5 terminal windows and run simultaneously:

**Terminal 1 - iot-0 (Coordinator):**
```bash
kubectl exec -it iot-0-xxxxx -c iot-node bash
python mnist-federated-learning-simulation.py mnist-fl-workflow-xyz789 --node-id iot-0 --max-rounds 5
```

**Terminal 2 - iot-1 (Participant):**
```bash
kubectl exec -it iot-1-xxxxx -c iot-node bash
python mnist-federated-learning-simulation.py mnist-fl-workflow-xyz789 --node-id iot-1 --max-rounds 5
```

**Terminal 3 - iot-2 (Participant):**
```bash
kubectl exec -it iot-2-xxxxx -c iot-node bash
python mnist-federated-learning-simulation.py mnist-fl-workflow-xyz789 --node-id iot-2 --max-rounds 5
```

**Terminal 4 - iot-3 (Participant):**
```bash
kubectl exec -it iot-3-xxxxx -c iot-node bash
python mnist-federated-learning-simulation.py mnist-fl-workflow-xyz789 --node-id iot-3 --max-rounds 5
```

**Terminal 5 - iot-4 (Participant):**
```bash
kubectl exec -it iot-4-xxxxx -c iot-node bash
python mnist-federated-learning-simulation.py mnist-fl-workflow-xyz789 --node-id iot-4 --max-rounds 5
```

**Alternative - Auto-detect node IDs:**
```bash
# On each IoT node, this will auto-detect the node ID
python mnist-federated-learning-simulation.py mnist-fl-workflow-xyz789 --auto-detect --max-rounds 5
```

## Step 6: Monitor Federated Learning Progress

### 6.1 Monitor Federated Schedule TP

```bash
kubectl logs -l app=federated-schedule-tp -f
```

You should see:
```
Processing federated schedule request for workflow mnist-fl-workflow-xyz789
Elected coordinator: compute-node-2 (highest resource score)
Created federated round mnist-fl-workflow-xyz789:1 with schedule_id abc-123
Node iot-0 joined federated round. 1/5 nodes submitted
Node iot-1 joined federated round. 2/5 nodes submitted
...
✓ Federated round ready with 5/5 nodes
```

### 6.2 Monitor Workflow Execution

```bash
kubectl logs pbft-0 -c task-executor -f
```

### 6.3 Monitor Individual Node Progress

```bash
kubectl logs iot-0-xxxxx -c iot-node -f
```

## Step 7: View Results

After 5 rounds, you should see output like:

```
🎉 MNIST Federated Learning Results 🎉

Round 5 Evaluation:
✅ Test Accuracy: 96.4%
📊 Performance Grade: A+ (Excellent)
🔒 Privacy Preserved: ✓ Raw data never left individual nodes
👥 Nodes Participated: 5/5
📈 Total Training Samples: 60,000 (distributed across nodes)
⏱️ Total Training Time: 12.3 minutes

Federated Learning Benefits:
✓ Data diversity: Trained on 5 different data distributions
✓ Sample efficiency: Leveraged 60,000 samples across nodes
✓ Privacy preservation: Raw data never left individual nodes
✓ Distributed computation: Training load distributed across devices
✓ Model robustness: Model exposed to diverse local data patterns

Next Round Suggestions:
• Model converging well - consider reducing learning rate
• Monitor for overfitting on local data distributions
• Consider implementing differential privacy
```

## Architecture Overview

### Consensus-Integrated Federated Learning Components

1. **Federated Schedule Transaction Processor** (Kubernetes pod)
   - Runs automatically on the cluster
   - Uses blockchain consensus to elect coordinators
   - Integrates with TrustMesh's existing scheduling architecture
   - Manages federated round coordination through consensus

2. **Enhanced Transaction Processors**
   - `scheduling-request-tp`: Detects federated workflows and manages shared schedule IDs
   - `iot-data-tp`: Supports multi-node data aggregation using Redis lists
   - `federated-schedule-tp`: Manages consensus-based coordinator election

3. **Enhanced Compute Nodes**
   - `task_executor`: Federated-aware data retrieval and processing
   - Auto-detects federated workflows and aggregates multi-node data
   - Respects TrustMesh's resource allocation and consensus principles

4. **MNIST FL Applications**
   - task1_local_training: Trains CNN models locally
   - task2_aggregate_models: Implements FedAvg algorithm with consensus coordination
   - task3_model_evaluation: Evaluates global model

### Consensus-Integrated Data Flow

```
IoT Nodes (iot-0 to iot-4) → Federated Schedule TP → Consensus Election → Coordinator Node
    ↓
Shared Schedule ID → Multi-Node Data Aggregation → Enhanced IoT Data TP
    ↓
Local Training (parallel) → Federated Task Executor → Model Parameters → Blockchain
    ↓  
Consensus-Coordinated Aggregation → Global Model → Evaluation → Results
```

### Key Improvements Over Standalone Coordinator

✅ **Consensus-Based Coordination**: Uses TrustMesh's PBFT consensus for coordinator election
✅ **Resource-Aware Scheduling**: Integrates with existing LCDWRR scheduling algorithm  
✅ **Blockchain Validation**: All coordination decisions validated through blockchain consensus
✅ **No Single Point of Failure**: Coordinator election distributes load across compute nodes
✅ **TrustMesh Compliance**: Fully integrates with existing transaction processor architecture

## Troubleshooting

### Common Issues

**1. Federated Schedule TP not starting**
```bash
kubectl describe pod -l app=federated-schedule-tp
kubectl logs -l app=federated-schedule-tp
```

**2. Nodes can't join federated round**
- Ensure all nodes start within 5 minutes
- Check federated schedule TP logs for round creation
- Verify Redis connectivity and blockchain consensus

**3. Low model accuracy**
- Increase training epochs in task1_local_training/process.py
- Ensure all 5 nodes are participating
- Check class distribution per node

**4. Workflow not executing**
- Verify application IDs in dependency graph
- Check task executor logs on compute nodes
- Ensure minimum node participation (3/5)

### Performance Tuning

**For better accuracy:**
- Increase epochs from 5 to 10
- Adjust learning rate (default: 0.01)
- Add data augmentation

**For faster training:**
- Reduce batch size for memory-constrained nodes
- Decrease validation split in training

## Clean Up

```bash
# Clean up applications and workflows
kubectl exec -it network-management-console-xxxxx -c application-deployment-client bash
python docker_image_client.py list_images
python docker_image_client.py delete_image app-id

# Clean entire cluster
chmod +x clean-k8s-environment.sh
./clean-k8s-environment.sh
```

## Key Improvements in This Implementation

✅ **Consensus-Integrated Architecture**: Federated learning now fully integrates with TrustMesh's consensus mechanisms

✅ **No Architectural Violations**: Respects TrustMesh's task delegation and resource allocation principles

✅ **Blockchain-Validated Coordination**: All federated round decisions validated through PBFT consensus

✅ **Automatic Transaction Processor Deployment**: `build-and-deploy-network.sh` deploys federated-schedule-tp automatically

✅ **Resource-Aware Coordinator Election**: Uses existing LCDWRR algorithm for optimal coordinator selection

✅ **Enhanced Data Aggregation**: Multi-node data collection through enhanced iot-data-tp

✅ **Fault Tolerant**: No single point of failure - coordinator election distributes responsibility

## Next Steps

1. **Advanced Features**: Implement differential privacy, compression
2. **Scaling**: Test with more nodes and different data distributions  
3. **Custom Models**: Experiment with different neural network architectures
4. **Production**: Add monitoring, alerting, and fault tolerance

---

**Congratulations!** You have successfully deployed and run privacy-preserving federated learning on TrustMesh with minimal manual setup. The system demonstrates how multiple IoT devices can collaboratively train a machine learning model while keeping their data completely private and secure.