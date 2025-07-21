# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TrustMesh is a blockchain-enabled distributed computing framework for trustless heterogeneous IoT environments. It implements a three-layer architecture combining permissioned blockchain (Hyperledger Sawtooth) with multi-phase PBFT consensus for secure task scheduling and execution.

## Architecture

The codebase follows a three-layer architecture:

### Network Management Layer
- `auto-docker-deployment/` - Application deployment infrastructure
- `manage-dependency-workflow/` - Workflow orchestration and dependency management

### Computation Layer  
- `compute-node/` - Docker-in-Docker compute nodes with blockchain consensus
- `scheduling/` - Task scheduling with multiple transaction processors (TPs)
- `peer-registry/` - Node registration and discovery

### Perception Layer
- `iot-node/` - IoT device simulation and blockchain interaction
- `sample-apps/` - Demo applications including cold-chain monitoring

## Development Commands

### Build and Deploy
```bash
# Build all Docker images (update DOCKER_USERNAME in script first)
chmod +x build-project.sh && ./build-project.sh

# Deploy entire network to K3s cluster
chmod +x build-and-deploy-network.sh && ./build-and-deploy-network.sh

# Clean up environment
chmod +x clean-k8s-environment.sh && ./clean-k8s-environment.sh
```

### Cluster Setup
```bash
# Set up K3s cluster (see k3s-cluster-setup-guide/)
cd k3s-cluster-setup-guide
chmod +x setup-k3s-server.sh && ./setup-k3s-server.sh
chmod +x setup-k3s-agent.sh && ./setup-k3s-agent.sh
```

### Verification
```bash
# Check pod status (all should be Running)
kubectl get pods

# Monitor system status
kubectl describe pods
```

## Technology Stack

- **Blockchain**: Hyperledger Sawtooth with PBFT consensus
- **Containerization**: Docker + Kubernetes (K3s)
- **Language**: Python 3.8
- **Database**: CouchDB cluster for distributed storage
- **Message Queue**: Redis cluster for coordination
- **Security**: ZMQ CURVE authentication, SSL/TLS certificates

## Key Components

### Transaction Processors (TPs)
Each component has its own TP for blockchain operations:
- `peer-registry-tp` - Node registration
- `docker-image-tp` - Application deployment  
- `dependency-management-tp` - Workflow management
- `scheduling-request-tp` - Task scheduling
- `schedule-confirmation-tp` - Schedule validation
- `status-update-tp` - Task status updates
- `iot-data-tp` - IoT data processing

### Application Deployment Workflow
1. Build and compress application with `build-project.sh`
2. Deploy using application deployment client:
   ```bash
   kubectl exec -it network-management-console-xxxxx -c application-deployment-client bash
   python docker_image_client.py deploy_image application.tar app_requirements.json
   ```
3. Create workflow with DAG specification in `graph.json`
4. Initiate processing from IoT nodes

## System Requirements

- **Compute Nodes**: Min 1 CPU core, 4GB RAM
- **Control Nodes**: Min 1 CPU core, 8GB RAM (Recommended: 8 cores, 32GB RAM)  
- **IoT Nodes**: Min 1 CPU core, 4GB RAM
- **Minimum viable cluster**: 6 nodes (1 control + 1 IoT + 4 compute)

## Development Notes

- Each component has its own `requirements.txt` and Dockerfile
- No centralized test framework - testing is component-specific
- Update `DOCKER_USERNAME` in build scripts before building
- All inter-node communication uses SSL/TLS and ZMQ CURVE authentication
- Performance: Framework overhead is 3.25-4.19 seconds for typical workflows