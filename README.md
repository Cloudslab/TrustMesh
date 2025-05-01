# TrustMesh

TrustMesh is a blockchain-enabled distributed computing framework designed for trustless heterogeneous IoT environments. It implements a novel three-layer architecture combining permissioned blockchain technology with a multi-phase Practical Byzantine Fault Tolerance (PBFT) consensus based protocol for scheduling decisions.

## Key Features

- **Three-Layer Architecture**:
   - Network Management Layer for system setup and application management
   - Computation Layer with blockchain-based consensus
   - Perception Layer for IoT device integration

- **Byzantine Fault Tolerance**: Maintains security and consensus while supporting non-deterministic scheduling algorithms

- **Heterogeneous Environment Support**: Designed to work across varied IoT devices and computing resources

- **Secure Workflow Management**: Implements complex workflow pipelines with dependency management

- **Efficient Resource Allocation**: Uses Least-Connected Dynamic Weighted Round Robin (LCDWRR) scheduling

## Pre-requisites

The framework has been extensively tested on Ubuntu machines but should support macOS and Windows. For the development environment, you'll need:
* Docker
* K3S Kubernetes Cluster (Instructions to Set-up provided below)
* Python 3.8

### System Requirements

Based on our experimental evaluation:
- Computation Nodes: Minimum 1 CPU core, 4GB RAM
- Control Nodes: Minimum 1 CPU core, 8GB RAM | Recommended 8 CPU cores, 32GB RAM
- IoT Nodes: Minimum 1 CPU core, 4GB RAM

## Installation

### 1. Clone the Repository

First, clone the TrustMesh repository and navigate to the project directory:

```bash
git clone https://github.com/murtazahr/TrustMesh.git
cd trustmesh
```

### 2. Set Up K3S Cluster

If you don't have a K3S cluster already configured:

1. Navigate to the cluster setup guide:
```bash
cd k3s-cluster-setup-guide
```

2. Follow the detailed instructions in `K3S_CLUSTER_SETUP.md` for:
   - Cluster initialization
   - Node configuration
   - Network setup
   - Required naming conventions


### 3. Build and Deploy

Once your K3S cluster is ready, follow these steps:

#### 3.1 Build Project Images

This step is required for first-time setup or when code changes are made.

**Prerequisites:**
- Update `DOCKER_USERNAME` in `build-project.sh` with your DockerHub username
- Login to Docker CLI using `docker login`
- Review and customize applications to be deployed to the network in `build-project.sh`

Run the build script:
```bash
chmod +x build-project.sh && ./build-project.sh
```

> **Note:** The script builds and compresses all application images for network deployment. Remove the cold-chain boilerplate applications if you don't need them.

#### 3.2 Deploy the Network

**Prerequisites:**
- Update the image urls for the containers based on your `DOCKER_USERNAME`

After successful image building, deploy the network:

```bash
chmod +x build-and-deploy-network.sh && ./build-and-deploy-network.sh
```

> **Important:** The deployment script will prompt for several configuration options. Have your network configuration details ready.

### 4. Verify Installation

After deployment, verify your installation:

```bash
kubectl get pods
```
> **Important:** All the pods should be in Running state. This may take a couple of minutes.

For troubleshooting and additional configuration options, please contact the authors.

### 5. Cleanup

To delete all network components and clean-up the environment:
```bash
chmod +x clean-k8s-environment.sh && ./clean-k8s-environment.sh
```

## Usage

### Deploying Applications

This section covers how to deploy applications to the network using the application deployment client.

#### Deployment Steps

1. Connect to the deployment client:
   ```bash
   kubectl exec -it network-management-console-xxxxx -c application-deployment-client bash
   ```

2. Deploy the application:
   ```bash
   python docker_image_client.py deploy_image application.tar app_requirements.json
   ```

   > **Important:** Upon successful deployment, an application ID will be generated. Save this ID as it's required for workflow creation.

#### Application Requirements

The `app_requirements.json` file specifies resource requirements for your application:

```json
{
    "memory": 110557184, 
    "cpu": 0.1, 
    "disk": 632085504
}
```

#### Additional Information

- The `application.tar` file is generated automatically when running `build-project.sh`
- For a complete list of supported commands, run:
  ```bash
  python docker_image_client.py
  ```

#### Troubleshooting

If you encounter deployment issues:
1. Verify your resource requirements are within cluster limits
2. Ensure the application image was built successfully
3. Check the deployment client logs for detailed error messages


### Creating Workflows

This section covers workflow creation using the workflow creation client.

#### Workflow Creation Steps

1. Connect to the workflow creation client:
   ```bash
   kubectl exec -it network-management-console-xxxxx -c workflow-creation-client bash
   ```

2. Create the workflow:
   ```bash
   python workflow_creation_client.py graph.json
   ```

   > **Important:** Upon successful workflow creation, a workflow ID will be generated. Save this ID as it's required to associate data processing requests to the correct workflow.

#### Workflow DAG

The `graph.json` file specifies the workflow as a DAG where each node is an application and edges represent the flow of data or order of execution:

```json
{
   "start": "7e5131e3-87ca-4eeb-a793-b581fe8e7916",
   "nodes": {
      "7e5131e3-87ca-4eeb-a793-b581fe8e7916": {"next": ["c2bcbd14-f998-4b72-9069-5f9483a459ba"]},
      "c2bcbd14-f998-4b72-9069-5f9483a459ba": {"next": ["b0244328-98b2-42b7-8974-40a73433ba4b"]},
      "b0244328-98b2-42b7-8974-40a73433ba4b": {"next": []}
   }
}
```

#### Troubleshooting

If you encounter workflow creation issues:
1. Verify all your application IDs are valid
2. Verify your graph.json contains a valid DAG with no cyclic dependencies
3. Check the workflow creation client logs for detailed error messages

### Initiating Data Processing Requests from IoT Nodes

This section provides the steps to initiate data processing requests for the sample cold-chain application.

1. Connect to the iot-node:
   ```bash
   kubectl exec -it iot-x-xxxxx -c iot-node bash
   ```

2. Create the workflow:
   ```bash
   python cold-chain-data-simulation.py <workflowID>
   ```
   
> **Note:** You may follow a similar pattern to define your custom iot-node processes.

## Architecture

TrustMesh implements a three-layer architecture:

1. **Network Management Layer**
   - Manages application deployment and workflow orchestration
   - Implements clients for application deployment and workflow management
   - Maintains local registry for container images

2. **Computation Layer**
   - Implements blockchain nodes and consensus mechanism
   - Manages data storage and processing
   - Handles task scheduling and execution

3. **Perception Layer**
   - Interfaces with IoT devices
   - Handles data collection and result delivery
   - Implements zero-process baseline approach

## Performance

Based on our experimental evaluation with a 21-node testbed and the cold-chain application:
- Request Round Trip (RRT) time: 33.54 - 36.34 seconds
- Framework Overhead: 3.25 - 4.19 seconds
- Scales effectively up to 16 computation nodes

> **Note:** Please keep in mind that we used several versions of the cold-chain anomaly detection application with varying computational requirements to attain these results. Only one of those variations is included in this project so your RRT may vary. However, you should find that the FO remains the same.

## License

This project is licensed under the GNU General Public License - see the [LICENSE](LICENSE) file for details.

## Citation

If you use TrustMesh in your research, please cite:
```
@INPROCEEDINGS{10978934,
  author={Rangwala, Murtaza and Buyya, Rajkumar},
  booktitle={2025 IEEE 22nd International Conference on Software Architecture (ICSA)}, 
  title={TrustMesh: A Blockchain-Enabled Trusted Distributed Computing Framework for Open Heterogeneous IoT Environments}, 
  year={2025},
  volume={},
  number={},
  pages={131-141},
  keywords={Fault tolerance;Trusted computing;Technological innovation;Software architecture;Scheduling algorithms;Fault tolerant systems;Trustless services;Internet of Things;Security;Resource management;Internet of Things;Distributed Systems;Edge Computing;Blockchains;Decentralized Applications;Trusted Computing},
  doi={10.1109/ICSA65012.2025.00022}}
```

## Authors

- Murtaza Rangwala - [Email](mailto:mrangwala@student.unimelb.edu.au)
- Rajkumar Buyya - [Email](mailto:rbuyya@unimelb.edu.au)

Cloud Computing and Distributed Systems (CLOUDS) Laboratory  
School of Computing and Information Systems  
The University of Melbourne, Australia
