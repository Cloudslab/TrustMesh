#!/bin/bash

# Main script content starts here

WORK_DIR=$(pwd)
DOCKER_USERNAME=murtazahr

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Build peer-registry-tp image
docker build -t $DOCKER_USERNAME/peer-registry-tp:latest ./peer-registry/peer-registry-tp
# Build docker-image-tp image
docker build -t $DOCKER_USERNAME/docker-image-tp:latest ./auto-docker-deployment/docker-image-tp

# Build MNIST Federated Learning sample applications FIRST
echo "Building MNIST Federated Learning applications..."
docker build -t $DOCKER_USERNAME/mnist-fl-local-training:latest ./sample-apps/mnist-federated-learning/task1_local_training
docker build -t $DOCKER_USERNAME/mnist-fl-aggregate-models:latest ./sample-apps/mnist-federated-learning/task2_aggregate_models
docker build -t $DOCKER_USERNAME/mnist-fl-model-evaluation:latest ./sample-apps/mnist-federated-learning/task3_model_evaluation

# Create application tar files for deployment
echo "Creating application tar files..."
mkdir -p ./sample-apps/mnist-federated-learning/deployment
docker save $DOCKER_USERNAME/mnist-fl-local-training:latest | gzip > ./sample-apps/mnist-federated-learning/deployment/mnist-fl-local-training.tar.gz
docker save $DOCKER_USERNAME/mnist-fl-aggregate-models:latest | gzip > ./sample-apps/mnist-federated-learning/deployment/mnist-fl-aggregate-models.tar.gz
docker save $DOCKER_USERNAME/mnist-fl-model-evaluation:latest | gzip > ./sample-apps/mnist-federated-learning/deployment/mnist-fl-model-evaluation.tar.gz

# Copy MNIST tar files to docker-image-client directory for Dockerfile access
echo "Copying MNIST tar files to docker-image-client directory..."
cp ./sample-apps/mnist-federated-learning/deployment/mnist-fl-local-training.tar.gz ./auto-docker-deployment/docker-image-client/
cp ./sample-apps/mnist-federated-learning/deployment/mnist-fl-aggregate-models.tar.gz ./auto-docker-deployment/docker-image-client/
cp ./sample-apps/mnist-federated-learning/deployment/mnist-fl-model-evaluation.tar.gz ./auto-docker-deployment/docker-image-client/

# NOW build docker-image-client with tar files available
echo "Building docker-image-client with MNIST applications..."
docker build -t $DOCKER_USERNAME/docker-image-client:latest ./auto-docker-deployment/docker-image-client

# Build dependency-management-tp image
docker build -t $DOCKER_USERNAME/dependency-management-tp:latest ./manage-dependency-workflow/dependency-management-tp
# Build workflow-creation-client image
docker build -t $DOCKER_USERNAME/workflow-creation-client:latest ./manage-dependency-workflow/workflow-creation-client
# Build scheduling-request-tp image
docker build -t $DOCKER_USERNAME/scheduling-request-tp:latest ./scheduling/scheduling-request-tp
# Build schedule-confirmation-tp image
docker build -t $DOCKER_USERNAME/schedule-confirmation-tp:latest ./scheduling/schedule-confirmation-tp
# Build schedule-status-update-tp image
docker build -t $DOCKER_USERNAME/schedule-status-update-tp:latest ./scheduling/status-update-tp
# Build iot-data-tp image
docker build -t $DOCKER_USERNAME/iot-data-tp:latest ./scheduling/iot-data-tp
# Build iot-node image
docker build -t $DOCKER_USERNAME/iot-node:latest ./iot-node
# Build compute-node image
docker build -t $DOCKER_USERNAME/compute-node:latest ./compute-node

# Build Federated Learning Coordinator
echo "Building Federated Learning Coordinator..."
# Build federated-schedule transaction processor
docker build -t $DOCKER_USERNAME/federated-schedule-tp:latest ./scheduling/federated-schedule-tp


# Push images to Docker Hub
docker push $DOCKER_USERNAME/peer-registry-tp:latest
docker push $DOCKER_USERNAME/docker-image-tp:latest
docker push $DOCKER_USERNAME/docker-image-client:latest
docker push $DOCKER_USERNAME/dependency-management-tp:latest
docker push $DOCKER_USERNAME/workflow-creation-client:latest
docker push $DOCKER_USERNAME/scheduling-request-tp:latest
docker push $DOCKER_USERNAME/schedule-confirmation-tp:latest
docker push $DOCKER_USERNAME/schedule-status-update-tp:latest
docker push $DOCKER_USERNAME/iot-data-tp:latest
docker push $DOCKER_USERNAME/compute-node:latest
docker push $DOCKER_USERNAME/iot-node:latest

# Push MNIST Federated Learning images
echo "Pushing MNIST Federated Learning images..."
docker push $DOCKER_USERNAME/mnist-fl-local-training:latest
docker push $DOCKER_USERNAME/mnist-fl-aggregate-models:latest
docker push $DOCKER_USERNAME/mnist-fl-model-evaluation:latest
docker push $DOCKER_USERNAME/federated-schedule-tp:latest

echo "All images built and pushed to registry successfully"
echo "MNIST Federated Learning application files created in ./sample-apps/mnist-federated-learning/deployment/"