#!/bin/bash

# Cleanup kubernetes environment if it exists.
echo "Cleaning up TrustMesh Kubernetes environment..."

# Delete main deployments
echo "Deleting main deployments..."
kubectl delete -f kubernetes-manifests/generated/redis-cluster.yaml --ignore-not-found=true
kubectl delete -f kubernetes-manifests/generated/blockchain-network-deployment.yaml --ignore-not-found=true
kubectl delete -f kubernetes-manifests/static/local-docker-registry-deployment.yaml --ignore-not-found=true
kubectl delete -f kubernetes-manifests/generated/couchdb-cluster-deployment.yaml --ignore-not-found=true
kubectl delete -f kubernetes-manifests/generated/config-and-secrets.yaml --ignore-not-found=true

# Delete federated learning jobs
echo "Deleting federated learning jobs..."
kubectl delete -f kubernetes-manifests/generated/mnist-validation-dataset-job.yaml --ignore-not-found=true
kubectl delete job mnist-validation-dataset-distributor --ignore-not-found=true

# Delete other jobs
echo "Deleting other jobs..."
kubectl delete -f kubernetes-manifests/generated/pbft-key-generation-job.yaml --ignore-not-found=true

# Delete secrets
echo "Deleting secrets..."
kubectl delete secret couchdb-certs --ignore-not-found=true
kubectl delete secret network-key-secret --ignore-not-found=true
kubectl delete secret redis-certificates --ignore-not-found=true
kubectl delete secret redis-password --ignore-not-found=true

# Delete persistent volume claims
echo "Deleting persistent volume claims..."
kubectl delete pvc -l app=redis-cluster --ignore-not-found=true
kubectl delete pvc -l app=couchdb-cluster --ignore-not-found=true
# Delete all remaining PVCs
kubectl delete pvc --all --ignore-not-found=true

# Delete persistent volumes
echo "Deleting persistent volumes..."
kubectl delete pv --all --ignore-not-found=true

# Delete storage classes (if any custom ones exist)
echo "Deleting custom storage classes..."
kubectl delete storageclass -l project=trustmesh --ignore-not-found=true

# Clean up any remaining pods, services, etc.
echo "Cleaning up remaining TrustMesh resources..."
kubectl delete pods,services,deployments,jobs,configmaps -l project=trustmesh --ignore-not-found=true

# Remove generated files
echo "Removing generated manifest files..."
sudo rm -r -f kubernetes-manifests/generated

echo "✅ TrustMesh Kubernetes environment cleanup completed!"