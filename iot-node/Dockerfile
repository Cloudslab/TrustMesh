# Now, use the Sawtooth shell image as the main base
FROM hyperledger/sawtooth-shell:chime AS sawtooth-shell

# Generate Sawtooth keys
RUN sawtooth keygen # root keys
RUN sawtooth keygen client # user keys

FROM python:3.8

# Copy Sawtooth keys from shell
COPY --from=sawtooth-shell /root/.sawtooth /root/.sawtooth

# Install Docker
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    nano \
    lsb-release

# Verify installations
RUN python --version

WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install packages from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt -f https://download.pytorch.org/whl/torch_stable.html

# Pre-download MNIST dataset to avoid runtime network issues
RUN python -c "import os; from torchvision import datasets, transforms; print('Pre-downloading MNIST dataset...'); os.makedirs('/tmp/mnist', exist_ok=True); transform = transforms.Compose([transforms.ToTensor()]); train_dataset = datasets.MNIST(root='/tmp/mnist', train=True, download=True, transform=transform); test_dataset = datasets.MNIST(root='/tmp/mnist', train=False, download=True, transform=transform); print(f'MNIST dataset downloaded: {len(train_dataset)} train samples, {len(test_dataset)} test samples')"

# Copy the client script
COPY transaction_initiator/* ./transaction_initiator/
COPY response_manager/* ./response_manager/
COPY *.py ./

# Federated learning is now integrated into core transaction processors

ENV SAWTOOTH_PRIVATE_KEY /root/.sawtooth/keys/client.priv

# Keep the container running
CMD ["tail", "-f", "/dev/null"]