#!/usr/bin/env python3
"""
MNIST Federated Learning Training Task

This is a server-based training application for MNIST federated learning in TrustMesh.
It receives training data and initial model weights from IoT nodes, performs local training,
and returns the updated model weights.
"""

import asyncio
import json
import logging
import numpy as np
import time
import traceback
from typing import Dict, List, Any, Tuple

# PyTorch for MNIST training
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Model configuration
NUM_CLASSES = 10
EPOCHS_PER_ROUND = 3
BATCH_SIZE = 32


class MNISTNet(nn.Module):
    """PyTorch CNN model for MNIST federated learning"""
    def __init__(self, num_classes=10):
        super(MNISTNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, kernel_size=3, padding=1)
        self.pool1 = nn.MaxPool2d(2, 2)
        self.conv2 = nn.Conv2d(32, 64, kernel_size=3, padding=1)
        self.pool2 = nn.MaxPool2d(2, 2)
        self.conv3 = nn.Conv2d(64, 64, kernel_size=3, padding=1)
        self.flatten = nn.Flatten()
        self.fc1 = nn.Linear(64 * 7 * 7, 64)  # 28x28 -> 14x14 -> 7x7 after pooling
        self.fc2 = nn.Linear(64, num_classes)
        self.dropout = nn.Dropout(0.2)
        
    def forward(self, x):
        x = self.pool1(F.relu(self.conv1(x)))
        x = self.pool2(F.relu(self.conv2(x)))
        x = F.relu(self.conv3(x))
        x = self.flatten(x)
        x = self.dropout(F.relu(self.fc1(x)))
        x = F.softmax(self.fc2(x), dim=1)
        return x


class MNISTFederatedTrainer:
    """MNIST Federated Learning Trainer for TrustMesh compute nodes"""
    
    def __init__(self):
        self.model = None
        self.training_history = []
        logger.info("Initialized MNIST Federated Trainer")
    
    def create_model(self) -> MNISTNet:
        """Create a simple CNN model for MNIST"""
        model = MNISTNet(num_classes=NUM_CLASSES)
        
        # Count parameters
        total_params = sum(p.numel() for p in model.parameters())
        logger.info(f"Created MNIST model with {total_params} parameters")
        return model
    
    def prepare_data(self, training_data: Dict[str, Any]) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare training data from IoT node input"""
        try:
            # Extract training data
            x_train = np.array(training_data['x_train'])
            y_train = np.array(training_data['y_train'])
            
            # Normalize pixel values to [0, 1]
            x_train = x_train.astype('float32') / 255.0
            
            logger.info(f"Prepared training data: {x_train.shape} samples, classes: {np.unique(y_train)}")
            return x_train, y_train
            
        except Exception as e:
            logger.error(f"Error preparing training data: {e}")
            raise
    
    def load_global_weights(self, global_weights: Dict[str, List]) -> bool:
        """Load global model weights from previous aggregation round"""
        try:
            if not global_weights or not self.model:
                return False
            
            # Convert lists to PyTorch tensors and create state_dict
            state_dict = {}
            for layer_name, weights in global_weights.items():
                state_dict[layer_name] = torch.tensor(weights, dtype=torch.float32)
            
            self.model.load_state_dict(state_dict)
            logger.info(f"Loaded global weights from {len(global_weights)} layers")
            return True
            
        except Exception as e:
            logger.error(f"Error loading global weights: {e}")
            return False
    
    def train_local_model(self, x_train: np.ndarray, y_train: np.ndarray) -> Dict[str, Any]:
        """Train the model locally on provided data"""
        try:
            logger.info(f"Starting local training on {len(x_train)} samples for {EPOCHS_PER_ROUND} epochs")
            
            # Convert numpy arrays to PyTorch tensors
            # Reshape x_train to (N, 1, 28, 28) for PyTorch CNN
            if len(x_train.shape) == 3:
                x_train = x_train.reshape(x_train.shape[0], 1, 28, 28)
            x_tensor = torch.tensor(x_train, dtype=torch.float32)
            y_tensor = torch.tensor(y_train, dtype=torch.long)
            
            # Create DataLoader for batch training
            dataset = TensorDataset(x_tensor, y_tensor)
            
            # Split into train and validation sets
            val_size = int(0.1 * len(dataset))
            train_size = len(dataset) - val_size
            train_dataset, val_dataset = torch.utils.data.random_split(dataset, [train_size, val_size])
            
            train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
            val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE, shuffle=False) if val_size > 0 else None
            
            # Set up optimizer and loss function
            optimizer = optim.Adam(self.model.parameters(), lr=0.001)
            criterion = nn.CrossEntropyLoss()
            
            self.model.train()
            
            # Training metrics tracking
            epoch_losses = []
            epoch_accuracies = []
            val_losses = []
            val_accuracies = []
            
            # Training loop
            for epoch in range(EPOCHS_PER_ROUND):
                epoch_loss = 0.0
                correct_predictions = 0
                total_samples = 0
                
                for batch_x, batch_y in train_loader:
                    optimizer.zero_grad()
                    
                    # Forward pass
                    outputs = self.model(batch_x)
                    loss = criterion(outputs, batch_y)
                    
                    # Backward pass
                    loss.backward()
                    optimizer.step()
                    
                    # Track metrics
                    epoch_loss += loss.item()
                    _, predicted = torch.max(outputs.data, 1)
                    total_samples += batch_y.size(0)
                    correct_predictions += (predicted == batch_y).sum().item()
                
                # Calculate epoch metrics
                avg_loss = epoch_loss / len(train_loader)
                accuracy = correct_predictions / total_samples
                epoch_losses.append(avg_loss)
                epoch_accuracies.append(accuracy)
                
                # Validation if available
                val_loss = None
                val_accuracy = None
                if val_loader:
                    self.model.eval()
                    val_epoch_loss = 0.0
                    val_correct = 0
                    val_total = 0
                    
                    with torch.no_grad():
                        for val_x, val_y in val_loader:
                            val_outputs = self.model(val_x)
                            val_batch_loss = criterion(val_outputs, val_y)
                            val_epoch_loss += val_batch_loss.item()
                            
                            _, val_predicted = torch.max(val_outputs.data, 1)
                            val_total += val_y.size(0)
                            val_correct += (val_predicted == val_y).sum().item()
                    
                    val_loss = val_epoch_loss / len(val_loader)
                    val_accuracy = val_correct / val_total
                    val_losses.append(val_loss)
                    val_accuracies.append(val_accuracy)
                    self.model.train()
                
                logger.info(f"Epoch {epoch+1}/{EPOCHS_PER_ROUND} - Loss: {avg_loss:.4f}, Accuracy: {accuracy:.4f}" + 
                           (f", Val Loss: {val_loss:.4f}, Val Accuracy: {val_accuracy:.4f}" if val_loss else ""))
            
            # Extract trained weights as state_dict
            trained_weights = {}
            for name, param in self.model.state_dict().items():
                trained_weights[name] = param.cpu().numpy().tolist()
            
            # Final metrics
            final_loss = epoch_losses[-1]
            final_accuracy = epoch_accuracies[-1]
            final_val_loss = val_losses[-1] if val_losses else None
            final_val_accuracy = val_accuracies[-1] if val_accuracies else None
            
            # Count parameters
            total_params = sum(p.numel() for p in self.model.parameters())
            
            training_result = {
                'trained_weights': trained_weights,
                'training_metrics': {
                    'final_loss': float(final_loss),
                    'final_accuracy': float(final_accuracy),
                    'val_loss': float(final_val_loss) if final_val_loss else None,
                    'val_accuracy': float(final_val_accuracy) if final_val_accuracy else None,
                    'epochs_trained': EPOCHS_PER_ROUND,
                    'samples_trained': len(x_train)
                },
                'model_info': {
                    'total_parameters': total_params,
                    'layer_count': len(list(self.model.named_parameters())),
                    'weight_layers': len(trained_weights)
                }
            }
            
            logger.info(f"Local training completed - Accuracy: {final_accuracy:.4f}, Loss: {final_loss:.4f}")
            return training_result
            
        except Exception as e:
            logger.error(f"Error during local training: {e}")
            raise
    
    def process_federated_training(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main processing function for federated training"""
        try:
            logger.info("Processing federated training request")
            
            # Extract input parameters
            node_id = input_data.get('node_id', 'unknown')
            round_number = input_data.get('round_number', 1)
            training_data = input_data
            global_weights = input_data.get('global_weights')  # From previous round
            
            logger.info(f"Training request from node {node_id}, round {round_number}")
            
            # Create or reset model
            self.model = self.create_model()
            
            # Load global weights if available (from previous aggregation round)
            if global_weights:
                logger.info("Loading global weights from previous round")
                self.load_global_weights(global_weights)
            else:
                logger.info("No global weights provided, using initial random weights")
            
            # Prepare training data
            x_train, y_train = self.prepare_data(training_data)
            
            # Perform local training
            training_result = self.train_local_model(x_train, y_train)
            
            # Add metadata
            training_result['metadata'] = {
                'node_id': node_id,
                'round_number': round_number,
                'assigned_classes': training_data.get('assigned_classes', []),
                'training_timestamp': time.time(),
                'samples_count': len(x_train),
                'global_weights_loaded': global_weights is not None
            }
            
            logger.info(f"Successfully completed federated training for node {node_id}")
            return training_result
            
        except Exception as e:
            logger.error(f"Error in federated training processing: {e}")
            raise


async def read_json(reader):
    """Read JSON data from the stream, similar to cold-chain monitoring pattern"""
    buffer = b""
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            raise ValueError("Connection closed before receiving a valid JSON object.")
        buffer += chunk
        try:
            return json.loads(buffer.decode())
        except json.JSONDecodeError:
            continue

async def handle_client(reader, writer):
    """Handle a single client request"""
    start_time = time.perf_counter()
    
    try:
        logger.info("New client connected for federated training")
        
        # Read JSON input data
        input_data = await read_json(reader)
        logger.info(f"Received JSON data with keys: {list(input_data.keys())}")
        
        # Validate required fields - expect data wrapped like cold-chain monitoring
        if 'data' not in input_data or not isinstance(input_data['data'], list):
            raise ValueError("'data' field (as a list) is required in the JSON input")
        
        # Extract the actual training data from the wrapped structure
        training_request = input_data['data'][0]  # First item contains the training data
        
        # Validate expected fields from IoT node
        if 'x_train' not in training_request or 'y_train' not in training_request:
            raise ValueError("'x_train' and 'y_train' fields are required in the training request")
        if 'initial_weights' not in training_request:
            raise ValueError("'initial_weights' field is required in the training request")
        
        # Restructure to match what process_federated_training expects
        training_data = {
            'training_data': {
                'x_train': training_request['x_train'],
                'y_train': training_request['y_train']
            },
            'initial_weights': training_request['initial_weights'],
            'node_id': training_request.get('node_id', 'unknown'),
            'round_number': training_request.get('round_number', 1),
            'assigned_classes': training_request.get('assigned_classes', []),
            'samples_count': training_request.get('samples_count', 0)
        }
        
        # Initialize trainer and process request
        trainer = MNISTFederatedTrainer()
        result = trainer.process_federated_training(training_data)
        
        # Wrap result in the expected format like cold-chain monitoring
        output = {
            "data": [result],  # Wrap the training result
            "total_task_time": input_data.get('total_task_time', 0) + time.perf_counter() - start_time
        }
        
        # Send response
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()
        
        logger.info("Federated training completed successfully")
        
    except Exception as e:
        logger.error(f"Federated training failed: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Send error response wrapped in expected format
        error_result = {
            "error": str(e),
            "status": "failed",
            "trained_weights": None,
            "training_metrics": None
        }
        
        output = {
            "data": [error_result],
            "total_task_time": input_data.get('total_task_time', 0) + time.perf_counter() - start_time if 'input_data' in locals() else 0
        }
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()
    
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    """Main server entry point"""
    server = await asyncio.start_server(handle_client, '0.0.0.0', 12345)
    addr = server.sockets[0].getsockname()
    logger.info(f'MNIST Federated Training Server serving on {addr}')
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())