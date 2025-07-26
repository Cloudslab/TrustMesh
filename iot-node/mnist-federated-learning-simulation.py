"""
MNIST Federated Learning Simulation for TrustMesh IoT Nodes

This script runs on individual IoT nodes to participate in federated learning.
Each node maintains its own MNIST data partition and contributes to global model training.
"""

import argparse
import asyncio
import logging
import time
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torchvision import datasets, transforms
from datetime import datetime
from typing import Dict, List, Optional
import json
import re
import os
import sys

# Add federated learning extension to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'federated-learning-extension'))

from transaction_initiator.transaction_initiator import transaction_creator
from response_manager.response_manager import IoTDeviceManager

# Import federated learning components - now part of core transaction initiator
FEDERATED_AVAILABLE = True

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TOTAL_NODES = 5
FEDERATED_ROUND_INTERVAL = 180  # 3 minutes between rounds
SAMPLES_PER_NODE = 2000


class MNISTNet(nn.Module):
    """PyTorch CNN model for MNIST (must match training task architecture)"""
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


class MNISTFederatedNode:
    """
    MNIST Federated Learning Node for TrustMesh
    
    Each node maintains a local partition of MNIST data and participates
    in federated learning rounds while preserving data privacy.
    """
    
    def __init__(self, node_id: str, device_manager: IoTDeviceManager, test_split: float = 0.2):
        self.node_id = node_id
        self.device_manager = device_manager
        self.node_index = self._extract_node_index(node_id)
        self.assigned_classes = self._get_assigned_classes()
        self.test_split = test_split
        self.data_partition = None
        self.local_model = None  # For local validation
        
        # Validate node configuration
        if self.node_index >= TOTAL_NODES:
            raise ValueError(f"Node index {self.node_index} exceeds total nodes {TOTAL_NODES}")
        
        # Load MNIST data partition with train/test split
        self._load_data_partition()
        
        logger.info(f"Initialized MNIST Federated Node {node_id}")
        logger.info(f"Node index: {self.node_index}")
        logger.info(f"Assigned digit classes: {self.assigned_classes}")
        logger.info(f"Training samples: {len(self.data_partition['x_train'])}")
        logger.info(f"Local test samples: {len(self.data_partition['x_test'])}")
    
    def _extract_node_index(self, node_id: str) -> int:
        """Extract 0-based node index from node ID"""
        match = re.search(r'(\d+)(?!.*\d)', node_id)
        if match:
            return int(match.group(1))
        raise ValueError(f"Could not extract node index from: {node_id}")
    
    def _get_assigned_classes(self) -> List[int]:
        """Get the 2 digit classes assigned to this node"""
        # Node 0: [0,1], Node 1: [2,3], Node 2: [4,5], Node 3: [6,7], Node 4: [8,9]
        start_class = self.node_index * 2
        return [start_class, start_class + 1]
    
    def _load_data_partition(self):
        """Load MNIST data partition for this node"""
        try:
            # Load MNIST dataset using torchvision
            transform = transforms.Compose([transforms.ToTensor()])
            train_dataset = datasets.MNIST(root='/tmp/mnist', train=True, download=True, transform=transform)
            test_dataset = datasets.MNIST(root='/tmp/mnist', train=False, download=True, transform=transform)
            
            # Convert to numpy arrays for compatibility
            x_train = train_dataset.data.numpy()
            y_train = train_dataset.targets.numpy()
            x_test = test_dataset.data.numpy()
            y_test = test_dataset.targets.numpy()
            
            logger.info(f"Loaded MNIST dataset: {len(x_train)} training samples total")
            
            # Create deterministic partition
            x_partition, y_partition = self._create_node_partition(x_train, y_train)
            
            # Split into train and local test sets
            split_idx = int(len(x_partition) * (1 - self.test_split))
            
            x_train_local = x_partition[:split_idx]
            y_train_local = y_partition[:split_idx]
            x_test_local = x_partition[split_idx:]
            y_test_local = y_partition[split_idx:]
            
            # Store partition with train/test split
            self.data_partition = {
                'x_train': x_train_local.tolist(),
                'y_train': y_train_local.tolist(),
                'x_test': x_test_local.tolist(),
                'y_test': y_test_local.tolist(),
                'node_index': self.node_index,
                'assigned_classes': self.assigned_classes,
                'train_samples': len(x_train_local),
                'test_samples': len(x_test_local),
                'train_distribution': self._calculate_class_distribution(y_train_local),
                'test_distribution': self._calculate_class_distribution(y_test_local)
            }
            
            logger.info(f"Created partition with {len(x_partition)} samples")
            
        except Exception as e:
            logger.error(f"Error loading MNIST data: {e}")
            raise
    
    def _create_node_partition(self, x_train: np.ndarray, y_train: np.ndarray):
        """Create deterministic data partition for this node"""
        # Set seed for reproducibility
        np.random.seed(42 + self.node_index)
        
        # Get indices for assigned classes
        indices = []
        for digit_class in self.assigned_classes:
            class_indices = np.where(y_train == digit_class)[0]
            indices.extend(class_indices)
        
        # Sort and select samples
        indices = sorted(indices)
        x_partition = x_train[indices]
        y_partition = y_train[indices]
        
        # Shuffle with node-specific seed
        perm = np.random.permutation(len(x_partition))
        x_partition = x_partition[perm]
        y_partition = y_partition[perm]
        
        return x_partition, y_partition
    
    def _calculate_class_distribution(self, y_data: np.ndarray) -> Dict[str, int]:
        """Calculate class distribution in the partition"""
        distribution = {}
        for digit in range(10):
            count = int(np.sum(y_data == digit))
            if count > 0:
                distribution[f'digit_{digit}'] = count
        return distribution
    
    def _get_initial_weights_for_round(self, round_number: int) -> Dict:
        """Get initial weights for the training round"""
        weight_init_start = time.time()
        
        if round_number == 1:
            # Round 1: Initialize with random weights
            logger.info(f"🎲 WEIGHT INITIALIZATION: Round 1 - Creating random weights")
            logger.info(f"   • Method: PyTorch default initialization")
            logger.info(f"   • Model: MNISTNet with 10 classes")
            
            model = MNISTNet(num_classes=10)
            
            # Extract weights as nested lists (JSON serializable)
            initial_weights = {}
            total_params = 0
            for name, param in model.state_dict().items():
                weight_array = param.cpu().numpy()
                initial_weights[name] = weight_array.tolist()
                layer_params = weight_array.size
                total_params += layer_params
                logger.info(f"   • {name}: {weight_array.shape} ({layer_params:,} params)")
            
            weight_init_duration = time.time() - weight_init_start
            logger.info(f"   • Total parameters: {total_params:,}")
            logger.info(f"   • Initialization time: {weight_init_duration:.3f}s")
            
            # Store for next rounds
            self.current_weights = initial_weights
            return initial_weights
        else:
            # Round 2+: Use weights from previous aggregation
            if hasattr(self, 'current_weights') and self.current_weights:
                logger.info(f"🔄 WEIGHT INITIALIZATION: Round {round_number} - Using aggregated weights")
                logger.info(f"   • Source: Previous global model aggregation")
                logger.info(f"   • Weight layers: {len(self.current_weights)}")
                
                # Calculate and log weight statistics
                total_params = 0
                for name, weights in self.current_weights.items():
                    layer_params = len(np.array(weights).flatten())
                    total_params += layer_params
                
                weight_init_duration = time.time() - weight_init_start
                logger.info(f"   • Total parameters: {total_params:,}")
                logger.info(f"   • Load time: {weight_init_duration:.3f}s")
                
                return self.current_weights
            else:
                # Fallback: random weights if no previous weights available
                logger.warning(f"⚠️ WEIGHT INITIALIZATION: Round {round_number} - Fallback to random weights")
                logger.warning(f"   • Reason: No previous aggregated weights found")
                logger.warning(f"   • This may indicate an aggregation failure in previous round")
                
                model = MNISTNet(num_classes=10)
                initial_weights = {}
                total_params = 0
                for name, param in model.state_dict().items():
                    weight_array = param.cpu().numpy()
                    initial_weights[name] = weight_array.tolist()
                    total_params += weight_array.size
                
                weight_init_duration = time.time() - weight_init_start
                logger.warning(f"   • Fallback parameters: {total_params:,}")
                logger.warning(f"   • Initialization time: {weight_init_duration:.3f}s")
                
                return initial_weights
    
    def is_coordinator(self) -> bool:
        """Check if this node is the round coordinator"""
        return self.node_index == 0
    
    def get_expected_nodes(self) -> List[str]:
        """Get list of expected node IDs"""
        return [f"iot-{i}" for i in range(TOTAL_NODES)]
    
    def submit_training_phase(self, workflow_id: str, round_number: int) -> Optional[str]:
        """Submit data for the training phase of federated learning"""
        training_start_time = time.time()
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🚀 FEDERATED LEARNING EVENT: Training Phase Started")
            logger.info(f"Node: {self.node_id} | Workflow: {workflow_id} | Round: {round_number}")
            logger.info(f"Timestamp: {datetime.now().isoformat()}")
            logger.info(f"{'='*60}")
            
            # Prepare training data payload (subset for this round)
            samples_per_round = min(500, len(self.data_partition['x_train']) // 2)  # Use subset
            start_idx = (round_number - 1) * samples_per_round % len(self.data_partition['x_train'])
            end_idx = min(start_idx + samples_per_round, len(self.data_partition['x_train']))
            
            round_x_data = self.data_partition['x_train'][start_idx:end_idx]
            round_y_data = self.data_partition['y_train'][start_idx:end_idx]
            
            # Get initial weights for this round
            logger.info(f"📊 DATA PREPARATION: Preparing training data for round {round_number}")
            logger.info(f"   • Data slice: indices {start_idx}-{end_idx} ({len(round_x_data)} samples)")
            logger.info(f"   • Assigned classes: {self.assigned_classes}")
            logger.info(f"   • Class distribution in training data: {dict(zip(*np.unique(round_y_data, return_counts=True)))}")
            
            initial_weights = self._get_initial_weights_for_round(round_number)
            
            training_data = {
                'node_id': self.node_id,
                'node_index': self.node_index,
                'round_number': round_number,
                'x_train': round_x_data,
                'y_train': round_y_data,
                'initial_weights': initial_weights,  # Include initial weights
                'assigned_classes': self.assigned_classes,
                'samples_count': len(round_x_data),
                'timestamp': datetime.now().isoformat(),
                'training_metadata': {
                    'start_idx': start_idx,
                    'end_idx': end_idx,
                    'total_node_samples': self.data_partition['train_samples'],
                    'class_distribution': self.data_partition['train_distribution']
                }
            }
            
            logger.info(f"🔢 WEIGHT INITIALIZATION: {'Random weights (Round 1)' if round_number == 1 else 'Previous aggregated weights'}")
            logger.info(f"   • Weight layers: {len(initial_weights)} layers")
            logger.info(f"   • Weight keys: {list(initial_weights.keys())[:3]}... (showing first 3)")
            
            if FEDERATED_AVAILABLE:
                logger.info(f"🔗 BLOCKCHAIN TRANSACTION: Submitting training data to TrustMesh")
                logger.info(f"   • Transaction type: Two-phase federated (Phase 1: Training)")
                logger.info(f"   • Payload size: {len(json.dumps(training_data))} bytes")
                
                # Phase 1: Submit training data
                tx_start_time = time.time()
                schedule_id = transaction_creator.create_two_phase_federated_transaction(
                    training_data=training_data,
                    workflow_id=workflow_id,
                    node_id=self.node_id,
                    iot_port="5000",
                    iot_public_key="dummy_public_key",
                    phase="training",
                    round_number=round_number
                )
                tx_duration = time.time() - tx_start_time
                logger.info(f"   • Transaction submitted in {tx_duration:.2f}s")
                
            else:
                logger.warning("Using standard transaction creator - federated learning disabled")
                schedule_id = transaction_creator.create_and_send_transactions(
                    iot_data=training_data,
                    workflow_id=workflow_id,
                    iot_port="5000",
                    iot_public_key="dummy_public_key"
                )
            
            training_duration = time.time() - training_start_time
            logger.info(f"✅ TRAINING PHASE COMPLETED SUCCESSFULLY")
            logger.info(f"   • Schedule ID: {schedule_id}")
            logger.info(f"   • Training samples: {len(round_x_data)}")
            logger.info(f"   • Assigned classes: {self.assigned_classes}")
            logger.info(f"   • Total phase duration: {training_duration:.2f}s")
            logger.info(f"   • Next step: Wait for compute node training completion")
            
            return schedule_id
            
        except Exception as e:
            training_duration = time.time() - training_start_time
            logger.error(f"❌ TRAINING PHASE FAILED")
            logger.error(f"   • Error: {str(e)}")
            logger.error(f"   • Duration before failure: {training_duration:.2f}s")
            logger.error(f"   • Round: {round_number} | Node: {self.node_id}")
            return None

    def evaluate_model_locally(self, model_weights: Dict) -> float:
        """Evaluate model on local test data"""
        evaluation_start = time.time()
        
        try:
            logger.info(f"🎦 LOCAL EVALUATION: Starting model evaluation")
            logger.info(f"   • Model weights: {len(model_weights)} layers")
            logger.info(f"   • Test dataset: {len(self.data_partition['x_test'])} samples")
            logger.info(f"   • Test classes: {self.assigned_classes}")
            
            # Create model architecture (must match training task)
            model = MNISTNet(num_classes=10)
            model.eval()
            
            # Load weights from state_dict format
            try:
                logger.info(f"   • Loading weights into model...")
                state_dict = {}
                total_params = 0
                for layer_name, weights in model_weights.items():
                    weight_tensor = torch.tensor(weights, dtype=torch.float32)
                    state_dict[layer_name] = weight_tensor
                    total_params += weight_tensor.numel()
                    
                model.load_state_dict(state_dict)
                logger.info(f"   • Successfully loaded {total_params:,} parameters")
                
            except Exception as e:
                logger.error(f"❌ WEIGHT LOADING FAILED: {e}")
                return 0.0
            
            # Prepare test data
            logger.info(f"   • Preparing test data...")
            x_test = np.array(self.data_partition['x_test']).astype('float32') / 255.0
            y_test = np.array(self.data_partition['y_test'])
            
            # Log class distribution in test set
            test_class_dist = dict(zip(*np.unique(y_test, return_counts=True)))
            logger.info(f"   • Test class distribution: {test_class_dist}")
            
            # Convert to PyTorch tensors and reshape for CNN (N, C, H, W)
            if len(x_test.shape) == 3:
                x_test = x_test.reshape(x_test.shape[0], 1, 28, 28)
            x_test_tensor = torch.tensor(x_test, dtype=torch.float32)
            y_test_tensor = torch.tensor(y_test, dtype=torch.long)
            
            logger.info(f"   • Input tensor shape: {x_test_tensor.shape}")
            logger.info(f"   • Target tensor shape: {y_test_tensor.shape}")
            
            # Evaluate on local test set
            logger.info(f"   • Running inference...")
            inference_start = time.time()
            
            with torch.no_grad():
                outputs = model(x_test_tensor)
                predictions = torch.argmax(outputs, dim=1)
                accuracy = (predictions == y_test_tensor).float().mean().item()
                
                # Calculate loss
                loss_fn = nn.CrossEntropyLoss()
                loss = loss_fn(outputs, y_test_tensor).item()
                
                # Calculate per-class accuracy for assigned classes
                class_accuracies = {}
                for class_idx in self.assigned_classes:
                    class_mask = (y_test_tensor == class_idx)
                    if class_mask.sum() > 0:
                        class_pred = predictions[class_mask]
                        class_target = y_test_tensor[class_mask]
                        class_acc = (class_pred == class_target).float().mean().item()
                        class_accuracies[f'class_{class_idx}'] = class_acc
            
            inference_duration = time.time() - inference_start
            evaluation_duration = time.time() - evaluation_start
            
            logger.info(f"✅ LOCAL EVALUATION COMPLETED")
            logger.info(f"   • Overall accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")
            logger.info(f"   • Loss: {loss:.4f}")
            logger.info(f"   • Test samples: {len(x_test)}")
            logger.info(f"   • Inference time: {inference_duration:.3f}s")
            logger.info(f"   • Total evaluation time: {evaluation_duration:.3f}s")
            
            # Log per-class accuracies
            if class_accuracies:
                logger.info(f"   • Per-class accuracies:")
                for class_name, class_acc in class_accuracies.items():
                    logger.info(f"     - {class_name}: {class_acc:.4f} ({class_acc*100:.2f}%)")
            
            # Store for tracking
            self.local_model = model
            
            return accuracy
            
        except Exception as e:
            evaluation_duration = time.time() - evaluation_start
            logger.error(f"❌ LOCAL EVALUATION FAILED")
            logger.error(f"   • Error: {str(e)}")
            logger.error(f"   • Duration before failure: {evaluation_duration:.3f}s")
            logger.error(f"   • Node: {self.node_id}")
            return 0.0

    def submit_aggregation_phase(self, workflow_id: str, round_number: int, trained_weights: Dict) -> bool:
        """Submit trained model weights for the aggregation phase"""
        aggregation_start_time = time.time()
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🔄 FEDERATED LEARNING EVENT: Aggregation Phase Started")
            logger.info(f"Node: {self.node_id} | Workflow: {workflow_id} | Round: {round_number}")
            logger.info(f"Timestamp: {datetime.now().isoformat()}")
            logger.info(f"{'='*60}")
            
            if not trained_weights:
                logger.error(f"❌ AGGREGATION PHASE FAILED: No trained weights available")
                logger.error(f"   • Expected weights from training phase but received empty/None")
                logger.error(f"   • Round: {round_number} | Node: {self.node_id}")
                return False
            
            logger.info(f"📊 WEIGHT ANALYSIS: Analyzing trained weights for aggregation")
            logger.info(f"   • Weight layers received: {len(trained_weights)}")
            logger.info(f"   • Layer names: {list(trained_weights.keys())}")
            
            # Calculate weight statistics for logging
            total_params = 0
            for layer_name, weights in trained_weights.items():
                if isinstance(weights, list):
                    layer_params = len(np.array(weights).flatten())
                    total_params += layer_params
                    logger.info(f"   • {layer_name}: {np.array(weights).shape} ({layer_params:,} params)")
            
            logger.info(f"   • Total parameters: {total_params:,}")
            
            # Prepare training data for metadata (needed for aggregation context)
            training_data = {
                'assigned_classes': self.assigned_classes,
                'x_train': [],  # Empty for aggregation phase
                'total_samples': self.data_partition.get('train_samples', 0)
            }
            
            logger.info(f"🔗 BLOCKCHAIN TRANSACTION: Submitting aggregation request")
            logger.info(f"   • Transaction type: Two-phase federated (Phase 2: Aggregation)")
            logger.info(f"   • Weight layers: {len(trained_weights)}")
            logger.info(f"   • Node classes: {self.assigned_classes}")
            logger.info(f"   • Training samples used: {self.data_partition.get('train_samples', 0)}")
            
            if FEDERATED_AVAILABLE:
                # Phase 2: Submit trained weights for aggregation
                tx_start_time = time.time()
                result = transaction_creator.create_two_phase_federated_transaction(
                    training_data=training_data,
                    workflow_id=workflow_id,
                    node_id=self.node_id,
                    iot_port="5000",
                    iot_public_key="dummy_public_key",
                    trained_weights=trained_weights,
                    phase="aggregation",
                    round_number=round_number
                )
                tx_duration = time.time() - tx_start_time
                aggregation_duration = time.time() - aggregation_start_time
                
                logger.info(f"✅ AGGREGATION PHASE COMPLETED SUCCESSFULLY")
                logger.info(f"   • Transaction result: {result}")
                logger.info(f"   • Transaction duration: {tx_duration:.2f}s")
                logger.info(f"   • Total phase duration: {aggregation_duration:.2f}s")
                logger.info(f"   • Weight layers submitted: {list(trained_weights.keys())}")
                logger.info(f"   • Next step: Wait for global model aggregation by compute nodes")
                
                return True
                
            else:
                logger.warning(f"⚠️ AGGREGATION PHASE SKIPPED: Federated learning extension not available")
                logger.warning(f"   • Round: {round_number} | Node: {self.node_id}")
                logger.warning(f"   • Trained weights will not be aggregated")
                return False
                
        except Exception as e:
            aggregation_duration = time.time() - aggregation_start_time
            logger.error(f"❌ AGGREGATION PHASE FAILED")
            logger.error(f"   • Error: {str(e)}")
            logger.error(f"   • Duration before failure: {aggregation_duration:.2f}s")
            logger.error(f"   • Round: {round_number} | Node: {self.node_id}")
            logger.error(f"   • Weights available: {bool(trained_weights)}")
            return False
    
    async def run_federated_learning(self, workflow_id: str, max_rounds: int = 5):
        """Run two-phase federated learning for specified rounds"""
        session_start_time = time.time()
        
        logger.info(f"\n{'#'*80}")
        logger.info(f"🎆 FEDERATED LEARNING SESSION STARTED")
        logger.info(f"{'#'*80}")
        logger.info(f"📱 Node Information:")
        logger.info(f"   • Node ID: {self.node_id}")
        logger.info(f"   • Node Index: {self.node_index}")
        logger.info(f"   • Assigned Classes: {self.assigned_classes}")
        logger.info(f"   • Training Samples: {self.data_partition['train_samples']}")
        logger.info(f"   • Test Samples: {self.data_partition['test_samples']}")
        logger.info(f"🔄 Session Configuration:")
        logger.info(f"   • Workflow ID: {workflow_id}")
        logger.info(f"   • Maximum Rounds: {max_rounds}")
        logger.info(f"   • Federated Extension: {'Available' if FEDERATED_AVAILABLE else 'Not Available'}")
        logger.info(f"   • Start Time: {datetime.now().isoformat()}")
        logger.info(f"{'#'*80}\n")
        
        try:
            # Initialize federated response manager for receiving aggregated models
            from response_manager.federated_response_manager import FederatedResponseManager
            fed_response_manager = FederatedResponseManager(self.node_id)
            
            # Start response manager in background
            import asyncio
            import threading
            
            def start_response_manager():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(fed_response_manager.start_federated_response_handler())
            
            response_thread = threading.Thread(target=start_response_manager, daemon=True)
            response_thread.start()
            logger.info("Started federated response manager")
            
            for round_num in range(1, max_rounds + 1):
                round_start_time = time.time()
                logger.info(f"\n{'='*80}")
                logger.info(f"🔥 FEDERATED LEARNING ROUND {round_num}/{max_rounds} STARTED")
                logger.info(f"{'='*80}")
                logger.info(f"🕰️ Round Metadata:")
                logger.info(f"   • Round Number: {round_num}")
                logger.info(f"   • Node: {self.node_id}")
                logger.info(f"   • Workflow: {workflow_id}")
                logger.info(f"   • Start Time: {datetime.now().isoformat()}")
                session_elapsed = time.time() - session_start_time
                logger.info(f"   • Session Elapsed: {session_elapsed:.1f}s")
                
                # Check convergence before continuing
                if not fed_response_manager.should_continue_learning(workflow_id):
                    logger.info(f"🏁 CONVERGENCE DETECTED - STOPPING EARLY")
                    logger.info(f"   • Stopped at round: {round_num}/{max_rounds}")
                    logger.info(f"   • Reason: Model has converged based on local validation")
                    logger.info(f"   • Node: {self.node_id}")
                    break
                
                # Phase 1: Submit training data and wait for training completion
                logger.info(f"\n🟦 PHASE 1: TRAINING PHASE")
                logger.info(f"   • Objective: Submit training data to TrustMesh for processing")
                logger.info(f"   • Expected outcome: Receive trained model weights")
                
                schedule_id = self.submit_training_phase(workflow_id, round_num)
                
                if not schedule_id:
                    logger.error(f"❌ ROUND {round_num} ABORTED - Training phase submission failed")
                    logger.error(f"   • Unable to submit training data to TrustMesh")
                    logger.error(f"   • Terminating federated learning session")
                    break
                
                # Wait for training to complete and get trained weights
                logger.info(f"\n⏳ WAITING FOR TRAINING COMPLETION")
                logger.info(f"   • Schedule ID: {schedule_id}")
                logger.info(f"   • Timeout: 120 seconds")
                logger.info(f"   • Waiting for compute node to process training data...")
                
                wait_start_time = time.time()
                trained_weights = await fed_response_manager.wait_for_training_completion(
                    workflow_id, schedule_id, timeout=120  # 2 minutes timeout
                )
                wait_duration = time.time() - wait_start_time
                
                if not trained_weights:
                    logger.error(f"❌ TRAINING FAILED OR TIMED OUT")
                    logger.error(f"   • Round: {round_num}")
                    logger.error(f"   • Schedule ID: {schedule_id}")
                    logger.error(f"   • Wait duration: {wait_duration:.1f}s")
                    logger.error(f"   • Skipping to next round...")
                    continue
                
                logger.info(f"✅ TRAINING COMPLETED SUCCESSFULLY")
                logger.info(f"   • Wait duration: {wait_duration:.1f}s")
                logger.info(f"   • Received weights: {len(trained_weights)} layers")
                logger.info(f"   • Weight layers: {list(trained_weights.keys())[:3]}... (showing first 3)")
                
                # Phase 2: Submit trained weights for aggregation
                logger.info(f"\n🟨 PHASE 2: AGGREGATION PHASE")
                logger.info(f"   • Objective: Submit trained weights for global aggregation")
                logger.info(f"   • Expected outcome: Contribute to FedAvg aggregation process")
                
                aggregation_success = self.submit_aggregation_phase(workflow_id, round_num, trained_weights)
                
                if not aggregation_success:
                    logger.error(f"❌ AGGREGATION SUBMISSION FAILED")
                    logger.error(f"   • Round: {round_num}")
                    logger.error(f"   • Unable to submit weights for aggregation")
                    logger.error(f"   • Skipping to next round...")
                    continue
                
                logger.info(f"✅ AGGREGATION SUBMISSION SUCCESSFUL")
                logger.info(f"   • Weights submitted to aggregation-request-tp")
                logger.info(f"   • Now waiting for global model aggregation...")
                
                # Wait for aggregated model
                logger.info(f"\n⏳ WAITING FOR GLOBAL MODEL AGGREGATION")
                logger.info(f"   • Round: {round_num}")
                logger.info(f"   • Timeout: 180 seconds (3 minutes)")
                logger.info(f"   • Waiting for aggregator to complete FedAvg and validation...")
                
                aggregation_wait_start = time.time()
                aggregated_weights = await fed_response_manager.wait_for_aggregated_model(workflow_id, round_num, timeout=180)
                aggregation_wait_duration = time.time() - aggregation_wait_start
                
                if aggregated_weights:
                    logger.info(f"✅ GLOBAL MODEL RECEIVED SUCCESSFULLY")
                    logger.info(f"   • Aggregation wait duration: {aggregation_wait_duration:.1f}s")
                    logger.info(f"   • Aggregated weights: {len(aggregated_weights)} layers")
                    logger.info(f"   • Model ready for local validation")
                    
                    # Store aggregated weights for next round
                    self.current_weights = aggregated_weights
                    
                    # Perform local validation on test data
                    logger.info(f"\n📊 LOCAL VALIDATION PHASE")
                    logger.info(f"   • Evaluating global model on local test set")
                    logger.info(f"   • Test samples: {self.data_partition['test_samples']}")
                    logger.info(f"   • Test classes: {self.assigned_classes}")
                    
                    validation_start = time.time()
                    local_accuracy = self.evaluate_model_locally(aggregated_weights)
                    validation_duration = time.time() - validation_start
                    
                    logger.info(f"✅ LOCAL VALIDATION COMPLETED")
                    logger.info(f"   • Validation duration: {validation_duration:.2f}s")
                    logger.info(f"   • Local accuracy: {local_accuracy:.4f}")
                    
                    # Update convergence tracker with local accuracy
                    fed_response_manager.update_local_validation_accuracy(workflow_id, round_num, local_accuracy)
                    
                    # Check if should continue based on local validation
                    if not fed_response_manager.should_continue_learning(workflow_id):
                        logger.info(f"🏁 CONVERGENCE DETECTED AFTER ROUND {round_num}")
                        logger.info(f"   • Based on local validation performance")
                        logger.info(f"   • Stopping federated learning session")
                        break
                else:
                    logger.error(f"❌ GLOBAL MODEL NOT RECEIVED")
                    logger.error(f"   • Aggregation wait duration: {aggregation_wait_duration:.1f}s")
                    logger.error(f"   • Possible timeout or aggregation failure")
                    logger.error(f"   • Continuing to next round with current weights...")
                
                # Brief pause between rounds
                round_duration = time.time() - round_start_time
                logger.info(f"\n✅ ROUND {round_num} COMPLETED")
                logger.info(f"   • Total round duration: {round_duration:.1f}s")
                logger.info(f"   • Phases completed: Training → Aggregation → Validation")
                
                if round_num < max_rounds:
                    logger.info(f"   • Preparing for round {round_num + 1}...")
                    logger.info(f"   • Inter-round pause: 10 seconds")
                    time.sleep(10)
                else:
                    logger.info(f"   • This was the final round ({max_rounds})")
            
            session_duration = time.time() - session_start_time
            
            logger.info(f"\n{'#'*80}")
            logger.info(f"🎆 FEDERATED LEARNING SESSION COMPLETED")
            logger.info(f"{'#'*80}")
            logger.info(f"📊 Session Summary:")
            logger.info(f"   • Node: {self.node_id}")
            logger.info(f"   • Workflow: {workflow_id}")
            logger.info(f"   • Total session duration: {session_duration:.1f}s ({session_duration/60:.1f} minutes)")
            logger.info(f"   • Rounds planned: {max_rounds}")
            logger.info(f"   • End time: {datetime.now().isoformat()}")
            
            # Show convergence status
            convergence_status = fed_response_manager.get_convergence_status(workflow_id)
            logger.info(f"📈 Final Convergence Status:")
            for key, value in convergence_status.items():
                logger.info(f"   • {key}: {value}")
            
            # Show round history
            round_history = fed_response_manager.get_round_history()
            logger.info(f"📋 Round Participation:")
            logger.info(f"   • Participated in {len(round_history)} aggregation rounds")
            for i, round_info in enumerate(round_history, 1):
                logger.info(f"   • Round {i}: {round_info.get('workflow_id', 'unknown')} (received at {datetime.fromtimestamp(round_info.get('received_time', 0)).strftime('%H:%M:%S')})")
            
            logger.info(f"{'#'*80}")
            
        except KeyboardInterrupt:
            session_duration = time.time() - session_start_time if 'session_start_time' in locals() else 0
            logger.info(f"\n⏹️ FEDERATED LEARNING SESSION INTERRUPTED")
            logger.info(f"   • Interrupted by user (Ctrl+C)")
            logger.info(f"   • Session duration before interruption: {session_duration:.1f}s")
            logger.info(f"   • Node: {self.node_id}")
        except Exception as e:
            session_duration = time.time() - session_start_time if 'session_start_time' in locals() else 0
            logger.error(f"\n❌ FEDERATED LEARNING SESSION FAILED")
            logger.error(f"   • Error: {str(e)}")
            logger.error(f"   • Session duration before failure: {session_duration:.1f}s")
            logger.error(f"   • Node: {self.node_id}")
            logger.error(f"   • Stack trace will follow...")
            raise
        finally:
            # Cleanup
            try:
                await fed_response_manager.shutdown()
            except:
                pass

    async def _wait_for_training_completion(self, fed_response_manager, workflow_id: str, 
                                          schedule_id: str, timeout: int = 120) -> Optional[Dict]:
        """Wait for training to complete and return trained weights"""
        import asyncio
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Check if trained weights are available
            weights_data = await fed_response_manager.get_trained_weights_for_aggregation(workflow_id, schedule_id)
            
            if weights_data and 'trained_weights' in weights_data:
                logger.info(f"✓ Training completed for schedule {schedule_id}")
                return weights_data['trained_weights']
            
            # Wait and check again
            await asyncio.sleep(5.0)
        
        logger.error(f"Training timeout for schedule {schedule_id}")
        return None

    async def _wait_for_aggregated_model(self, fed_response_manager, workflow_id: str, 
                                       round_number: int, timeout: int = 180) -> Optional[Dict]:
        """Wait for aggregated model to be received and return weights"""
        import asyncio
        
        start_time = time.time()
        model_key = f"{workflow_id}_round_{round_number}"
        
        while time.time() - start_time < timeout:
            # Check if aggregated model is available
            if model_key in fed_response_manager.aggregated_models:
                logger.info(f"✓ Received aggregated model for round {round_number}")
                
                # Log model details
                model_data = fed_response_manager.aggregated_models[model_key]
                logger.info(f"  Participating nodes: {model_data['participating_nodes']}")
                logger.info(f"  Aggregator: {model_data['aggregator_node']}")
                
                # Return aggregated weights for local validation
                return model_data.get('aggregated_weights', {})
            
            # Wait and check again
            await asyncio.sleep(5.0)
        
        logger.warning(f"Aggregated model timeout for round {round_number}")
        return None


def auto_detect_node_id() -> Optional[str]:
    """Auto-detect node ID from hostname or environment"""
    try:
        import socket
        hostname = socket.gethostname()
        
        # Check if running in Kubernetes pod
        if 'iot-' in hostname:
            # Extract iot-X pattern
            match = re.search(r'iot-(\d+)', hostname)
            if match:
                return f"iot-{match.group(1)}"
        
        # Check environment variable
        node_id = os.getenv('IOT_NODE_ID')
        if node_id:
            return node_id
        
        return None
        
    except Exception as e:
        logger.error(f"Error auto-detecting node ID: {e}")
        return None


def main():
    """Main entry point"""
    startup_time = time.time()
    
    logger.info(f"🚀 MAIN: Application startup initiated")
    logger.info(f"   • Startup time: {datetime.now().isoformat()}")
    logger.info(f"   • Python version: {sys.version.split()[0]}")
    
    parser = argparse.ArgumentParser(
        description='MNIST Federated Learning Node for TrustMesh'
    )
    parser.add_argument(
        '--workflow-id',
        type=str,
        required=True,
        help='Workflow ID for the federated learning experiment'
    )
    parser.add_argument(
        '--node-id',
        type=str,
        help='Node ID override (by default auto-detects from hostname)'
    )
    parser.add_argument(
        '--max-rounds',
        type=int,
        default=5,
        help='Maximum number of federated rounds (default: 5)'
    )
    
    logger.info(f"📝 ARGS: Parsing command line arguments")
    args = parser.parse_args()
    logger.info(f"   • Workflow ID: {args.workflow_id}")
    logger.info(f"   • Node ID (override): {args.node_id}")
    logger.info(f"   • Max rounds: {args.max_rounds}")
    
    # Determine node ID - auto-detect by default, override if provided
    node_id = args.node_id
    
    if not node_id:
        logger.info(f"🔍 NODE ID: Auto-detecting from hostname (default behavior)...")
        node_id = auto_detect_node_id()
        if node_id:
            logger.info(f"   • Auto-detected node ID: {node_id}")
        else:
            logger.warning(f"   • Auto-detection failed")
    else:
        logger.info(f"   • Using provided node ID: {node_id}")
    
    if not node_id:
        logger.error(f"❌ NODE ID: Required but could not be determined")
        logger.error(f"   • Auto-detection failed - no 'iot-X' pattern in hostname and no IOT_NODE_ID env var")
        logger.error(f"   • Use --node-id parameter to override")
        logger.error(f"   • Example: python mnist-federated-learning-simulation.py --workflow-id workflow-123 --node-id iot-0")
        return
    
    logger.info(f"✅ NODE ID: Successfully determined as {node_id}")
    
    # Print startup banner
    print(f"\n{'='*80}")
    print(f"🤖 MNIST FEDERATED LEARNING NODE")
    print(f"{'='*80}")
    print(f"🏷️  Node ID: {node_id}")
    print(f"🔄 Workflow ID: {args.workflow_id}")
    print(f"🔢 Max Rounds: {args.max_rounds}")
    print(f"🔌 Federated Extension: {'Available' if FEDERATED_AVAILABLE else 'Not Available'}")
    print(f"🕰️ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    logger.info(f"🏁 STARTUP BANNER: Displayed to user")
    
    try:
        logger.info(f"📱 DEVICE MANAGER: Initializing IoT device manager")
        logger.info(f"   • Device ID: {node_id}")
        
        # Initialize response manager
        device_manager = IoTDeviceManager(source=node_id, port=5555)
        logger.info(f"   • Device manager initialized successfully")
        
        # Create federated node
        logger.info(f"🤖 FEDERATED NODE: Creating MNIST federated learning node")
        logger.info(f"   • Node ID: {node_id}")
        logger.info(f"   • Loading MNIST data partition...")
        
        fl_node = MNISTFederatedNode(
            node_id=node_id,
            device_manager=device_manager
        )
        
        node_init_duration = time.time() - startup_time
        logger.info(f"   • Node initialized in {node_init_duration:.2f}s")
        logger.info(f"   • Training samples: {fl_node.data_partition['train_samples']}")
        logger.info(f"   • Test samples: {fl_node.data_partition['test_samples']}")
        logger.info(f"   • Assigned classes: {fl_node.assigned_classes}")
        
        # Run federated learning
        logger.info(f"🚀 LAUNCHING: Starting federated learning session")
        logger.info(f"   • Workflow: {args.workflow_id}")
        logger.info(f"   • Max rounds: {args.max_rounds}")
        logger.info(f"   • Node ready for federated learning")
        
        await_start_time = time.time()
        asyncio.run(fl_node.run_federated_learning(
            workflow_id=args.workflow_id,
            max_rounds=args.max_rounds
        ))
        
        total_duration = time.time() - startup_time
        session_duration = time.time() - await_start_time
        
        logger.info(f"✅ APPLICATION COMPLETED SUCCESSFULLY")
        logger.info(f"   • Total application runtime: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")
        logger.info(f"   • Federated learning session: {session_duration:.1f}s ({session_duration/60:.1f} minutes)")
        logger.info(f"   • Node initialization: {node_init_duration:.2f}s")
        logger.info(f"   • End time: {datetime.now().isoformat()}")
        
    except KeyboardInterrupt:
        total_duration = time.time() - startup_time
        logger.info(f"⏹️ APPLICATION INTERRUPTED BY USER")
        logger.info(f"   • Runtime before interruption: {total_duration:.1f}s")
        logger.info(f"   • Node: {node_id}")
        
    except Exception as e:
        total_duration = time.time() - startup_time
        logger.error(f"❌ APPLICATION FAILED")
        logger.error(f"   • Error: {str(e)}")
        logger.error(f"   • Runtime before failure: {total_duration:.1f}s")
        logger.error(f"   • Node: {node_id}")
        logger.error(f"   • Workflow: {args.workflow_id}")
        logger.error(f"   • Stack trace will follow...")
        raise


if __name__ == "__main__":
    main()