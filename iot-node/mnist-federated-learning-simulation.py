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
import tensorflow as tf
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
            # Load MNIST dataset
            (x_train, y_train), (x_test, y_test) = tf.keras.datasets.mnist.load_data()
            
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
    
    def is_coordinator(self) -> bool:
        """Check if this node is the round coordinator"""
        return self.node_index == 0
    
    def get_expected_nodes(self) -> List[str]:
        """Get list of expected node IDs"""
        return [f"iot-{i}" for i in range(TOTAL_NODES)]
    
    def submit_training_phase(self, workflow_id: str, round_number: int) -> Optional[str]:
        """Submit data for the training phase of federated learning"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Node {self.node_id}: Training Phase - Round {round_number}")
            logger.info(f"{'='*60}")
            
            # Prepare training data payload (subset for this round)
            samples_per_round = min(500, len(self.data_partition['x_train']) // 2)  # Use subset
            start_idx = (round_number - 1) * samples_per_round % len(self.data_partition['x_train'])
            end_idx = min(start_idx + samples_per_round, len(self.data_partition['x_train']))
            
            round_x_data = self.data_partition['x_train'][start_idx:end_idx]
            round_y_data = self.data_partition['y_train'][start_idx:end_idx]
            
            training_data = {
                'node_id': self.node_id,
                'node_index': self.node_index,
                'round_number': round_number,
                'x_train': round_x_data,
                'y_train': round_y_data,
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
            
            logger.info(f"Training on {len(round_x_data)} samples from classes {self.assigned_classes}")
            
            if FEDERATED_AVAILABLE:
                logger.info(f"Submitting training data for node {self.node_id}")
                
                # Phase 1: Submit training data
                schedule_id = transaction_creator.create_two_phase_federated_transaction(
                    training_data=training_data,
                    workflow_id=workflow_id,
                    node_id=self.node_id,
                    iot_port="5000",
                    iot_public_key="dummy_public_key",
                    phase="training",
                    round_number=round_number
                )
                
            else:
                logger.warning("Using standard transaction creator - federated learning disabled")
                schedule_id = transaction_creator.create_and_send_transactions(
                    iot_data=training_data,
                    workflow_id=workflow_id,
                    iot_port="5000",
                    iot_public_key="dummy_public_key"
                )
            
            logger.info(f"✓ Successfully submitted training phase for round {round_number}")
            logger.info(f"  Schedule ID: {schedule_id}")
            logger.info(f"  Training samples: {len(round_x_data)}")
            logger.info(f"  Classes: {self.assigned_classes}")
            
            return schedule_id
            
        except Exception as e:
            logger.error(f"✗ Failed to submit training phase for round {round_number}: {e}")
            return None

    def evaluate_model_locally(self, model_weights: Dict) -> float:
        """Evaluate model on local test data"""
        try:
            # Create model architecture (must match training task)
            model = tf.keras.Sequential([
                tf.keras.layers.Reshape((28, 28, 1), input_shape=(28, 28)),
                tf.keras.layers.Conv2D(32, (3, 3), activation='relu'),
                tf.keras.layers.MaxPooling2D((2, 2)),
                tf.keras.layers.Conv2D(64, (3, 3), activation='relu'),
                tf.keras.layers.MaxPooling2D((2, 2)),
                tf.keras.layers.Conv2D(64, (3, 3), activation='relu'),
                tf.keras.layers.Flatten(),
                tf.keras.layers.Dense(64, activation='relu'),
                tf.keras.layers.Dense(10, activation='softmax')
            ])
            
            model.compile(
                optimizer='adam',
                loss='sparse_categorical_crossentropy',
                metrics=['accuracy']
            )
            
            # Load weights
            weight_arrays = []
            for layer_name in sorted(model_weights.keys()):
                weight_arrays.append(np.array(model_weights[layer_name]))
            
            model.set_weights(weight_arrays)
            
            # Prepare test data
            x_test = np.array(self.data_partition['x_test']).astype('float32') / 255.0
            y_test = np.array(self.data_partition['y_test'])
            
            # Reshape if needed
            if len(x_test.shape) == 3:
                x_test = x_test.reshape(x_test.shape[0], 28, 28, 1)
            
            # Evaluate on local test set
            loss, accuracy = model.evaluate(x_test, y_test, verbose=0)
            
            logger.info(f"Local validation - Accuracy: {accuracy:.4f}, Loss: {loss:.4f} on {len(x_test)} samples")
            
            # Store for tracking
            self.local_model = model
            
            return accuracy
            
        except Exception as e:
            logger.error(f"Error in local model evaluation: {e}")
            return 0.0

    def submit_aggregation_phase(self, workflow_id: str, round_number: int, trained_weights: Dict) -> bool:
        """Submit trained model weights for the aggregation phase"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Node {self.node_id}: Aggregation Phase - Round {round_number}")
            logger.info(f"{'='*60}")
            
            if not trained_weights:
                logger.error("No trained weights available for aggregation")
                return False
            
            # Prepare training data for metadata (needed for aggregation context)
            training_data = {
                'assigned_classes': self.assigned_classes,
                'x_train': [],  # Empty for aggregation phase
                'total_samples': self.data_partition['total_samples']
            }
            
            logger.info(f"Submitting trained weights from {len(trained_weights)} layers")
            
            if FEDERATED_AVAILABLE:
                # Phase 2: Submit trained weights for aggregation
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
                
                logger.info(f"✓ Successfully submitted aggregation request for round {round_number}")
                logger.info(f"  Node: {self.node_id}")
                logger.info(f"  Weight layers: {list(trained_weights.keys())}")
                
                return True
                
            else:
                logger.warning("Federated learning not available - skipping aggregation phase")
                return False
                
        except Exception as e:
            logger.error(f"✗ Failed to submit aggregation phase for round {round_number}: {e}")
            return False
    
    async def run_federated_learning(self, workflow_id: str, max_rounds: int = 5):
        """Run two-phase federated learning for specified rounds"""
        logger.info(f"\n{'#'*60}")
        logger.info(f"# Starting Two-Phase MNIST Federated Learning")
        logger.info(f"# Node: {self.node_id}")
        logger.info(f"# Workflow: {workflow_id}")
        logger.info(f"# Max Rounds: {max_rounds}")
        logger.info(f"# Federated Extension: {'Available' if FEDERATED_AVAILABLE else 'Not Available'}")
        logger.info(f"{'#'*60}\n")
        
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
                logger.info(f"\n🔄 Starting Round {round_num}/{max_rounds}")
                
                # Check convergence before continuing
                if not fed_response_manager.should_continue_learning(workflow_id):
                    logger.info(f"🛑 Convergence detected - stopping at round {round_num}")
                    break
                
                # Phase 1: Submit training data and wait for training completion
                logger.info(f"📊 Phase 1: Training Phase")
                schedule_id = self.submit_training_phase(workflow_id, round_num)
                
                if not schedule_id:
                    logger.error(f"Failed to submit training phase for round {round_num}, aborting...")
                    break
                
                # Wait for training to complete and get trained weights
                logger.info(f"⏳ Waiting for training completion...")
                trained_weights = await self._wait_for_training_completion(
                    fed_response_manager, workflow_id, schedule_id, timeout=120  # 2 minutes timeout
                )
                
                if not trained_weights:
                    logger.error(f"Training failed or timed out for round {round_num}")
                    continue
                
                # Phase 2: Submit trained weights for aggregation
                logger.info(f"🔗 Phase 2: Aggregation Phase")
                aggregation_success = self.submit_aggregation_phase(workflow_id, round_num, trained_weights)
                
                if not aggregation_success:
                    logger.error(f"Failed to submit aggregation phase for round {round_num}")
                    continue
                
                # Wait for aggregated model
                logger.info(f"⏳ Waiting for aggregated model from round {round_num}...")
                aggregated_weights = await self._wait_for_aggregated_model(fed_response_manager, workflow_id, round_num, timeout=180)
                
                if aggregated_weights:
                    # Perform local validation on test data
                    logger.info(f"📊 Performing local validation on test set...")
                    local_accuracy = self.evaluate_model_locally(aggregated_weights)
                    
                    # Update convergence tracker with local accuracy
                    fed_response_manager.update_local_validation_accuracy(workflow_id, round_num, local_accuracy)
                    
                    # Check if should continue based on local validation
                    if not fed_response_manager.should_continue_learning(workflow_id):
                        logger.info(f"🛑 Convergence detected after round {round_num} based on local validation - stopping")
                        break
                
                # Brief pause between rounds
                if round_num < max_rounds:
                    logger.info(f"⏳ Round {round_num} complete, preparing for next round...")
                    time.sleep(10)
            
            logger.info(f"\n✅ Completed federated learning with {max_rounds} rounds")
            
            # Show convergence status
            convergence_status = fed_response_manager.get_convergence_status(workflow_id)
            logger.info(f"📈 Final Convergence Status: {convergence_status}")
            
            # Show round history
            round_history = fed_response_manager.get_round_history()
            logger.info(f"📋 Participated in {len(round_history)} aggregation rounds")
            
        except KeyboardInterrupt:
            logger.info(f"\n⏹️ Federated learning interrupted by user")
        except Exception as e:
            logger.error(f"\n❌ Error in federated learning: {e}")
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
    parser = argparse.ArgumentParser(
        description='MNIST Federated Learning Node for TrustMesh'
    )
    parser.add_argument(
        'workflow_id',
        type=str,
        help='Workflow ID for the federated learning experiment'
    )
    parser.add_argument(
        '--node-id',
        type=str,
        help='Node ID (e.g., iot-0, iot-1, ..., iot-4)'
    )
    parser.add_argument(
        '--max-rounds',
        type=int,
        default=5,
        help='Maximum number of federated rounds (default: 5)'
    )
    parser.add_argument(
        '--auto-detect',
        action='store_true',
        help='Auto-detect node ID from hostname'
    )
    
    args = parser.parse_args()
    
    # Determine node ID
    node_id = args.node_id
    
    if not node_id and args.auto_detect:
        node_id = auto_detect_node_id()
        if node_id:
            logger.info(f"Auto-detected node ID: {node_id}")
    
    if not node_id:
        logger.error("Node ID is required. Use --node-id or --auto-detect")
        return
    
    # Print startup banner
    print(f"\n{'='*60}")
    print(f"MNIST Federated Learning Node")
    print(f"{'='*60}")
    print(f"Node ID: {node_id}")
    print(f"Workflow ID: {args.workflow_id}")
    print(f"Max Rounds: {args.max_rounds}")
    print(f"Federated Extension: {'Available' if FEDERATED_AVAILABLE else 'Not Available'}")
    print(f"{'='*60}\n")
    
    try:
        # Initialize response manager
        device_manager = IoTDeviceManager(device_id=node_id)
        
        # Create federated node
        fl_node = MNISTFederatedNode(
            node_id=node_id,
            device_manager=device_manager
        )
        
        # Run federated learning
        asyncio.run(fl_node.run_federated_learning(
            workflow_id=args.workflow_id,
            max_rounds=args.max_rounds
        ))
        
    except Exception as e:
        logger.error(f"Failed to run federated learning: {e}")
        raise


if __name__ == "__main__":
    main()