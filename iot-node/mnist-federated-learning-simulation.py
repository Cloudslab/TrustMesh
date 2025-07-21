"""
MNIST Federated Learning Simulation for TrustMesh IoT Nodes

This script runs on individual IoT nodes to participate in federated learning.
Each node maintains its own MNIST data partition and contributes to global model training.
"""

import argparse
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
    
    def __init__(self, node_id: str, device_manager: IoTDeviceManager):
        self.node_id = node_id
        self.device_manager = device_manager
        self.node_index = self._extract_node_index(node_id)
        self.assigned_classes = self._get_assigned_classes()
        self.data_partition = None
        
        # Validate node configuration
        if self.node_index >= TOTAL_NODES:
            raise ValueError(f"Node index {self.node_index} exceeds total nodes {TOTAL_NODES}")
        
        # Load MNIST data partition
        self._load_data_partition()
        
        logger.info(f"Initialized MNIST Federated Node {node_id}")
        logger.info(f"Node index: {self.node_index}")
        logger.info(f"Assigned digit classes: {self.assigned_classes}")
        logger.info(f"Training samples: {len(self.data_partition['x_train'])}")
    
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
            
            # Store partition
            self.data_partition = {
                'x_train': x_partition.tolist(),
                'y_train': y_partition.tolist(),
                'node_index': self.node_index,
                'assigned_classes': self.assigned_classes,
                'total_samples': len(x_partition),
                'class_distribution': self._calculate_class_distribution(y_partition)
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
    
    def submit_federated_round(self, workflow_id: str, round_number: int) -> Optional[str]:
        """Submit data for a federated learning round"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Node {self.node_id}: Submitting Round {round_number}")
            logger.info(f"{'='*60}")
            
            # Prepare data payload
            data_payload = {
                'node_id': self.node_id,
                'node_index': self.node_index,
                'round_number': round_number,
                'data_partition': self.data_partition,
                'timestamp': datetime.now().isoformat(),
                'node_metadata': {
                    'assigned_classes': self.assigned_classes,
                    'total_samples': self.data_partition['total_samples'],
                    'class_distribution': self.data_partition['class_distribution']
                }
            }
            
            if FEDERATED_AVAILABLE:
                logger.info(f"Submitting federated data for node {self.node_id} (consensus-integrated)")
                
                # Submit using consensus-integrated federated transaction creator
                schedule_id = transaction_creator.create_federated_learning_transactions(
                    iot_data=data_payload,
                    workflow_id=workflow_id,
                    iot_port="5000",  # Default IoT port
                    iot_public_key="dummy_public_key",  # Will be replaced with actual key
                    node_id=self.node_id
                )
                
            else:
                # Fallback to standard transaction creator (non-federated)
                logger.warning("Using standard transaction creator - this will create separate workflows per node")
                schedule_id = transaction_creator.create_and_send_transactions(
                    iot_data=data_payload,
                    workflow_id=workflow_id,
                    iot_port="5000",
                    iot_public_key="dummy_public_key"
                )
            
            logger.info(f"✓ Successfully submitted round {round_number}")
            logger.info(f"  Schedule ID: {schedule_id}")
            logger.info(f"  Samples contributed: {self.data_partition['total_samples']}")
            logger.info(f"  Classes: {self.assigned_classes}")
            
            return schedule_id
            
        except Exception as e:
            logger.error(f"✗ Failed to submit round {round_number}: {e}")
            return None
    
    def run_federated_learning(self, workflow_id: str, max_rounds: int = 5):
        """Run federated learning for specified rounds"""
        logger.info(f"\n{'#'*60}")
        logger.info(f"# Starting MNIST Federated Learning")
        logger.info(f"# Node: {self.node_id}")
        logger.info(f"# Workflow: {workflow_id}")
        logger.info(f"# Max Rounds: {max_rounds}")
        logger.info(f"# Federated Extension: {'Available' if FEDERATED_AVAILABLE else 'Not Available'}")
        logger.info(f"{'#'*60}\n")
        
        try:
            for round_num in range(1, max_rounds + 1):
                # Submit round
                schedule_id = self.submit_federated_round(workflow_id, round_num)
                
                if not schedule_id:
                    logger.error(f"Failed to submit round {round_num}, aborting...")
                    break
                
                # Wait between rounds (except last)
                if round_num < max_rounds:
                    wait_time = FEDERATED_ROUND_INTERVAL
                    logger.info(f"⏳ Waiting {wait_time}s for round {round_num} to complete...")
                    
                    # Show progress
                    for i in range(0, wait_time, 30):
                        remaining = wait_time - i
                        logger.info(f"   {remaining}s remaining...")
                        time.sleep(min(30, remaining))
            
            logger.info(f"\n✅ Completed {max_rounds} federated learning rounds")
            
        except KeyboardInterrupt:
            logger.info(f"\n⏹️ Federated learning interrupted by user")
        except Exception as e:
            logger.error(f"\n❌ Error in federated learning: {e}")
            raise


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
        fl_node.run_federated_learning(
            workflow_id=args.workflow_id,
            max_rounds=args.max_rounds
        )
        
    except Exception as e:
        logger.error(f"Failed to run federated learning: {e}")
        raise


if __name__ == "__main__":
    main()