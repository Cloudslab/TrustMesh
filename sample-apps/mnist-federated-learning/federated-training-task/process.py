#!/usr/bin/env python3
"""
MNIST Federated Learning Training Task

This is the single training application for MNIST federated learning in TrustMesh.
It receives training data from IoT nodes, performs local training, and returns trained model weights.
"""

import json
import logging
import numpy as np
import os
import sys
import time
from typing import Dict, List, Any, Tuple

# TensorFlow/Keras for MNIST training
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Model configuration
MNIST_INPUT_SHAPE = (28, 28, 1)
NUM_CLASSES = 10
EPOCHS_PER_ROUND = 3
BATCH_SIZE = 32


class MNISTFederatedTrainer:
    """MNIST Federated Learning Trainer for TrustMesh compute nodes"""
    
    def __init__(self):
        self.model = None
        self.training_history = []
        logger.info("Initialized MNIST Federated Trainer")
    
    def create_model(self) -> keras.Model:
        """Create a simple CNN model for MNIST"""
        model = keras.Sequential([
            layers.Reshape(MNIST_INPUT_SHAPE, input_shape=(28, 28)),
            layers.Conv2D(32, (3, 3), activation='relu'),
            layers.MaxPooling2D((2, 2)),
            layers.Conv2D(64, (3, 3), activation='relu'),
            layers.MaxPooling2D((2, 2)),
            layers.Conv2D(64, (3, 3), activation='relu'),
            layers.Flatten(),
            layers.Dense(64, activation='relu'),
            layers.Dense(NUM_CLASSES, activation='softmax')
        ])
        
        model.compile(
            optimizer='adam',
            loss='sparse_categorical_crossentropy',
            metrics=['accuracy']
        )
        
        logger.info(f"Created MNIST model with {model.count_params()} parameters")
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
            
            # Convert lists back to numpy arrays and set weights
            weight_arrays = []
            for layer_name in sorted(global_weights.keys()):
                weight_arrays.append(np.array(global_weights[layer_name]))
            
            self.model.set_weights(weight_arrays)
            logger.info(f"Loaded global weights from {len(global_weights)} layers")
            return True
            
        except Exception as e:
            logger.error(f"Error loading global weights: {e}")
            return False
    
    def train_local_model(self, x_train: np.ndarray, y_train: np.ndarray) -> Dict[str, Any]:
        """Train the model locally on provided data"""
        try:
            logger.info(f"Starting local training on {len(x_train)} samples for {EPOCHS_PER_ROUND} epochs")
            
            # Train the model
            history = self.model.fit(
                x_train, y_train,
                batch_size=BATCH_SIZE,
                epochs=EPOCHS_PER_ROUND,
                validation_split=0.1,
                verbose=1
            )
            
            # Extract trained weights
            trained_weights = {}
            for i, layer_weights in enumerate(self.model.get_weights()):
                layer_name = f"layer_{i}"
                trained_weights[layer_name] = layer_weights.tolist()
            
            # Compute training metrics
            final_loss = history.history['loss'][-1]
            final_accuracy = history.history['accuracy'][-1]
            val_loss = history.history['val_loss'][-1] if 'val_loss' in history.history else None
            val_accuracy = history.history['val_accuracy'][-1] if 'val_accuracy' in history.history else None
            
            training_result = {
                'trained_weights': trained_weights,
                'training_metrics': {
                    'final_loss': float(final_loss),
                    'final_accuracy': float(final_accuracy),
                    'val_loss': float(val_loss) if val_loss else None,
                    'val_accuracy': float(val_accuracy) if val_accuracy else None,
                    'epochs_trained': EPOCHS_PER_ROUND,
                    'samples_trained': len(x_train)
                },
                'model_info': {
                    'total_parameters': self.model.count_params(),
                    'layer_count': len(self.model.layers),
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


def main():
    """Main entry point for the federated training task"""
    try:
        logger.info("Starting MNIST Federated Learning Training Task")
        
        # Read input data from stdin (TrustMesh standard)
        input_data = json.loads(sys.stdin.read())
        logger.info(f"Received input data with keys: {list(input_data.keys())}")
        
        # Initialize trainer
        trainer = MNISTFederatedTrainer()
        
        # Process federated training
        result = trainer.process_federated_training(input_data)
        
        # Output result to stdout (TrustMesh standard)
        print(json.dumps(result))
        logger.info("Training task completed successfully")
        
    except Exception as e:
        logger.error(f"Training task failed: {e}")
        error_result = {
            'error': str(e),
            'status': 'failed',
            'trained_weights': None,
            'training_metrics': None
        }
        print(json.dumps(error_result))
        sys.exit(1)


if __name__ == "__main__":
    main()