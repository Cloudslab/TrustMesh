import asyncio
import json
import logging
import time
import traceback
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers, models
import pickle
import base64

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MNISTLocalTrainer:
    def __init__(self):
        self.model = None
        self.epochs = 5
        self.batch_size = 32
        self.learning_rate = 0.01
        
    def create_model(self):
        """Create a simple CNN model for MNIST classification"""
        model = models.Sequential([
            layers.Conv2D(32, (3, 3), activation='relu', input_shape=(28, 28, 1)),
            layers.MaxPooling2D((2, 2)),
            layers.Conv2D(64, (3, 3), activation='relu'),
            layers.MaxPooling2D((2, 2)),
            layers.Conv2D(64, (3, 3), activation='relu'),
            layers.Flatten(),
            layers.Dense(64, activation='relu'),
            layers.Dense(10, activation='softmax')
        ])
        
        model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=self.learning_rate),
                     loss='sparse_categorical_crossentropy',
                     metrics=['accuracy'])
        
        return model
    
    def prepare_data(self, data_partition):
        """Prepare MNIST data partition for training"""
        try:
            # Convert data partition to numpy arrays
            x_train = np.array(data_partition['x_train'], dtype=np.float32)
            y_train = np.array(data_partition['y_train'], dtype=np.int32)
            
            # Reshape and normalize
            x_train = x_train.reshape(-1, 28, 28, 1) / 255.0
            
            logger.info(f"Prepared training data: {x_train.shape} samples")
            return x_train, y_train
            
        except Exception as e:
            logger.error(f"Error preparing data: {e}")
            raise
    
    def load_global_model(self, model_weights):
        """Load global model weights if provided"""
        try:
            if model_weights:
                # Decode base64 encoded weights
                weights_data = base64.b64decode(model_weights)
                weights = pickle.loads(weights_data)
                
                # Create model and set weights
                self.model = self.create_model()
                self.model.set_weights(weights)
                logger.info("Loaded global model weights")
            else:
                # Initialize new model for first round
                self.model = self.create_model()
                logger.info("Initialized new model for first training round")
                
        except Exception as e:
            logger.error(f"Error loading global model: {e}")
            # Fallback to new model
            self.model = self.create_model()
    
    def train_local_model(self, x_train, y_train):
        """Train model on local data partition"""
        try:
            start_time = time.time()
            
            # Train the model
            history = self.model.fit(
                x_train, y_train,
                epochs=self.epochs,
                batch_size=self.batch_size,
                validation_split=0.2,
                verbose=1
            )
            
            training_time = time.time() - start_time
            
            # Get final metrics
            final_loss = history.history['loss'][-1]
            final_accuracy = history.history['accuracy'][-1]
            
            logger.info(f"Local training completed in {training_time:.2f}s")
            logger.info(f"Final loss: {final_loss:.4f}, Final accuracy: {final_accuracy:.4f}")
            
            return {
                'training_time': training_time,
                'final_loss': final_loss,
                'final_accuracy': final_accuracy,
                'samples_trained': len(x_train)
            }
            
        except Exception as e:
            logger.error(f"Error during local training: {e}")
            raise
    
    def get_model_weights(self):
        """Extract and encode model weights for transmission"""
        try:
            weights = self.model.get_weights()
            
            # Serialize and encode weights
            weights_data = pickle.dumps(weights)
            encoded_weights = base64.b64encode(weights_data).decode('utf-8')
            
            return encoded_weights
            
        except Exception as e:
            logger.error(f"Error extracting model weights: {e}")
            raise
    
    def process_federated_training(self, node_submission):
        """Process training for a single node in federated learning"""
        try:
            start_time = time.time()
            
            # Extract node data
            node_id = node_submission['node_id']
            node_data = node_submission['data']
            
            # Extract training parameters
            data_partition = node_data.get('data_partition', {})
            round_number = node_data.get('round_number', 1)
            global_model_weights = node_data.get('global_model_weights', None)
            
            logger.info(f"Starting local training for node {node_id}, round {round_number}")
            
            # Load global model weights
            self.load_global_model(global_model_weights)
            
            # Prepare training data
            x_train, y_train = self.prepare_data(data_partition)
            
            # Train local model
            training_metrics = self.train_local_model(x_train, y_train)
            
            # Extract updated model weights
            updated_weights = self.get_model_weights()
            
            processing_time = time.time() - start_time
            
            result = {
                'node_id': node_id,
                'round_number': round_number,
                'model_weights': updated_weights,
                'training_metrics': training_metrics,
                'processing_time': processing_time,
                'node_metadata': node_data.get('node_metadata', {})
            }
            
            logger.info(f"Local training completed for node {node_id} in {processing_time:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in processing training request: {e}")
            raise

async def read_json(reader):
    """Read JSON data from stream"""
    try:
        data = await reader.read(1024 * 1024 * 10)  # 10MB buffer for model data
        if not data:
            return None
        
        json_str = data.decode('utf-8')
        return json.loads(json_str)
        
    except Exception as e:
        logger.error(f"Error reading JSON: {e}")
        return None

async def handle_client(reader, writer):
    """Handle client connections for local training requests"""
    client_addr = writer.get_extra_info('peername')
    logger.info(f"Client connected from {client_addr}")
    
    try:
        # Read input data
        input_data = await read_json(reader)
        if input_data is None:
            raise ValueError("No input data received")
        
        # Check if this is federated data
        federated_metadata = input_data.get('federated_metadata', {})
        is_federated = federated_metadata.get('is_federated', False)
        
        if is_federated:
            logger.info(f"Processing federated learning request with {len(input_data['data'])} nodes")
            
            # Process each node's training data
            trainer = MNISTLocalTrainer()
            results = []
            
            for node_submission in input_data['data']:
                try:
                    result = trainer.process_federated_training(node_submission)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error training node {node_submission.get('node_id', 'unknown')}: {e}")
                    # Continue with other nodes
            
            # Package results for aggregation
            output_data = {
                'data': results,
                'total_task_time': input_data.get('total_task_time', 0.0) + sum(r['processing_time'] for r in results),
                'federated_metadata': {
                    'is_federated': True,
                    'nodes_trained': len(results),
                    'round_number': results[0]['round_number'] if results else 1
                }
            }
            
        else:
            # Handle single node training (backward compatibility)
            trainer = MNISTLocalTrainer()
            result = trainer.process_training_request(input_data)
            
            output_data = {
                'data': [result],
                'total_task_time': input_data.get('total_task_time', 0.0) + result['processing_time']
            }
        
        # Send response
        response = json.dumps(output_data, indent=2)
        writer.write(response.encode('utf-8'))
        await writer.drain()
        
        logger.info(f"Successfully processed training request from {client_addr}")
        
    except Exception as e:
        logger.error(f"Error handling client {client_addr}: {e}")
        logger.error(traceback.format_exc())
        
        # Send error response
        error_response = {
            'error': str(e),
            'total_task_time': input_data.get('total_task_time', 0.0) if 'input_data' in locals() else 0.0
        }
        response = json.dumps(error_response)
        writer.write(response.encode('utf-8'))
        await writer.drain()
        
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    """Start the local training server"""
    logger.info("Starting MNIST Local Training Server on port 12345")
    
    server = await asyncio.start_server(
        handle_client, 
        '0.0.0.0', 
        12345
    )
    
    addr = server.sockets[0].getsockname()
    logger.info(f"Local training server running on {addr[0]}:{addr[1]}")
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())