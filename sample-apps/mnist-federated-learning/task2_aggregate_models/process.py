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
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FederatedAggregator:
    def __init__(self):
        self.model = None
        
    def create_model(self):
        """Create the same CNN model architecture as used in local training"""
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
        
        model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
                     loss='sparse_categorical_crossentropy',
                     metrics=['accuracy'])
        
        return model
    
    def decode_model_weights(self, encoded_weights: str) -> List[np.ndarray]:
        """Decode base64 encoded model weights"""
        try:
            weights_data = base64.b64decode(encoded_weights)
            weights = pickle.loads(weights_data)
            return weights
        except Exception as e:
            logger.error(f"Error decoding model weights: {e}")
            raise
    
    def encode_model_weights(self, weights: List[np.ndarray]) -> str:
        """Encode model weights as base64 string"""
        try:
            weights_data = pickle.dumps(weights)
            encoded_weights = base64.b64encode(weights_data).decode('utf-8')
            return encoded_weights
        except Exception as e:
            logger.error(f"Error encoding model weights: {e}")
            raise
    
    def validate_federated_round(self, client_updates: List[Dict[str, Any]]) -> bool:
        """Validate that we have updates from all expected nodes"""
        expected_nodes = 5  # iot-0, iot-1, iot-2, iot-3, iot-4
        expected_classes = {
            0: [0, 1], 1: [2, 3], 2: [4, 5], 3: [6, 7], 4: [8, 9]
        }
        
        if len(client_updates) != expected_nodes:
            raise ValueError(f"Expected {expected_nodes} client updates, got {len(client_updates)}")
        
        # Validate each node has correct classes
        nodes_present = set()
        for update in client_updates:
            # Extract node metadata
            node_metadata = update.get('node_metadata', {})
            if not node_metadata:
                # Try getting node index from node_id
                node_id = update.get('node_id', '')
                try:
                    import re
                    match = re.search(r'(\d+)(?!.*\d)', node_id)
                    if match:
                        node_index = int(match.group(1))
                        assigned_classes = [node_index * 2, node_index * 2 + 1]
                        node_metadata = {
                            'node_index': node_index,
                            'assigned_classes': assigned_classes
                        }
                except:
                    pass
            
            node_index = node_metadata.get('node_index')
            assigned_classes = node_metadata.get('assigned_classes', [])
            
            if node_index is None:
                logger.warning(f"Missing node_index in update from {update.get('node_id', 'unknown')}")
                continue
            
            if node_index in nodes_present:
                raise ValueError(f"Duplicate update from node {node_index}")
            
            nodes_present.add(node_index)
            
            expected_for_node = expected_classes.get(node_index, [])
            if assigned_classes and assigned_classes != expected_for_node:
                logger.warning(f"Node {node_index} has classes {assigned_classes}, expected {expected_for_node}")
        
        # Check if we have minimum required nodes
        if len(nodes_present) < 3:
            raise ValueError(f"Insufficient nodes: {len(nodes_present)}, minimum required: 3")
        
        logger.info(f"✓ Validated updates from {len(nodes_present)} nodes: {sorted(nodes_present)}")
        return True
    
    def federated_averaging(self, client_updates: List[Dict[str, Any]]) -> List[np.ndarray]:
        """
        Implement FedAvg algorithm to aggregate model weights from multiple clients
        """
        try:
            if not client_updates:
                raise ValueError("No client updates provided for aggregation")
            
            # Extract weights and sample counts from all clients
            all_weights = []
            sample_counts = []
            
            for update in client_updates:
                weights = self.decode_model_weights(update['model_weights'])
                samples = update['training_metrics']['samples_trained']
                
                all_weights.append(weights)
                sample_counts.append(samples)
                
            logger.info(f"Aggregating weights from {len(all_weights)} clients")
            logger.info(f"Sample counts: {sample_counts}")
            
            # Calculate total samples for weighted averaging
            total_samples = sum(sample_counts)
            
            # Initialize aggregated weights with zeros
            aggregated_weights = []
            for layer_idx in range(len(all_weights[0])):
                layer_shape = all_weights[0][layer_idx].shape
                aggregated_weights.append(np.zeros(layer_shape, dtype=np.float32))
            
            # Weighted average of weights based on number of samples
            for client_idx, (weights, sample_count) in enumerate(zip(all_weights, sample_counts)):
                weight_factor = sample_count / total_samples
                
                for layer_idx in range(len(weights)):
                    aggregated_weights[layer_idx] += weight_factor * weights[layer_idx]
                
                logger.info(f"Client {client_idx}: weight factor = {weight_factor:.4f}, samples = {sample_count}")
            
            logger.info("Federated averaging completed successfully")
            return aggregated_weights
            
        except Exception as e:
            logger.error(f"Error in federated averaging: {e}")
            raise
    
    def calculate_aggregation_metrics(self, client_updates: List[Dict[str, Any]]) -> Dict[str, float]:
        """Calculate metrics from the aggregation round"""
        try:
            total_samples = 0
            total_loss = 0.0
            total_accuracy = 0.0
            total_training_time = 0.0
            
            for update in client_updates:
                metrics = update['training_metrics']
                samples = metrics['samples_trained']
                
                total_samples += samples
                total_loss += metrics['final_loss'] * samples
                total_accuracy += metrics['final_accuracy'] * samples
                total_training_time += metrics['training_time']
            
            # Calculate weighted averages
            avg_loss = total_loss / total_samples if total_samples > 0 else 0.0
            avg_accuracy = total_accuracy / total_samples if total_samples > 0 else 0.0
            avg_training_time = total_training_time / len(client_updates) if client_updates else 0.0
            
            return {
                'total_samples': total_samples,
                'num_clients': len(client_updates),
                'avg_loss': avg_loss,
                'avg_accuracy': avg_accuracy,
                'avg_training_time': avg_training_time
            }
            
        except Exception as e:
            logger.error(f"Error calculating aggregation metrics: {e}")
            return {}
    
    def process_aggregation_request(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Main processing function for federated model aggregation"""
        try:
            start_time = time.time()
            
            # Extract client updates from input
            client_updates = input_data.get('data', [])
            federated_metadata = input_data.get('federated_metadata', {})
            round_number = federated_metadata.get('round_number', 1)
            
            if not client_updates:
                raise ValueError("No client updates found in input data")
            
            logger.info(f"Starting aggregation for round {round_number} with {len(client_updates)} clients")
            
            # Validate that we have expected nodes
            self.validate_federated_round(client_updates)
            
            # Log node participation details
            for update in client_updates:
                node_id = update.get('node_id', 'unknown')
                samples = update.get('training_metrics', {}).get('samples_trained', 0)
                accuracy = update.get('training_metrics', {}).get('final_accuracy', 0)
                logger.info(f"  Node {node_id}: {samples} samples, accuracy: {accuracy:.4f}")
            
            # Perform federated averaging
            aggregated_weights = self.federated_averaging(client_updates)
            
            # Encode aggregated weights for transmission
            encoded_weights = self.encode_model_weights(aggregated_weights)
            
            # Calculate aggregation metrics
            metrics = self.calculate_aggregation_metrics(client_updates)
            
            processing_time = time.time() - start_time
            
            result = {
                'round_number': round_number,
                'aggregated_model_weights': encoded_weights,
                'aggregation_metrics': metrics,
                'processing_time': processing_time,
                'nodes_participated': len(client_updates),
                'data': [{
                    'global_model_weights': encoded_weights,
                    'round_number': round_number + 1,
                    'aggregation_complete': True,
                    'nodes_participated': len(client_updates),
                    'aggregation_metrics': metrics
                }]
            }
            
            logger.info(f"✅ Aggregation completed for round {round_number} in {processing_time:.2f}s")
            logger.info(f"📊 Metrics - Avg Loss: {metrics.get('avg_loss', 0):.4f}, "
                       f"Avg Accuracy: {metrics.get('avg_accuracy', 0):.4f}")
            logger.info(f"👥 Participated Nodes: {metrics.get('num_clients', 0)}, "
                       f"Total Samples: {metrics.get('total_samples', 0)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in processing aggregation request: {e}")
            raise

async def read_json(reader):
    """Read JSON data from stream"""
    try:
        data = await reader.read(1024 * 1024 * 20)  # 20MB buffer for model weights
        if not data:
            return None
        
        json_str = data.decode('utf-8')
        return json.loads(json_str)
        
    except Exception as e:
        logger.error(f"Error reading JSON: {e}")
        return None

async def handle_client(reader, writer):
    """Handle client connections for aggregation requests"""
    client_addr = writer.get_extra_info('peername')
    logger.info(f"Client connected from {client_addr}")
    
    try:
        # Read input data
        input_data = await read_json(reader)
        if input_data is None:
            raise ValueError("No input data received")
        
        # Process aggregation request
        aggregator = FederatedAggregator()
        result = aggregator.process_aggregation_request(input_data)
        
        # Add total task time from input
        total_task_time = input_data.get('total_task_time', 0.0)
        result['total_task_time'] = total_task_time + result['processing_time']
        
        # Send response
        response = json.dumps(result, indent=2)
        writer.write(response.encode('utf-8'))
        await writer.drain()
        
        logger.info(f"Successfully processed aggregation request from {client_addr}")
        
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
    """Start the aggregation server"""
    logger.info("Starting MNIST Federated Aggregation Server on port 12345")
    
    server = await asyncio.start_server(
        handle_client, 
        '0.0.0.0', 
        12345
    )
    
    addr = server.sockets[0].getsockname()
    logger.info(f"Aggregation server running on {addr[0]}:{addr[1]}")
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())