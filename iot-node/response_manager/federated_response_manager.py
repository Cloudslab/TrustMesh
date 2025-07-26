"""
Federated Response Manager for IoT Nodes

This module handles receiving aggregated model weights from the federated learning system
and manages the two-phase federated learning flow for IoT nodes.
"""

import asyncio
import json
import logging
import os
import ssl
import tempfile
import time
import zmq
import zmq.asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime

from coredis import RedisCluster
from coredis.exceptions import RedisError

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

# ZMQ configuration
ZMQ_RESPONSE_PORT = int(os.getenv('ZMQ_RESPONSE_PORT', '5555'))

logger = logging.getLogger(__name__)


class FederatedResponseManager:
    """
    Manages federated learning responses for IoT nodes.
    Handles receiving aggregated model weights and coordinating federated rounds.
    """
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.redis = None
        self.zmq_context = None
        self.zmq_socket = None
        self.running = False
        self.aggregated_models = {}
        self.round_history = []
        
        # Event-driven coordination
        self.model_events = {}  # workflow_round -> asyncio.Event
        self.training_events = {}  # schedule_id -> asyncio.Event
        self.convergence_tracker = {}
        
        # Initialize connections
        self._initialize_redis()
        self._initialize_zmq()
        
        logger.info(f"Initialized FederatedResponseManager for node {node_id}")

    def _initialize_redis(self):
        """Initialize Redis connection"""
        logger.info("Starting Redis initialization for federated response manager")
        temp_files = []
        try:
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            ssl_context.maximum_version = ssl.TLSVersion.TLSv1_3

            if REDIS_SSL_CA:
                ca_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.crt')
                ca_file.write(REDIS_SSL_CA)
                ca_file.flush()
                temp_files.append(ca_file.name)
                ssl_context.load_verify_locations(cafile=ca_file.name)

            if REDIS_SSL_CERT and REDIS_SSL_KEY:
                cert_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.crt')
                key_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.key')
                cert_file.write(REDIS_SSL_CERT)
                key_file.write(REDIS_SSL_KEY)
                cert_file.flush()
                key_file.flush()
                temp_files.extend([cert_file.name, key_file.name])
                ssl_context.load_cert_chain(
                    certfile=cert_file.name,
                    keyfile=key_file.name
                )

            logger.info(f"Attempting to connect to Redis cluster at {REDIS_HOST}:{REDIS_PORT}")

            self.redis = RedisCluster(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                ssl=True,
                ssl_context=ssl_context,
                decode_responses=True
            )

            # Test connection
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.redis.ping())
            loop.close()
            
            logger.info("Connected to Redis cluster successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
        finally:
            for file_path in temp_files:
                try:
                    os.unlink(file_path)
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")

    def _initialize_zmq(self):
        """Initialize ZMQ socket for receiving responses"""
        try:
            self.zmq_context = zmq.asyncio.Context()
            self.zmq_socket = self.zmq_context.socket(zmq.REP)
            self.zmq_socket.bind(f"tcp://*:{ZMQ_RESPONSE_PORT}")
            
            logger.info(f"ZMQ response socket bound to port {ZMQ_RESPONSE_PORT}")
            
        except Exception as e:
            logger.error(f"Failed to initialize ZMQ: {str(e)}")
            raise

    async def start_federated_response_handler(self):
        """Start the federated response handler (ZMQ only)"""
        logger.info(f"Starting federated response handler for node {self.node_id} (ZMQ only)")
        self.running = True
        
        # Only start ZMQ response handler (compute-to-IoT communication)
        zmq_task = asyncio.create_task(self._handle_zmq_responses())
        
        try:
            await zmq_task
        except Exception as e:
            logger.error(f"Error in federated response handler: {e}")
        finally:
            self.running = False

    async def _handle_zmq_responses(self):
        """Handle ZMQ responses from compute nodes (both training results and aggregated models)"""
        try:
            while self.running:
                try:
                    # Wait for response with timeout
                    response_bytes = await asyncio.wait_for(
                        self.zmq_socket.recv(), 
                        timeout=5.0
                    )
                    
                    response_data = json.loads(response_bytes.decode())
                    
                    # Determine response type and process accordingly
                    event_type = response_data.get('event_type')
                    
                    if event_type == 'aggregated_model_ready':
                        # Handle aggregated model from aggregator compute node
                        await self._handle_aggregated_model(response_data)
                    else:
                        # Handle training results or other compute responses
                        await self._process_compute_response(response_data)
                    
                    # Send acknowledgment
                    ack = json.dumps({"status": "received", "timestamp": time.time()})
                    await self.zmq_socket.send_string(ack)
                    
                except asyncio.TimeoutError:
                    # No message received, continue
                    continue
                except Exception as e:
                    logger.error(f"Error handling ZMQ response: {e}")
                    # Send error acknowledgment
                    error_ack = json.dumps({"status": "error", "message": str(e)})
                    try:
                        await self.zmq_socket.send_string(error_ack)
                    except:
                        pass
                        
        except Exception as e:
            logger.error(f"Error in ZMQ response handler: {e}")

    async def _handle_aggregated_model(self, model_data: Dict[str, Any]):
        """Handle received aggregated model"""
        try:
            logger.info(f"Received aggregated model for node {self.node_id}")
            
            workflow_id = model_data.get('workflow_id')
            round_number = model_data.get('round_number')
            aggregated_weights = model_data.get('aggregated_weights')
            participating_nodes = model_data.get('participating_nodes', [])
            aggregator_node = model_data.get('aggregator_node')
            
            if not all([workflow_id, round_number is not None, aggregated_weights]):
                logger.error("Invalid aggregated model data received")
                return
            
            logger.info(f"Processing aggregated model - Workflow: {workflow_id}, Round: {round_number}")
            logger.info(f"Participating nodes: {participating_nodes}")
            logger.info(f"Aggregator node: {aggregator_node}")
            
            # Store aggregated model
            model_key = f"{workflow_id}_round_{round_number}"
            self.aggregated_models[model_key] = {
                'workflow_id': workflow_id,
                'round_number': round_number,
                'aggregated_weights': aggregated_weights,
                'participating_nodes': participating_nodes,
                'aggregator_node': aggregator_node,
                'received_time': time.time()
            }
            
            # Evaluate model and update convergence tracking
            await self._evaluate_and_track_convergence(workflow_id, round_number, aggregated_weights)
            
            # Store round history
            self.round_history.append({
                'workflow_id': workflow_id,
                'round_number': round_number,
                'node_id': self.node_id,
                'received_time': time.time(),
                'participating_nodes': participating_nodes
            })
            
            logger.info(f"Successfully processed aggregated model for round {round_number}")
            
            # NEW: Signal event for waiting coroutines
            if model_key in self.model_events:
                self.model_events[model_key].set()
                logger.info(f"Signaled model event for {model_key}")
            
        except Exception as e:
            logger.error(f"Error handling aggregated model: {e}")

    async def _process_compute_response(self, response_data: Dict[str, Any]):
        """Process responses from compute nodes (training results)"""
        try:
            logger.info(f"Processing compute response for node {self.node_id}")
            
            schedule_id = response_data.get('schedule_id')
            status = response_data.get('status')
            result_data = response_data.get('result', {})
            
            if status == 'COMPLETE' and 'trained_weights' in result_data:
                # Training completed, extract trained weights
                trained_weights = result_data['trained_weights']
                workflow_id = response_data.get('workflow_id')
                
                logger.info(f"Training completed for schedule {schedule_id}, extracting weights")
                
                # Store trained weights for potential aggregation request
                await self._store_trained_weights(workflow_id, schedule_id, trained_weights, result_data)
                
            elif status == 'ERROR':
                logger.error(f"Training failed for schedule {schedule_id}: {result_data.get('error')}")
                
            else:
                logger.info(f"Received intermediate response - Status: {status}")
                
        except Exception as e:
            logger.error(f"Error processing compute response: {e}")

    async def _store_trained_weights(self, workflow_id: str, schedule_id: str, 
                                   trained_weights: Dict, result_data: Dict):
        """Store trained weights for later aggregation request"""
        try:
            weights_key = f"trained_weights_{self.node_id}_{workflow_id}_{schedule_id}"
            
            weights_data = {
                'node_id': self.node_id,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id,
                'trained_weights': trained_weights,
                'training_metadata': result_data.get('metadata', {}),
                'training_completed_time': time.time()
            }
            
            # Store in Redis for later retrieval
            await self.redis.setex(weights_key, 3600, json.dumps(weights_data))  # 1 hour TTL
            
            logger.info(f"Stored trained weights for {self.node_id} - {workflow_id}")
            
            # NEW: Signal event for waiting coroutines
            if schedule_id in self.training_events:
                self.training_events[schedule_id].set()
                logger.info(f"Signaled training event for {schedule_id}")
            
        except Exception as e:
            logger.error(f"Error storing trained weights: {e}")

    async def _evaluate_and_track_convergence(self, workflow_id: str, round_number: int, 
                                            aggregated_weights: Dict):
        """Track convergence based on local validation - actual evaluation happens elsewhere"""
        try:
            # Note: We don't evaluate here anymore - the main simulation will:
            # 1. Receive aggregated weights
            # 2. Evaluate on local test data  
            # 3. Update convergence tracker
            # This method now just logs that we received the model
            
            logger.info(f"Received aggregated model for {workflow_id} round {round_number}")
            logger.info(f"Local validation will be performed by the main simulation")
            
            # Store blockchain validation metrics if available (for logging only)
            model_key = f"{workflow_id}_round_{round_number}"
            stored_model = self.aggregated_models.get(model_key, {})
            validation_metrics = stored_model.get('validation_metrics', {})
            blockchain_accuracy = validation_metrics.get('accuracy', 0.0)
            
            if blockchain_accuracy > 0.0:
                logger.info(f"Blockchain consensus validation score: {blockchain_accuracy:.3f}")
            
        except Exception as e:
            logger.error(f"Error in convergence tracking: {e}")

    async def get_trained_weights_for_aggregation(self, workflow_id: str, schedule_id: str) -> Optional[Dict]:
        """Retrieve trained weights for aggregation request"""
        try:
            weights_key = f"trained_weights_{self.node_id}_{workflow_id}_{schedule_id}"
            weights_data = await self.redis.get(weights_key)
            
            if weights_data:
                return json.loads(weights_data)
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving trained weights: {e}")
            return None

    def should_continue_learning(self, workflow_id: str) -> bool:
        """Check if node should continue participating in federated learning"""
        try:
            if workflow_id in self.convergence_tracker:
                return self.convergence_tracker[workflow_id]['should_continue']
            return True  # Continue by default
            
        except Exception as e:
            logger.error(f"Error checking convergence: {e}")
            return True

    def get_convergence_status(self, workflow_id: str) -> Dict[str, Any]:
        """Get convergence status for a workflow"""
        try:
            return self.convergence_tracker.get(workflow_id, {
                'rounds_without_improvement': 0,
                'best_accuracy': 0.0,
                'current_accuracy': 0.0,
                'should_continue': True
            })
        except Exception as e:
            logger.error(f"Error getting convergence status: {e}")
            return {}

    def update_local_validation_accuracy(self, workflow_id: str, round_number: int, local_accuracy: float):
        """Update convergence tracker with local validation accuracy"""
        try:
            if workflow_id not in self.convergence_tracker:
                self.convergence_tracker[workflow_id] = {
                    'rounds_without_improvement': 0,
                    'best_accuracy': 0.0,
                    'current_accuracy': 0.0,
                    'should_continue': True
                }
            
            tracker = self.convergence_tracker[workflow_id]
            tracker['current_accuracy'] = local_accuracy
            
            if local_accuracy > tracker['best_accuracy']:
                tracker['best_accuracy'] = local_accuracy
                tracker['rounds_without_improvement'] = 0
                logger.info(f"Local accuracy improved to {local_accuracy:.3f}")
            else:
                tracker['rounds_without_improvement'] += 1
                logger.info(f"No improvement for {tracker['rounds_without_improvement']} rounds")
            
            # Check convergence (stop after 3 rounds without improvement)
            if tracker['rounds_without_improvement'] >= 3:
                tracker['should_continue'] = False
                logger.info(f"Convergence detected for {workflow_id} based on local validation - stopping participation")
            
        except Exception as e:
            logger.error(f"Error updating local validation accuracy: {e}")
    
    def get_round_history(self) -> List[Dict[str, Any]]:
        """Get history of federated rounds"""
        return self.round_history
    
    async def wait_for_aggregated_model(self, workflow_id: str, round_number: int, timeout: int = 180) -> Optional[Dict]:
        """Wait for aggregated model using event-driven approach instead of polling"""
        model_key = f"{workflow_id}_round_{round_number}"
        
        # Check if model already exists
        if model_key in self.aggregated_models:
            logger.info(f"Aggregated model for {model_key} already available")
            return self.aggregated_models[model_key].get('aggregated_weights', {})
        
        # Create event for this model if it doesn't exist
        if model_key not in self.model_events:
            self.model_events[model_key] = asyncio.Event()
            logger.info(f"Created event for {model_key}")
        
        try:
            # Wait for event to be signaled
            logger.info(f"Waiting for aggregated model event: {model_key} (timeout: {timeout}s)")
            await asyncio.wait_for(self.model_events[model_key].wait(), timeout=timeout)
            
            # Event was signaled, get the model
            if model_key in self.aggregated_models:
                logger.info(f"✓ Received aggregated model for {model_key}")
                
                # Log model details
                model_data = self.aggregated_models[model_key]
                logger.info(f"  Participating nodes: {model_data['participating_nodes']}")
                logger.info(f"  Aggregator: {model_data['aggregator_node']}")
                
                return model_data.get('aggregated_weights', {})
            else:
                logger.warning(f"Event signaled but no model found for {model_key}")
                return None
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for aggregated model: {model_key}")
            return None
        finally:
            # Clean up event
            if model_key in self.model_events:
                del self.model_events[model_key]
                logger.debug(f"Cleaned up event for {model_key}")
    
    async def wait_for_training_completion(self, workflow_id: str, schedule_id: str, timeout: int = 120) -> Optional[Dict]:
        """Wait for training completion using event-driven approach instead of polling"""
        # Check if weights already exist
        weights_data = await self.get_trained_weights_for_aggregation(workflow_id, schedule_id)
        if weights_data and 'trained_weights' in weights_data:
            logger.info(f"Training weights for {schedule_id} already available")
            return weights_data['trained_weights']
        
        # Create event for this training if it doesn't exist
        if schedule_id not in self.training_events:
            self.training_events[schedule_id] = asyncio.Event()
            logger.info(f"Created training event for {schedule_id}")
        
        try:
            # Wait for event to be signaled
            logger.info(f"Waiting for training completion event: {schedule_id} (timeout: {timeout}s)")
            await asyncio.wait_for(self.training_events[schedule_id].wait(), timeout=timeout)
            
            # Event was signaled, get the weights
            weights_data = await self.get_trained_weights_for_aggregation(workflow_id, schedule_id)
            if weights_data and 'trained_weights' in weights_data:
                logger.info(f"✓ Training completed for schedule {schedule_id}")
                return weights_data['trained_weights']
            else:
                logger.warning(f"Event signaled but no weights found for {schedule_id}")
                return None
                
        except asyncio.TimeoutError:
            logger.error(f"Training timeout for schedule {schedule_id}")
            return None
        finally:
            # Clean up event
            if schedule_id in self.training_events:
                del self.training_events[schedule_id]
                logger.debug(f"Cleaned up training event for {schedule_id}")

    async def shutdown(self):
        """Shutdown the federated response manager"""
        logger.info(f"Shutting down federated response manager for node {self.node_id}")
        self.running = False
        
        if self.zmq_socket:
            self.zmq_socket.close()
        if self.zmq_context:
            self.zmq_context.term()
        
        logger.info("Federated response manager shutdown complete")