"""
Federated Response Manager for IoT Nodes

This module handles receiving aggregated model weights from the federated learning system
and manages the two-phase federated learning flow for IoT nodes.
"""

import asyncio
import json
import logging
import numpy as np
import os
import time
import zmq
import zmq.asyncio
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator
from typing import Dict, Any, Optional, List
from datetime import datetime

# ZMQ configuration for receiving aggregated models from compute nodes
ZMQ_RESPONSE_PORT = int(os.getenv('ZMQ_RESPONSE_PORT', '5555'))

logger = logging.getLogger(__name__)


class FederatedResponseManager:
    """
    Manages federated learning responses for IoT nodes.
    Handles receiving aggregated model weights and coordinating federated rounds.
    """
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.zmq_context = None
        self.zmq_socket = None
        self.running = False
        self.aggregated_models = {}
        self.round_history = []
        
        # Event-driven coordination
        self.model_events = {}  # workflow_round -> asyncio.Event
        self.training_events = {}  # schedule_id -> asyncio.Event
        self.convergence_tracker = {}
        
        # Local storage for trained weights (instead of Redis)
        self.trained_weights_storage = {}
        
        # CURVE authentication keys (same as IoTDeviceManager)
        self._public_key, self._private_key = self._generate_keys(node_id)
        
        # Initialize ZMQ connection for receiving models
        self._initialize_zmq()
        
        logger.info(f"Initialized FederatedResponseManager for node {node_id}")

    # Redis not needed for IoT nodes - they only receive models via ZMQ

    @staticmethod
    def _generate_keys(source):
        """Generate CURVE authentication keys (same as IoTDeviceManager)"""
        keys_dir = os.path.join(os.getcwd(), 'keys/', source)
        os.makedirs(keys_dir, exist_ok=True)
        server_public_file, server_secret_file = zmq.auth.create_certificates(keys_dir, "server")
        server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
        return server_public, server_secret

    def _initialize_zmq(self):
        """Initialize ZMQ socket for receiving responses with CURVE authentication"""
        try:
            self.zmq_context = zmq.asyncio.Context()
            
            # Start an authenticator for this context (same as IoTDeviceManager)
            self.auth = ThreadAuthenticator(self.zmq_context)
            self.auth.start()
            self.auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)
            
            self.zmq_socket = self.zmq_context.socket(zmq.REP)
            # Configure CURVE authentication (same as IoTDeviceManager)
            self.zmq_socket.curve_secretkey = self._private_key
            self.zmq_socket.curve_publickey = self._public_key
            self.zmq_socket.curve_server = True  # must come before bind
            self.zmq_socket.bind(f"tcp://*:{ZMQ_RESPONSE_PORT}")
            
            logger.info(f"ZMQ response socket bound to port {ZMQ_RESPONSE_PORT} with CURVE authentication")
            logger.info(f"Public key: {self.public_key}")
            
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
                    
                    # Try to parse as JSON first (for structured responses)
                    try:
                        response_data = json.loads(response_bytes.decode())
                        logger.info(f"Received structured JSON response: {list(response_data.keys())}")
                        
                        # Determine response type and process accordingly
                        event_type = response_data.get('event_type')
                        
                        if event_type == 'aggregated_model_ready':
                            # Handle aggregated model from aggregator compute node
                            logger.info(f"Received aggregated model ready event")
                            await self._handle_aggregated_model(response_data)
                        else:
                            # Handle training results or other compute responses
                            await self._process_compute_response(response_data)
                            
                    except json.JSONDecodeError:
                        # Handle plain text responses (standard ResponseManager format)
                        response_text = response_bytes.decode()
                        logger.info(f"Received plain text response from compute node")
                        logger.info(f"Response content: {response_text[:200]}..." if len(response_text) > 200 else f"Response content: {response_text}")
                        
                        # Try to parse the text as JSON (task execution result)
                        try:
                            response_data = json.loads(response_text)
                            await self._process_compute_response(response_data)
                        except json.JSONDecodeError:
                            logger.warning(f"Could not parse response as JSON: {response_text[:100]}...")
                    
                    # Send acknowledgment (standard IoT device response)
                    ack = "Message received securely"
                    await self.zmq_socket.send_string(ack)
                    
                except asyncio.TimeoutError:
                    # No message received, continue
                    continue
                except Exception as e:
                    logger.error(f"Error handling ZMQ response: {e}")
                    # Send error acknowledgment
                    try:
                        await self.zmq_socket.send_string("Error processing message")
                    except:
                        pass
                        
        except Exception as e:
            logger.error(f"Error in ZMQ response handler: {e}")

    async def _handle_aggregated_model(self, model_data: Dict[str, Any]):
        """Handle received aggregated model"""
        receive_time = time.time()
        
        try:
            logger.info(f"📨 AGGREGATED MODEL RECEIVED")
            logger.info(f"   • Recipient node: {self.node_id}")
            logger.info(f"   • Received at: {time.strftime('%H:%M:%S', time.localtime(receive_time))}")
            
            # Extract model data
            workflow_id = model_data.get('workflow_id')
            round_number = model_data.get('round_number')
            aggregated_weights = model_data.get('aggregated_weights')
            participating_nodes = model_data.get('participating_nodes', [])
            aggregator_node = model_data.get('aggregator_node')
            validation_metrics = model_data.get('validation_metrics', {})
            
            logger.info(f"📊 MODEL METADATA:")
            logger.info(f"   • Workflow ID: {workflow_id}")
            logger.info(f"   • Round number: {round_number}")
            logger.info(f"   • Aggregator node: {aggregator_node}")
            logger.info(f"   • Participating nodes: {participating_nodes} ({len(participating_nodes)} total)")
            
            # Validate required fields
            if not all([workflow_id, round_number is not None, aggregated_weights]):
                logger.error(f"❌ INVALID MODEL DATA")
                logger.error(f"   • Missing required fields: workflow_id={bool(workflow_id)}, round_number={bool(round_number is not None)}, weights={bool(aggregated_weights)}")
                return
            
            # Log validation metrics if available
            if validation_metrics:
                logger.info(f"🔍 VALIDATION METRICS:")
                for key, value in validation_metrics.items():
                    if isinstance(value, float):
                        logger.info(f"   • {key}: {value:.4f}")
                    else:
                        logger.info(f"   • {key}: {value}")
            
            # Log weight information
            if aggregated_weights:
                weight_layers = len(aggregated_weights)
                total_params = 0
                for layer_name, weights in aggregated_weights.items():
                    if isinstance(weights, list):
                        layer_params = len(np.array(weights).flatten())
                        total_params += layer_params
                
                logger.info(f"📊 AGGREGATED WEIGHTS:")
                logger.info(f"   • Weight layers: {weight_layers}")
                logger.info(f"   • Total parameters: {total_params:,}")
                logger.info(f"   • Layer names: {list(aggregated_weights.keys())[:3]}... (showing first 3)")
            
            # Store aggregated model
            model_key = f"{workflow_id}_round_{round_number}"
            self.aggregated_models[model_key] = {
                'workflow_id': workflow_id,
                'round_number': round_number,
                'aggregated_weights': aggregated_weights,
                'participating_nodes': participating_nodes,
                'aggregator_node': aggregator_node,
                'received_time': receive_time,
                'validation_metrics': validation_metrics
            }
            
            logger.info(f"💾 MODEL STORED: Cached with key {model_key}")
            
            # Evaluate model and update convergence tracking
            logger.info(f"📈 CONVERGENCE TRACKING: Updating convergence status")
            await self._evaluate_and_track_convergence(workflow_id, round_number, aggregated_weights)
            
            # Store round history
            self.round_history.append({
                'workflow_id': workflow_id,
                'round_number': round_number,
                'node_id': self.node_id,
                'received_time': receive_time,
                'participating_nodes': participating_nodes,
                'aggregator_node': aggregator_node
            })
            
            logger.info(f"📋 HISTORY UPDATED: Round added to participation history")
            
            # Signal event for waiting coroutines
            if model_key in self.model_events:
                self.model_events[model_key].set()
                logger.info(f"🚨 EVENT SIGNALED: Notified waiting coroutines for {model_key}")
            else:
                logger.info(f"🔕 NO WAITERS: No active event listeners for {model_key}")
            
            logger.info(f"✅ AGGREGATED MODEL PROCESSING COMPLETED")
            
        except Exception as e:
            logger.error(f"❌ ERROR HANDLING AGGREGATED MODEL")
            logger.error(f"   • Error: {str(e)}")
            logger.error(f"   • Node: {self.node_id}")
            logger.error(f"   • Model data keys: {list(model_data.keys()) if isinstance(model_data, dict) else 'Not a dict'}")

    async def _process_compute_response(self, response_data: Dict[str, Any]):
        """Process responses from compute nodes (training results)"""
        try:
            logger.info(f"Processing compute response for node {self.node_id}")
            logger.info(f"Response data keys: {list(response_data.keys())}")
            
            # Handle TrustMesh task execution format from task_executor
            schedule_id = response_data.get('schedule_id')
            workflow_id = response_data.get('workflow_id')
            
            if 'data' in response_data and isinstance(response_data['data'], list) and response_data['data']:
                result_data = response_data['data'][0]  # Extract first item from data array
                
                if 'trained_weights' in result_data:
                    # Training completed successfully
                    trained_weights = result_data['trained_weights']
                    
                    logger.info(f"Training completed for schedule {schedule_id}, extracting weights")
                    logger.info(f"Trained weights layers: {len(trained_weights) if trained_weights else 0}")
                    
                    # Store trained weights for potential aggregation request
                    await self._store_trained_weights(workflow_id, schedule_id, trained_weights, result_data)
                else:
                    logger.warning(f"No trained weights found in response data for schedule {schedule_id}")
            else:
                logger.warning(f"Invalid or empty data in response for schedule {schedule_id}")
                
        except Exception as e:
            logger.error(f"Error processing compute response: {e}")
            logger.error(f"Response data: {response_data}")

    async def _store_trained_weights(self, workflow_id: str, schedule_id: str, 
                                   trained_weights: Dict, result_data: Dict):
        """Store trained weights for later aggregation request"""
        storage_start_time = time.time()
        
        try:
            logger.info(f"💾 STORING TRAINED WEIGHTS")
            logger.info(f"   • Node: {self.node_id}")
            logger.info(f"   • Workflow: {workflow_id}")
            logger.info(f"   • Schedule ID: {schedule_id}")
            
            weights_key = f"trained_weights_{self.node_id}_{schedule_id}"
            
            # Log weight information
            if trained_weights:
                weight_layers = len(trained_weights)
                total_params = 0
                for layer_name, weights in trained_weights.items():
                    if isinstance(weights, list):
                        layer_params = len(np.array(weights).flatten())
                        total_params += layer_params
                
                logger.info(f"   • Weight layers: {weight_layers}")
                logger.info(f"   • Total parameters: {total_params:,}")
                logger.info(f"   • Layer names: {list(trained_weights.keys())[:3]}... (showing first 3)")
            else:
                logger.warning(f"   • No trained weights to store (empty or None)")
            
            # Prepare storage data
            weights_data = {
                'node_id': self.node_id,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id,
                'trained_weights': trained_weights,
                'training_metadata': result_data.get('metadata', {}),
                'training_completed_time': time.time()
            }
            
            # Log metadata if available
            metadata = result_data.get('metadata', {})
            if metadata:
                logger.info(f"   • Training metadata: {metadata}")
            
            # Store in local memory for later retrieval
            logger.info(f"   • Storing locally with key: {weights_key}")
            
            self.trained_weights_storage[weights_key] = weights_data
            
            storage_duration = time.time() - storage_start_time
            logger.info(f"✅ TRAINED WEIGHTS STORED SUCCESSFULLY")
            logger.info(f"   • Storage duration: {storage_duration:.3f}s")
            logger.info(f"   • Storage key: {weights_key}")
            
            # Signal event for waiting coroutines
            if schedule_id in self.training_events:
                self.training_events[schedule_id].set()
                logger.info(f"🚨 EVENT SIGNALED: Notified waiting coroutines for {schedule_id}")
            else:
                logger.info(f"🔕 NO WAITERS: No active event listeners for {schedule_id}")
            
        except Exception as e:
            storage_duration = time.time() - storage_start_time
            logger.error(f"❌ ERROR STORING TRAINED WEIGHTS")
            logger.error(f"   • Error: {str(e)}")
            logger.error(f"   • Node: {self.node_id}")
            logger.error(f"   • Schedule ID: {schedule_id}")
            logger.error(f"   • Duration before failure: {storage_duration:.3f}s")

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
            weights_key = f"trained_weights_{self.node_id}_{schedule_id}"
            weights_data = self.trained_weights_storage.get(weights_key)
            
            if weights_data:
                return weights_data
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
        wait_start_time = time.time()
        
        logger.info(f"🔍 FEDERATED RESPONSE: Starting wait for aggregated model")
        logger.info(f"   • Model key: {model_key}")
        logger.info(f"   • Node: {self.node_id}")
        logger.info(f"   • Timeout: {timeout}s")
        
        # Check if model already exists
        if model_key in self.aggregated_models:
            logger.info(f"⚡ AGGREGATED MODEL: Already available (cached)")
            model_data = self.aggregated_models[model_key]
            logger.info(f"   • Participating nodes: {model_data['participating_nodes']}")
            logger.info(f"   • Aggregator: {model_data['aggregator_node']}")
            logger.info(f"   • Retrieved from cache in {time.time() - wait_start_time:.3f}s")
            return model_data.get('aggregated_weights', {})
        
        # Create event for this model if it doesn't exist
        if model_key not in self.model_events:
            self.model_events[model_key] = asyncio.Event()
            logger.info(f"🟠 EVENT CREATED: Event handler created for {model_key}")
        
        try:
            # Wait for event to be signaled
            logger.info(f"⏳ WAITING: For aggregated model event (max {timeout}s)")
            await asyncio.wait_for(self.model_events[model_key].wait(), timeout=timeout)
            
            wait_duration = time.time() - wait_start_time
            
            # Event was signaled, get the model
            if model_key in self.aggregated_models:
                logger.info(f"✅ AGGREGATED MODEL RECEIVED SUCCESSFULLY")
                logger.info(f"   • Wait duration: {wait_duration:.2f}s")
                
                # Log model details
                model_data = self.aggregated_models[model_key]
                logger.info(f"   • Participating nodes: {model_data['participating_nodes']}")
                logger.info(f"   • Aggregator node: {model_data['aggregator_node']}")
                logger.info(f"   • Model received at: {time.strftime('%H:%M:%S', time.localtime(model_data['received_time']))}")
                
                # Log weight information
                aggregated_weights = model_data.get('aggregated_weights', {})
                if aggregated_weights:
                    logger.info(f"   • Weight layers: {len(aggregated_weights)}")
                    logger.info(f"   • Layer names: {list(aggregated_weights.keys())[:3]}... (showing first 3)")
                
                return aggregated_weights
            else:
                logger.warning(f"⚠️ EVENT SIGNALED BUT MODEL MISSING")
                logger.warning(f"   • Model key: {model_key}")
                logger.warning(f"   • Wait duration: {wait_duration:.2f}s")
                logger.warning(f"   • This may indicate a race condition")
                return None
                
        except asyncio.TimeoutError:
            wait_duration = time.time() - wait_start_time
            logger.error(f"❌ AGGREGATED MODEL TIMEOUT")
            logger.error(f"   • Model key: {model_key}")
            logger.error(f"   • Timeout duration: {wait_duration:.2f}s (max: {timeout}s)")
            logger.error(f"   • Possible causes: Aggregation failure, network issues, insufficient nodes")
            return None
        finally:
            # Clean up event
            if model_key in self.model_events:
                del self.model_events[model_key]
                logger.debug(f"🧹 CLEANUP: Removed event for {model_key}")
    
    async def wait_for_training_completion(self, workflow_id: str, schedule_id: str, timeout: int = 120) -> Optional[Dict]:
        """Wait for training completion using event-driven approach instead of polling"""
        wait_start_time = time.time()
        
        logger.info(f"🔍 TRAINING RESPONSE: Starting wait for training completion")
        logger.info(f"   • Schedule ID: {schedule_id}")
        logger.info(f"   • Workflow: {workflow_id}")
        logger.info(f"   • Node: {self.node_id}")
        logger.info(f"   • Timeout: {timeout}s")
        
        # Check if weights already exist
        weights_data = await self.get_trained_weights_for_aggregation(workflow_id, schedule_id)
        if weights_data and 'trained_weights' in weights_data:
            logger.info(f"⚡ TRAINING WEIGHTS: Already available (cached)")
            logger.info(f"   • Training completed at: {time.strftime('%H:%M:%S', time.localtime(weights_data.get('training_completed_time', 0)))}")
            logger.info(f"   • Retrieved from cache in {time.time() - wait_start_time:.3f}s")
            
            trained_weights = weights_data['trained_weights']
            if trained_weights:
                logger.info(f"   • Weight layers: {len(trained_weights)}")
                logger.info(f"   • Layer names: {list(trained_weights.keys())[:3]}... (showing first 3)")
            
            return trained_weights
        
        # Create event for this training if it doesn't exist
        if schedule_id not in self.training_events:
            self.training_events[schedule_id] = asyncio.Event()
            logger.info(f"🟠 EVENT CREATED: Training event handler created for {schedule_id}")
        
        try:
            # Wait for event to be signaled
            logger.info(f"⏳ WAITING: For training completion event (max {timeout}s)")
            await asyncio.wait_for(self.training_events[schedule_id].wait(), timeout=timeout)
            
            wait_duration = time.time() - wait_start_time
            
            # Event was signaled, get the weights
            weights_data = await self.get_trained_weights_for_aggregation(workflow_id, schedule_id)
            if weights_data and 'trained_weights' in weights_data:
                logger.info(f"✅ TRAINING COMPLETED SUCCESSFULLY")
                logger.info(f"   • Wait duration: {wait_duration:.2f}s")
                logger.info(f"   • Training completed at: {time.strftime('%H:%M:%S', time.localtime(weights_data.get('training_completed_time', 0)))}")
                
                trained_weights = weights_data['trained_weights']
                if trained_weights:
                    logger.info(f"   • Weight layers received: {len(trained_weights)}")
                    logger.info(f"   • Layer names: {list(trained_weights.keys())[:3]}... (showing first 3)")
                
                # Log training metadata if available
                metadata = weights_data.get('training_metadata', {})
                if metadata:
                    logger.info(f"   • Training metadata: {metadata}")
                
                return trained_weights
            else:
                logger.warning(f"⚠️ EVENT SIGNALED BUT WEIGHTS MISSING")
                logger.warning(f"   • Schedule ID: {schedule_id}")
                logger.warning(f"   • Wait duration: {wait_duration:.2f}s")
                logger.warning(f"   • This may indicate a training failure or race condition")
                return None
                
        except asyncio.TimeoutError:
            wait_duration = time.time() - wait_start_time
            logger.error(f"❌ TRAINING COMPLETION TIMEOUT")
            logger.error(f"   • Schedule ID: {schedule_id}")
            logger.error(f"   • Timeout duration: {wait_duration:.2f}s (max: {timeout}s)")
            logger.error(f"   • Possible causes: Compute node failure, resource exhaustion, task timeout")
            return None
        finally:
            # Clean up event
            if schedule_id in self.training_events:
                del self.training_events[schedule_id]
                logger.debug(f"🧹 CLEANUP: Removed training event for {schedule_id}")

    @property
    def public_key(self):
        """Get the public key of the federated response manager (same as IoTDeviceManager)"""
        return self._public_key.decode('ascii')

    async def shutdown(self):
        """Shutdown the federated response manager"""
        logger.info(f"Shutting down federated response manager for node {self.node_id}")
        self.running = False
        
        if hasattr(self, 'auth') and self.auth:
            self.auth.stop()
        if self.zmq_socket:
            self.zmq_socket.close()
        if self.zmq_context:
            self.zmq_context.term()
        
        logger.info("Federated response manager shutdown complete")