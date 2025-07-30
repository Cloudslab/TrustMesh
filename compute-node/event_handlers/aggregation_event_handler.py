import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import time
import traceback
import threading
import numpy as np
import zmq
import zmq.asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any

from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.client_state_pb2 import ClientStateGetRequest, ClientStateGetResponse
from sawtooth_sdk.protobuf.events_pb2 import EventSubscription, EventList
from sawtooth_sdk.protobuf.client_event_pb2 import ClientEventsSubscribeRequest, ClientEventsSubscribeResponse
from sawtooth_sdk.protobuf.validator_pb2 import Message
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_sdk.protobuf.client_batch_submit_pb2 import ClientBatchSubmitResponse
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from coredis import RedisCluster

# Family configurations
AGGREGATION_REQUEST_FAMILY_NAME = 'aggregation-request'
AGGREGATION_CONFIRMATION_FAMILY_NAME = 'aggregation-confirmation'
# Namespace constants - must match the TPs exactly
AGGREGATION_REQUEST_NAMESPACE = hashlib.sha512(AGGREGATION_REQUEST_FAMILY_NAME.encode()).hexdigest()[:6]
AGGREGATION_CONFIRMATION_NAMESPACE = hashlib.sha512(AGGREGATION_CONFIRMATION_FAMILY_NAME.encode()).hexdigest()[:6]
WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/root.priv')

# Aggregation configuration
AGGREGATION_TIMEOUT = int(os.getenv('AGGREGATION_TIMEOUT', '180'))  # 3 minutes
MIN_NODES_FOR_AGGREGATION = int(os.getenv('MIN_NODES_FOR_AGGREGATION', '3'))

logger = logging.getLogger(__name__)


def load_private_key():
    try:
        with open(PRIVATE_KEY_FILE, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as e:
        raise IOError(f"Failed to load private key from {PRIVATE_KEY_FILE}: {str(e)}") from e


def initialize_redis():
    logger.info("Starting Redis initialization for aggregation handler")
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

        redis_instance = RedisCluster(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=True,
            ssl_context=ssl_context,
            decode_responses=True
        )

        asyncio.get_event_loop().run_until_complete(redis_instance.ping())
        logger.info("Connected to Redis cluster successfully")

        return redis_instance

    except Exception as e:
        logger.error(f"Failed to connect to Redis: {str(e)}")
        raise
    finally:
        for file_path in temp_files:
            try:
                os.unlink(file_path)
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")


class FederatedAggregator:
    """Handles federated learning aggregation using FedAvg algorithm"""
    
    def __init__(self, node_id: str, redis_client):
        self.node_id = node_id
        self.redis = redis_client
        self.pending_aggregations = {}
        
        # Create a dedicated event loop for this aggregator if one doesn't exist
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            # No event loop in current thread, create a new one
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        
        logger.info(f"Initialized FederatedAggregator for node {node_id}")

    async def start_aggregation_collection(self, aggregation_id: str, workflow_id: str, 
                                         global_round_number: int, expected_nodes: List[str]):
        """Start collecting model weights from participating nodes"""
        try:
            start_time = time.time()
            deadline = start_time + AGGREGATION_TIMEOUT
            logger.info(f"🔄 AGGREGATION STARTED - ID: {aggregation_id}")
            logger.info(f"  └─ Workflow: {workflow_id}")
            logger.info(f"  └─ Global Round: {global_round_number}")
            logger.info(f"  └─ Timer Duration: {AGGREGATION_TIMEOUT}s")
            logger.info(f"  └─ Deadline: {time.strftime('%H:%M:%S', time.localtime(deadline))}")
            logger.info(f"  └─ Expected Nodes: {expected_nodes}")
            
            # Store aggregation metadata
            self.pending_aggregations[aggregation_id] = {
                'workflow_id': workflow_id,
                'global_round_number': global_round_number,  # Use global round instead of IoT round
                'expected_nodes': expected_nodes,
                'start_time': start_time
            }
            
            # Start collection timer
            logger.info(f"⏱️  Starting {AGGREGATION_TIMEOUT}s timer for {aggregation_id}")
            asyncio.create_task(self._collection_timer(aggregation_id))
            
        except Exception as e:
            logger.error(f"❌ Error in aggregation collection: {e}")
            raise

    async def _collection_timer(self, aggregation_id: str):
        """Timer that expires after AGGREGATION_TIMEOUT and locks the round"""
        try:
            logger.info(f"⏱️  Timer started for {aggregation_id} - waiting {AGGREGATION_TIMEOUT}s")
            start_wait = time.time()
            
            # Log countdown at intervals
            for i in range(0, AGGREGATION_TIMEOUT, 10):
                if aggregation_id not in self.pending_aggregations:
                    logger.info(f"⏹️  Timer cancelled - aggregation {aggregation_id} no longer pending")
                    return
                    
                remaining = AGGREGATION_TIMEOUT - i
                if remaining > 10:
                    logger.info(f"⏳ Timer update - {remaining}s remaining for {aggregation_id}")
                    await asyncio.sleep(10)
                else:
                    await asyncio.sleep(remaining)
                    break
            
            elapsed = time.time() - start_wait
            logger.info(f"⏰ TIMER EXPIRED for {aggregation_id} after {elapsed:.1f}s")
            
            # Check if aggregation is still pending
            if aggregation_id not in self.pending_aggregations:
                logger.info(f"⏹️  Aggregation {aggregation_id} already completed or failed")
                return
                
            aggregation_info = self.pending_aggregations[aggregation_id]
            
            # Fetch current state from blockchain to check contributions
            logger.info(f"🔍 Fetching current aggregation state from blockchain")
            round_data = await self._fetch_aggregation_round_from_blockchain(aggregation_id)
            
            if round_data:
                participating_nodes = round_data.get('participating_nodes', [])
                collected_count = len(participating_nodes)
            else:
                logger.error(f"❌ Could not fetch aggregation state for {aggregation_id}")
                collected_count = 0
                participating_nodes = []
            
            logger.info(f"📊 Timer Expiry Status for {aggregation_id}:")
            logger.info(f"  └─ Nodes Collected: {collected_count}/{MIN_NODES_FOR_AGGREGATION}")
            logger.info(f"  └─ Participating Nodes: {participating_nodes}")
            
            if collected_count >= MIN_NODES_FOR_AGGREGATION:
                logger.info(f"✅ Sufficient nodes collected - proceeding to lock round")
                # Step 1: Lock the round to prevent new nodes from joining
                await self._lock_aggregation_round(aggregation_id)
                # Step 2: Perform aggregation
                await self._perform_aggregation(aggregation_id)
            else:
                logger.error(f"❌ Insufficient nodes collected: {collected_count}/{MIN_NODES_FOR_AGGREGATION}")
                await self._handle_aggregation_failure(aggregation_id, "insufficient_nodes")
                
        except Exception as e:
            logger.error(f"❌ Error in collection timer for {aggregation_id}: {e}")
            await self._handle_aggregation_failure(aggregation_id, str(e))

    async def _fetch_aggregation_round_from_blockchain(self, aggregation_id: str) -> Dict:
        """Fetch aggregation round data from blockchain state"""
        try:
            # Create a state get request
            aggregation_address = self._make_aggregation_address(aggregation_id)
            
            request = ClientStateGetRequest(
                state_root='',  # Use current state root
                addresses=[aggregation_address]
            )
            
            # Send request to validator
            response_future = stream.send(
                message_type=Message.CLIENT_STATE_GET_REQUEST,
                content=request.SerializeToString()
            )
            
            response = ClientStateGetResponse()
            response.ParseFromString(response_future.result().content)
            
            if response.status != ClientStateGetResponse.OK:
                logger.error(f"Failed to get state: {response.status}")
                return None
                
            # Parse the aggregation data
            for entry in response.entries:
                if entry.address == aggregation_address:
                    return json.loads(entry.data.decode())
                    
            logger.error(f"Aggregation {aggregation_id} not found in state")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching aggregation round: {e}")
            return None
    
    def _make_aggregation_address(self, aggregation_id):
        """Generate blockchain address for aggregation request"""
        return AGGREGATION_REQUEST_NAMESPACE + hashlib.sha512(aggregation_id.encode()).hexdigest()[:64]
    
    async def _fetch_model_weights_from_couchdb(self, doc_id: str) -> Dict:
        """Fetch model weights from CouchDB"""
        try:
            # Get document from CouchDB
            with ThreadPoolExecutor() as executor:
                future = executor.submit(self._get_couchdb_document, doc_id)
                doc = future.result()
                
            if not doc:
                logger.error(f"Document {doc_id} not found in CouchDB")
                return None
                
            # Verify content hash for integrity
            model_weights = doc.get('model_weights')
            stored_hash = doc.get('content_hash')
            
            if model_weights and stored_hash:
                computed_hash = hashlib.sha256(json.dumps(model_weights, sort_keys=True).encode()).hexdigest()
                if computed_hash == stored_hash:
                    return model_weights
                else:
                    logger.error(f"Content hash mismatch for {doc_id}")
                    return None
            else:
                logger.error(f"Missing weights or hash in document {doc_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching model weights from CouchDB: {e}")
            return None
    
    def _get_couchdb_document(self, doc_id: str) -> Dict:
        """Get document from CouchDB (synchronous wrapper)"""
        try:
            # Use the same CouchDB client pattern as aggregation-confirmation-tp
            from ibmcloudant.cloudant_v1 import CloudantV1
            from ibm_cloud_sdk_core.authenticators import BasicAuthenticator
            
            # Initialize CouchDB client (reuse connection logic)
            COUCHDB_URL = f"https://{os.getenv('COUCHDB_HOST', 'couchdb-0.default.svc.cluster.local:6984')}"
            COUCHDB_USERNAME = os.getenv('COUCHDB_USER')
            COUCHDB_PASSWORD = os.getenv('COUCHDB_PASSWORD')
            COUCHDB_DB = 'model_weights'
            
            authenticator = BasicAuthenticator(COUCHDB_USERNAME, COUCHDB_PASSWORD)
            couchdb_client = CloudantV1(authenticator=authenticator)
            couchdb_client.set_service_url(COUCHDB_URL)
            
            # Get document
            result = couchdb_client.get_document(db=COUCHDB_DB, doc_id=doc_id).get_result()
            return result
            
        except Exception as e:
            logger.error(f"Error getting document from CouchDB: {e}")
            return None

    async def _lock_aggregation_round(self, aggregation_id: str):
        """Lock the aggregation round to prevent new nodes from joining"""
        try:
            logger.info(f"🔒 LOCKING aggregation round {aggregation_id}")
            
            aggregation_info = self.pending_aggregations[aggregation_id]
            
            # Submit status update transaction to lock the round
            payload = {
                'aggregation_id': aggregation_id,
                'new_status': 'locked',
                'workflow_id': aggregation_info['workflow_id'],
                'global_round_number': aggregation_info['global_round_number']
            }
            
            logger.info(f"📤 Submitting lock transaction for {aggregation_id}")
            
            # Create and submit status update transaction
            with ThreadPoolExecutor() as executor:
                future = executor.submit(self._create_and_submit_status_update_transaction, payload)
                result = future.result()
                
                if result.get('status') == 'SUCCESS':
                    logger.info(f"✅ Successfully locked aggregation round {aggregation_id}")
                    logger.info(f"  └─ Status: collecting → locked")
                else:
                    logger.error(f"❌ Failed to lock round: {result.get('message')}")
                    raise Exception(f"Failed to lock round: {result.get('message')}")
                    
        except Exception as e:
            logger.error(f"❌ Error locking aggregation round {aggregation_id}: {e}")
            raise

    async def _perform_aggregation(self, aggregation_id: str):
        """Perform FedAvg aggregation after round is locked"""
        try:
            logger.info(f"🧮 PERFORMING FedAvg aggregation for {aggregation_id}")
            
            aggregation_info = self.pending_aggregations[aggregation_id]
            
            # Fetch the aggregation round data from blockchain to get all contributions
            logger.info(f"🔍 Fetching aggregation round data from blockchain")
            round_data = await self._fetch_aggregation_round_from_blockchain(aggregation_id)
            
            if not round_data:
                raise Exception(f"Failed to fetch aggregation round {aggregation_id} from blockchain")
                
            # Extract node contributions metadata from blockchain data
            node_contributions = round_data.get('node_contributions', {})
            participating_nodes = round_data.get('participating_nodes', [])
            
            logger.info(f"📊 Aggregation Details:")
            logger.info(f"  └─ Participating Nodes: {participating_nodes}")
            logger.info(f"  └─ Node Count: {len(participating_nodes)}")
            logger.info(f"  └─ Status: {round_data.get('status')}")
            
            # Fetch model weights from CouchDB using document IDs
            logger.info(f"🔍 Fetching model weights from CouchDB")
            node_weights = {}
            for node_id in participating_nodes:
                if node_id in node_contributions:
                    weights_doc_id = node_contributions[node_id]['weights_doc_id']
                    weights = await self._fetch_model_weights_from_couchdb(weights_doc_id)
                    if weights:
                        node_weights[node_id] = weights
                        logger.info(f"  └─ Fetched weights for {node_id}")
                    else:
                        logger.error(f"❌ Failed to fetch weights for {node_id} from {weights_doc_id}")
                else:
                    logger.error(f"❌ Missing contribution metadata from {node_id}")
            
            # Compute FedAvg
            logger.info(f"🔧 Computing FedAvg with {len(node_weights)} nodes")
            aggregated_weights = self._compute_fedavg(node_weights, participating_nodes)
            
            # Store aggregated weights before submitting confirmation
            aggregation_info['aggregated_weights'] = aggregated_weights
            
            logger.info(f"📤 Submitting aggregation confirmation transaction")
            
            # Submit aggregation confirmation transaction (this will set status to 'confirmed')
            await self._submit_aggregation_confirmation(
                aggregation_id,
                aggregated_weights,
                participating_nodes,
                aggregation_info['workflow_id'],
                aggregation_info['global_round_number']
            )
            
            # Don't clean up yet - wait for confirmation event to broadcast
            
            logger.info(f"✅ Successfully completed aggregation {aggregation_id}")
            logger.info(f"  └─ Status: locked → confirmed (pending)")
            
        except Exception as e:
            logger.error(f"❌ Error performing aggregation: {e}")
            await self._handle_aggregation_failure(aggregation_id, str(e))

    def _compute_fedavg(self, node_weights: Dict[str, Dict], participating_nodes: List[str]) -> Dict:
        """Compute FedAvg aggregation"""
        logger.info(f"Computing FedAvg for {len(participating_nodes)} nodes")
        
        aggregated_weights = {}
        
        # Get first node's weights structure
        first_node = participating_nodes[0]
        weight_structure = node_weights[first_node]
        
        # Initialize aggregated weights
        for layer_name in weight_structure:
            aggregated_weights[layer_name] = np.zeros_like(np.array(weight_structure[layer_name]))
        
        # Weighted averaging (equal weights for simplicity)
        for node_id in participating_nodes:
            node_weight_dict = node_weights[node_id]
            weight_factor = 1.0 / len(participating_nodes)
            
            for layer_name in node_weight_dict:
                layer_weights = np.array(node_weight_dict[layer_name])
                aggregated_weights[layer_name] += weight_factor * layer_weights
        
        # Convert back to lists for JSON serialization
        for layer_name in aggregated_weights:
            aggregated_weights[layer_name] = aggregated_weights[layer_name].tolist()
        
        logger.info("FedAvg computation completed")
        return aggregated_weights

    async def _submit_aggregation_confirmation(self, aggregation_id: str, aggregated_weights: Dict,
                                             participating_nodes: List[str], workflow_id: str, global_round_number: int):
        """Submit aggregation confirmation transaction"""
        try:
            logger.info(f"Submitting aggregation confirmation for {aggregation_id} (global round {global_round_number})")
            
            payload = {
                'aggregation_id': aggregation_id,
                'aggregator_node': self.node_id,
                'aggregated_weights': aggregated_weights,
                'participating_nodes': participating_nodes,
                'workflow_id': workflow_id,
                'global_round_number': global_round_number,  # Use global round instead of IoT round
                'aggregation_time': time.time()
            }
            
            # Create and submit transaction
            with ThreadPoolExecutor() as executor:
                future = executor.submit(self._create_and_submit_confirmation_transaction, payload)
                result = future.result()
                
                logger.info(f"Aggregation confirmation submitted: {result}")
                
                # Broadcast aggregated model to participating nodes via ZMQ
                await self._broadcast_aggregated_model(
                    aggregated_weights, 
                    participating_nodes, 
                    workflow_id, 
                    global_round_number
                )
                
        except Exception as e:
            logger.error(f"Error submitting aggregation confirmation: {e}")
            raise

    def _create_and_submit_status_update_transaction(self, payload: Dict) -> Dict:
        """Create and submit status update transaction using aggregation-confirmation-tp"""
        try:
            # Add action field to specify this is a lock operation
            lock_payload = {
                **payload,
                'action': 'lock'
            }
            
            # Use aggregation-confirmation transaction processor for status updates
            # Include all namespaces that both TPs declare to ensure proper serialization
            status_txn = create_transaction(
                AGGREGATION_CONFIRMATION_FAMILY_NAME,
                '1.0',
                lock_payload,
                [AGGREGATION_CONFIRMATION_NAMESPACE, AGGREGATION_REQUEST_NAMESPACE, WORKFLOW_NAMESPACE],
                [AGGREGATION_CONFIRMATION_NAMESPACE, AGGREGATION_REQUEST_NAMESPACE, WORKFLOW_NAMESPACE]
            )
            
            batch = create_batch([status_txn])
            return submit_batch(batch)
            
        except Exception as e:
            logger.error(f"Error creating status update transaction: {e}")
            raise
    
    def _create_and_submit_confirmation_transaction(self, payload: Dict) -> Dict:
        """Create and submit aggregation confirmation transaction"""
        try:
            # Add action field to specify this is a confirmation operation
            confirmation_payload = {
                **payload,
                'action': 'confirm'
            }
            
            confirmation_txn = create_transaction(
                AGGREGATION_CONFIRMATION_FAMILY_NAME,
                '1.0',
                confirmation_payload,
                [AGGREGATION_CONFIRMATION_NAMESPACE, AGGREGATION_REQUEST_NAMESPACE, WORKFLOW_NAMESPACE],
                [AGGREGATION_CONFIRMATION_NAMESPACE, AGGREGATION_REQUEST_NAMESPACE, WORKFLOW_NAMESPACE]
            )
            
            batch = create_batch([confirmation_txn])
            return submit_batch(batch)
            
        except Exception as e:
            logger.error(f"Error creating confirmation transaction: {e}")
            raise

    async def _broadcast_aggregated_model(self, aggregated_weights: Dict, participating_nodes: List[str],
                                        workflow_id: str, global_round_number: int, validation_metrics: Dict = None):
        """Broadcast aggregated model to all participating IoT nodes via ZMQ"""
        try:
            logger.info(f"Broadcasting aggregated model to {len(participating_nodes)} nodes via ZMQ (global round {global_round_number})")
            
            # Create ZMQ context and socket for broadcasting
            zmq_context = zmq.asyncio.Context()
            
            # Broadcast data
            broadcast_data = {
                'event_type': 'aggregated_model_ready',
                'workflow_id': workflow_id,
                'round_number': global_round_number,  # Use global round for IoT nodes
                'aggregated_weights': aggregated_weights,
                'participating_nodes': participating_nodes,
                'aggregator_node': self.node_id,
                'timestamp': time.time(),
                'status': 'COMPLETE',
                'validation_metrics': validation_metrics or {}
            }
            
            # Send to each participating IoT node via ZMQ
            for node_id in participating_nodes:
                try:
                    # Connect to IoT node's ZMQ response port
                    zmq_socket = zmq_context.socket(zmq.REQ)
                    zmq_socket.connect(f"tcp://{node_id}:5555")  # Standard IoT node ZMQ port
                    
                    # Send aggregated model
                    await zmq_socket.send_string(json.dumps(broadcast_data))
                    
                    # Wait for acknowledgment
                    response = await asyncio.wait_for(zmq_socket.recv_string(), timeout=10.0)
                    response_data = json.loads(response)
                    
                    if response_data.get('status') == 'received':
                        logger.info(f"✓ Successfully broadcasted aggregated model to {node_id}")
                    else:
                        logger.warning(f"⚠ Unexpected response from {node_id}: {response_data}")
                    
                    zmq_socket.close()
                    
                except Exception as e:
                    logger.error(f"✗ Failed to broadcast to {node_id}: {e}")
                    if 'zmq_socket' in locals():
                        zmq_socket.close()
            
            # Also publish to Redis pub/sub for inter-compute coordination
            inter_compute_event = {
                'event_type': 'aggregation_completed',
                'workflow_id': workflow_id,
                'global_round_number': global_round_number,  # Use global round for consistency
                'aggregator_node': self.node_id,
                'participating_nodes': participating_nodes,
                'timestamp': time.time()
            }
            await self.redis.publish('federated_events', json.dumps(inter_compute_event))
            
            zmq_context.term()
            logger.info("Aggregated model broadcast completed")
            
        except Exception as e:
            logger.error(f"Error broadcasting aggregated model: {e}")
            if 'zmq_context' in locals():
                zmq_context.term()

    async def _handle_aggregation_failure(self, aggregation_id: str, error_reason: str):
        """Handle aggregation failure"""
        try:
            logger.error(f"Aggregation {aggregation_id} failed: {error_reason}")
            
            # Store failure information
            failure_data = {
                'aggregation_id': aggregation_id,
                'error_reason': error_reason,
                'aggregator_node': self.node_id,
                'failure_time': time.time()
            }
            
            await self.redis.setex(f"aggregation_failure_{aggregation_id}", 3600, json.dumps(failure_data))
            
            # Publish failure event
            failure_event = {
                'event_type': 'aggregation_failed',
                'aggregation_id': aggregation_id,
                'error_reason': error_reason,
                'aggregator_node': self.node_id,
                'timestamp': time.time()
            }
            
            await self.redis.publish('federated_events', json.dumps(failure_event))
            
            # Clean up
            if aggregation_id in self.pending_aggregations:
                del self.pending_aggregations[aggregation_id]
                
        except Exception as e:
            logger.error(f"Error handling aggregation failure: {e}")


def handle_aggregation_event(event, aggregator: FederatedAggregator):
    """Handle aggregation-related events"""
    if event.event_type == "aggregation-request":
        logger.info("Aggregation Event Handler: aggregation-request event")
        
        aggregation_id = None
        workflow_id = None
        node_id = None
        global_round_number = None
        aggregator_node = None
        
        for attr in event.attributes:
            if attr.key == "aggregation_id":
                aggregation_id = attr.value
            elif attr.key == "workflow_id":
                workflow_id = attr.value
            elif attr.key == "node_id":
                node_id = attr.value
            elif attr.key == "global_round_number":
                global_round_number = int(attr.value)
            elif attr.key == "aggregator_node":
                aggregator_node = attr.value
        
        if aggregator_node == aggregator.node_id:
            logger.info(f"🎯 This node ({aggregator.node_id}) is assigned as aggregator for {aggregation_id}")
            
            # Get expected nodes from environment or default
            expected_nodes = os.getenv('FEDERATED_EXPECTED_NODES', 'iot-0,iot-1,iot-2,iot-3,iot-4').split(',')
            
            # Start aggregation collection asynchronously
            # Use run_coroutine_threadsafe to schedule the task in the aggregator's event loop
            try:
                loop = aggregator.loop
                future = asyncio.run_coroutine_threadsafe(
                    aggregator.start_aggregation_collection(
                        aggregation_id, workflow_id, global_round_number, expected_nodes
                    ), loop
                )
                logger.info(f"Scheduled aggregation collection task for {aggregation_id}")
                # Log any immediate exceptions
                loop.call_soon_threadsafe(lambda: logger.info(f"Event loop is running: {loop.is_running()}"))
            except Exception as e:
                logger.error(f"❌ Error scheduling aggregation collection: {e}")
                logger.error(traceback.format_exc())
        else:
            # This node received the event but is not the designated aggregator
            logger.info(f"📢 Received aggregation event for {aggregation_id}")
            logger.info(f"  └─ Aggregator: {aggregator_node} (not this node)")
            logger.info(f"  └─ Contributing Node: {node_id}")
            logger.info(f"  └─ Global Round: {global_round_number}")
            
            # Note: All nodes receive all events, but only the aggregator acts on them
            # The aggregator will fetch all contributions from blockchain when timer expires
            
    elif event.event_type == "aggregation-confirmation":
        logger.info("Aggregation Event Handler: aggregation-confirmation event")
        # Extract validation metrics and broadcast to IoT nodes
        aggregation_id = None
        aggregator_node = None
        workflow_id = None
        global_round_number = None
        validation_score = None
        participating_nodes = None
        
        for attr in event.attributes:
            if attr.key == "aggregation_id":
                aggregation_id = attr.value
            elif attr.key == "aggregator_node":
                aggregator_node = attr.value
            elif attr.key == "workflow_id":
                workflow_id = attr.value
            elif attr.key == "global_round_number":
                global_round_number = int(attr.value)
            elif attr.key == "validation_score":
                validation_score = float(attr.value)
            elif attr.key == "participating_nodes":
                participating_nodes = json.loads(attr.value)
        
        # If this node was the aggregator, get the aggregated weights and broadcast with validation metrics
        if aggregator_node == aggregator.node_id and aggregation_id in aggregator.pending_aggregations:
            logger.info(f"Broadcasting confirmed aggregation {aggregation_id} with validation score {validation_score}")
            aggregation_info = aggregator.pending_aggregations[aggregation_id]
            
            # Get aggregated weights from memory
            aggregated_weights = aggregation_info.get('aggregated_weights', {})
            
            # Create validation metrics
            validation_metrics = {
                'accuracy': validation_score,  # The validation score IS the accuracy on MNIST validation set
                'validation_score': validation_score,
                'validated_by_blockchain': True
            }
            
            # Broadcast with validation metrics
            # Use run_coroutine_threadsafe to schedule the task in the aggregator's event loop
            try:
                loop = aggregator.loop
                future = asyncio.run_coroutine_threadsafe(
                    aggregator._broadcast_aggregated_model(
                        aggregated_weights,
                        participating_nodes,
                        workflow_id,
                        global_round_number,
                        validation_metrics
                    ), loop
                )
                logger.info(f"Scheduled broadcast task for {aggregation_id}")
            except Exception as e:
                logger.error(f"Error scheduling broadcast: {e}")
                logger.error(traceback.format_exc())
        
    else:
        logger.info(f"Aggregation Event Handler: Unhandled event type: {event.event_type}")


def create_transaction(family_name, family_version, payload, inputs, outputs):
    payload_bytes = json.dumps(payload).encode()

    txn_header = TransactionHeader(
        family_name=family_name,
        family_version=family_version,
        inputs=inputs,
        outputs=outputs,
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=hashlib.sha512(payload_bytes).hexdigest(),
        nonce=hex(int(time.time()))
    ).SerializeToString()

    signature = signer.sign(txn_header)

    txn = Transaction(
        header=txn_header,
        header_signature=signature,
        payload=payload_bytes
    )

    return txn


def create_batch(transactions):
    batch_header = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=[t.header_signature for t in transactions],
    ).SerializeToString()

    signature = signer.sign(batch_header)

    return Batch(
        header=batch_header,
        transactions=transactions,
        header_signature=signature
    )


def submit_batch(batch):
    batch_list = BatchList(batches=[batch])
    response = stream.send(
        message_type=Message.CLIENT_BATCH_SUBMIT_REQUEST,
        content=batch_list.SerializeToString()
    ).result()
    return process_future_result(response)


def process_future_result(future_result):
    response = ClientBatchSubmitResponse()
    response.ParseFromString(future_result.content)

    if response.status == ClientBatchSubmitResponse.OK:
        return {
            "status": "SUCCESS",
            "message": "Batch submitted successfully"
        }
    elif response.status == ClientBatchSubmitResponse.INVALID_BATCH:
        return {
            "status": "FAILURE",
            "message": "Invalid batch submitted",
            "error_details": response.error_message
        }
    else:
        return {
            "status": "FAILURE",
            "message": f"Batch submission failed with status: {response.status}",
            "error_details": response.error_message
        }


def run_event_loop_in_thread(loop):
    """Run the event loop in a separate thread"""
    asyncio.set_event_loop(loop)
    loop.run_forever()

def main():
    logger.info("Starting Aggregation Event Handler")
    
    # Create a new event loop for async operations
    loop = asyncio.new_event_loop()
    
    # Run the event loop in a separate thread
    loop_thread = threading.Thread(target=run_event_loop_in_thread, args=(loop,), daemon=True)
    loop_thread.start()
    logger.info("Event loop thread started")
    
    # Initialize aggregator with the event loop
    aggregator = FederatedAggregator(node_id, redis)
    aggregator.loop = loop  # Ensure aggregator uses our event loop

    # Set up event subscriptions
    aggregation_request_event = EventSubscription(
        event_type="aggregation-request"
    )
    
    aggregation_confirmation_event = EventSubscription(
        event_type="aggregation-confirmation"
    )

    request = ClientEventsSubscribeRequest(
        subscriptions=[aggregation_request_event, aggregation_confirmation_event]
    )

    logger.info(f"Subscribing request: {request}")
    response_future = stream.send(
        message_type=Message.CLIENT_EVENTS_SUBSCRIBE_REQUEST,
        content=request.SerializeToString()
    )
    response = ClientEventsSubscribeResponse()
    response.ParseFromString(response_future.result().content)

    if response.status != ClientEventsSubscribeResponse.OK:
        logger.error(f"Subscription failed: {response.response_message}")
        return

    logger.info("Aggregation Handler: Subscription successful. Listening for events...")

    while True:
        try:
            msg_future = stream.receive()
            msg = msg_future.result()
            if msg.message_type == Message.CLIENT_EVENTS:
                event_list = EventList()
                event_list.ParseFromString(msg.content)
                for event in event_list.events:
                    handle_aggregation_event(event, aggregator)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error receiving message: {e}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    redis = initialize_redis()
    node_id = os.getenv('NODE_ID')
    stream = Stream(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(load_private_key())
    main()