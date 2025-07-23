"""
Federated Schedule Transaction Processor for TrustMesh

This transaction processor handles consensus-based coordination of federated learning
workflows by integrating with TrustMesh's existing scheduling architecture.
"""

import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import time
import traceback
import uuid
from typing import Dict, List, Optional

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.log import init_console_logging
from sawtooth_sdk.processor.core import TransactionProcessor
from coredis import RedisCluster
from coredis.exceptions import RedisError

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

# Constants
FEDERATED_SCHEDULE_FAMILY_NAME = 'federated-schedule'
FEDERATED_SCHEDULE_FAMILY_VERSION = '1.0'
FEDERATED_SCHEDULE_NAMESPACE = hashlib.sha512(FEDERATED_SCHEDULE_FAMILY_NAME.encode('utf-8')).hexdigest()[0:6]

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class FederatedScheduleState:
    """Manages blockchain state for federated learning workflows"""
    
    def __init__(self, context, redis_client=None):
        self.context = context
        self._timeout = 3
        self.redis_client = redis_client

    def _make_address(self, key: str) -> str:
        """Generate blockchain address for federated schedule data"""
        return FEDERATED_SCHEDULE_NAMESPACE + hashlib.sha512(key.encode('utf-8')).hexdigest()[-64:]
    
    def _sync_state_to_redis(self, blockchain_address: str, state_data: str, redis_key: str = None):
        """Sync blockchain state to Redis for cross-TP communication"""
        if not self.redis_client:
            return
        
        try:
            # Create a new event loop for this thread if needed
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Store with blockchain address for direct queries
            redis_blockchain_key = f"blockchain_state_{blockchain_address}"
            loop.run_until_complete(self.redis_client.set(redis_blockchain_key, state_data, ex=3600))  # 1 hour expiration
            
            # Store with semantic key if provided
            if redis_key:
                loop.run_until_complete(self.redis_client.set(redis_key, state_data, ex=3600))
            
            LOGGER.info(f"Synced blockchain state to Redis: {redis_key or redis_blockchain_key}")
            
        except Exception as e:
            LOGGER.warning(f"Failed to sync state to Redis: {e}")

    def set_federated_round(self, workflow_id: str, round_number: int, fed_round_data: Dict):
        """Store federated round information on blockchain"""
        address = self._make_address(f"fed_round_{workflow_id}_{round_number}")
        
        state_data = json.dumps({
            'workflow_id': workflow_id,
            'round_number': round_number,
            'schedule_id': fed_round_data['schedule_id'],
            'coordinator_node': fed_round_data['coordinator_node'],
            'expected_nodes': fed_round_data['expected_nodes'],
            'min_nodes_required': fed_round_data['min_nodes_required'],
            'status': fed_round_data['status'],
            'created_time': fed_round_data['created_time'],
            'participants': fed_round_data.get('participants', [])
        })
        
        addresses = self.context.set_state({address: state_data}, timeout=self._timeout)
        if len(addresses) < 1:
            raise InternalError("State error: failed to store federated round data")
        
        # Sync blockchain state to Redis for cross-TP communication
        self._sync_state_to_redis(address, state_data, f"fed_round:{workflow_id}:{round_number}")

    def get_federated_round(self, workflow_id: str, round_number: int) -> Optional[Dict]:
        """Retrieve federated round information from blockchain"""
        address = self._make_address(f"fed_round_{workflow_id}_{round_number}")
        
        state_entries = self.context.get_state([address], timeout=self._timeout)
        if state_entries:
            return json.loads(state_entries[0].data)
        return None

    def update_federated_round_participants(self, workflow_id: str, round_number: int, 
                                          node_id: str, participant_data: Dict):
        """Update participant information for a federated round"""
        fed_round = self.get_federated_round(workflow_id, round_number)
        if not fed_round:
            raise InvalidTransaction(f"Federated round {workflow_id}:{round_number} not found")
        
        # Add participant
        participants = fed_round.get('participants', [])
        # Remove existing entry for this node
        participants = [p for p in participants if p['node_id'] != node_id]
        participants.append({
            'node_id': node_id,
            'joined_time': time.time(),
            **participant_data
        })
        
        fed_round['participants'] = participants
        
        # Update status if threshold reached
        if len(participants) >= fed_round['min_nodes_required']:
            fed_round['status'] = 'ready'
        
        if len(participants) == len(fed_round['expected_nodes']):
            fed_round['status'] = 'complete'
        
        self.set_federated_round(workflow_id, round_number, fed_round)

    def set_coordinator_election(self, workflow_id: str, round_number: int, 
                               coordinator_node: str, election_data: Dict):
        """Store coordinator election results"""
        address = self._make_address(f"coordinator_election_{workflow_id}_{round_number}")
        
        state_data = json.dumps({
            'workflow_id': workflow_id,
            'round_number': round_number,
            'coordinator_node': coordinator_node,
            'election_time': time.time(),
            'election_criteria': election_data
        })
        
        addresses = self.context.set_state({address: state_data}, timeout=self._timeout)
        if len(addresses) < 1:
            raise InternalError("State error: failed to store coordinator election")
        
        # Sync blockchain state to Redis for cross-TP communication
        self._sync_state_to_redis(address, state_data, f"fed_coordinator:{workflow_id}:{round_number}")


class FederatedScheduleTransactionHandler(TransactionHandler):
    """Transaction handler for federated learning schedule coordination"""
    
    def __init__(self):
        self.redis_client = None
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._initialize_redis()
    
    def _initialize_redis(self):
        """Initialize Redis cluster connection with SSL"""
        LOGGER.info("Starting Redis initialization")
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
            else:
                LOGGER.warning("REDIS_SSL_CA is empty or not set")

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
            else:
                LOGGER.warning("REDIS_SSL_CERT or REDIS_SSL_KEY is empty or not set")

            LOGGER.info(f"Attempting to connect to Redis cluster at {REDIS_HOST}:{REDIS_PORT}")
            self.redis_client = RedisCluster(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                ssl=True,
                ssl_context=ssl_context,
                decode_responses=True
            )

            self.loop.run_until_complete(self.redis_client.ping())
            LOGGER.info("Connected to Redis cluster successfully")
        except Exception as e:
            LOGGER.warning(f"Could not connect to Redis: {e}")
            LOGGER.warning(traceback.format_exc())
            self.redis_client = None
        finally:
            # Clean up temporary files
            for temp_file in temp_files:
                try:
                    os.unlink(temp_file)
                    LOGGER.debug(f"Temporary file deleted: {temp_file}")
                except Exception:
                    pass

    @property
    def family_name(self):
        return FEDERATED_SCHEDULE_FAMILY_NAME

    @property
    def family_versions(self):
        return [FEDERATED_SCHEDULE_FAMILY_VERSION]

    @property
    def namespaces(self):
        return [FEDERATED_SCHEDULE_NAMESPACE]

    def apply(self, transaction, context):
        """Process federated schedule transactions"""
        
        state = FederatedScheduleState(context, self.redis_client)
        
        try:
            payload = json.loads(transaction.payload.decode('utf-8'))
        except ValueError:
            raise InvalidTransaction("Invalid payload format")
        
        action = payload.get('action')
        
        if action == 'create_federated_round':
            return self._create_federated_round(payload, state)
        elif action == 'join_federated_round':
            return self._join_federated_round(payload, state)
        elif action == 'elect_coordinator':
            return self._elect_coordinator(payload, state)
        else:
            raise InvalidTransaction(f"Unknown action: {action}")

    def _create_federated_round(self, payload: Dict, state: FederatedScheduleState):
        """Create a new federated learning round with consensus-elected coordinator"""
        
        workflow_id = payload.get('workflow_id')
        round_number = payload.get('round_number', 1)
        expected_nodes = payload.get('expected_nodes', [])
        min_nodes_required = payload.get('min_nodes_required', 3)
        
        if not workflow_id or not expected_nodes:
            raise InvalidTransaction("Missing required fields: workflow_id, expected_nodes")
        
        # Check if round already exists
        existing_round = state.get_federated_round(workflow_id, round_number)
        if existing_round:
            raise InvalidTransaction(f"Federated round {workflow_id}:{round_number} already exists")
        
        # Elect coordinator using consensus algorithm
        coordinator_node = self._consensus_elect_coordinator()
        
        # Generate shared schedule ID
        schedule_id = str(uuid.uuid4())
        
        # Create federated round
        fed_round_data = {
            'schedule_id': schedule_id,
            'coordinator_node': coordinator_node,
            'expected_nodes': expected_nodes,
            'min_nodes_required': min_nodes_required,
            'status': 'initializing',
            'created_time': time.time(),
            'participants': []
        }
        
        # Store on blockchain
        state.set_federated_round(workflow_id, round_number, fed_round_data)
        
        # Store coordinator election details
        election_data = self._get_coordinator_election_data(coordinator_node)
        state.set_coordinator_election(workflow_id, round_number, coordinator_node, election_data)
        
        LOGGER.info(f"Created federated round {workflow_id}:{round_number} with coordinator {coordinator_node}")
        LOGGER.info(f"Schedule ID: {schedule_id}, Expected nodes: {expected_nodes}")

    def _join_federated_round(self, payload: Dict, state: FederatedScheduleState):
        """Join an existing federated round"""
        
        workflow_id = payload.get('workflow_id')
        round_number = payload.get('round_number', 1)
        node_id = payload.get('node_id')
        node_data = payload.get('node_data', {})
        
        if not all([workflow_id, node_id]):
            raise InvalidTransaction("Missing required fields: workflow_id, node_id")
        
        # Get existing round
        fed_round = state.get_federated_round(workflow_id, round_number)
        if not fed_round:
            raise InvalidTransaction(f"Federated round {workflow_id}:{round_number} not found")
        
        # Validate node is expected
        if node_id not in fed_round['expected_nodes']:
            raise InvalidTransaction(f"Node {node_id} not expected in round {workflow_id}:{round_number}")
        
        # Check if already joined
        existing_participants = [p['node_id'] for p in fed_round.get('participants', [])]
        if node_id in existing_participants:
            raise InvalidTransaction(f"Node {node_id} already joined round {workflow_id}:{round_number}")
        
        # Update participants
        state.update_federated_round_participants(workflow_id, round_number, node_id, node_data)
        
        LOGGER.info(f"Node {node_id} joined federated round {workflow_id}:{round_number}")

    def _elect_coordinator(self, payload: Dict, state: FederatedScheduleState):
        """Manually elect a coordinator (for testing/override purposes)"""
        
        workflow_id = payload.get('workflow_id')
        round_number = payload.get('round_number', 1)
        coordinator_node = payload.get('coordinator_node')
        
        if not all([workflow_id, coordinator_node]):
            raise InvalidTransaction("Missing required fields: workflow_id, coordinator_node")
        
        # Store coordinator election
        election_data = {'manual_election': True, 'timestamp': time.time()}
        state.set_coordinator_election(workflow_id, round_number, coordinator_node, election_data)
        
        LOGGER.info(f"Manually elected coordinator {coordinator_node} for {workflow_id}:{round_number}")

    def _consensus_elect_coordinator(self) -> str:
        """Elect coordinator using consensus algorithm based on compute node resources"""
        
        if not self.redis_client:
            # Fallback to default if Redis unavailable
            LOGGER.warning("Redis unavailable, using default coordinator")
            return "compute-node-1"
        
        try:
            # Create a new event loop for this thread if needed
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Get all compute nodes and their resources
            compute_nodes = {}
            
            # Scan for compute node resource keys
            async def scan_compute_nodes():
                async for key in self.redis_client.scan_iter(match="resources_compute-node-*"):
                    try:
                        node_name = key.replace("resources_", "")
                        resource_data = await self.redis_client.get(key)
                        
                        if resource_data:
                            resources = json.loads(resource_data)
                            cpu_available = resources.get('cpu_available', 0)
                            memory_available = resources.get('memory_available', 0)
                            
                            # Calculate resource score (higher is better)
                            resource_score = cpu_available * 1000 + memory_available
                            compute_nodes[node_name] = {
                                'score': resource_score,
                                'cpu_available': cpu_available,
                                'memory_available': memory_available
                            }
                            
                    except Exception as e:
                        LOGGER.warning(f"Error processing node {key}: {e}")
                        continue
            
            loop.run_until_complete(scan_compute_nodes())
            
            if not compute_nodes:
                LOGGER.warning("No compute nodes found, using default")
                return "compute-node-1"
            
            # Select node with highest resource score
            elected_node = max(compute_nodes.keys(), key=lambda x: compute_nodes[x]['score'])
            
            LOGGER.info(f"Elected coordinator: {elected_node} (score: {compute_nodes[elected_node]['score']})")
            return elected_node
            
        except Exception as e:
            LOGGER.error(f"Error in coordinator election: {e}")
            return "compute-node-1"  # Fallback
    
    def _get_coordinator_election_data(self, coordinator_node: str) -> Dict:
        """Get election criteria data for the selected coordinator"""
        
        if not self.redis_client:
            return {'method': 'fallback', 'reason': 'redis_unavailable'}
        
        try:
            # Create a new event loop for this thread if needed
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            resource_key = f"resources_{coordinator_node}"
            resource_data = loop.run_until_complete(self.redis_client.get(resource_key))
            
            if resource_data:
                resources = json.loads(resource_data)
                return {
                    'method': 'resource_based',
                    'cpu_available': resources.get('cpu_available', 0),
                    'memory_available': resources.get('memory_available', 0),
                    'election_time': time.time()
                }
            else:
                return {'method': 'default', 'reason': 'no_resource_data'}
                
        except Exception as e:
            LOGGER.warning(f"Error getting election data: {e}")
            return {'method': 'error', 'error': str(e)}


def main():
    """Main function to run the federated schedule transaction processor"""
    
    init_console_logging(verbose_level=2)
    
    try:
        validator_url = os.getenv('VALIDATOR_URL', 'tcp://sawtooth-validator:4004')
        processor = TransactionProcessor(url=validator_url)
        handler = FederatedScheduleTransactionHandler()
        processor.add_handler(handler)
        
        LOGGER.info("Starting Federated Schedule Transaction Processor")
        processor.start()
        
    except KeyboardInterrupt:
        LOGGER.info("Federated Schedule TP interrupted")
    except Exception as e:
        LOGGER.error(f"Error in Federated Schedule TP: {e}")
        raise


if __name__ == '__main__':
    main()