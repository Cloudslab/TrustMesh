import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import time
import traceback

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.core import TransactionProcessor
from coredis import RedisCluster
from coredis.exceptions import RedisError
from ibmcloudant.cloudant_v1 import CloudantV1
from ibm_cloud_sdk_core.authenticators import BasicAuthenticator
from ibm_cloud_sdk_core import ApiException

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

# CouchDB configuration for model weights storage
COUCHDB_URL = f"https://{os.getenv('COUCHDB_HOST', 'couchdb-0.default.svc.cluster.local:6984')}"
COUCHDB_USERNAME = os.getenv('COUCHDB_USER')
COUCHDB_PASSWORD = os.getenv('COUCHDB_PASSWORD')
COUCHDB_DB = 'model_weights'  # Dedicated database for model weights
COUCHDB_SSL_CA = os.getenv('COUCHDB_SSL_CA')
COUCHDB_SSL_CERT = os.getenv('COUCHDB_SSL_CERT')
COUCHDB_SSL_KEY = os.getenv('COUCHDB_SSL_KEY')

# Sawtooth configuration
FAMILY_NAME = 'aggregation-request'
FAMILY_VERSION = '1.0'
# This namespace is shared with aggregation-confirmation-tp to force serial execution
AGGREGATION_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]
WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
# Also include confirmation namespace to ensure serialization
CONFIRMATION_NAMESPACE = hashlib.sha512('aggregation-confirmation'.encode()).hexdigest()[:6]

# Federated learning configuration
AGGREGATION_TIMEOUT = int(os.getenv('AGGREGATION_TIMEOUT', '180'))  # 3 minutes default
MIN_NODES_FOR_AGGREGATION = int(os.getenv('MIN_NODES_FOR_AGGREGATION', '1'))  # Minimum 1 node for aggregation

logger = logging.getLogger(__name__)


class AggregationRequestTransactionHandler(TransactionHandler):
    def __init__(self):
        self.redis = None
        self.couchdb_client = None
        self.couchdb_certs = []
        self.loop = asyncio.get_event_loop()
        self._initialize_redis()
        self._initialize_couchdb()

    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        # Must include all namespaces to force serial execution with aggregation-confirmation-tp
        return [AGGREGATION_NAMESPACE, WORKFLOW_NAMESPACE, CONFIRMATION_NAMESPACE]

    def _initialize_redis(self):
        logger.info("Starting Redis initialization")
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
                logger.warning("REDIS_SSL_CA is empty or not set")

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
                logger.warning("REDIS_SSL_CERT or REDIS_SSL_KEY is empty or not set")

            logger.info(f"Attempting to connect to Redis cluster at {REDIS_HOST}:{REDIS_PORT}")
            self.redis = RedisCluster(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                ssl=True,
                ssl_context=ssl_context,
                decode_responses=True
            )

            self.loop.run_until_complete(self.redis.ping())
            logger.info("Connected to Redis cluster successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        finally:
            for file_path in temp_files:
                try:
                    os.unlink(file_path)
                    logger.debug(f"Temporary file deleted: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")
    
    def _initialize_couchdb(self):
        """Initialize CouchDB connection following framework patterns"""
        logger.info("Starting CouchDB initialization for aggregation request TP")
        try:
            authenticator = BasicAuthenticator(COUCHDB_USERNAME, COUCHDB_PASSWORD)
            self.couchdb_client = CloudantV1(authenticator=authenticator)
            self.couchdb_client.set_service_url(COUCHDB_URL)

            cert_files = None

            if COUCHDB_SSL_CA:
                ca_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.crt', delete=False)
                ca_file.write(COUCHDB_SSL_CA)
                ca_file.flush()
                self.couchdb_certs.append(ca_file)
                ssl_verify = ca_file.name
            else:
                logger.warning("COUCHDB_SSL_CA is empty or not set")
                ssl_verify = False

            if COUCHDB_SSL_CERT and COUCHDB_SSL_KEY:
                cert_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.crt', delete=False)
                key_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.key', delete=False)
                cert_file.write(COUCHDB_SSL_CERT)
                key_file.write(COUCHDB_SSL_KEY)
                cert_file.flush()
                key_file.flush()
                self.couchdb_certs.extend([cert_file, key_file])
                cert_files = (cert_file.name, key_file.name)
            else:
                logger.warning("COUCHDB_SSL_CERT or COUCHDB_SSL_KEY is empty or not set")

            # Set the SSL configuration for the client
            self.couchdb_client.set_http_config({
                'verify': ssl_verify,
                'cert': cert_files
            })

            # Test connection by checking if database exists
            try:
                self.couchdb_client.get_database_information(db=COUCHDB_DB).get_result()
                logger.info(f"Successfully connected to CouchDB database '{COUCHDB_DB}'")
            except Exception as db_error:
                logger.warning(f"Database '{COUCHDB_DB}' not accessible: {db_error}")

        except Exception as e:
            logger.error(f"Failed to initialize CouchDB connection: {str(e)}")
            self.cleanup_couchdb_temp_files()
            raise
    
    def cleanup_couchdb_temp_files(self):
        """Clean up temporary certificate files"""
        for cert_file in self.couchdb_certs:
            try:
                cert_file.close()
                os.unlink(cert_file.name)
                logger.debug(f"Cleaned up temporary cert file: {cert_file.name}")
            except Exception as e:
                logger.warning(f"Failed to clean up cert file: {e}")
    
    def _store_model_weights_in_couchdb(self, doc_id, model_weights, node_id, aggregation_id):
        """Store model weights in CouchDB following framework patterns"""
        try:
            # Create content hash for integrity verification
            content_hash = hashlib.sha256(json.dumps(model_weights, sort_keys=True).encode()).hexdigest()
            
            # Create document structure
            doc = {
                '_id': doc_id,
                'aggregation_id': aggregation_id,
                'node_id': node_id,
                'model_weights': model_weights,
                'content_hash': content_hash,
                'timestamp': int(time.time()),
                'metadata': {
                    'stored_by': 'aggregation-request-tp',
                    'storage_version': '1.0'
                }
            }
            
            # Store in CouchDB
            result = self.couchdb_client.put_document(
                db=COUCHDB_DB,
                doc_id=doc_id,
                document=doc
            ).get_result()
            
            logger.info(f"Stored model weights in CouchDB: {doc_id}")
            return {
                'success': True,
                'doc_id': doc_id,
                'content_hash': content_hash,
                'rev': result.get('rev')
            }
            
        except Exception as e:
            logger.error(f"Failed to store model weights in CouchDB: {e}")
            return None

    def apply(self, transaction, context):
        logger.info("Entering apply method for aggregation request")
        try:
            payload = json.loads(transaction.payload.decode())

            self._handle_aggregation_request(payload, context)

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
            raise InvalidTransaction("Invalid payload: not a valid JSON")
        except KeyError as e:
            logger.error(f"Missing key in payload: {str(e)}")
            raise InvalidTransaction(f"Invalid payload: missing {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _handle_aggregation_request(self, payload, context):
        logger.info("Entering _handle_aggregation_request method")
        
        workflow_id = payload['workflow_id']
        node_id = payload['node_id']
        model_weights = payload['model_weights']
        iot_round_number = payload.get('round_number', 1)  # IoT node's local round - ignore for aggregation logic
        
        logger.info(f"Aggregation request - Workflow: {workflow_id}, Node: {node_id}, IoT Round: {iot_round_number} (ignored)")

        # Validate workflow exists (any valid TrustMesh workflow can support aggregation)
        if not self._validate_workflow_exists(context, workflow_id):
            logger.error(f"Invalid workflow ID: {workflow_id}")
            raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

        # Check for existing aggregation round (use workflow_id only, ignore IoT round numbers)
        existing_round = self._get_existing_aggregation_round(context, workflow_id)
        
        if existing_round:
            # Join existing aggregation round
            aggregation_id = existing_round['aggregation_id']
            aggregator_node = existing_round['aggregator_node']
            global_round = existing_round['global_round_number']
            logger.info(f"Joining existing aggregation round {aggregation_id} (global round {global_round}) with aggregator {aggregator_node}")
            
            # Add this node's contribution to the round
            self._add_node_to_aggregation_round(context, aggregation_id, node_id, model_weights, payload)
            
        else:
            # Create new aggregation round - use consensus to elect aggregator
            aggregator_node = self.loop.run_until_complete(self._select_aggregator())
            global_round_number = self._get_next_global_round_number(context, workflow_id)
            # Use deterministic aggregation ID based on workflow and global round only
            aggregation_id = self._make_deterministic_aggregation_id(workflow_id, global_round_number)
            
            logger.info(f"Creating new aggregation round {aggregation_id} (global round {global_round_number}) with aggregator {aggregator_node}")
            
            # Create aggregation round record
            self._create_aggregation_round(context, workflow_id, aggregation_id, aggregator_node, 
                                         global_round_number, node_id, model_weights, payload)

        # Get the global round number from the aggregation record
        aggregation_round = existing_round if existing_round else self._get_aggregation_round(context, aggregation_id)
        global_round_number = aggregation_round.get('global_round_number', 1)
        
        # Emit aggregation request event with model weights for aggregator to store in CouchDB
        context.add_event(
            event_type="aggregation-request",
            attributes=[
                ("workflow_id", workflow_id),
                ("aggregation_id", aggregation_id),
                ("node_id", node_id),
                ("global_round_number", str(global_round_number)),
                ("aggregator_node", aggregator_node),
                ("weights_doc_id", f"{aggregation_id}_{node_id}_weights"),
                ("weights_hash", hashlib.sha256(json.dumps(model_weights, sort_keys=True).encode()).hexdigest()),
                ("model_weights", json.dumps(model_weights)),  # Include model weights in event for aggregator
                ("metadata", json.dumps(payload.get('metadata', {}))),
                ("timestamp", str(int(time.time())))
            ]
        )
        
        logger.info(f"Aggregation request processed for node {node_id} in global round {global_round_number}")

    def _validate_workflow_exists(self, context, workflow_id):
        """Validate that the workflow exists (any TrustMesh workflow can support aggregation)"""
        try:
            address = self._make_workflow_address(workflow_id)
            state_entries = context.get_state([address])
            if state_entries:
                workflow_data = json.loads(state_entries[0].data.decode())
                logger.info(f"Workflow {workflow_id} exists and can support aggregation")
                return True
            logger.warning(f"Workflow {workflow_id} not found in blockchain state")
            return False
        except Exception as e:
            logger.error(f"Error validating workflow existence: {e}")
            return False

    def _get_existing_aggregation_round(self, context, workflow_id):
        """Check if an aggregation round already exists for this workflow (ignore IoT round numbers)"""
        try:
            # Get the current active aggregation round ID for this workflow
            active_round_address = self._make_active_round_address(workflow_id)
            state_entries = context.get_state([active_round_address])
            
            if not state_entries:
                logger.info(f"No active aggregation round found for workflow {workflow_id}")
                return None
            
            # Get the aggregation ID from the active round tracker
            active_round_data = json.loads(state_entries[0].data.decode())
            active_aggregation_id = active_round_data.get('aggregation_id')
            
            if not active_aggregation_id:
                logger.warning(f"Active round tracker exists but no aggregation_id found")
                return None
                
            # Now get the actual aggregation round data
            aggregation_address = self._make_aggregation_address(active_aggregation_id)
            round_entries = context.get_state([aggregation_address])
            
            if not round_entries:
                logger.warning(f"Active round {active_aggregation_id} not found in state")
                return None
                
            round_data = json.loads(round_entries[0].data.decode())
            
            # Check if the round is still collecting (not locked or confirmed)
            if round_data.get('status') == 'collecting':
                logger.info(f"Found existing active aggregation round: {round_data['aggregation_id']} (global round {round_data.get('global_round_number', 'unknown')})")
                return round_data
            else:
                logger.info(f"Aggregation round {active_aggregation_id} has status: {round_data.get('status')} - creating new round")
                return None
                
        except Exception as e:
            logger.error(f"Error checking for existing aggregation round: {e}")
            return None

    def _create_aggregation_round(self, context, workflow_id, aggregation_id, aggregator_node, 
                                 global_round_number, initial_node_id, initial_weights, initial_payload):
        """Create a new aggregation round"""
        try:
            address = self._make_aggregation_address(aggregation_id)
            
            # Create document ID and content hash for model weights (store metadata only in blockchain)
            initial_weights_doc_id = f"{aggregation_id}_{initial_node_id}_weights"
            content_hash = hashlib.sha256(json.dumps(initial_weights, sort_keys=True).encode()).hexdigest()
            
            logger.info(f"Prepared model weights metadata for {initial_node_id}: {initial_weights_doc_id}")
            
            round_data = {
                'aggregation_id': aggregation_id,
                'workflow_id': workflow_id,
                'global_round_number': global_round_number,  # Use global round instead of IoT round
                'aggregator_node': aggregator_node,
                'status': 'collecting',
                'participating_nodes': [initial_node_id],
                'node_contributions': {
                    initial_node_id: {
                        'weights_doc_id': initial_weights_doc_id,
                        'weights_hash': content_hash,
                        'metadata': initial_payload.get('metadata', {})
                    }
                },
                'min_nodes_required': MIN_NODES_FOR_AGGREGATION,
                'expected_nodes': []  # Time-based aggregation - any number of nodes can participate
            }
            
            # Store the aggregation round data
            context.set_state({
                address: json.dumps(round_data).encode()
            })
            
            # Also store this as the active round for the workflow
            active_round_address = self._make_active_round_address(workflow_id)
            active_round_data = {
                'workflow_id': workflow_id,
                'aggregation_id': aggregation_id
            }
            context.set_state({
                active_round_address: json.dumps(active_round_data).encode()
            })
            
            logger.info(f"Created aggregation round {aggregation_id} for workflow {workflow_id} (global round {global_round_number})")
            
        except Exception as e:
            logger.error(f"Error creating aggregation round: {e}")
            raise InvalidTransaction(f"Failed to create aggregation round: {e}")

    def _add_node_to_aggregation_round(self, context, aggregation_id, node_id, model_weights, payload):
        """Add a node's contribution to an existing aggregation round"""
        try:
            address = self._make_aggregation_address(aggregation_id)
            state_entries = context.get_state([address])
            
            if not state_entries:
                raise InvalidTransaction(f"Aggregation round {aggregation_id} not found")
            
            round_data = json.loads(state_entries[0].data.decode())
            
            # Check if node already contributed
            if node_id in round_data['participating_nodes']:
                logger.warning(f"Node {node_id} already contributed to round {aggregation_id}")
                return
            
            # Check if round is still collecting
            if round_data['status'] != 'collecting':
                raise InvalidTransaction(f"Aggregation round {aggregation_id} is no longer collecting contributions")
            
            # Create document ID and content hash for model weights (store metadata only in blockchain)
            weights_doc_id = f"{aggregation_id}_{node_id}_weights"
            content_hash = hashlib.sha256(json.dumps(model_weights, sort_keys=True).encode()).hexdigest()
            
            logger.info(f"Prepared model weights metadata for {node_id}: {weights_doc_id}")
            
            # Add node contribution metadata (no weights in blockchain)
            round_data['participating_nodes'].append(node_id)
            round_data['node_contributions'][node_id] = {
                'weights_doc_id': weights_doc_id,
                'weights_hash': content_hash,
                'metadata': payload.get('metadata', {})
            }
            
            # Update state
            context.set_state({
                address: json.dumps(round_data).encode()
            })
            
            logger.info(f"Added node {node_id} to aggregation round {aggregation_id}")
            logger.info(f"Round now has {len(round_data['participating_nodes'])} participating nodes")
            
        except Exception as e:
            logger.error(f"Error adding node to aggregation round: {e}")
            raise InvalidTransaction(f"Failed to add node to aggregation round: {e}")


    def _get_next_global_round_number(self, context, workflow_id):
        """Get the next global round number for this workflow"""
        try:
            # Check for existing global round counter
            counter_address = self._make_global_round_counter_address(workflow_id)
            state_entries = context.get_state([counter_address])
            
            if state_entries:
                counter_data = json.loads(state_entries[0].data.decode())
                current_round = counter_data.get('current_global_round', 0)
                next_round = current_round + 1
            else:
                next_round = 1
            
            # Update the global round counter
            counter_data = {
                'workflow_id': workflow_id,
                'current_global_round': next_round
            }
            
            context.set_state({
                counter_address: json.dumps(counter_data).encode()
            })
            
            logger.info(f"Generated global round number {next_round} for workflow {workflow_id}")
            return next_round
            
        except Exception as e:
            logger.error(f"Error getting next global round number: {e}")
            return 1  # Fallback to round 1

    def _get_aggregation_round(self, context, aggregation_id):
        """Get aggregation round data by ID"""
        try:
            address = self._make_aggregation_address(aggregation_id)
            state_entries = context.get_state([address])
            
            if state_entries:
                return json.loads(state_entries[0].data.decode())
            return None
            
        except Exception as e:
            logger.error(f"Error getting aggregation round {aggregation_id}: {e}")
            return None

    @staticmethod
    def _make_global_round_counter_address(workflow_id):
        """Generate blockchain address for global round counter"""
        counter_key = f"{workflow_id}_global_round_counter"
        return AGGREGATION_NAMESPACE + hashlib.sha512(counter_key.encode()).hexdigest()[:64]
    
    @staticmethod
    def _make_active_round_address(workflow_id):
        """Generate blockchain address for active aggregation round tracker"""
        active_key = f"{workflow_id}_active_round"
        return AGGREGATION_NAMESPACE + hashlib.sha512(active_key.encode()).hexdigest()[:64]

    async def _select_aggregator(self):
        """Select aggregator node using deterministic algorithm with consensus"""
        logger.info("Entering _select_aggregator method")
        try:
            if self.redis is None:
                logger.error("Redis connection not initialized")
                raise InvalidTransaction("Redis connection not initialized")

            # Get all resource keys first, then sort for deterministic order
            resource_keys = []
            async for key in self.redis.scan_iter(match='resources_*'):
                resource_keys.append(key)
            
            # Sort keys to ensure deterministic order across validators
            resource_keys.sort()
            
            node_resources = []
            logger.info("Scanning Redis for resource data")
            for key in resource_keys:
                node_id = key.split('_', 1)[1]
                logger.debug(f"Fetching data for node: {node_id}")
                redis_data = await self.redis.get(key)
                if redis_data:
                    resource_data = json.loads(redis_data)
                    logger.debug(f"Resource data for node {node_id}: {resource_data}")
                    node_resources.append({
                        'id': node_id,
                        'resources': resource_data
                    })

            if not node_resources:
                logger.error("No resource data available")
                raise InvalidTransaction("No resource data available")

            logger.info("Selecting node with most available resources as aggregator")
            # Sort by node_id for deterministic tie-breaking when resources are equal
            selected_node = max(node_resources, key=lambda x: (self._calculate_available_resources(x['resources']), x['id']))
            logger.info(f"Selected aggregator node: {selected_node['id']}")
            return selected_node['id']
        except RedisError as e:
            logger.error(f"Error accessing Redis: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction("Failed to access node resource data")
        except Exception as e:
            logger.error(f"Unexpected error in _select_aggregator: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(f"Error selecting aggregator: {str(e)}")

    @staticmethod
    def _calculate_available_resources(resources):
        """Calculate available resources for node selection"""
        cpu_available = resources['cpu']['total'] * (1 - resources['cpu']['used_percent'] / 100)
        memory_available = resources['memory']['total'] * (1 - resources['memory']['used_percent'] / 100)
        return cpu_available + memory_available

    @staticmethod
    def _make_aggregation_address(aggregation_id):
        """Generate blockchain address for aggregation round"""
        return AGGREGATION_NAMESPACE + hashlib.sha512(aggregation_id.encode()).hexdigest()[:64]

    @staticmethod
    def _make_aggregation_address_prefix(workflow_id):
        """Generate address prefix for searching aggregation rounds"""
        prefix_key = f"{workflow_id}_agg"
        return AGGREGATION_NAMESPACE + hashlib.sha512(prefix_key.encode()).hexdigest()[:64]

    @staticmethod
    def _make_deterministic_aggregation_id(workflow_id, global_round_number):
        """Generate deterministic aggregation ID that will be identical across all validators"""
        return f"{workflow_id}_agg_{global_round_number}"
    
    @staticmethod
    def _make_workflow_address(workflow_id):
        """Generate blockchain address for workflow data"""
        return WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]


def main():
    logger.info("Starting Aggregation Request Transaction Processor")
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = AggregationRequestTransactionHandler()
    processor.add_handler(handler)
    try:
        logger.info("Starting processor")
        processor.start()
    except KeyboardInterrupt:
        logger.info("Stopping processor")
        pass
    except Exception as e:
        logger.error(f"Processor error: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("Stopping processor")
        processor.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()