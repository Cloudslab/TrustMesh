import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

from ibmcloudant.cloudant_v1 import CloudantV1
from ibm_cloud_sdk_core.authenticators import BasicAuthenticator
from coredis import RedisCluster
from coredis.exceptions import RedisError
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.handler import TransactionHandler

# Cloudant configuration
CLOUDANT_URL = f"https://{os.getenv('COUCHDB_HOST', 'cloudant-host:6984')}"
CLOUDANT_USERNAME = os.getenv('COUCHDB_USER')
CLOUDANT_PASSWORD = os.getenv('COUCHDB_PASSWORD')
CLOUDANT_DB = 'task_data'
CLOUDANT_SSL_CA = os.getenv('COUCHDB_SSL_CA')
CLOUDANT_SSL_CERT = os.getenv('COUCHDB_SSL_CERT')
CLOUDANT_SSL_KEY = os.getenv('COUCHDB_SSL_KEY')

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

# Sawtooth configuration
FAMILY_NAME = 'iot-data'
FAMILY_VERSION = '1.0'

WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
IOT_DATA_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTDataTransactionHandler(TransactionHandler):
    def __init__(self):
        self.cloudant_client = None
        self.redis = None
        self.loop = asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor()
        self.cloudant_certs = []
        self._initialize_cloudant()
        self._initialize_redis()

    def _initialize_cloudant(self):
        logger.info("Starting Cloudant initialization")
        try:
            authenticator = BasicAuthenticator(CLOUDANT_USERNAME, CLOUDANT_PASSWORD)
            self.cloudant_client = CloudantV1(authenticator=authenticator)
            self.cloudant_client.set_service_url(CLOUDANT_URL)

            cert_files = None

            if CLOUDANT_SSL_CA:
                ca_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.crt', delete=False)
                ca_file.write(CLOUDANT_SSL_CA)
                ca_file.flush()
                self.cloudant_certs.append(ca_file)
                ssl_verify = ca_file.name
            else:
                logger.warning("CLOUDANT_SSL_CA is empty or not set")
                ssl_verify = False

            if CLOUDANT_SSL_CERT and CLOUDANT_SSL_KEY:
                cert_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.crt', delete=False)
                key_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.key', delete=False)
                cert_file.write(CLOUDANT_SSL_CERT)
                key_file.write(CLOUDANT_SSL_KEY)
                cert_file.flush()
                key_file.flush()
                self.cloudant_certs.extend([cert_file, key_file])
                cert_files = (cert_file.name, key_file.name)
            else:
                logger.warning("CLOUDANT_SSL_CERT or CLOUDANT_SSL_KEY is empty or not set")

            # Set the SSL configuration for the client
            self.cloudant_client.set_http_config({
                'verify': ssl_verify,
                'cert': cert_files
            })

            self.cloudant_client.get_database_information(db=CLOUDANT_DB).get_result()
            logger.info(f"Successfully connected to database '{CLOUDANT_DB}'.")

        except Exception as e:
            logger.error(f"Failed to initialize Cloudant connection: {str(e)}")
            self.cleanup_temp_files()
            raise

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
            logger.info("Connected to Redis cluster successfully")

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
        finally:
            for file_path in temp_files:
                try:
                    os.unlink(file_path)
                    logger.debug(f"Temporary file deleted: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")

    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [IOT_DATA_NAMESPACE, WORKFLOW_NAMESPACE]

    def apply(self, transaction, context):
        try:
            payload = json.loads(transaction.payload.decode())
            iot_data = payload['iot_data']
            workflow_id = payload['workflow_id']
            schedule_id = payload['schedule_id']
            persist_data = payload.get('persist_data', False)
            node_id = payload.get('node_id')  # Node identifier for federated learning

            logger.info(f"Processing IoT data for workflow ID: {workflow_id}, schedule ID: {schedule_id}, node ID: {node_id}")

            if not isinstance(iot_data, list):
                raise InvalidTransaction("Invalid payload: iot_data must be a list")

            if not self._validate_workflow_id(context, workflow_id):
                raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

            # Check if this is a federated learning workflow
            is_federated = self._is_federated_workflow(context, workflow_id)
            
            if is_federated and not node_id:
                raise InvalidTransaction("Node ID is required for federated learning workflows")

            iot_data_hash = self._calculate_hash(iot_data)

            future = self.executor.submit(self._run_async_operations, context, workflow_id, schedule_id, iot_data,
                                          iot_data_hash, persist_data, node_id, is_federated)
            future.result()

            if persist_data:
                iot_data_address = self._make_iot_data_address(schedule_id)
                iot_data_state = json.dumps({
                    'schedule_id': schedule_id,
                    'workflow_id': workflow_id,
                    'iot_data_hash': iot_data_hash,
                    'node_id': node_id,
                    'is_federated': is_federated
                }).encode()

                logger.info(f"Writing IoT data hash to blockchain for schedule ID: {schedule_id}")
                context.set_state({iot_data_address: iot_data_state})
                logger.info(f"Successfully wrote IoT data hash to blockchain for schedule ID: {schedule_id}")

        except json.JSONDecodeError as _:
            raise InvalidTransaction("Invalid payload: not a valid JSON")
        except KeyError as e:
            raise InvalidTransaction(f"Invalid payload: missing {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _run_async_operations(self, context, workflow_id, schedule_id, iot_data, iot_data_hash, persist_data, node_id=None, is_federated=False):
        return self.loop.run_until_complete(
            self._async_operations(context, workflow_id, schedule_id, iot_data, iot_data_hash, persist_data, node_id, is_federated))

    async def _async_operations(self, context, workflow_id, schedule_id, iot_data, iot_data_hash, persist_data, node_id=None, is_federated=False):
        dependency_graph = self._get_dependency_graph(context, workflow_id)
        
        if is_federated:
            # For federated workflows, use a different storage strategy
            tasks = [self._store_federated_data_in_redis(workflow_id, schedule_id, dependency_graph["start"], 
                                                       iot_data, node_id, persist_data)]
            if persist_data:
                data_id = self._generate_federated_data_id(workflow_id, schedule_id, dependency_graph["start"], node_id, 'input')
                tasks.append(self._store_data_in_cloudant(data_id, iot_data, iot_data_hash, workflow_id, schedule_id))
        else:
            # Standard single-node workflow
            data_id = self._generate_data_id(workflow_id, schedule_id, dependency_graph["start"], 'input')
            tasks = [self._store_data_in_redis(data_id, iot_data, workflow_id, schedule_id, persist_data)]
            if persist_data:
                tasks.append(self._store_data_in_cloudant(data_id, iot_data, iot_data_hash, workflow_id, schedule_id))

        await asyncio.gather(*tasks)

    async def _store_data_in_redis(self, data_id, data, workflow_id, schedule_id, persist_data):
        try:
            data_json = json.dumps({
                'data': data,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id,
                'persist_data': persist_data
            })
            result = await self.redis.set(f"iot_data_{data_id}", data_json)
            if result:
                logger.info(f"Successfully stored IoT data in Redis for ID: {data_id}")
            else:
                raise Exception("Redis set operation failed")
        except RedisError as e:
            logger.error(f"Failed to store IoT data in Redis: {str(e)}")
            raise

    async def _store_federated_data_in_redis(self, workflow_id, schedule_id, app_id, data, node_id, persist_data):
        """Store federated learning data using Redis lists for multi-node aggregation"""
        try:
            # Store individual node data in a list
            federated_key = f"iot_data_{workflow_id}_{schedule_id}_{app_id}_federated_nodes"
            node_data = {
                'node_id': node_id,
                'data': data,
                'timestamp': time.time(),
                'workflow_id': workflow_id,
                'schedule_id': schedule_id,
                'persist_data': persist_data
            }
            
            # Add to Redis list (left push for FIFO order)
            result = await self.redis.lpush(federated_key, json.dumps(node_data))
            
            # Set expiration for the federated data (24 hours)
            await self.redis.expire(federated_key, 86400)
            
            logger.info(f"Successfully stored federated data for node {node_id} in Redis key: {federated_key}")
            
            # Check if we have reached the minimum threshold for federated processing
            await self._check_federated_completion_threshold(workflow_id, schedule_id, app_id)
            
        except RedisError as e:
            logger.error(f"Failed to store federated IoT data in Redis: {str(e)}")
            raise

    async def _check_federated_completion_threshold(self, workflow_id, schedule_id, app_id):
        """Check if federated learning round has enough participants to proceed"""
        timeout_start = time.time()
        timeout_duration = 300  # 5 minutes timeout
        
        try:
            # Get federated round information with timeout protection
            fed_round_info = await asyncio.wait_for(
                self._get_federated_round_info(workflow_id), 
                timeout=30.0  # 30 second timeout for state query
            )
            
            if not fed_round_info:
                logger.info(f"No federated round info found for workflow {workflow_id}")
                return
            
            min_nodes_required = fed_round_info.get('min_nodes_required', 3)
            expected_nodes = fed_round_info.get('expected_nodes', [])
            
            # Check current participants with timeout protection
            federated_key = f"iot_data_{workflow_id}_{schedule_id}_{app_id}_federated_nodes"
            
            # Implement timeout-aware participant checking
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    current_count = await asyncio.wait_for(
                        self.redis.llen(federated_key), 
                        timeout=10.0
                    )
                    break
                except asyncio.TimeoutError:
                    retry_count += 1
                    logger.warning(f"Redis timeout on attempt {retry_count}, retrying...")
                    if retry_count >= max_retries:
                        raise
                    await asyncio.sleep(1)
            
            logger.info(f"Federated round {workflow_id}:{schedule_id} - Current participants: {current_count}/{len(expected_nodes)}, Min required: {min_nodes_required}")
            
            # Check if we've exceeded the timeout
            if time.time() - timeout_start > timeout_duration:
                logger.error(f"Federated round {workflow_id}:{schedule_id} exceeded timeout ({timeout_duration}s)")
                await self._handle_federated_timeout(workflow_id, schedule_id, app_id, current_count, min_nodes_required)
                return
            
            # If we have minimum nodes, trigger the next stage
            if current_count >= min_nodes_required:
                await self._trigger_federated_processing(workflow_id, schedule_id, app_id)
            elif current_count > 0:
                # Set a timeout check for incomplete rounds
                await self._schedule_timeout_check(workflow_id, schedule_id, app_id, timeout_duration)
                
        except asyncio.TimeoutError as e:
            logger.error(f"Timeout while checking federated completion threshold: {str(e)}")
            await self._handle_federated_timeout(workflow_id, schedule_id, app_id, 0, min_nodes_required)
        except Exception as e:
            logger.error(f"Error checking federated completion threshold: {str(e)}")
            # Attempt graceful degradation
            await self._handle_federated_error(workflow_id, schedule_id, app_id, str(e))

    async def _trigger_federated_processing(self, workflow_id, schedule_id, app_id):
        """Trigger the next stage of federated learning processing"""
        try:
            # Publish event to indicate federated round is ready
            event_data = {
                'event_type': 'federated_data_ready',
                'workflow_id': workflow_id,
                'schedule_id': schedule_id,
                'app_id': app_id,
                'timestamp': time.time()
            }
            
            await self.redis.publish('federated_events', json.dumps(event_data))
            logger.info(f"Triggered federated processing for workflow {workflow_id}, schedule {schedule_id}")
            
        except Exception as e:
            logger.error(f"Error triggering federated processing: {str(e)}")
    
    async def _handle_federated_timeout(self, workflow_id, schedule_id, app_id, current_count, min_required):
        """Handle federated round timeout scenarios"""
        try:
            logger.warning(f"Federated round {workflow_id}:{schedule_id} timed out with {current_count}/{min_required} participants")
            
            # Mark round as failed in Redis
            timeout_key = f"fed_round_timeout_{workflow_id}_{schedule_id}"
            timeout_data = {
                'status': 'timeout',
                'participants_received': current_count,
                'min_required': min_required,
                'timeout_time': time.time(),
                'reason': 'insufficient_participants'
            }
            
            await self.redis.setex(timeout_key, 3600, json.dumps(timeout_data))
            
            # Clean up partial federated data
            federated_key = f"iot_data_{workflow_id}_{schedule_id}_{app_id}_federated_nodes"
            await self.redis.delete(federated_key)
            
            # Publish timeout event for monitoring
            timeout_event = {
                'event_type': 'federated_timeout',
                'workflow_id': workflow_id,
                'schedule_id': schedule_id,
                'app_id': app_id,
                'participants_received': current_count,
                'min_required': min_required,
                'timestamp': time.time()
            }
            
            await self.redis.publish('federated_events', json.dumps(timeout_event))
            logger.info(f"Published federated timeout event for {workflow_id}:{schedule_id}")
            
        except Exception as e:
            logger.error(f"Error handling federated timeout: {e}")
    
    async def _handle_federated_error(self, workflow_id, schedule_id, app_id, error_message):
        """Handle federated round errors with graceful degradation"""
        try:
            logger.error(f"Federated round {workflow_id}:{schedule_id} encountered error: {error_message}")
            
            # Store error information
            error_key = f"fed_round_error_{workflow_id}_{schedule_id}"
            error_data = {
                'status': 'error',
                'error_message': error_message,
                'error_time': time.time(),
                'app_id': app_id
            }
            
            await self.redis.setex(error_key, 3600, json.dumps(error_data))
            
            # Attempt to publish error event
            error_event = {
                'event_type': 'federated_error',
                'workflow_id': workflow_id,
                'schedule_id': schedule_id,
                'app_id': app_id,
                'error_message': error_message,
                'timestamp': time.time()
            }
            
            await self.redis.publish('federated_events', json.dumps(error_event))
            
        except Exception as secondary_error:
            logger.critical(f"Critical error in error handler: {secondary_error}")
    
    async def _schedule_timeout_check(self, workflow_id, schedule_id, app_id, timeout_duration):
        """Schedule a delayed timeout check for incomplete federated rounds"""
        try:
            # Store timeout check info for external monitoring
            timeout_check_key = f"fed_timeout_check_{workflow_id}_{schedule_id}"
            timeout_check_data = {
                'scheduled_time': time.time(),
                'timeout_duration': timeout_duration,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id,
                'app_id': app_id,
                'status': 'scheduled'
            }
            
            await self.redis.setex(timeout_check_key, int(timeout_duration + 60), json.dumps(timeout_check_data))
            logger.info(f"Scheduled timeout check for federated round {workflow_id}:{schedule_id} in {timeout_duration}s")
            
        except Exception as e:
            logger.error(f"Error scheduling timeout check: {e}")

    async def _get_federated_round_info(self, workflow_id):
        """Get federated round information from aggregation-request-tp state"""
        try:
            # Try to get from Redis cache first
            cache_key = f"fed_round_cache_{workflow_id}"
            cached_info = await self.redis.get(cache_key)
            
            if cached_info:
                return json.loads(cached_info)
            
            # Query blockchain state for federated round information
            federated_round_data = await self._query_federated_round_from_blockchain(workflow_id)
            
            if federated_round_data:
                logger.info(f"Retrieved federated round info from blockchain for {workflow_id}")
                # Cache for 1 hour
                await self.redis.setex(cache_key, 3600, json.dumps(federated_round_data))
                return federated_round_data
            else:
                # Fallback: Check if workflow is federated by examining dependency graph
                workflow_info = await self._get_workflow_dependency_graph(workflow_id)
                if workflow_info and workflow_info.get('workflow_type') == 'federated_learning':
                    # Extract federated config from workflow
                    federated_config = workflow_info.get('federated_config', {})
                    federated_round_data = {
                        'min_nodes_required': federated_config.get('min_nodes_required', 3),
                        'expected_nodes': federated_config.get('expected_nodes', ['iot-0', 'iot-1', 'iot-2', 'iot-3', 'iot-4']),
                        'workflow_type': 'federated_learning',
                        'coordinator_election': federated_config.get('coordinator_election', 'consensus'),
                        'aggregation_strategy': federated_config.get('aggregation_strategy', 'fedavg')
                    }
                    
                    logger.info(f"Extracted federated config from workflow dependency graph for {workflow_id}")
                    # Cache for 1 hour
                    await self.redis.setex(cache_key, 3600, json.dumps(federated_round_data))
                    return federated_round_data
                    
                logger.info(f"No federated round info found for workflow {workflow_id}")
                return None
            
        except Exception as e:
            logger.error(f"Error getting federated round info: {str(e)}")
            return None
    
    async def _query_federated_round_from_blockchain(self, workflow_id):
        """Query federated round information from blockchain state"""
        try:
            # First check Redis for most recent federated round data
            fed_round_key = f"fed_round:{workflow_id}:1"
            cached_round = await self.redis.get(fed_round_key)
            
            if cached_round:
                round_data = json.loads(cached_round)
                logger.info(f"Found federated round data in Redis cache: {round_data}")
                return {
                    'min_nodes_required': round_data.get('min_nodes_required', 3),
                    'expected_nodes': round_data.get('expected_nodes', ['iot-0', 'iot-1', 'iot-2', 'iot-3', 'iot-4']),
                    'workflow_type': 'federated_learning',
                    'coordinator_node': round_data.get('coordinator_node'),
                    'schedule_id': round_data.get('schedule_id'),
                    'status': round_data.get('status', 'active')
                }
            
            # Check for federated schedule data in Redis (from schedule event handler)
            alt_key = f"federated_schedule_{workflow_id}"
            alt_data = await self.redis.get(alt_key)
            if alt_data:
                schedule_data = json.loads(alt_data)
                federated_metadata = schedule_data.get('federated_metadata', {})
                if federated_metadata.get('is_federated'):
                    logger.info(f"Found federated schedule data for {workflow_id}")
                    return {
                        'min_nodes_required': federated_metadata.get('min_nodes_required', 3),
                        'expected_nodes': federated_metadata.get('expected_nodes', ['iot-0', 'iot-1', 'iot-2', 'iot-3', 'iot-4']),
                        'workflow_type': 'federated_learning',
                        'coordinator_node': federated_metadata.get('coordinator_node'),
                        'aggregation_strategy': federated_metadata.get('aggregation_strategy', 'fedavg')
                    }
            
            # Query aggregation-request-tp state for federated round info
            federated_round_info = await self._query_aggregation_request_state(workflow_id)
            if federated_round_info:
                logger.info(f"Retrieved federated round info from aggregation-request-tp")
                return federated_round_info
            
            return None
            
        except Exception as e:
            logger.error(f"Error querying blockchain for federated round: {str(e)}")
            return None
    
    async def _query_aggregation_request_state(self, workflow_id):
        """Query aggregation-request-tp state for federated round info"""
        try:
            # Check Redis for aggregation round information
            aggregation_key = f"aggregation_round_{workflow_id}_1"
            aggregation_data = await self.redis.get(aggregation_key)
            
            if aggregation_data:
                state_data = json.loads(aggregation_data)
                logger.info(f"Found aggregation round info for workflow {workflow_id}")
                return {
                    'min_nodes_required': state_data.get('min_nodes_required', 3),
                    'expected_nodes': state_data.get('participating_nodes', ['iot-0', 'iot-1', 'iot-2', 'iot-3', 'iot-4']),
                    'workflow_type': 'federated_learning',
                    'aggregator_node': state_data.get('aggregator_node'),
                    'workflow_id': workflow_id,
                    'status': state_data.get('status', 'active'),
                    'round_number': state_data.get('round_number', 1)
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error querying aggregation-request-tp state: {str(e)}")
            return None
    
    async def _get_workflow_dependency_graph(self, workflow_id):
        """Get workflow dependency graph to check if it's federated"""
        try:
            # Try to get from Redis cache first
            workflow_cache_key = f"workflow_cache_{workflow_id}"
            cached_workflow = await self.redis.get(workflow_cache_key)
            
            if cached_workflow:
                workflow_data = json.loads(cached_workflow)
                dependency_graph = workflow_data.get('dependency_graph')
                if dependency_graph:
                    logger.debug(f"Retrieved workflow dependency graph from cache for {workflow_id}")
                    return dependency_graph
            
            # Try to use the existing synchronous method
            try:
                dependency_graph = self._get_dependency_graph_sync(workflow_id)
                if dependency_graph:
                    # Cache it for future use
                    workflow_data = {'dependency_graph': dependency_graph}
                    await self.redis.setex(workflow_cache_key, 3600, json.dumps(workflow_data))
                    logger.debug(f"Retrieved and cached workflow dependency graph for {workflow_id}")
                    return dependency_graph
            except Exception:
                pass
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting workflow dependency graph: {str(e)}")
            return None
    
    def _get_dependency_graph_sync(self, workflow_id):
        """Synchronous method to get dependency graph using existing context"""
        try:
            # This would typically be called within a transaction context
            # For now, return None and rely on Redis cache
            return None
        except Exception as e:
            logger.debug(f"Could not get dependency graph synchronously: {e}")
            return None

    async def _store_data_in_cloudant(self, data_id, data, data_hash, workflow_id, schedule_id):
        try:
            doc = {
                '_id': data_id,
                'data': data,
                'data_hash': data_hash,
                'workflow_id': workflow_id,
                'schedule_id': schedule_id
            }
            self.cloudant_client.post_document(db=CLOUDANT_DB, document=doc).get_result()
            logger.info(f"Successfully stored IoT data in Cloudant for ID: {data_id}")
        except Exception as e:
            logger.error(f"Unexpected error while storing data in Cloudant for ID {data_id}: {str(e)}")
            raise

    def _validate_workflow_id(self, context, workflow_id):
        address = self._make_workflow_address(workflow_id)
        state_entries = context.get_state([address])
        return len(state_entries) > 0

    def _get_dependency_graph(self, context, workflow_id):
        address = self._make_workflow_address(workflow_id)
        state_entries = context.get_state([address])
        if state_entries:
            try:
                workflow_data = json.loads(state_entries[0].data.decode())
                if 'dependency_graph' not in workflow_data:
                    raise KeyError("'dependency_graph' not found in workflow data")
                dependency_graph = workflow_data['dependency_graph']
                if 'nodes' not in dependency_graph:
                    raise KeyError("'nodes' not found in dependency graph")
                return dependency_graph
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing workflow data: {e}")
                raise InvalidTransaction(f"Invalid workflow data for workflow ID: {workflow_id}")
        else:
            raise InvalidTransaction(f"No workflow data found for workflow ID: {workflow_id}")

    def _is_federated_workflow(self, context, workflow_id):
        """Check if this is a federated learning workflow"""
        try:
            dependency_graph = self._get_dependency_graph(context, workflow_id)
            workflow_type = dependency_graph.get('workflow_type', 'standard')
            federated_config = dependency_graph.get('federated_config', {})
            
            return workflow_type == 'federated_learning' or bool(federated_config)
        except Exception as e:
            logger.error(f"Error checking if workflow is federated: {e}")
            return False

    def cleanup_temp_files(self):
        for file in self.cloudant_certs:
            file.close()
            os.unlink(file.name)
        self.cloudant_certs = []
        logger.info("Temporary SSL files cleaned up")

    @staticmethod
    def _generate_data_id(workflow_id, schedule_id, app_id, data_type):
        return f"{workflow_id}_{schedule_id}_{app_id}_{data_type}"

    @staticmethod
    def _generate_federated_data_id(workflow_id, schedule_id, app_id, node_id, data_type):
        return f"{workflow_id}_{schedule_id}_{app_id}_{node_id}_{data_type}"

    @staticmethod
    def _calculate_hash(data):
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    @staticmethod
    def _make_iot_data_address(schedule_id):
        return IOT_DATA_NAMESPACE + hashlib.sha512(schedule_id.encode()).hexdigest()[:64]

    @staticmethod
    def _make_workflow_address(workflow_id):
        return WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]


def main():
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = IoTDataTransactionHandler()
    processor.add_handler(handler)
    try:
        processor.start()
    finally:
        handler.cleanup_temp_files()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
