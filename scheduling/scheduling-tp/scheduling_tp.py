import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import traceback

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
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

# Sawtooth configuration
FAMILY_NAME = 'iot-schedule'
FAMILY_VERSION = '1.0'
WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
SCHEDULE_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTScheduleTransactionHandler(TransactionHandler):
    def __init__(self):
        self.redis = None
        self.loop = asyncio.get_event_loop()
        self._initialize_redis()

    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [SCHEDULE_NAMESPACE, WORKFLOW_NAMESPACE]

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
            logger.error(traceback.format_exc())
            raise
        finally:
            for file_path in temp_files:
                try:
                    os.unlink(file_path)
                    logger.debug(f"Temporary file deleted: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")

    def apply(self, transaction, context):
        logger.info("Entering apply method")
        try:
            logger.debug(f"Transaction payload: {transaction.payload}")
            payload = json.loads(transaction.payload.decode())
            logger.info(f"Decoded payload: {payload}")
            action = payload['action']
            logger.info(f"Action: {action}")

            if action == 'request_schedule':
                logger.info("Handling schedule request")
                self._handle_schedule_request(payload, context)
            elif action == 'submit_schedule_hash':
                logger.info("Handling schedule hash submission")
                self._handle_schedule_hash(payload, context)
            else:
                logger.error(f"Invalid action: {action}")
                raise InvalidTransaction(f"Invalid action: {action}")

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

    def _handle_schedule_request(self, payload, context):
        logger.info("Entering _handle_schedule_request method")
        workflow_id = payload['workflow_id']
        schedule_id = payload['schedule_id']
        logger.info(f"Workflow ID: {workflow_id}, Schedule ID: {schedule_id}")

        if not self._validate_workflow_id(context, workflow_id):
            logger.error(f"Invalid workflow ID: {workflow_id}")
            raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

        logger.info("Selecting scheduler")
        scheduler_node = self._select_scheduler()
        logger.info(f"Selected scheduler node: {scheduler_node}")

        schedule_address = self._make_schedule_address(schedule_id)
        schedule_data = {
            'status': 'PENDING',
            'workflow_id': workflow_id,
            'schedule_id': schedule_id,
            'assigned_scheduler': scheduler_node
        }

        logger.info(f"Setting state for schedule {schedule_id}")
        context.set_state({
            schedule_address: json.dumps(schedule_data).encode()
        })

        logger.info(f"Schedule request {schedule_id} assigned to node {scheduler_node}")

    def _handle_schedule_hash(self, payload, context):
        logger.info("Entering _handle_schedule_hash method")
        schedule_id = payload['schedule_id']
        schedule_hash = payload['schedule_hash']
        logger.info(f"Schedule ID: {schedule_id}, Hash: {schedule_hash}")

        schedule_address = self._make_schedule_address(schedule_id)
        state_entries = context.get_state([schedule_address])
        if state_entries:
            schedule_data = json.loads(state_entries[0].data.decode())
            logger.info(f"Current schedule data: {schedule_data}")
            if schedule_data['status'] != 'PENDING':
                logger.error(f"Invalid schedule status for hash submission: {schedule_data['status']}")
                raise InvalidTransaction(f"Invalid schedule status for hash submission: {schedule_data['status']}")

            schedule_data['status'] = 'COMPLETED'
            schedule_data['schedule_hash'] = schedule_hash

            logger.info(f"Updating state for schedule {schedule_id}")
            context.set_state({
                schedule_address: json.dumps(schedule_data).encode()
            })

            logger.info(f"Schedule hash recorded for schedule ID: {schedule_id}")
        else:
            logger.error(f"No pending schedule found for ID: {schedule_id}")
            raise InvalidTransaction(f"No pending schedule found for ID: {schedule_id}")

    def _validate_workflow_id(self, context, workflow_id):
        logger.info(f"Validating workflow ID: {workflow_id}")
        address = self._make_workflow_address(workflow_id)
        state_entries = context.get_state([address])
        is_valid = len(state_entries) > 0
        logger.info(f"Workflow ID {workflow_id} is valid: {is_valid}")
        return is_valid

    def _select_scheduler(self):
        logger.info("Entering _select_scheduler method")
        try:
            if self.redis is None:
                logger.error("Redis connection not initialized")
                raise InvalidTransaction("Redis connection not initialized")

            node_resources = []
            logger.info("Scanning Redis for resource data")
            keys = self.loop.run_until_complete(self.redis.scan(match='resources_*'))
            logger.debug(f"Found keys: {keys}")
            for key in keys[1]:  # keys[1] contains the matched keys
                node_id = key.split('_', 1)[1]
                logger.debug(f"Fetching data for node: {node_id}")
                redis_data = self.loop.run_until_complete(self.redis.get(key))
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

            logger.info("Selecting node with most available resources")
            selected_node = max(node_resources, key=lambda x: self._calculate_available_resources(x['resources']))
            logger.info(f"Selected node: {selected_node['id']}")
            return selected_node['id']
        except RedisError as e:
            logger.error(f"Error accessing Redis: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction("Failed to access node resource data")
        except Exception as e:
            logger.error(f"Unexpected error in _select_scheduler: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(f"Error selecting scheduler: {str(e)}")

    @staticmethod
    def _calculate_available_resources(resources):
        cpu_available = resources['cpu']['total'] * (1 - resources['cpu']['used_percent'] / 100)
        memory_available = resources['memory']['total'] * (1 - resources['memory']['used_percent'] / 100)
        return cpu_available + memory_available  # Simple sum, could be weighted if needed

    @staticmethod
    def _make_schedule_address(schedule_id):
        return SCHEDULE_NAMESPACE + hashlib.sha512(schedule_id.encode()).hexdigest()[:64]

    @staticmethod
    def _make_workflow_address(workflow_id):
        return WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]


def main():
    logger.info("Starting IoT Schedule Transaction Processor")
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = IoTScheduleTransactionHandler()
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
