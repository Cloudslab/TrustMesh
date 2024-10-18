import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile

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
        self._initialize_redis()
        self.loop = asyncio.get_event_loop()

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
            raise
        finally:
            for file_path in temp_files:
                try:
                    os.unlink(file_path)
                    logger.debug(f"Temporary file deleted: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")

    def apply(self, transaction, context):
        try:
            payload = json.loads(transaction.payload.decode())
            action = payload['action']

            if action == 'request_schedule':
                asyncio.run_coroutine_threadsafe(self._handle_schedule_request(payload, context), self.loop).result()
            elif action == 'submit_schedule_hash':
                asyncio.run_coroutine_threadsafe(self._handle_schedule_hash(payload, context), self.loop).result()
            else:
                raise InvalidTransaction(f"Invalid action: {action}")

        except json.JSONDecodeError as _:
            raise InvalidTransaction("Invalid payload: not a valid JSON")
        except KeyError as e:
            raise InvalidTransaction(f"Invalid payload: missing {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            raise InvalidTransaction(str(e))

    async def _handle_schedule_request(self, payload, context):
        workflow_id = payload['workflow_id']
        schedule_id = payload['schedule_id']

        if not await self._validate_workflow_id(context, workflow_id):
            raise InvalidTransaction(f"Invalid workflow ID: {workflow_id}")

        # Deterministically select a scheduler
        scheduler_node = await self._select_scheduler()

        schedule_address = self._make_schedule_address(schedule_id)
        schedule_data = {
            'status': 'PENDING',
            'workflow_id': workflow_id,
            'schedule_id': schedule_id,
            'assigned_scheduler': scheduler_node
        }

        await context.set_state({
            schedule_address: json.dumps(schedule_data).encode()
        })

        logger.info(f"Schedule request {schedule_id} assigned to node {scheduler_node}")

    async def _handle_schedule_hash(self, payload, context):
        schedule_id = payload['schedule_id']
        schedule_hash = payload['schedule_hash']

        schedule_address = self._make_schedule_address(schedule_id)
        state_entries = await context.get_state([schedule_address])
        if state_entries:
            schedule_data = json.loads(state_entries[0].data.decode())
            if schedule_data['status'] != 'PENDING':
                raise InvalidTransaction(f"Invalid schedule status for hash submission: {schedule_data['status']}")

            schedule_data['status'] = 'COMPLETED'
            schedule_data['schedule_hash'] = schedule_hash

            await context.set_state({
                schedule_address: json.dumps(schedule_data).encode()
            })

            logger.info(f"Schedule hash recorded for schedule ID: {schedule_id}")
        else:
            raise InvalidTransaction(f"No pending schedule found for ID: {schedule_id}")

    async def _validate_workflow_id(self, context, workflow_id):
        address = self._make_workflow_address(workflow_id)
        state_entries = await context.get_state([address])
        return len(state_entries) > 0

    async def _select_scheduler(self):

        try:
            node_resources = []
            async for key in self.redis.scan_iter(match='resources_*'):
                node_id = key.split('_', 1)[1]
                redis_data = await self.redis.get(key)
                if redis_data:
                    resource_data = json.loads(redis_data)
                    node_resources.append({
                        'id': node_id,
                        'resources': resource_data
                    })

            # Select the node with the most available resources
            selected_node = max(node_resources, key=lambda x: self._calculate_available_resources(x['resources']))
            return selected_node['id']
        except RedisError as e:
            logger.error(f"Error accessing Redis: {str(e)}")
            raise InvalidTransaction("Failed to access node resource data")

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
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = IoTScheduleTransactionHandler()
    processor.add_handler(handler)
    processor.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
