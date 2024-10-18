import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import time
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.events_pb2 import EventSubscription, EventFilter, EventList
from sawtooth_sdk.protobuf.client_event_pb2 import ClientEventsSubscribeRequest, ClientEventsSubscribeResponse
from sawtooth_sdk.protobuf.validator_pb2 import Message
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_sdk.protobuf.client_batch_submit_pb2 import ClientBatchSubmitResponse
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from helper.scheduler import create_scheduler
from coredis import RedisCluster

FAMILY_NAME = 'iot-schedule'
FAMILY_VERSION = '1.0'
SCHEDULE_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]
WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:64]

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/root.priv')

logger = logging.getLogger(__name__)


class ScheduleGenerator:
    def __init__(self, validator_url):
        self.validator_url = validator_url
        self.stream = Stream(validator_url)
        self.redis = None
        self.node_id = os.getenv('NODE_ID')
        self.context = create_context('secp256k1')
        self.signer = CryptoFactory(self.context).new_signer(self.load_private_key())
        self._initialize_redis()

    @staticmethod
    def load_private_key():
        try:
            with open(PRIVATE_KEY_FILE, 'r') as key_reader:
                private_key_str = key_reader.read().strip()
                return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
        except IOError as e:
            raise IOError(f"Failed to load private key from {PRIVATE_KEY_FILE}: {str(e)}") from e

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

    async def start(self):
        await self.stream.connect()
        await self.subscribe_to_events()

        while True:
            msg = await self.stream.receive()
            if msg.message_type == Message.CLIENT_EVENTS:
                event_list = EventList()
                event_list.ParseFromString(msg.content)
                for event in event_list.events:
                    await self.handle_event(event)

    async def subscribe_to_events(self):
        block_commit_subscription = EventSubscription(
            event_type="sawtooth/block-commit"
        )
        state_delta_subscription = EventSubscription(
            event_type="sawtooth/state-delta",
            filters=[EventFilter(
                key="address",
                match_string=f"^{SCHEDULE_NAMESPACE}.*",
                filter_type=EventFilter.REGEX_ANY
            )]
        )

        request = ClientEventsSubscribeRequest(
            subscriptions=[block_commit_subscription, state_delta_subscription]
        )

        response = await self.stream.send(
            message_type=Message.CLIENT_EVENTS_SUBSCRIBE_REQUEST,
            content=request.SerializeToString()
        )
        response = ClientEventsSubscribeResponse()
        response.ParseFromString(response)

        if response.status != ClientEventsSubscribeResponse.OK:
            raise Exception(f"Failed to subscribe to events: {response.response_message}")

        logger.info("Successfully subscribed to events")

    async def handle_event(self, event):
        if event.event_type == "sawtooth/state-delta":
            await self.handle_state_delta(event)
        elif event.event_type == "sawtooth/block-commit":
            logger.info("New block committed")
        else:
            logger.info(f"Received unhandled event type: {event.event_type}")

    async def handle_state_delta(self, event):
        for state_change in event.data:
            if state_change.address.startswith(SCHEDULE_NAMESPACE):
                schedule_data = json.loads(state_change.value)
                if schedule_data['status'] == 'PENDING' and schedule_data['assigned_scheduler'] == self.node_id:
                    await self.generate_schedule(schedule_data)

    async def generate_schedule(self, schedule_data):
        workflow_id = schedule_data['workflow_id']
        schedule_id = schedule_data['schedule_id']
        try:
            dependency_graph = await self.get_dependency_graph(workflow_id)
            app_requirements = await self.get_app_requirements(dependency_graph['nodes'])

            scheduler = create_scheduler("lcdwrr", dependency_graph, app_requirements, {"redis-client": self.redis})
            schedule_result = await scheduler.schedule()

            await self.store_schedule_in_redis(schedule_id, schedule_result, workflow_id)

            # Calculate schedule hash
            schedule_hash = hashlib.sha256(json.dumps(schedule_result, sort_keys=True).encode()).hexdigest()

            # Submit schedule hash to blockchain
            result = await self.submit_schedule_hash(schedule_id, schedule_hash)

            logger.info(f"Generated and stored schedule for {schedule_id}, hash submitted to blockchain: {result}")
        except Exception as e:
            logger.error(f"Error generating schedule: {str(e)}")
            await self.update_schedule_status(schedule_id, 'FAILED')

    async def get_dependency_graph(self, workflow_id):
        address = WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]
        state_entry = await self.stream.get_state([address])
        if state_entry:
            workflow_data = json.loads(state_entry[0].data)
            return workflow_data['dependency_graph']
        else:
            raise Exception(f"No workflow data found for workflow ID: {workflow_id}")

    async def get_app_requirements(self, app_ids):
        app_requirements = {}
        for app_id in app_ids:
            address = DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]
            state_entry = await self.stream.get_state([address])
            if state_entry:
                app_data = json.loads(state_entry[0].data)
                app_requirements[app_id] = {
                    "memory": app_data["resource_requirements"]["memory"],
                    "cpu": app_data["resource_requirements"]["cpu"],
                    "disk": app_data["resource_requirements"]["disk"]
                }
            else:
                raise Exception(f"No requirements found for app ID: {app_id}")
        return app_requirements

    async def store_schedule_in_redis(self, schedule_id, schedule_result, workflow_id):
        schedule_data = {
            'schedule_id': schedule_id,
            'schedule': schedule_result,
            'workflow_id': workflow_id,
            'status': 'ACTIVE'
        }
        schedule_json = json.dumps(schedule_data)
        key = f"schedule_{schedule_id}"

        await self.redis.set(key, schedule_json)
        await self.redis.publish("schedule", schedule_json)

    async def submit_schedule_hash(self, schedule_id, schedule_hash):
        payload = json.dumps({
            'action': 'submit_schedule_hash',
            'schedule_id': schedule_id,
            'schedule_hash': schedule_hash
        }).encode()

        transaction = self.create_transaction(payload)
        batch = self.create_batch([transaction])
        return await self.submit_batch(batch)

    def create_transaction(self, payload):
        header = TransactionHeader(
            family_name=FAMILY_NAME,
            family_version=FAMILY_VERSION,
            inputs=[SCHEDULE_NAMESPACE],
            outputs=[SCHEDULE_NAMESPACE],
            signer_public_key=self.signer.get_public_key().as_hex(),
            batcher_public_key=self.signer.get_public_key().as_hex(),
            dependencies=[],
            payload_sha512=hashlib.sha512(payload).hexdigest(),
            nonce=hex(int(time.time()))
        ).SerializeToString()

        signature = self.signer.sign(header)

        return Transaction(
            header=header,
            payload=payload,
            header_signature=signature
        )

    def create_batch(self, transactions):
        batch_header = BatchHeader(
            signer_public_key=self.signer.get_public_key().as_hex(),
            transaction_ids=[t.header_signature for t in transactions],
        ).SerializeToString()

        signature = self.signer.sign(batch_header)

        return Batch(
            header=batch_header,
            transactions=transactions,
            header_signature=signature
        )

    async def submit_batch(self, batch):
        batch_list = BatchList(batches=[batch])
        future = await self.stream.send(
            message_type='CLIENT_BATCH_SUBMIT_REQUEST',
            content=batch_list.SerializeToString()
        )
        return self.process_future_result(future)

    @staticmethod
    def process_future_result(future_result):
        try:
            result = future_result.result()
            response = ClientBatchSubmitResponse()
            response.ParseFromString(result.content)

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
        except Exception as e:
            return {
                "status": "ERROR",
                "message": "An error occurred while processing the submission result",
                "error_details": str(e)
            }

    async def update_schedule_status(self, schedule_id, status):
        address = SCHEDULE_NAMESPACE + hashlib.sha512(schedule_id.encode()).hexdigest()[:64]
        state_entry = await self.stream.get_state([address])
        if state_entry:
            schedule_data = json.loads(state_entry[0].data)
            schedule_data['status'] = status
            await self.stream.set_state({address: json.dumps(schedule_data).encode()})


def main():
    validator_url = os.getenv('VALIDATOR_URL', 'tcp://validator:4004')
    generator = ScheduleGenerator(validator_url)
    asyncio.run(generator.start())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
