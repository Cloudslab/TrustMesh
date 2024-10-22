import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import time
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.client_state_pb2 import ClientStateGetRequest, ClientStateGetResponse
from sawtooth_sdk.protobuf.events_pb2 import EventSubscription, EventList
from sawtooth_sdk.protobuf.client_event_pb2 import ClientEventsSubscribeRequest, ClientEventsSubscribeResponse
from sawtooth_sdk.protobuf.validator_pb2 import Message
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_sdk.protobuf.client_batch_submit_pb2 import ClientBatchSubmitResponse
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from helper.scheduler import create_scheduler
from coredis import RedisCluster

SCHEDULE_CONFIRMATION_FAMILY_NAME = 'schedule-confirmation'
SCHEDULE_CONFIRMATION_FAMILY_VERSION = '1.0'
STATUS_FAMILY_NAME = 'schedule-status'
STATUS_FAMILY_VERSION = '1.0'
SCHEDULE_NAMESPACE = hashlib.sha512(SCHEDULE_CONFIRMATION_FAMILY_NAME.encode()).hexdigest()[:6]
STATUS_NAMESPACE = hashlib.sha512(STATUS_FAMILY_NAME.encode()).hexdigest()[:6]
WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/root.priv')

logger = logging.getLogger(__name__)


def load_private_key():
    try:
        with open(PRIVATE_KEY_FILE, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as e:
        raise IOError(f"Failed to load private key from {PRIVATE_KEY_FILE}: {str(e)}") from e


async def initialize_redis():
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

        redis_instance = await RedisCluster(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=True,
            ssl_context=ssl_context,
            decode_responses=True
        )

        logger.info("Connected to Redis cluster successfully")

        return redis_instance

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


def handle_event(event):
    if event.event_type == "schedule-request":
        workflow_id = None
        schedule_id = None
        assigned_scheduler = None
        for attr in event.attributes:
            if attr.key == "workflow_id":
                workflow_id = attr.value
            elif attr.key == "schedule_id":
                schedule_id = attr.value
            elif attr.key == "assigned_scheduler":
                assigned_scheduler = attr.value
        if workflow_id and schedule_id and assigned_scheduler == node_id:
            generate_schedule(workflow_id, schedule_id)
    elif event.event_type == "schedule-confirmation":
        schedule_id = None
        workflow_id = None
        schedule = None
        schedule_proposer = None
        for attr in event.attributes:
            if attr.key == "schedule_id":
                schedule_id = attr.value
            elif attr.key == "workflow_id":
                workflow_id = attr.value
            elif attr.key == "schedule":
                schedule = attr.value
            elif attr.key == "schedule_proposer":
                schedule_proposer = attr.value
        if schedule and schedule_id and workflow_id and schedule_proposer == node_id:
            publish_schedule(schedule_id, schedule, workflow_id)
    elif event.event_type == "sawtooth/block-commit":
        logger.info("New block committed")
    else:
        logger.info(f"Received unhandled event type: {event.event_type}")


def generate_schedule(workflow_id, schedule_id):
    try:
        dependency_graph = get_dependency_graph(workflow_id)

        logger.info(f"Generating Schedule with dependency graph: {dependency_graph}")

        app_requirements = get_app_requirements(dependency_graph['nodes'])

        logger.info(f"Generating Schedule with app requirements: {app_requirements}")

        scheduler = create_scheduler("lcdwrr", dependency_graph, app_requirements, {"redis-client": redis})

        logger.info(f"Scheduler Instance Created Successfully")

        schedule_result = scheduler.schedule()

        logger.info(f"Schedule Result: {schedule_result}")

        result = submit_schedule(schedule_id, schedule_result, workflow_id)

        logger.info(f"Generated and stored schedule for {schedule_id}, hash submitted to blockchain: {result}")
    except Exception as e:
        logger.error(f"Error generating schedule: {str(e)}")


def publish_schedule(schedule_id, schedule, workflow_id):
    try:
        store_schedule_in_redis(schedule_id, schedule, workflow_id)
    except Exception as e:
        logger.error(f"Error publishing schedule: {str(e)}")


def get_dependency_graph(workflow_id):
    address = WORKFLOW_NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]
    state_entry = get_state(address)
    if state_entry:
        workflow_data = json.loads(state_entry)
        return workflow_data['dependency_graph']
    else:
        raise Exception(f"No workflow data found for workflow ID: {workflow_id}")


def get_app_requirements(app_ids):
    app_requirements = {}
    for app_id in app_ids:
        address = DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]
        state_entry = get_state(address)
        if state_entry:
            app_data = json.loads(state_entry)
            logger.info(f"App Requirements for {app_id}: {app_data}")
            app_requirements[app_id] = {
                "memory": app_data["resource_requirements"]["memory"],
                "cpu": app_data["resource_requirements"]["cpu"],
                "disk": app_data["resource_requirements"]["disk"]
            }
        else:
            raise Exception(f"No requirements found for app ID: {app_id}")
    return app_requirements


def get_state(address):
    request = ClientStateGetRequest(
        state_root='',
        address=address
    )
    response = stream.send(
        message_type=Message.CLIENT_STATE_GET_REQUEST,
        content=request.SerializeToString()
    ).result()

    response_proto = ClientStateGetResponse()
    response_proto.ParseFromString(response.content)

    if response_proto.status == ClientStateGetResponse.OK:
        return response_proto.value
    else:
        return None


def store_schedule_in_redis(schedule_id, schedule_result, workflow_id):
    schedule_data = {
        'schedule_id': schedule_id,
        'schedule': schedule_result,
        'workflow_id': workflow_id,
        'status': 'ACTIVE'
    }
    schedule_json = json.dumps(schedule_data)
    key = f"schedule_{schedule_id}"

    redis.set(key, schedule_json)
    redis.publish("schedule", schedule_json)


def submit_schedule(schedule_id, schedule, workflow_id):
    payload = json.dumps({
        'schedule_id': schedule_id,
        'schedule': schedule,
        'workflow_id': workflow_id,
        'schedule_proposer': node_id
    }).encode()

    schedule_txn = create_transaction(SCHEDULE_CONFIRMATION_FAMILY_NAME, SCHEDULE_CONFIRMATION_FAMILY_VERSION, payload,
                                      [SCHEDULE_NAMESPACE], [SCHEDULE_NAMESPACE])

    status_payload = {
        "schedule_id": schedule_id,
        "workflow_id": workflow_id,
        "timestamp": int(time.time()),
        "status": "ACTIVE"
    }
    status_inputs = [STATUS_NAMESPACE]
    status_outputs = [STATUS_NAMESPACE]
    status_txn = create_transaction(STATUS_FAMILY_NAME, STATUS_FAMILY_VERSION,
                                    status_payload, status_inputs, status_outputs)

    batch = create_batch([schedule_txn, status_txn])
    return submit_batch(batch)


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


def main():
    logger.info("Starting Schedule Generation Event Handler")

    block_commit_subscription = EventSubscription(
        event_type="sawtooth/block-commit"
    )

    schedule_request_event = EventSubscription(
        event_type="schedule-request"
    )

    schedule_confirmation_event = EventSubscription(
        event_type="schedule-confirmation"
    )

    request = ClientEventsSubscribeRequest(
        subscriptions=[block_commit_subscription, schedule_request_event, schedule_confirmation_event]
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

    logger.info("Schedule Generation Handler: Subscription successful. Listening for events...")

    while True:
        try:
            msg_future = stream.receive()
            msg = msg_future.result()
            if msg.message_type == Message.CLIENT_EVENTS:
                event_list = EventList()
                event_list.ParseFromString(msg.content)
                for event in event_list.events:
                    handle_event(event)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error receiving message: {e}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    redis = asyncio.get_event_loop().run_until_complete(initialize_redis())
    node_id = os.getenv('NODE_ID')
    stream = Stream(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(load_private_key())
    main()
