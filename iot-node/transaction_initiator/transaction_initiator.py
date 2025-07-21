import json
import uuid
import time
import hashlib
import logging
import os
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from sawtooth_sdk.messaging.stream import Stream
from threading import Lock

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SCHEDULE_FAMILY_NAME = 'schedule-request'
SCHEDULE_FAMILY_VERSION = '1.0'
STATUS_FAMILY_NAME = 'schedule-status'
STATUS_FAMILY_VERSION = '1.0'
IOT_DATA_FAMILY_NAME = 'iot-data'
IOT_DATA_FAMILY_VERSION = '1.0'
IOT_DATA_NAMESPACE = hashlib.sha512(IOT_DATA_FAMILY_NAME.encode()).hexdigest()[:6]
SCHEDULE_NAMESPACE = hashlib.sha512(SCHEDULE_FAMILY_NAME.encode()).hexdigest()[:6]
STATUS_NAMESPACE = hashlib.sha512(STATUS_FAMILY_NAME.encode()).hexdigest()[:6]
WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]

PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/client.priv')
# Get comma-separated list of validator URLs from environment variable
VALIDATOR_URLS = os.getenv('VALIDATOR_URLS', 'tcp://validator:4004').split(',')
IOT_URL = os.getenv('IOT_URL', 'tcp://iot')


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as ex:
        raise IOError(f"Failed to load private key from {key_file}: {str(ex)}") from ex


def create_transaction(signer, family_name, family_version, payload, inputs, outputs):
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


def create_batch(transactions, signer):
    batch_header = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=[txn.header_signature for txn in transactions],
    ).SerializeToString()

    signature = signer.sign(batch_header)

    batch = Batch(
        header=batch_header,
        header_signature=signature,
        transactions=transactions
    )

    return batch


class TransactionCreator:
    _instance = None
    _lock = Lock()
    _current_validator_index = 0

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(TransactionCreator, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            private_key = load_private_key(PRIVATE_KEY_FILE)
            context = create_context('secp256k1')
            self.signer = CryptoFactory(context).new_signer(private_key)
            self._initialized = True

    def get_next_validator_url(self):
        """Get the next validator URL using round-robin selection"""
        with self._lock:
            url = VALIDATOR_URLS[self._current_validator_index]
            self._current_validator_index = (self._current_validator_index + 1) % len(VALIDATOR_URLS)
            return url

    def submit_batch(self, batch):
        """Submit batch to the next validator in the round-robin sequence"""
        validator_url = self.get_next_validator_url()
        logger.info(f"Submitting batch to validator: {validator_url}")

        try:
            stream = Stream(validator_url)
            future = stream.send(
                message_type='CLIENT_BATCH_SUBMIT_REQUEST',
                content=BatchList(batches=[batch]).SerializeToString()
            )
            result = future.result()
            return result
        except Exception as e:
            logger.error(f"Failed to submit to validator {validator_url}: {str(e)}")
            # If submission fails, try the next validator
            if len(VALIDATOR_URLS) > 1:
                validator_url = self.get_next_validator_url()
                logger.info(f"Retrying with next validator: {validator_url}")
                stream = Stream(validator_url)
                future = stream.send(
                    message_type='CLIENT_BATCH_SUBMIT_REQUEST',
                    content=BatchList(batches=[batch]).SerializeToString()
                )
                return future.result()
            raise

    def create_and_send_transactions(self, iot_data, workflow_id, iot_port, iot_public_key, node_id=None, federated_schedule_id=None):
        try:
            # For federated learning, use provided schedule_id, otherwise generate new one
            if federated_schedule_id:
                schedule_id = federated_schedule_id
                logger.info(f"Using federated schedule ID: {schedule_id} for node {node_id}")
            else:
                schedule_id = str(uuid.uuid4())
                logger.info(f"Generated new schedule ID: {schedule_id}")
            
            timestamp = int(time.time())

            schedule_payload = {
                "workflow_id": workflow_id,
                "schedule_id": schedule_id,
                "source_url": f"{IOT_URL}:{iot_port}",
                "source_public_key": iot_public_key,
                "timestamp": timestamp
            }
            
            # Add node_id for federated learning workflows
            if node_id:
                schedule_payload["node_id"] = node_id

            schedule_inputs = [SCHEDULE_NAMESPACE, WORKFLOW_NAMESPACE]
            schedule_outputs = [SCHEDULE_NAMESPACE]
            schedule_txn = create_transaction(self.signer, SCHEDULE_FAMILY_NAME, SCHEDULE_FAMILY_VERSION,
                                              schedule_payload, schedule_inputs, schedule_outputs)

            # Create schedule-status transaction
            status_payload = {
                "schedule_id": schedule_id,
                "workflow_id": workflow_id,
                "timestamp": timestamp,
                "status": "REQUESTED"
            }
            
            if node_id:
                status_payload["node_id"] = node_id
                
            status_inputs = [STATUS_NAMESPACE]
            status_outputs = [STATUS_NAMESPACE]
            status_txn = create_transaction(self.signer, STATUS_FAMILY_NAME, STATUS_FAMILY_VERSION,
                                            status_payload, status_inputs, status_outputs)

            # Create iot-data transaction
            iot_data_payload = {
                "iot_data": iot_data,
                "workflow_id": workflow_id,
                "schedule_id": schedule_id
            }
            
            # Add node_id for federated learning workflows
            if node_id:
                iot_data_payload["node_id"] = node_id
                
            iot_data_inputs = [IOT_DATA_NAMESPACE, WORKFLOW_NAMESPACE]
            iot_data_outputs = [IOT_DATA_NAMESPACE]
            iot_data_txn = create_transaction(self.signer, IOT_DATA_FAMILY_NAME, IOT_DATA_FAMILY_VERSION,
                                              iot_data_payload, iot_data_inputs, iot_data_outputs)

            # Create and submit batch with all three transactions
            batch = create_batch([schedule_txn, status_txn, iot_data_txn], self.signer)
            result = self.submit_batch(batch)

            logger.info({
                "message": "Data submitted successfully",
                "result": str(result),
                "schedule_id": schedule_id
            })

            return schedule_id

        except Exception as ex:
            logger.error(f"Error creating and sending transactions: {str(ex)}")
            raise

    def create_federated_learning_transactions(self, iot_data, workflow_id, iot_port, iot_public_key, node_id):
        """
        Create transactions for federated learning workflows.
        This method handles the coordination needed for multiple nodes to share the same schedule_id.
        """
        try:
            # Check if a federated round already exists for this workflow
            federated_schedule_id = self._get_or_create_federated_schedule_id(workflow_id, node_id)
            
            logger.info(f"Creating federated learning transactions for node {node_id} with schedule_id {federated_schedule_id}")
            
            # Use the shared schedule_id for federated learning
            return self.create_and_send_transactions(
                iot_data=iot_data,
                workflow_id=workflow_id,
                iot_port=iot_port,
                iot_public_key=iot_public_key,
                node_id=node_id,
                federated_schedule_id=federated_schedule_id
            )
            
        except Exception as e:
            logger.error(f"Error creating federated learning transactions: {str(e)}")
            raise

    def _get_or_create_federated_schedule_id(self, workflow_id, node_id):
        """
        Get existing federated schedule ID or create a new one for coordinator node.
        In a production environment, this would query the federated-schedule-tp.
        """
        try:
            # For this implementation, we'll use a deterministic approach
            # The first node (iot-0) creates the schedule_id, others join
            
            if node_id == 'iot-0':
                # Coordinator node creates new federated round
                schedule_id = str(uuid.uuid4())
                logger.info(f"Coordinator node {node_id} created federated schedule_id: {schedule_id}")
                
                # In production, this would submit a transaction to federated-schedule-tp
                # to record the federated round
                self._record_federated_round(workflow_id, schedule_id, node_id)
                
                return schedule_id
            else:
                # Participant node joins existing round
                # In production, this would query the federated-schedule-tp for existing rounds
                existing_schedule_id = self._get_existing_federated_round(workflow_id)
                
                if existing_schedule_id:
                    logger.info(f"Participant node {node_id} joining existing federated round: {existing_schedule_id}")
                    return existing_schedule_id
                else:
                    logger.warning(f"No existing federated round found for {workflow_id}, node {node_id} will wait or create fallback")
                    # Fallback: create a new schedule_id (this should be rare)
                    return str(uuid.uuid4())
                    
        except Exception as e:
            logger.error(f"Error getting/creating federated schedule ID: {str(e)}")
            # Fallback to regular schedule ID generation
            return str(uuid.uuid4())

    def _record_federated_round(self, workflow_id, schedule_id, coordinator_node):
        """
        Record federated round information.
        In production, this would submit a transaction to federated-schedule-tp.
        """
        logger.info(f"Recording federated round: workflow={workflow_id}, schedule={schedule_id}, coordinator={coordinator_node}")
        # This is a placeholder - in production this would create a transaction
        # to the federated-schedule-tp to record the round information

    def _get_existing_federated_round(self, workflow_id):
        """
        Check for existing federated rounds for this workflow.
        In production, this would query the federated-schedule-tp.
        """
        logger.info(f"Checking for existing federated round for workflow: {workflow_id}")
        # This is a placeholder - in production this would query the blockchain
        # or Redis for existing federated round information
        return None  # For now, always return None (no existing round found)


# This can be imported and used by various data sources
transaction_creator = TransactionCreator()