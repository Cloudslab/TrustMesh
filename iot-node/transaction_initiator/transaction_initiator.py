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
AGGREGATION_REQUEST_FAMILY_NAME = 'aggregation-request'
AGGREGATION_REQUEST_FAMILY_VERSION = '1.0'
IOT_DATA_NAMESPACE = hashlib.sha512(IOT_DATA_FAMILY_NAME.encode()).hexdigest()[:6]
SCHEDULE_NAMESPACE = hashlib.sha512(SCHEDULE_FAMILY_NAME.encode()).hexdigest()[:6]
STATUS_NAMESPACE = hashlib.sha512(STATUS_FAMILY_NAME.encode()).hexdigest()[:6]
WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]
DOCKER_IMAGE_NAMESPACE = hashlib.sha512('docker-image'.encode()).hexdigest()[:6]
AGGREGATION_NAMESPACE = hashlib.sha512(AGGREGATION_REQUEST_FAMILY_NAME.encode()).hexdigest()[:6]
# Include confirmation namespace to ensure serial execution with aggregation-confirmation-tp
CONFIRMATION_NAMESPACE = hashlib.sha512('aggregation-confirmation'.encode()).hexdigest()[:6]

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
        from sawtooth_sdk.protobuf.client_batch_submit_pb2 import ClientBatchSubmitResponse
        
        validator_url = self.get_next_validator_url()
        logger.info(f"Submitting batch to validator: {validator_url}")

        try:
            stream = Stream(validator_url)
            future = stream.send(
                message_type='CLIENT_BATCH_SUBMIT_REQUEST',
                content=BatchList(batches=[batch]).SerializeToString()
            )
            result = future.result()
            
            # Parse the ClientBatchSubmitResponse to check actual status
            response = ClientBatchSubmitResponse()
            response.ParseFromString(result.content)
            
            # Check the actual status code
            logger.info(f"Batch submission response - Status code: {response.status}")
            if response.status == ClientBatchSubmitResponse.OK:
                logger.info(f"Batch submitted successfully with status OK (code: {response.status})")
                return result
            elif response.status == ClientBatchSubmitResponse.INVALID_BATCH:
                logger.error(f"Batch submission failed: INVALID_BATCH (code: {response.status})")
                raise Exception(f"Invalid batch submission: {response}")
            elif response.status == ClientBatchSubmitResponse.INTERNAL_ERROR:
                logger.error(f"Batch submission failed: INTERNAL_ERROR (code: {response.status})")
                raise Exception(f"Internal error during batch submission: {response}")
            elif response.status == ClientBatchSubmitResponse.QUEUE_FULL:
                logger.error(f"Batch submission failed: QUEUE_FULL (code: {response.status})")
                raise Exception(f"Validator queue full: {response}")
            else:
                logger.error(f"Batch submission failed with unknown status: {response.status}")
                raise Exception(f"Unknown status during batch submission: {response}")
                
        except Exception as e:
            logger.error(f"Failed to submit to validator {validator_url}: {str(e)}")
            # If submission fails, try the next validator
            if len(VALIDATOR_URLS) > 1:
                validator_url = self.get_next_validator_url()
                logger.info(f"Retrying with next validator: {validator_url}")
                try:
                    stream = Stream(validator_url)
                    future = stream.send(
                        message_type='CLIENT_BATCH_SUBMIT_REQUEST',
                        content=BatchList(batches=[batch]).SerializeToString()
                    )
                    result = future.result()
                    
                    # Parse the response for retry as well
                    response = ClientBatchSubmitResponse()
                    response.ParseFromString(result.content)
                    
                    logger.info(f"Retry batch submission response - Status code: {response.status}")
                    if response.status == ClientBatchSubmitResponse.OK:
                        logger.info(f"Batch submitted successfully on retry with status OK (code: {response.status})")
                        return result
                    else:
                        logger.error(f"Retry also failed with status: {response.status}")
                        raise Exception(f"Retry failed with status {response.status}: {response}")
                except Exception as retry_e:
                    logger.error(f"Retry to validator {validator_url} also failed: {str(retry_e)}")
                    raise
            raise

    def create_and_send_transactions(self, iot_data, workflow_id, iot_port, iot_public_key):
        try:
            # Always generate a new schedule_id for each transaction
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


    def create_aggregation_request(self, workflow_id, node_id, model_weights, round_number=None, metadata=None):
        """
        Create and submit aggregation request transaction for federated learning.
        This is the second phase of federated learning where trained model weights are submitted for aggregation.
        Note: round_number from IoT nodes is ignored - global rounds are managed by the aggregation-request-tp.
        """
        try:
            logger.info(f"Creating aggregation request for node {node_id}, workflow {workflow_id} (IoT round {round_number} ignored)")
            
            timestamp = int(time.time())
            
            aggregation_payload = {
                "workflow_id": workflow_id,
                "node_id": node_id,
                "model_weights": model_weights,
                "timestamp": timestamp,
                "metadata": metadata or {}
            }
            
            # Only include round_number for backward compatibility, but it will be ignored by the TP
            if round_number is not None:
                aggregation_payload["round_number"] = round_number
            
            # Include all namespaces that aggregation TPs declare to force serial execution
            aggregation_inputs = [AGGREGATION_NAMESPACE, WORKFLOW_NAMESPACE, CONFIRMATION_NAMESPACE]
            aggregation_outputs = [AGGREGATION_NAMESPACE, CONFIRMATION_NAMESPACE]
            
            aggregation_txn = create_transaction(
                self.signer, 
                AGGREGATION_REQUEST_FAMILY_NAME, 
                AGGREGATION_REQUEST_FAMILY_VERSION,
                aggregation_payload, 
                aggregation_inputs, 
                aggregation_outputs
            )
            
            # Create batch and submit
            batch = create_batch([aggregation_txn], self.signer)
            result = self.submit_batch(batch)
            
            logger.info({
                "message": "Aggregation request submitted successfully",
                "result": str(result),
                "node_id": node_id,
                "workflow_id": workflow_id,
                "round_number": round_number
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating aggregation request: {str(e)}")
            raise

    def create_two_phase_federated_transaction(self, training_data, workflow_id, node_id, 
                                             iot_port, iot_public_key, trained_weights=None, 
                                             phase="training", round_number=1):
        """
        Create transactions for two-phase federated learning flow.
        Phase 1: Submit training data for local training (uses standard TrustMesh flow)
        Phase 2: Submit trained model weights for aggregation (uses new aggregation-request-tp)
        """
        try:
            if phase == "training":
                logger.info(f"Phase 1: Submitting training data for node {node_id}")
                # Use standard TrustMesh flow for training - no node_id needed
                return self.create_and_send_transactions(
                    iot_data=training_data,
                    workflow_id=workflow_id,
                    iot_port=iot_port,
                    iot_public_key=iot_public_key
                )
                
            elif phase == "aggregation":
                if not trained_weights:
                    raise ValueError("Trained weights are required for aggregation phase")
                
                logger.info(f"Phase 2: Submitting trained weights for aggregation for node {node_id}")
                return self.create_aggregation_request(
                    workflow_id=workflow_id,
                    node_id=node_id,
                    model_weights=trained_weights,
                    round_number=round_number,
                    metadata={
                        "training_samples": len(training_data.get('x_train', [])) if training_data else 0,
                        "node_classes": training_data.get('assigned_classes', []) if training_data else [],
                        "training_timestamp": int(time.time())
                    }
                )
            else:
                raise ValueError(f"Invalid phase: {phase}. Must be 'training' or 'aggregation'")
                
        except Exception as e:
            logger.error(f"Error in two-phase federated transaction: {str(e)}")
            raise


# This can be imported and used by various data sources
transaction_creator = TransactionCreator()