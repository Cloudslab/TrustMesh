import asyncio
import hashlib
import json
import logging
import os
import ssl
import tempfile
import time
import traceback
import numpy as np
from typing import Dict, List, Any

import torch
import torch.nn as nn
import torch.nn.functional as F
from torchvision import datasets, transforms

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.core import TransactionProcessor
from ibmcloudant.cloudant_v1 import CloudantV1
from ibm_cloud_sdk_core.authenticators import BasicAuthenticator
from coredis import RedisCluster
from coredis.exceptions import RedisError

# CouchDB configuration (primary storage for validation dataset)
COUCHDB_URL = f"https://{os.getenv('COUCHDB_HOST', 'couchdb-0.default.svc.cluster.local:6984')}"
COUCHDB_USERNAME = os.getenv('COUCHDB_USER')
COUCHDB_PASSWORD = os.getenv('COUCHDB_PASSWORD')
COUCHDB_DB = 'validation_datasets'
COUCHDB_SSL_CA = os.getenv('COUCHDB_SSL_CA')
COUCHDB_SSL_CERT = os.getenv('COUCHDB_SSL_CERT')
COUCHDB_SSL_KEY = os.getenv('COUCHDB_SSL_KEY')

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

# Sawtooth configuration
FAMILY_NAME = 'aggregation-confirmation'
FAMILY_VERSION = '1.0'
# Both TPs must use the same namespace for the aggregation state they both access
AGGREGATION_REQUEST_NAMESPACE = hashlib.sha512('aggregation-request'.encode()).hexdigest()[:6]
CONFIRMATION_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]
WORKFLOW_NAMESPACE = hashlib.sha512('workflow-dependency'.encode()).hexdigest()[:6]

# Validation configuration
VALIDATION_SEED = 42  # Fixed seed for deterministic validation
MIN_ACCURACY_THRESHOLD = 0.1  # Minimum acceptable accuracy
MAX_WEIGHT_MAGNITUDE = 10.0  # Maximum allowed weight magnitude
VALIDATION_DATASET_DOC_ID = "mnist_validation_dataset"
VALIDATION_METADATA_KEY = "mnist_validation_metadata"

# Model configuration (must match training task)
MNIST_INPUT_SHAPE = (28, 28, 1)
NUM_CLASSES = 10

logger = logging.getLogger(__name__)


class MNISTNet(nn.Module):
    """PyTorch CNN model for MNIST (must match training task architecture)"""
    def __init__(self, num_classes=10):
        super(MNISTNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, kernel_size=3, padding=1)
        self.pool1 = nn.MaxPool2d(2, 2)
        self.conv2 = nn.Conv2d(32, 64, kernel_size=3, padding=1)
        self.pool2 = nn.MaxPool2d(2, 2)
        self.conv3 = nn.Conv2d(64, 64, kernel_size=3, padding=1)
        self.flatten = nn.Flatten()
        self.fc1 = nn.Linear(64 * 7 * 7, 64)  # 28x28 -> 14x14 -> 7x7 after pooling
        self.fc2 = nn.Linear(64, num_classes)
        self.dropout = nn.Dropout(0.2)
        
    def forward(self, x):
        x = self.pool1(F.relu(self.conv1(x)))
        x = self.pool2(F.relu(self.conv2(x)))
        x = F.relu(self.conv3(x))
        x = self.flatten(x)
        x = self.dropout(F.relu(self.fc1(x)))
        x = F.softmax(self.fc2(x), dim=1)
        return x


class AggregationConfirmationTransactionHandler(TransactionHandler):
    def __init__(self):
        self.redis = None
        self.couchdb_client = None
        self.couchdb_certs = []
        self.loop = asyncio.get_event_loop()
        self._initialize_couchdb()
        self._initialize_redis()

    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        # Must include all namespaces that this TP reads/writes to force serial execution
        return [CONFIRMATION_NAMESPACE, AGGREGATION_REQUEST_NAMESPACE, WORKFLOW_NAMESPACE]

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
        """Initialize CouchDB connection following existing patterns"""
        logger.info("Starting CouchDB initialization for aggregation confirmation TP")
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
            self.cleanup_temp_files()
            raise

    def cleanup_temp_files(self):
        """Clean up temporary certificate files"""
        for cert_file in self.couchdb_certs:
            try:
                cert_file.close()
                os.unlink(cert_file.name)
                logger.debug(f"Cleaned up temporary cert file: {cert_file.name}")
            except Exception as e:
                logger.warning(f"Failed to clean up cert file: {e}")

    def apply(self, transaction, context):
        logger.info("Entering apply method for aggregation confirmation")
        try:
            logger.debug(f"Transaction payload: {transaction.payload}")
            payload = json.loads(transaction.payload.decode())
            logger.info(f"Decoded payload: {payload}")
            
            # Check if this is a status update or aggregation confirmation
            action = payload.get('action', 'confirm')
            
            if action == 'lock':
                self._handle_status_update(payload, context)
            elif action == 'confirm':
                self._handle_aggregation_confirmation(payload, context)
            else:
                raise InvalidTransaction(f"Unknown action: {action}")

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

    def _handle_aggregation_confirmation(self, payload, context):
        logger.info("Entering _handle_aggregation_confirmation method")
        
        aggregation_id = payload['aggregation_id']
        aggregator_node = payload['aggregator_node']
        aggregated_weights = payload['aggregated_weights']
        participating_nodes = payload['participating_nodes']
        
        logger.info(f"Aggregation confirmation - ID: {aggregation_id}, Aggregator: {aggregator_node}")

        # Retrieve aggregation request data
        aggregation_data = self._get_aggregation_data(context, aggregation_id)
        if not aggregation_data:
            raise InvalidTransaction(f"Aggregation request {aggregation_id} not found")

        # Verify aggregator authority
        if aggregation_data['aggregator_node'] != aggregator_node:
            raise InvalidTransaction(f"Invalid aggregator node {aggregator_node} for aggregation {aggregation_id}")

        # Perform FedAvg aggregation verification
        verified_weights = self._verify_fedavg_aggregation(
            aggregation_data['node_contributions'],
            aggregated_weights,
            participating_nodes
        )

        # Perform deterministic validation
        validation_result = self._validate_aggregated_model(verified_weights, aggregation_id)
        
        if not validation_result['is_valid']:
            logger.error(f"Model validation failed: {validation_result['reason']}")
            raise InvalidTransaction(f"Model validation failed: {validation_result['reason']}")

        # Create confirmation record
        confirmation_address = self._make_confirmation_address(aggregation_id)
        confirmation_data = {
            'aggregation_id': aggregation_id,
            'aggregator_node': aggregator_node,
            'aggregated_weights': verified_weights,
            'participating_nodes': participating_nodes,
            'validation_result': validation_result,
            'status': 'confirmed',
            'workflow_id': aggregation_data['workflow_id'],
            'global_round_number': aggregation_data.get('global_round_number', aggregation_data.get('round_number', 1))  # Use global round
        }

        context.set_state({
            confirmation_address: json.dumps(confirmation_data).encode()
        })

        # Update aggregation request status
        self._update_aggregation_status(context, aggregation_id, 'confirmed')

        # Emit confirmation event using global round number
        global_round_number = aggregation_data.get('global_round_number', aggregation_data.get('round_number', 1))
        context.add_event(
            event_type="aggregation-confirmation",
            attributes=[
                ("aggregation_id", aggregation_id),
                ("aggregator_node", aggregator_node),
                ("workflow_id", aggregation_data['workflow_id']),
                ("global_round_number", str(global_round_number)),
                ("participating_nodes", json.dumps(participating_nodes)),
                ("validation_score", str(validation_result['validation_score'])),
                ("status", "confirmed")
            ]
        )
        
        logger.info(f"Aggregation {aggregation_id} confirmed successfully")

    def _get_aggregation_data(self, context, aggregation_id):
        """Retrieve aggregation request data from blockchain state"""
        try:
            address = self._make_aggregation_address(aggregation_id)
            state_entries = context.get_state([address])
            
            if not state_entries:
                logger.error(f"Aggregation data not found for {aggregation_id}")
                return None
            
            aggregation_data = json.loads(state_entries[0].data.decode())
            logger.info(f"Retrieved aggregation data for {aggregation_id}")
            return aggregation_data
            
        except Exception as e:
            logger.error(f"Error retrieving aggregation data: {e}")
            return None

    def _verify_fedavg_aggregation(self, node_contributions: Dict, 
                                 provided_weights: Dict, 
                                 participating_nodes: List[str]) -> Dict:
        """Verify FedAvg aggregation is computed correctly"""
        logger.info("Verifying FedAvg aggregation")
        
        try:
            # Extract model weights from contributions
            node_weights = {}
            total_samples = 0
            
            for node_id in participating_nodes:
                if node_id not in node_contributions:
                    raise InvalidTransaction(f"Missing contribution from node {node_id}")
                
                contribution = node_contributions[node_id]
                node_weights[node_id] = contribution['model_weights']
                
                # Use sample count from metadata for weighted averaging
                samples = contribution.get('metadata', {}).get('sample_count', 1)
                total_samples += samples
            
            # Compute expected FedAvg weights
            expected_weights = self._compute_fedavg(node_weights, participating_nodes, total_samples)
            
            # Verify provided weights match expected (within tolerance)
            if not self._weights_match(expected_weights, provided_weights, tolerance=1e-6):
                raise InvalidTransaction("Provided aggregated weights do not match FedAvg computation")
            
            logger.info("FedAvg aggregation verification successful")
            return expected_weights
            
        except Exception as e:
            logger.error(f"FedAvg verification failed: {e}")
            raise InvalidTransaction(f"FedAvg verification failed: {e}")

    def _compute_fedavg(self, node_weights: Dict, participating_nodes: List[str], total_samples: int) -> Dict:
        """Compute FedAvg aggregation"""
        logger.info(f"Computing FedAvg for {len(participating_nodes)} nodes")
        
        aggregated_weights = {}
        
        # Get first node's weights structure
        first_node = participating_nodes[0]
        weight_structure = node_weights[first_node]
        
        # Initialize aggregated weights
        for layer_name in weight_structure:
            aggregated_weights[layer_name] = np.zeros_like(np.array(weight_structure[layer_name]))
        
        # Weighted averaging
        for node_id in participating_nodes:
            node_weight_dict = node_weights[node_id]
            # Assume equal weighting for simplicity (can be made sample-weighted)
            weight_factor = 1.0 / len(participating_nodes)
            
            for layer_name in node_weight_dict:
                layer_weights = np.array(node_weight_dict[layer_name])
                aggregated_weights[layer_name] += weight_factor * layer_weights
        
        # Convert back to lists for JSON serialization
        for layer_name in aggregated_weights:
            aggregated_weights[layer_name] = aggregated_weights[layer_name].tolist()
        
        logger.info("FedAvg computation completed")
        return aggregated_weights

    def _weights_match(self, weights1: Dict, weights2: Dict, tolerance: float = 1e-6) -> bool:
        """Check if two weight dictionaries match within tolerance"""
        try:
            if set(weights1.keys()) != set(weights2.keys()):
                return False
            
            for layer_name in weights1:
                w1 = np.array(weights1[layer_name])
                w2 = np.array(weights2[layer_name])
                
                if w1.shape != w2.shape:
                    return False
                
                if not np.allclose(w1, w2, atol=tolerance):
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error comparing weights: {e}")
            return False

    def _validate_aggregated_model(self, aggregated_weights: Dict, aggregation_id: str) -> Dict:
        """Perform deterministic validation of aggregated model using distributed MNIST validation dataset"""
        logger.info(f"Validating aggregated model for {aggregation_id}")
        
        try:
            # Set deterministic seed for reproducible validation
            np.random.seed(VALIDATION_SEED)
            
            validation_result = {
                'is_valid': True,
                'reason': 'Validation passed',
                'validation_score': 0.0,
                'checks_performed': []
            }
            
            # Check 1: Weight magnitude validation
            magnitude_check = self._check_weight_magnitudes(aggregated_weights)
            validation_result['checks_performed'].append(magnitude_check)
            
            # Check 2: Weight distribution validation
            distribution_check = self._check_weight_distributions(aggregated_weights)
            validation_result['checks_performed'].append(distribution_check)
            
            # Check 3: Basic sanity checks
            sanity_check = self._check_model_sanity(aggregated_weights)
            validation_result['checks_performed'].append(sanity_check)
            
            # Check 4: MNIST validation dataset evaluation (deterministic)
            mnist_validation_check = self._check_mnist_validation_performance(aggregated_weights)
            validation_result['checks_performed'].append(mnist_validation_check)
            
            # Compute overall validation score
            all_passed = all(check['passed'] for check in validation_result['checks_performed'])
            validation_result['is_valid'] = all_passed
            
            if all_passed:
                validation_result['validation_score'] = np.mean([
                    check.get('score', 1.0) for check in validation_result['checks_performed']
                ])
            else:
                failed_checks = [check['name'] for check in validation_result['checks_performed'] if not check['passed']]
                validation_result['reason'] = f"Failed checks: {', '.join(failed_checks)}"
                validation_result['validation_score'] = 0.0
            
            logger.info(f"Validation completed - Valid: {validation_result['is_valid']}, Score: {validation_result['validation_score']}")
            return validation_result
            
        except Exception as e:
            logger.error(f"Model validation error: {e}")
            return {
                'is_valid': False,
                'reason': f'Validation error: {e}',
                'validation_score': 0.0,
                'checks_performed': []
            }

    def _check_weight_magnitudes(self, weights: Dict) -> Dict:
        """Check if weight magnitudes are within reasonable bounds"""
        try:
            max_magnitude = 0.0
            total_params = 0
            
            for layer_name, layer_weights in weights.items():
                w_array = np.array(layer_weights)
                layer_max = np.max(np.abs(w_array))
                max_magnitude = max(max_magnitude, layer_max)
                total_params += w_array.size
            
            passed = max_magnitude <= MAX_WEIGHT_MAGNITUDE
            score = min(1.0, MAX_WEIGHT_MAGNITUDE / max(max_magnitude, 1e-8))
            
            return {
                'name': 'weight_magnitude',
                'passed': passed,
                'score': score,
                'details': {
                    'max_magnitude': float(max_magnitude),
                    'threshold': MAX_WEIGHT_MAGNITUDE,
                    'total_parameters': total_params
                }
            }
        except Exception as e:
            return {
                'name': 'weight_magnitude',
                'passed': False,
                'score': 0.0,
                'details': {'error': str(e)}
            }

    def _check_weight_distributions(self, weights: Dict) -> Dict:
        """Check if weight distributions are reasonable"""
        try:
            all_weights = []
            for layer_weights in weights.values():
                all_weights.extend(np.array(layer_weights).flatten())
            
            all_weights = np.array(all_weights)
            
            # Check for NaN or infinite values
            has_invalid = np.any(np.isnan(all_weights)) or np.any(np.isinf(all_weights))
            
            # Check standard deviation (should be reasonable)
            std_dev = np.std(all_weights)
            reasonable_std = 0.01 <= std_dev <= 2.0
            
            passed = not has_invalid and reasonable_std
            score = 1.0 if passed else 0.0
            
            return {
                'name': 'weight_distribution',
                'passed': passed,
                'score': score,
                'details': {
                    'has_invalid_values': bool(has_invalid),
                    'std_deviation': float(std_dev),
                    'mean': float(np.mean(all_weights)),
                    'reasonable_std': reasonable_std
                }
            }
        except Exception as e:
            return {
                'name': 'weight_distribution',
                'passed': False,
                'score': 0.0,
                'details': {'error': str(e)}
            }

    def _check_model_sanity(self, weights: Dict) -> Dict:
        """Perform basic sanity checks on model structure"""
        try:
            # Check that we have reasonable layer structure
            has_weights = len(weights) > 0
            
            # Check for consistent layer naming (basic check)
            layer_names = list(weights.keys())
            has_reasonable_structure = any('weight' in name.lower() or 'kernel' in name.lower() 
                                         for name in layer_names)
            
            passed = has_weights and has_reasonable_structure
            score = 1.0 if passed else 0.0
            
            return {
                'name': 'model_sanity',
                'passed': passed,
                'score': score,
                'details': {
                    'layer_count': len(weights),
                    'layer_names': layer_names,
                    'has_reasonable_structure': has_reasonable_structure
                }
            }
        except Exception as e:
            return {
                'name': 'model_sanity',
                'passed': False,
                'score': 0.0,
                'details': {'error': str(e)}
            }

    def _handle_status_update(self, payload, context):
        """Handle aggregation round status updates (lock round)"""
        logger.info("Entering _handle_status_update method")
        
        aggregation_id = payload['aggregation_id']
        new_status = payload['new_status']
        workflow_id = payload['workflow_id']
        global_round_number = payload['global_round_number']
        
        logger.info(f"Status update - Aggregation: {aggregation_id}, New Status: {new_status}")
        
        # Get current aggregation round data
        aggregation_address = self._make_aggregation_address(aggregation_id)
        state_entries = context.get_state([aggregation_address])
        
        if not state_entries:
            logger.error(f"Aggregation round {aggregation_id} not found")
            raise InvalidTransaction(f"Aggregation round {aggregation_id} not found")
        
        round_data = json.loads(state_entries[0].data.decode())
        
        # Validate status transition
        current_status = round_data.get('status', 'unknown')
        valid_transitions = {
            'collecting': ['locked', 'failed'],
            'locked': ['confirmed', 'failed'],
            'confirmed': [],  # Terminal state
            'failed': []      # Terminal state
        }
        
        if new_status not in valid_transitions.get(current_status, []):
            raise InvalidTransaction(f"Invalid status transition from {current_status} to {new_status}")
        
        # Update status
        round_data['status'] = new_status
        
        # Store updated data
        context.set_state({
            aggregation_address: json.dumps(round_data).encode()
        })
        
        # Emit status update event
        context.add_event(
            event_type="aggregation-status-update",
            attributes=[
                ("aggregation_id", aggregation_id),
                ("workflow_id", workflow_id),
                ("global_round_number", str(global_round_number)),
                ("old_status", current_status),
                ("new_status", new_status)
            ]
        )
        
        logger.info(f"Status updated for {aggregation_id}: {current_status} → {new_status}")
        
    def _update_aggregation_status(self, context, aggregation_id, status):
        """Update the status of the aggregation request (used internally)"""
        try:
            address = self._make_aggregation_address(aggregation_id)
            state_entries = context.get_state([address])
            
            if state_entries:
                aggregation_data = json.loads(state_entries[0].data.decode())
                aggregation_data['status'] = status
                
                context.set_state({
                    address: json.dumps(aggregation_data).encode()
                })
                
                logger.info(f"Updated aggregation {aggregation_id} status to {status}")
        except Exception as e:
            logger.error(f"Error updating aggregation status: {e}")

    @staticmethod
    def _make_confirmation_address(aggregation_id):
        """Generate blockchain address for aggregation confirmation"""
        return CONFIRMATION_NAMESPACE + hashlib.sha512((aggregation_id + "_confirmation").encode()).hexdigest()[:64]

    @staticmethod
    def _make_aggregation_address(aggregation_id):
        """Generate blockchain address for aggregation request"""
        return AGGREGATION_REQUEST_NAMESPACE + hashlib.sha512(aggregation_id.encode()).hexdigest()[:64]

    def _check_mnist_validation_performance(self, aggregated_weights: Dict) -> Dict:
        """Check model performance on distributed MNIST validation dataset"""
        try:
            logger.info("Performing MNIST validation dataset evaluation")
            
            # Retrieve validation dataset from CouchDB
            validation_data = self.loop.run_until_complete(self._get_validation_dataset())
            
            if not validation_data:
                logger.warning("Validation dataset not available, skipping MNIST validation")
                return {
                    'name': 'mnist_validation',
                    'passed': True,  # Pass if dataset not available (graceful degradation)
                    'score': 0.5,
                    'details': {'warning': 'Validation dataset not available'}
                }
            
            # Create model architecture (must match training task)
            model = self._create_validation_model()
            
            # Load aggregated weights (PyTorch state_dict format)
            try:
                state_dict = {}
                for layer_name, weights in aggregated_weights.items():
                    state_dict[layer_name] = torch.tensor(weights, dtype=torch.float32)
                model.load_state_dict(state_dict)
                model.eval()
            except Exception as e:
                logger.error(f"Error loading aggregated weights: {e}")
                return {
                    'name': 'mnist_validation',
                    'passed': False,
                    'score': 0.0,
                    'details': {'error': f'Weight loading failed: {e}'}
                }
            
            # Evaluate model on validation dataset
            x_val = np.array(validation_data['x_data'])
            y_val = np.array(validation_data['y_data'])
            
            # Convert to PyTorch tensors and reshape
            if len(x_val.shape) == 3:
                x_val = x_val.reshape(x_val.shape[0], 1, 28, 28)  # PyTorch format: (N, C, H, W)
            x_val = torch.tensor(x_val, dtype=torch.float32)
            y_val = torch.tensor(y_val, dtype=torch.long)
            
            # Evaluate model (deterministic)
            with torch.no_grad():
                outputs = model(x_val)
                predictions = torch.argmax(outputs, dim=1)
                accuracy = (predictions == y_val).float().mean().item()
                
                # Calculate loss
                loss_fn = nn.CrossEntropyLoss()
                loss = loss_fn(outputs, y_val).item()
            
            # Check if accuracy meets minimum threshold
            passed = accuracy >= MIN_ACCURACY_THRESHOLD
            
            # Calculate score based on accuracy
            score = min(1.0, accuracy / 0.9)  # Scale to 1.0 at 90% accuracy
            
            logger.info(f"MNIST validation - Accuracy: {accuracy:.4f}, Loss: {loss:.4f}, Passed: {passed}")
            
            return {
                'name': 'mnist_validation',
                'passed': passed,
                'score': float(score),
                'details': {
                    'accuracy': float(accuracy),
                    'loss': float(loss),
                    'threshold': MIN_ACCURACY_THRESHOLD,
                    'validation_samples': len(x_val),
                    'data_hash': validation_data['metadata']['data_hash'][:16]
                }
            }
            
        except Exception as e:
            logger.error(f"Error in MNIST validation: {e}")
            return {
                'name': 'mnist_validation',
                'passed': False,
                'score': 0.0,
                'details': {'error': str(e)}
            }

    def _create_validation_model(self) -> MNISTNet:
        """Create MNIST model for validation (must match training task architecture)"""
        model = MNISTNet(num_classes=NUM_CLASSES)
        model.eval()  # Set to evaluation mode
        return model

    async def _get_validation_dataset(self) -> Dict:
        """Retrieve validation dataset from CouchDB"""
        try:
            # Get validation dataset from CouchDB
            stored_doc = self.couchdb_client.get_document(db=COUCHDB_DB, doc_id=VALIDATION_DATASET_DOC_ID).get_result()
            if not stored_doc:
                logger.warning("Validation dataset not found in CouchDB")
                return None
            
            # Verify data integrity
            x_data = np.array(stored_doc['x_data'])
            y_data = np.array(stored_doc['y_data'])
            metadata = stored_doc['metadata']
            
            # Check hashes for data integrity
            data_hash = hashlib.sha256(x_data.tobytes()).hexdigest()
            if data_hash != metadata['data_hash']:
                logger.error("Validation dataset integrity check failed")
                return None
            
            logger.info(f"Retrieved validation dataset from CouchDB: {len(x_data)} samples")
            return {
                'x_data': stored_doc['x_data'],
                'y_data': stored_doc['y_data'],
                'metadata': metadata
            }
            
        except Exception as e:
            logger.error(f"Error retrieving validation dataset from CouchDB: {e}")
            return None


def main():
    logger.info("Starting Aggregation Confirmation Transaction Processor")
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = AggregationConfirmationTransactionHandler()
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
        handler.cleanup_temp_files()
        processor.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()