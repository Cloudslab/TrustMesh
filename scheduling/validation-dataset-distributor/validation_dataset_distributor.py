#!/usr/bin/env python3
"""
MNIST Validation Dataset Distributor

This component distributes a standardized MNIST validation dataset to all blockchain nodes
for deterministic consensus validation in the aggregation-confirmation-tp.
"""

import asyncio
import hashlib
import json
import logging
import numpy as np
import os
import ssl
import tempfile
import time
from typing import Dict, List, Tuple

import torch
import torchvision
from torchvision import datasets, transforms
from ibmcloudant.cloudant_v1 import CloudantV1
from ibm_cloud_sdk_core.authenticators import BasicAuthenticator
from coredis import RedisCluster
from coredis.exceptions import RedisError

# CouchDB configuration (primary storage)
COUCHDB_URL = f"https://{os.getenv('COUCHDB_HOST', 'couchdb-0.default.svc.cluster.local:6984')}"
COUCHDB_USERNAME = os.getenv('COUCHDB_USER')
COUCHDB_PASSWORD = os.getenv('COUCHDB_PASSWORD')
COUCHDB_DB = 'validation_datasets'
COUCHDB_SSL_CA = os.getenv('COUCHDB_SSL_CA')
COUCHDB_SSL_CERT = os.getenv('COUCHDB_SSL_CERT')
COUCHDB_SSL_KEY = os.getenv('COUCHDB_SSL_KEY')

# Redis configuration (for metadata caching)
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

# Validation dataset configuration
VALIDATION_SEED = 42  # Fixed seed for reproducible validation dataset
VALIDATION_SAMPLES_PER_CLASS = 100  # 100 samples per class = 1000 total samples
VALIDATION_DATASET_DOC_ID = "mnist_validation_dataset"
VALIDATION_METADATA_CACHE_KEY = "mnist_validation_metadata"  # Redis cache key

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MNISTValidationDatasetDistributor:
    """Distributes MNIST validation dataset to CouchDB for persistent storage and consensus validation"""
    
    def __init__(self):
        self.couchdb_client = None
        self.redis = None
        self.couchdb_certs = []
        self._initialize_couchdb()
        self._initialize_redis()
        logger.info("Initialized MNIST Validation Dataset Distributor (CouchDB + Redis)")
    
    def _initialize_redis(self):
        """Initialize Redis connection"""
        logger.info("Starting Redis initialization for validation dataset distributor")
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

            logger.info(f"Attempting to connect to Redis cluster at {REDIS_HOST}:{REDIS_PORT}")

            self.redis = RedisCluster(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                ssl=True,
                ssl_context=ssl_context,
                decode_responses=False  # Keep binary for numpy arrays
            )

            # Test connection
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.redis.ping())
            loop.close()
            
            logger.info("Connected to Redis cluster successfully for metadata caching")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
        finally:
            for file_path in temp_files:
                try:
                    os.unlink(file_path)
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")
    
    def _initialize_couchdb(self):
        """Initialize CouchDB connection following existing patterns"""
        logger.info("Starting CouchDB initialization for validation dataset distributor")
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

            # Test connection by checking if database exists or create it
            try:
                self.couchdb_client.get_database_information(db=COUCHDB_DB).get_result()
                logger.info(f"Successfully connected to CouchDB database '{COUCHDB_DB}'")
            except Exception as db_error:
                # Database might not exist, try to create it
                logger.info(f"Database '{COUCHDB_DB}' not found, creating it...")
                self.couchdb_client.put_database(db=COUCHDB_DB).get_result()
                logger.info(f"Successfully created CouchDB database '{COUCHDB_DB}'")

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
    
    def create_validation_dataset(self) -> Tuple[np.ndarray, np.ndarray, Dict]:
        """Create standardized MNIST validation dataset"""
        try:
            logger.info("Creating MNIST validation dataset")
            
            # Set seed for reproducibility
            np.random.seed(VALIDATION_SEED)
            torch.manual_seed(VALIDATION_SEED)
            
            # Load MNIST test dataset (we'll use this as our validation set)
            transform = transforms.Compose([transforms.ToTensor()])
            test_dataset = datasets.MNIST(root='/tmp/mnist', train=False, download=True, transform=transform)
            
            # Convert to numpy arrays for compatibility with existing code
            x_test = test_dataset.data.numpy()
            y_test = test_dataset.targets.numpy()
            
            # Normalize pixel values to [0, 1]
            x_test = x_test.astype('float32') / 255.0
            
            # Create balanced validation set
            validation_x = []
            validation_y = []
            
            for digit_class in range(10):
                # Get indices for this digit class
                class_indices = np.where(y_test == digit_class)[0]
                
                # Select fixed number of samples using deterministic selection
                selected_indices = class_indices[:VALIDATION_SAMPLES_PER_CLASS]
                
                validation_x.extend(x_test[selected_indices])
                validation_y.extend(y_test[selected_indices])
            
            validation_x = np.array(validation_x)
            validation_y = np.array(validation_y)
            
            # Create metadata
            metadata = {
                'total_samples': len(validation_x),
                'samples_per_class': VALIDATION_SAMPLES_PER_CLASS,
                'num_classes': 10,
                'seed': VALIDATION_SEED,
                'shape': validation_x.shape,
                'created_time': time.time(),
                'class_distribution': {str(i): VALIDATION_SAMPLES_PER_CLASS for i in range(10)},
                'data_hash': hashlib.sha256(validation_x.tobytes()).hexdigest(),
                'labels_hash': hashlib.sha256(validation_y.tobytes()).hexdigest()
            }
            
            logger.info(f"Created validation dataset: {validation_x.shape} samples")
            logger.info(f"Class distribution: {metadata['class_distribution']}")
            logger.info(f"Data hash: {metadata['data_hash'][:16]}...")
            
            return validation_x, validation_y, metadata
            
        except Exception as e:
            logger.error(f"Error creating validation dataset: {e}")
            raise
    
    async def distribute_validation_dataset(self):
        """Distribute validation dataset to CouchDB for persistent storage"""
        try:
            logger.info("Starting validation dataset distribution to CouchDB")
            
            # Create validation dataset
            validation_x, validation_y, metadata = self.create_validation_dataset()
            
            # Prepare document for CouchDB storage
            validation_doc = {
                '_id': VALIDATION_DATASET_DOC_ID,
                'type': 'mnist_validation_dataset',
                'x_data': validation_x.tolist(),  # Convert to list for JSON serialization
                'y_data': validation_y.tolist(),
                'metadata': metadata,
                'created_time': time.time(),
                'version': '1.0'
            }
            
            # Store in CouchDB (permanent storage)
            try:
                self.couchdb_client.post_document(db=COUCHDB_DB, document=validation_doc).get_result()
                logger.info(f"Successfully stored validation dataset in CouchDB database '{COUCHDB_DB}'")
            except Exception as e:
                # Handle document conflict (already exists)
                if "conflict" in str(e).lower():
                    logger.info(f"Validation dataset already exists in CouchDB, updating...")
                    # Get existing document to get revision
                    existing_doc = self.couchdb_client.get_document(db=COUCHDB_DB, doc_id=VALIDATION_DATASET_DOC_ID).get_result()
                    validation_doc['_rev'] = existing_doc['_rev']
                    self.couchdb_client.post_document(db=COUCHDB_DB, document=validation_doc).get_result()
                    logger.info(f"Successfully updated validation dataset in CouchDB")
                else:
                    raise e
            
            # Cache metadata in Redis for quick access (optional, with shorter TTL)
            if self.redis:
                await self.redis.setex(
                    VALIDATION_METADATA_CACHE_KEY,
                    3600,  # 1 hour cache
                    json.dumps(metadata)
                )
                logger.info(f"Cached validation dataset metadata in Redis")
            
            logger.info(f"Successfully distributed validation dataset to CouchDB")
            logger.info(f"Document ID: {VALIDATION_DATASET_DOC_ID}")
            logger.info(f"Database: {COUCHDB_DB}")
            logger.info(f"Total samples: {metadata['total_samples']}")
            logger.info(f"Data integrity hash: {metadata['data_hash'][:16]}...")
            
            # Test retrieval to ensure data integrity
            await self._test_dataset_retrieval()
            
        except Exception as e:
            logger.error(f"Error distributing validation dataset: {e}")
            raise
    
    async def _test_dataset_retrieval(self):
        """Test that the distributed dataset can be retrieved correctly from CouchDB"""
        try:
            logger.info("Testing validation dataset retrieval from CouchDB")
            
            # Retrieve dataset from CouchDB
            stored_doc = self.couchdb_client.get_document(db=COUCHDB_DB, doc_id=VALIDATION_DATASET_DOC_ID).get_result()
            
            if not stored_doc:
                raise ValueError("Validation dataset not found in CouchDB")
            
            # Verify data integrity
            x_data = np.array(stored_doc['x_data'])
            y_data = np.array(stored_doc['y_data'])
            metadata = stored_doc['metadata']
            
            # Check hashes
            data_hash = hashlib.sha256(x_data.tobytes()).hexdigest()
            labels_hash = hashlib.sha256(y_data.tobytes()).hexdigest()
            
            if data_hash != metadata['data_hash']:
                raise ValueError("Data integrity check failed: data hash mismatch")
            
            if labels_hash != metadata['labels_hash']:
                raise ValueError("Data integrity check failed: labels hash mismatch")
            
            logger.info("✓ Validation dataset retrieval test passed (CouchDB)")
            logger.info(f"✓ Data integrity verified - hash: {data_hash[:16]}...")
            logger.info(f"✓ Document revision: {stored_doc.get('_rev', 'N/A')}")
            
        except Exception as e:
            logger.error(f"Dataset retrieval test failed: {e}")
            raise
    
    async def get_validation_dataset_info(self) -> Dict:
        """Get validation dataset metadata from CouchDB"""
        try:
            # Get from CouchDB
            stored_doc = self.couchdb_client.get_document(db=COUCHDB_DB, doc_id=VALIDATION_DATASET_DOC_ID).get_result()
            if stored_doc and 'metadata' in stored_doc:
                return stored_doc['metadata']
            return None
        except Exception as e:
            logger.error(f"Error getting validation dataset info: {e}")
            return None
    
    async def verify_dataset_availability(self) -> bool:
        """Verify that validation dataset is available in CouchDB"""
        try:
            # Check if dataset document exists in CouchDB
            stored_doc = self.couchdb_client.get_document(db=COUCHDB_DB, doc_id=VALIDATION_DATASET_DOC_ID).get_result()
            return bool(stored_doc and 'x_data' in stored_doc and 'y_data' in stored_doc)
            
        except Exception as e:
            logger.debug(f"Validation dataset not found in CouchDB: {e}")
            return False
    
    async def cleanup_validation_dataset(self):
        """Clean up validation dataset from CouchDB (delete document)"""
        try:
            # Get existing document to get revision
            existing_doc = self.couchdb_client.get_document(db=COUCHDB_DB, doc_id=VALIDATION_DATASET_DOC_ID).get_result()
            if existing_doc:
                self.couchdb_client.delete_document(db=COUCHDB_DB, doc_id=VALIDATION_DATASET_DOC_ID, rev=existing_doc['_rev']).get_result()
                logger.info("Validation dataset cleaned up from CouchDB")
        except Exception as e:
            logger.error(f"Error cleaning up validation dataset: {e}")


async def main():
    """Main entry point for validation dataset distribution"""
    try:
        logger.info("Starting MNIST Validation Dataset Distribution")
        
        distributor = MNISTValidationDatasetDistributor()
        
        # Check if dataset already exists in CouchDB
        if await distributor.verify_dataset_availability():
            logger.info("Validation dataset already exists in CouchDB")
            info = await distributor.get_validation_dataset_info()
            if info:
                logger.info(f"Existing dataset info: {info['total_samples']} samples, created at {time.ctime(info['created_time'])}")
        else:
            logger.info("Distributing new validation dataset to CouchDB")
            await distributor.distribute_validation_dataset()
        
        logger.info("Validation dataset distribution completed successfully")
        
    except Exception as e:
        logger.error(f"Validation dataset distribution failed: {e}")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())