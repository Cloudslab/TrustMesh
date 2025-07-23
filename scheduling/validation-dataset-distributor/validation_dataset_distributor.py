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

import tensorflow as tf
from coredis import RedisCluster
from coredis.exceptions import RedisError

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

# Validation dataset configuration
VALIDATION_SEED = 42  # Fixed seed for reproducible validation dataset
VALIDATION_SAMPLES_PER_CLASS = 100  # 100 samples per class = 1000 total samples
VALIDATION_DATASET_KEY = "mnist_validation_dataset"
VALIDATION_METADATA_KEY = "mnist_validation_metadata"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MNISTValidationDatasetDistributor:
    """Distributes MNIST validation dataset to all blockchain nodes for consensus validation"""
    
    def __init__(self):
        self.redis = None
        self._initialize_redis()
        logger.info("Initialized MNIST Validation Dataset Distributor")
    
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
            
            logger.info("Connected to Redis cluster successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
        finally:
            for file_path in temp_files:
                try:
                    os.unlink(file_path)
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")
    
    def create_validation_dataset(self) -> Tuple[np.ndarray, np.ndarray, Dict]:
        """Create standardized MNIST validation dataset"""
        try:
            logger.info("Creating MNIST validation dataset")
            
            # Set seed for reproducibility
            np.random.seed(VALIDATION_SEED)
            tf.random.set_seed(VALIDATION_SEED)
            
            # Load MNIST test dataset (we'll use this as our validation set)
            (_, _), (x_test, y_test) = tf.keras.datasets.mnist.load_data()
            
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
        """Distribute validation dataset to Redis for all blockchain nodes"""
        try:
            logger.info("Starting validation dataset distribution")
            
            # Create validation dataset
            validation_x, validation_y, metadata = self.create_validation_dataset()
            
            # Serialize data for Redis storage
            validation_data = {
                'x_data': validation_x.tolist(),  # Convert to list for JSON serialization
                'y_data': validation_y.tolist(),
                'metadata': metadata
            }
            
            # Store in Redis with long expiration (24 hours)
            await self.redis.setex(
                VALIDATION_DATASET_KEY, 
                86400,  # 24 hours
                json.dumps(validation_data)
            )
            
            # Store metadata separately for quick access
            await self.redis.setex(
                VALIDATION_METADATA_KEY,
                86400,  # 24 hours
                json.dumps(metadata)
            )
            
            logger.info(f"Successfully distributed validation dataset to Redis")
            logger.info(f"Dataset key: {VALIDATION_DATASET_KEY}")
            logger.info(f"Total samples: {metadata['total_samples']}")
            logger.info(f"Data integrity hash: {metadata['data_hash'][:16]}...")
            
            # Test retrieval to ensure data integrity
            await self._test_dataset_retrieval()
            
        except Exception as e:
            logger.error(f"Error distributing validation dataset: {e}")
            raise
    
    async def _test_dataset_retrieval(self):
        """Test that the distributed dataset can be retrieved correctly"""
        try:
            logger.info("Testing validation dataset retrieval")
            
            # Retrieve dataset
            stored_data = await self.redis.get(VALIDATION_DATASET_KEY)
            if not stored_data:
                raise ValueError("Validation dataset not found in Redis")
            
            dataset = json.loads(stored_data)
            
            # Verify data integrity
            x_data = np.array(dataset['x_data'])
            y_data = np.array(dataset['y_data'])
            metadata = dataset['metadata']
            
            # Check hashes
            data_hash = hashlib.sha256(x_data.tobytes()).hexdigest()
            labels_hash = hashlib.sha256(y_data.tobytes()).hexdigest()
            
            if data_hash != metadata['data_hash']:
                raise ValueError("Data integrity check failed: data hash mismatch")
            
            if labels_hash != metadata['labels_hash']:
                raise ValueError("Data integrity check failed: labels hash mismatch")
            
            logger.info("✓ Validation dataset retrieval test passed")
            logger.info(f"✓ Data integrity verified - hash: {data_hash[:16]}...")
            
        except Exception as e:
            logger.error(f"Dataset retrieval test failed: {e}")
            raise
    
    async def get_validation_dataset_info(self) -> Dict:
        """Get validation dataset metadata"""
        try:
            metadata_json = await self.redis.get(VALIDATION_METADATA_KEY)
            if metadata_json:
                return json.loads(metadata_json)
            return None
        except Exception as e:
            logger.error(f"Error getting validation dataset info: {e}")
            return None
    
    async def verify_dataset_availability(self) -> bool:
        """Verify that validation dataset is available in Redis"""
        try:
            # Check if both dataset and metadata exist
            dataset_exists = await self.redis.exists(VALIDATION_DATASET_KEY)
            metadata_exists = await self.redis.exists(VALIDATION_METADATA_KEY)
            
            return bool(dataset_exists and metadata_exists)
            
        except Exception as e:
            logger.error(f"Error verifying dataset availability: {e}")
            return False
    
    async def cleanup_validation_dataset(self):
        """Clean up validation dataset from Redis"""
        try:
            await self.redis.delete(VALIDATION_DATASET_KEY)
            await self.redis.delete(VALIDATION_METADATA_KEY)
            logger.info("Validation dataset cleaned up from Redis")
        except Exception as e:
            logger.error(f"Error cleaning up validation dataset: {e}")


async def main():
    """Main entry point for validation dataset distribution"""
    try:
        logger.info("Starting MNIST Validation Dataset Distribution")
        
        distributor = MNISTValidationDatasetDistributor()
        
        # Check if dataset already exists
        if await distributor.verify_dataset_availability():
            logger.info("Validation dataset already exists in Redis")
            info = await distributor.get_validation_dataset_info()
            if info:
                logger.info(f"Existing dataset info: {info['total_samples']} samples, created at {time.ctime(info['created_time'])}")
        else:
            logger.info("Distributing new validation dataset")
            await distributor.distribute_validation_dataset()
        
        logger.info("Validation dataset distribution completed successfully")
        
    except Exception as e:
        logger.error(f"Validation dataset distribution failed: {e}")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())