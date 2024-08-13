import hashlib
import logging
import os
import sys
import docker

from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch
from sawtooth_signing import create_context, CryptoFactory, secp256k1
from sawtooth_sdk.messaging.stream import Stream

logger = logging.getLogger(__name__)

FAMILY_NAME = 'docker-image'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]
REGISTRY_URL = os.getenv('REGISTRY_URL', 'sawtooth-registry:5000')

# Path to the private key file
PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/etc/sawtooth/keys/user.priv')


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as e:
        raise IOError(f"Failed to load private key from {key_file}: {str(e)}") from e


def hash_and_push_docker_image(tar_path):
    logger.info(f"Processing Docker image from tar: {tar_path}")
    client = docker.from_env()

    # Calculate hash
    sha256_hash = hashlib.sha256()
    with open(tar_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            sha256_hash.update(chunk)
    image_hash = sha256_hash.hexdigest()
    logger.info(f"Calculated image hash: {image_hash}")

    # Load image from tar
    with open(tar_path, 'rb') as f:
        image = client.images.load(f.read())[0]

    # Tag and push to local registry
    image_name = image.tags[0] if image.tags else f"image-{image_hash[:12]}"
    registry_image_name = f"{REGISTRY_URL}/{image_name}"
    image.tag(registry_image_name)
    logger.info(f"Pushing Docker image to local registry: {registry_image_name}")
    client.images.push(registry_image_name)
    logger.info("Image pushed successfully")

    return image_hash, registry_image_name


def create_transaction(image_hash, image_name, signer):
    logger.info(f"Creating transaction for image: {image_name} with hash: {image_hash}")
    payload = f"{image_hash},{image_name}".encode()

    header = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[NAMESPACE],
        outputs=[NAMESPACE],
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=hashlib.sha512(payload).hexdigest(),
    ).SerializeToString()

    signature = signer.sign(header)

    transaction = Transaction(header=header, payload=payload, header_signature=signature)

    logger.info(f"Transaction created with signature: {signature}")
    return transaction


def create_batch(transactions, signer):
    logger.info(f"Creating batch for transactions: {transactions}")
    batch_header = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=[t.header_signature for t in transactions],
    ).SerializeToString()

    signature = signer.sign(batch_header)

    batch = Batch(
        header=batch_header,
        transactions=transactions,
        header_signature=signature,
    )

    logger.info(f"Batch created with signature: {signature}")
    return batch


def submit_batch(batch):
    logger.info("Submitting batch to validator")
    stream = Stream(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))

    future = stream.send(
        message_type='CLIENT_BATCH_SUBMIT_REQUEST',
        content=batch.SerializeToString()
    )

    result = future.result()
    logger.info(f"Submitted batch to validator: {result}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python docker_image_client.py <path_to_docker_image.tar>")
        sys.exit(1)

    tar_path = sys.argv[1]

    if not os.path.exists(tar_path):
        print(f"Error: File {tar_path} does not exist")
        sys.exit(1)

    try:
        private_key = load_private_key(PRIVATE_KEY_FILE)
    except IOError as e:
        logger.error(str(e))
        sys.exit(1)

    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(private_key)

    image_hash, registry_image_name = hash_and_push_docker_image(tar_path)
    transaction = create_transaction(image_hash, registry_image_name, signer)
    batch = create_batch([transaction], signer)
    submit_batch(batch)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()