import hashlib
import logging
import os
import docker
from docker import errors

from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.events_pb2 import EventSubscription, EventList
from sawtooth_sdk.protobuf.client_event_pb2 import ClientEventsSubscribeRequest, ClientEventsSubscribeResponse
from sawtooth_sdk.protobuf.validator_pb2 import Message

logger = logging.getLogger(__name__)

FAMILY_NAME = "docker-image"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]
REGISTRY_URL = os.getenv('REGISTRY_URL', 'sawtooth-registry:5000')

# Port management
START_PORT = 12345
END_PORT = 13345
used_ports = set()

# Increased timeout (in seconds)
DOCKER_TIMEOUT = 180


def get_next_available_port():
    for port in range(START_PORT, END_PORT):
        if port not in used_ports:
            used_ports.add(port)
            return port
    raise Exception("No available ports")


def release_port(port):
    used_ports.remove(port)


def handle_event(event):
    logger.info(f"Handling event: {event.event_type}")
    if event.event_type == "docker-image-action":
        image_hash = None
        image_name = None
        app_id = None
        action = None
        for attr in event.attributes:
            if attr.key == "image_hash":
                image_hash = attr.value
            elif attr.key == "image_name":
                image_name = attr.value
            elif attr.key == "app_id":
                app_id = attr.value
            elif attr.key == "action":
                action = attr.value
        if image_hash and image_name and app_id and action:
            logger.info(
                f"Processing action: {action} for image: {image_name} with hash: {image_hash}, app_id: {app_id}")
            process_docker_action(action, image_hash, image_name, app_id)
        else:
            logger.warning("Received docker-image-action event with incomplete data")
    elif event.event_type == "sawtooth/block-commit":
        logger.info("New block committed")
    else:
        logger.info(f"Received unhandled event type: {event.event_type}")


def process_docker_action(action, image_hash, image_name, app_id):
    client = docker.from_env(timeout=DOCKER_TIMEOUT)
    container_name = f"sawtooth-{app_id}"

    if action == "deploy_image":
        deploy_image(client, image_name, image_hash, container_name)
    elif action == "deploy_container":
        deploy_container(client, image_name, container_name)
    elif action == "remove_container":
        remove_container(client, container_name)
    elif action == "remove_image":
        remove_image(client, image_name, container_name)
    else:
        logger.warning(f"Unknown action: {action}")


def deploy_image(client, image_name, image_hash, container_name):
    logger.info(f"Deploying image: {image_name}")
    try:
        image = client.images.pull(image_name)
        if verify_image(client, image.id, image_hash):
            deploy_container(client, image_name, container_name)
        else:
            logger.error("Image verification failed")
    except docker.errors.ImageNotFound:
        logger.error(f"Image {image_name} not found")
    except Exception as e:
        logger.error(f"Error deploying image: {str(e)}")


def deploy_container(client, image_name, container_name):
    logger.info(f"Deploying container: {container_name}")
    host_port = None
    try:
        # Remove existing container if it exists
        remove_container(client, container_name)

        # Get the next available port
        host_port = get_next_available_port()

        container = client.containers.run(
            image_name,
            name=container_name,
            detach=True,
            ports={'12345/tcp': host_port}  # Map container's 12345 to host's dynamic port
        )
        logger.info(f"Container started: {container.id}, mapped to host port: {host_port}")
    except docker.errors.ContainerError as e:
        logger.error(f"Error starting container: {str(e)}")
        if host_port is not None:
            release_port(host_port)


def remove_container(client, container_name):
    logger.info(f"Removing container: {container_name}")
    try:
        container = client.containers.get(container_name)
        # Get the host port before removing the container
        container_info = client.api.inspect_container(container.id)
        host_port = int(container_info['NetworkSettings']['Ports']['12345/tcp'][0]['HostPort'])

        container.remove(force=True)
        logger.info(f"Container {container_name} removed")

        # Release the port
        release_port(host_port)
        logger.info(f"Released host port: {host_port}")
    except docker.errors.NotFound:
        logger.info(f"Container {container_name} not found, no action needed")
    except Exception as e:
        logger.error(f"Error removing container: {str(e)}")


def remove_image(client, image_name, container_name):
    logger.info(f"Removing image: {image_name}")
    remove_container(client, container_name)
    try:
        client.images.remove(image_name, force=True)
        logger.info(f"Image {image_name} removed")
    except docker.errors.ImageNotFound:
        logger.info(f"Image {image_name} not found, no action needed")
    except Exception as e:
        logger.error(f"Error removing image: {str(e)}")


def verify_image(client, image_id, stored_digest):
    logger.info(f"Verifying image: {image_id}")
    image_inspect = client.api.inspect_image(image_id)
    pulled_digest = image_inspect['RepoDigests'][0].split('@')[1] if image_inspect['RepoDigests'] else None

    if not pulled_digest:
        logger.error("Could not obtain content digest for pulled image")
        return False

    if pulled_digest != stored_digest:
        logger.error(f"Image digest mismatch. Expected: {stored_digest} Got: {pulled_digest}")
        return False

    logger.info("Image digest verified successfully")
    return True


def main():
    logger.info("Starting Docker Image Event Handler")
    stream = Stream(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))

    block_commit_subscription = EventSubscription(
        event_type="sawtooth/block-commit"
    )

    docker_image_subscription = EventSubscription(
        event_type="docker-image-action"
    )

    request = ClientEventsSubscribeRequest(
        subscriptions=[block_commit_subscription, docker_image_subscription]
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

    logger.info("Docker Image Handler: Subscription successful. Listening for events...")

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
