import hashlib
import json
import logging
import os
import traceback

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.core import TransactionProcessor
from sawtooth_sdk.processor.exceptions import AuthorizationException

FAMILY_NAME = 'workflow-dependency'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

DOCKER_IMAGE_FAMILY = 'docker-image'
DOCKER_IMAGE_NAMESPACE = hashlib.sha512(DOCKER_IMAGE_FAMILY.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class WorkflowTransactionHandler(TransactionHandler):
    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [NAMESPACE, DOCKER_IMAGE_NAMESPACE]

    def apply(self, transaction, context):
        try:
            logger.info("Applying transaction")
            logger.debug(f"Transaction details: {transaction}")

            header_signature = getattr(transaction, 'header_signature', None)
            logger.info(f"Processing transaction: {header_signature}")

            try:
                payload = json.loads(transaction.payload.decode())
                logger.debug(f"Decoded payload: {json.dumps(payload)}")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid payload: not a valid JSON. Error: {str(e)}")
                raise InvalidTransaction('Invalid payload: not a valid JSON')

            action = payload.get('action')
            workflow_id = payload.get('workflow_id')
            dependency_graph = payload.get('dependency_graph')

            if action != 'create' or not workflow_id or not dependency_graph:
                logger.error("Invalid payload: missing required fields")
                raise InvalidTransaction("Invalid payload: missing required fields")

            logger.info(f"Processing create action for workflow ID: {workflow_id}")
            logger.debug(f"Dependency graph for workflow {workflow_id}: {json.dumps(dependency_graph)}")

            self._create_workflow(context, workflow_id, dependency_graph)

        except InvalidTransaction as e:
            logger.error(f"Invalid transaction: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in apply method: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(f"Unexpected error: {str(e)}")

    def _create_workflow(self, context, workflow_id, dependency_graph):
        logger.info(f"Creating workflow: {workflow_id}")
        try:
            self._validate_dependency_graph(context, dependency_graph)

            workflow_data = {
                'workflow_id': workflow_id,
                'dependency_graph': dependency_graph
            }
            state_data = json.dumps(workflow_data).encode()
            state_address = self._make_workflow_address(workflow_id)
            logger.debug(f"Storing workflow at address: {state_address}")
            context.set_state({state_address: state_data})
            logger.info(f"Workflow {workflow_id} created successfully")
        except InvalidTransaction as e:
            logger.error(f"Failed to create workflow {workflow_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in _create_workflow: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _validate_dependency_graph(self, context, dependency_graph):
        logger.info("Validating dependency graph")
        try:
            app_ids = self._get_all_app_ids(dependency_graph)
            logger.debug(f"App IDs in dependency graph: {app_ids}")
            for app_id in app_ids:
                app_address = self._make_app_address(app_id)
                logger.debug(f"Checking app_id {app_id} at address {app_address}")
                state_entries = context.get_state([app_address])
                if not state_entries or not state_entries[0].data:
                    logger.error(f"Invalid or non-existent app_id in dependency graph: {app_id}")
                    raise InvalidTransaction(f'Invalid or non-existent app_id in dependency graph: {app_id}')
            logger.info("Dependency graph validated successfully")
        except AuthorizationException as e:
            logger.error(f"Authorization error when validating dependency graph: {str(e)}")
            raise InvalidTransaction(f"Authorization error: {str(e)}")
        except InvalidTransaction:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in _validate_dependency_graph: {str(e)}")
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _get_all_app_ids(self, dependency_graph):
        logger.debug("Extracting all app IDs from dependency graph")
        app_ids = set()
        for node in dependency_graph['nodes']:
            app_ids.add(node)
            app_ids.update(dependency_graph['nodes'][node].get('next', []))
        logger.debug(f"Extracted app IDs: {app_ids}")
        return app_ids

    def _make_workflow_address(self, workflow_id):
        address = NAMESPACE + hashlib.sha512(workflow_id.encode()).hexdigest()[:64]
        logger.debug(f"Generated workflow address: {address} for workflow ID: {workflow_id}")
        return address

    def _make_app_address(self, app_id):
        address = DOCKER_IMAGE_NAMESPACE + hashlib.sha512(app_id.encode()).hexdigest()[:64]
        logger.debug(f"Generated app address: {address} for app ID: {app_id}")
        return address


def main():
    logger.info("Starting Workflow Transaction Processor")
    try:
        processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
        handler = WorkflowTransactionHandler()
        processor.add_handler(handler)
        logger.info("Workflow Transaction Handler added to processor")
        processor.start()
    except KeyboardInterrupt:
        logger.info("Workflow Transaction Processor interrupted. Exiting.")
    except Exception as e:
        logger.error(f"Unexpected error in Workflow Transaction Processor: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("Workflow Transaction Processor stopped")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    main()
