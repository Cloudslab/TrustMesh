import asyncio
import hashlib
import json
import logging
import os
import traceback

from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from sawtooth_sdk.processor.core import TransactionProcessor

# Sawtooth configuration
FAMILY_NAME = 'schedule-confirmation'
FAMILY_VERSION = '1.0'
SCHEDULE_NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

logger = logging.getLogger(__name__)


class IoTScheduleTransactionHandler(TransactionHandler):
    def __init__(self):
        self.loop = asyncio.get_event_loop()

    @property
    def family_name(self):
        return FAMILY_NAME

    @property
    def family_versions(self):
        return [FAMILY_VERSION]

    @property
    def namespaces(self):
        return [SCHEDULE_NAMESPACE]

    def apply(self, transaction, context):
        try:
            payload = json.loads(transaction.payload.decode())

            self._handle_schedule(payload, context)

        except json.JSONDecodeError as e:
            raise InvalidTransaction("Invalid payload: not a valid JSON")
        except KeyError as e:
            raise InvalidTransaction(f"Invalid payload: missing {str(e)}")
        except Exception as e:
            logger.error(traceback.format_exc())
            raise InvalidTransaction(str(e))

    def _handle_schedule(self, payload, context):
        workflow_id = payload['workflow_id']
        schedule_id = payload['schedule_id']
        schedule = payload['schedule']
        schedule_proposer = payload['schedule_proposer']
        source_url, source_public_key = payload['source_url'], payload['source_public_key']

        logger.info(f"Workflow ID: {workflow_id}, Schedule ID: {schedule_id}, Schedule Proposer: {schedule_proposer}")

        logger.info("Confirming schedule")

        schedule_address = self._make_schedule_address(schedule_id)
        schedule_data = {
            'workflow_id': workflow_id,
            'schedule_id': schedule_id,
            'schedule': schedule,
            'schedule_proposer': schedule_proposer
        }

        logger.info(f"Setting state for schedule {schedule_id}")
        context.set_state({
            schedule_address: json.dumps(schedule_data).encode()
        })

        logger.info(f"Schedule {schedule_id} proposed by node {schedule_proposer} is confirmed")

        # Convert schedule dict to string for event attribute
        schedule_str = json.dumps(schedule)

        # Emit an event
        context.add_event(
            event_type="schedule-confirmation",
            attributes=[("workflow_id", workflow_id),
                        ("schedule_id", schedule_id),
                        ("schedule", schedule_str),
                        ("source_url", source_url),
                        ("source_public_key", source_public_key),
                        ("schedule_proposer", schedule_proposer)]
        )
        logger.info(f"Emitted event for schedule confirmation. workflow_id: {workflow_id}, "
                    f"schedule_id: {schedule_id}, schedule_proposer: {schedule_proposer}")

    @staticmethod
    def _make_schedule_address(schedule_id):
        return SCHEDULE_NAMESPACE + hashlib.sha512((schedule_id + "_confirmation").encode()).hexdigest()[:64]


def main():
    logger.info("Starting IoT Schedule Transaction Processor")
    processor = TransactionProcessor(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))
    handler = IoTScheduleTransactionHandler()
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
        processor.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
