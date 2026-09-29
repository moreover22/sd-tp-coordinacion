from common import message_protocol
import logging
import uuid

logger = logging.getLogger("root")

class MessageHandler:

    def __init__(self):
        self._client_id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        logger.info(f"Serializing data message with client ID: {self._client_id}")
        logger.info(f"Message to serialize: {message}")
        [fruit, amount] = message
        return message_protocol.internal.serialize([self._client_id, fruit, amount])

    def serialize_eof_message(self, message):
        logger.info(f"Serializing EOF message with client ID: {self._client_id}")
        logger.info(f"Message to serialize: {message}")
        return message_protocol.internal.serialize([self._client_id])

    def deserialize_result_message(self, message):
        client_id, *fields = message_protocol.internal.deserialize(message)
        if client_id != self._client_id:
            logger.debug(f"Received message for a different client ID: {client_id}. Expected: {self._client_id}")
            return None
        logger.info(f"Deserialized result message: {fields}")
        return fields
