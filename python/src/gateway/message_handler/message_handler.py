import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().hex
        self.data_count = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.data_count += 1
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id, self.data_count])

    def deserialize_result_message(self, message):
        [client_id, fields] = message_protocol.internal.deserialize(message)
        if client_id != self.client_id:
            return None
        return fields
