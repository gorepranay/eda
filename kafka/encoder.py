import base64

class EncoDeco:
    encoding_type='utf-8'

    def __init__(self):
        pass

    @classmethod
    def encode(cls, msg):
        msg_bytes = msg.encode(cls.encoding_type)
        base64_bytes = base64.b64encode(msg_bytes)
        return base64_bytes

    @classmethod
    def decode(cls, base64_msg):
        msg_bytes = base64.b64decode(base64_msg)
        msg = msg_bytes.decode(cls.encoding_type)
        return msg
