
from typing import Dict, Any

import msgpack

from ..typeddict import TypedDict


# ------------------------------------------------------------------------------
#
class Message(TypedDict):

    _schema = {
        '_msg_type': str,
    }

    _defaults = {
        '_msg_type': None,
    }

    _msg_types = dict()


    # --------------------------------------------------------------------------
    #
    def _verify(self):
        assert self._msg_type


    @staticmethod
    def register_msg_type(msg_type, msg_class):
        Message._msg_types[msg_type] = msg_class


    @staticmethod
    def deserialize(data: Dict[str, Any]):

        msg_type = data.get('_msg_type')

        if msg_type is None:
            raise ValueError('no message type defined')

        if msg_type not in Message._msg_types:
            known = list(Message._msg_types.keys())
            raise ValueError('unknown message type [%s]: %s' % (msg_type, known))

        return Message._msg_types[msg_type](from_dict=data)


    def packb(self):
        return msgpack.packb(self)

    @staticmethod
    def unpackb(bdata):
        return Message.deserialize(msgpack.unpackb(bdata))


# ------------------------------------------------------------------------------

