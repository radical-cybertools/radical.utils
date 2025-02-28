
import json
import msgpack

from .typeddict import as_dict, TypedDict

# ------------------------------------------------------------------------------
#
class _ClassType:

    def __init__(self, ctype, encode, decode):

        self.ctype : type     = ctype
        self.encode: callable = encode
        self.decode: callable = decode


_ctypes = dict()


# ------------------------------------------------------------------------------
#
def register_serializable(cls, encode=None, decode=None):
    '''
    register a class for json and msgpack serialization / deserialization.

    Args:
        cls    (type):     class type to register
        encode (callable): converts class instance into encodable data structure
        decode (callable): recreates the class instance from that data structure
    '''

    if encode is None: encode = cls
    if decode is None: decode = cls

    _ctypes[cls.__name__] = _ClassType(cls, encode, decode)

register_serializable(TypedDict)


# ------------------------------------------------------------------------------
#
class _json_encoder(json.JSONEncoder):
    '''
    internal methods to encode registered classes to json
    '''

    def encode(self, o, *args, **kw):
        tmp = as_dict(o, _annotate=True)
        return super().encode(tmp, *args, **kw)

    def default(self, o):
        print('== encode: %s' % o)
        for cname,methods in _ctypes.items():
            if isinstance(o, methods.ctype):
                return {'_type': cname,
                        'as_str': methods.encode(o)}
        return super().default(o)


# ------------------------------------------------------------------------------
#
def _json_decoder(obj):
    '''
    internal methods to decode registered classes from json
    '''
    otype = obj.get('_type')
    if not otype:
        return obj

    methods = _ctypes.get(otype)
    if not methods:
        return obj

    del obj['_type']
    if 'as_str' in obj:
        return methods.decode(obj['as_str'])

    return methods.decode(obj)


# ------------------------------------------------------------------------------
#
def _msgpack_encoder(obj):
    '''
    internal methods to encode registered classes to msgpack
    '''
    for cname,methods in _ctypes.items():
        if isinstance(obj, methods.ctype):
            return {'__%s__' % cname: True, 'as_str': methods.encode(obj)}
    return obj


# ------------------------------------------------------------------------------
#
def _msgpack_decoder(obj):
    '''
    internal methods to decode registered classes from msgpack
    '''
    for cname,methods in _ctypes.items():
        if '__%s__' % cname in obj:
            return methods.decode(obj['as_str'])
    return obj


# ------------------------------------------------------------------------------
#
def to_json(data):
    '''
    convert data to json, using registered classes for serialization

    Args:
        data (object): data to be serialized

    Returns:
        str: json serialized data
    '''
    return json.dumps(data, sort_keys=True, indent=4, ensure_ascii=False,
                            cls=_json_encoder)


# ------------------------------------------------------------------------------
#
def from_json(data):
    '''
    convert json data to python data structures, using registered classes for
    deserialization

    Args:
        data (str): json data to be deserialized

    Returns:
        object: deserialized data
    '''
    return json.loads(data, object_hook=_json_decoder)


# ------------------------------------------------------------------------------
#
def to_msgpack(data):
    '''
    convert data to msgpack, using registered classes for serialization

    Args:
        data (object): data to be serialized

    Returns:
        bytes: msgpack serialized data
    '''
    return msgpack.packb(data, default=_msgpack_encoder, use_bin_type=True)


# ------------------------------------------------------------------------------
#
def from_msgpack(data):
    '''
    convert msgpack data to python data structures, using registered classes for
    deserialization

    Args:
        data (bytes): msgpack data to be deserialized

    Returns:
        object: deserialized data
    '''
    return msgpack.unpackb(data, object_hook=_msgpack_decoder, raw=False)


# ------------------------------------------------------------------------------

