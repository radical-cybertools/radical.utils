
import copy
import json
import msgpack


class _CType:

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

    global _ctypes
    _ctypes[cls.__name__] = _CType(cls, encode, decode)


# ------------------------------------------------------------------------------
#

class _json_encoder(json.JSONEncoder):
    '''
    internal methods to encode registered classes to json
    '''

    def encode(self, o, *args, **kw):

        from .typeddict import TypedDict
        if isinstance(o, TypedDict):
          # print('TypedDict: %s' % type(o).__name__)
            o = copy.deepcopy(o)
            o['_xtype'] = type(o).__name__
        return super().encode(o, *args, **kw)

    def default(self, obj):
      # print('encode: %s' % obj)
        for cname,methods in _ctypes.items():
            if isinstance(obj, methods.ctype):
                return {'_xtype': cname,
                        'as_str': methods.encode(obj)}
        return super().default(obj)

# ------------------------------------------------------------------------------
#
def _json_decoder(obj):
    '''
    internal methods to decode registered classes from json
    '''
  # print('decode: %s' % obj)
    for cname, methods in _ctypes.items():
      # print('check %s' % cname)
        if '_xtype' in obj and obj['_xtype'] == cname:
            del obj['_xtype']
          # print('found %s' % cname)
            if 'as_str' in obj:
                return methods.decode(obj['as_str'])
            return methods.decode(obj)
    return obj


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

