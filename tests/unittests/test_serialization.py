#!/usr/bin/env python3

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2024, RADICAL@Rutgers"
__license__   = "MIT"

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_serialization():

    class Complex(object):

        def __init__(self, real, imag):
            self.real = real
            self.imag = imag

        def __eq__(self, other):
            return self.real == other.real and self.imag == other.imag

        def serialize(self):
            return {'real': self.real, 'imag': self.imag}

        @classmethod
        def deserialize(cls, data):
            return cls(data['real'], data['imag'])


    ru.register_serializable(Complex, encode=Complex.serialize,
                                      decode=Complex.deserialize)

    old = {'foo': {'complex_number': Complex(1, 2)}}
    new = ru.from_json(ru.to_json(old))

    assert old == new

    new = ru.from_msgpack(ru.to_msgpack(old))

    assert old == new


# ------------------------------------------------------------------------------
#
def test_serialization_typed_dict():

    class A(ru.TypedDict):
        _schema = {'s': str, 'i': int}

    class B(ru.TypedDict):
        _schema = {'a': A}

    old = B(a=A(s='buz', i=42))
    new = ru.from_json(ru.to_json(old))

    assert old == new
    assert isinstance(old, B)      and isinstance(new, B)
    assert isinstance(old['a'], A) and isinstance(new['a'], A)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_serialization()
    test_serialization_typed_dict()


# ------------------------------------------------------------------------------

