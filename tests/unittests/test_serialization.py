#!/usr/bin/env python3

__author__    = "Radical.Utils Development Team (Andre Merzky)"
__copyright__ = "Copyright 2024, RADICAL@Rutgers"
__license__   = "MIT"

import os

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

    data     = {'foo': {'complex_number': Complex(1, 2)}}
    json_str = ru.to_json(data)
    new_data = ru.from_json(json_str)

    assert data == new_data

    msgpack_str = ru.to_msgpack(data)
    new_data    = ru.from_msgpack(msgpack_str)

    assert data == new_data

    class A(ru.TypedDict):
        _schema = {'s': str,
                   'i': int}

    class B(ru.TypedDict):
        _schema = {'a': A}

    old_data = B(a=A(i=42, s='buz'))
    tmp      = ru.to_json(old_data)
    new_data = ru.from_json(tmp )

    assert old_data == new_data
    assert type(old_data) == type(new_data)
    assert type(old_data['a']) == type(new_data['a'])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_serialization()


# ------------------------------------------------------------------------------

