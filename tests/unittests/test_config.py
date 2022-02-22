#!/usr/bin/env python3

# pylint: disable=protected-access

import os

from unittest import TestCase

import radical.utils as ru


# ------------------------------------------------------------------------------
#
class ConfigTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_config(self):

        base = os.path.abspath(os.path.dirname(__file__))
        cfg_files = '%s/data/resource_*.json' % base

        with self.assertRaises(ValueError):
            ru.Config(path='foo', cfg='bar')

        cfg1 = ru.Config(name=cfg_files)
        self.assertEqual(cfg1.yale._query('grace.agent_launch_method'), 'bar')
        self.assertEqual(cfg1['yale']['grace']['agent_launch_method'],  'bar')
        self.assertIsNone(cfg1._query('yale.grace.no_launch_method'))
        with self.assertRaises(KeyError):
            # key is not set
            _ = cfg1['yale']['grace']['no_launch_method']            # noqa F841

        os.environ['FOO'] = 'GSISSH'
        # `agent_launch_method` is defined by env variable `$FOO`

        cfg2 = ru.Config(name=cfg_files)
        self.assertEqual(cfg2.yale.grace.agent_launch_method, 'GSISSH')

        # env variables are not expanded
        cfg3 = ru.Config(name=cfg_files, expand=False)
        self.assertEqual(cfg3.yale.grace.agent_launch_method, '${FOO:bar}')

        env  = {'FOO': 'baz'}
        cfg4 = ru.Config(name=cfg_files, env=env)
        self.assertEqual(cfg4['yale']['grace']['agent_launch_method'], 'baz')

        # test `cls._self_default` flag
        cfg5 = ru.Config(from_dict={'foo_0': {'foo_1': {'foo2': 'bar'}}})
        self.assertEqual(cfg5.foo_0.foo_1.foo2, 'bar')
        self.assertIsInstance(cfg5.foo_0.foo_1, ru.Config)

    # --------------------------------------------------------------------------
    #
    def test_default_config(self):

        dc1 = ru.DefaultConfig()
        self.assertIn(type(dc1), ru.Singleton._instances)
        self.assertEqual(dc1, ru.Singleton._instances[type(dc1)])

        c = ru.Config(module='radical.utils', category='utils', name='default')
        self.assertEqual(c.as_dict(), dc1.as_dict())

        # do not change other attributes for other tests consistency
        dc1.report_dir = '/tmp'

        dc2 = ru.DefaultConfig()  # check that there is ony one instance
        self.assertEqual(id(dc1), id(dc2))
        self.assertEqual(dc1, dc2)

# ------------------------------------------------------------------------------

