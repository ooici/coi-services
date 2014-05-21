#!/usr/bin/env python

"""
@package ion.agents.platform.util.test.test_network_util
@file    ion/agents/platform/util/test/test_network_util.py
@author  Carlos Rueda
@brief   Test cases for network_util.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

#
# bin/nosetests -sv ion.agents.platform.util.test.test_network_util:Test.test_create_node_network
# bin/nosetests -sv ion.agents.platform.util.test.test_network_util:Test.test_serialization_deserialization
# bin/nosetests -sv ion.agents.platform.util.test.test_network_util:Test.test_compute_checksum
# bin/nosetests -sv ion.agents.platform.util.test.test_network_util:Test.test_create_network_definition_from_ci_config_bad
# bin/nosetests -sv ion.agents.platform.util.test.test_network_util:Test.test_create_network_definition_from_ci_config
#

from pyon.public import log
import logging

from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.platform.exceptions import PlatformDefinitionException

from pyon.util.containers import DotDict

from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr

import unittest


@attr('UNIT', group='sa')
class Test(IonUnitTestCase):

    def test_create_node_network(self):

        # small valid map:
        plat_map = [('R', ''), ('a', 'R'), ]
        pnodes = NetworkUtil.create_node_network(plat_map)
        for p, q in plat_map: self.assertTrue(p in pnodes and q in pnodes)

        # duplicate 'a' but valid (same parent)
        plat_map = [('R', ''), ('a', 'R'), ('a', 'R')]
        NetworkUtil.create_node_network(plat_map)
        for p, q in plat_map: self.assertTrue(p in pnodes and q in pnodes)

        with self.assertRaises(PlatformDefinitionException):
            # invalid empty map
            plat_map = []
            NetworkUtil.create_node_network(plat_map)

        with self.assertRaises(PlatformDefinitionException):
            # no dummy root (id = '')
            plat_map = [('R', 'x')]
            NetworkUtil.create_node_network(plat_map)

        with self.assertRaises(PlatformDefinitionException):
            # multiple regular roots
            plat_map = [('R1', ''), ('R2', ''), ]
            NetworkUtil.create_node_network(plat_map)

        with self.assertRaises(PlatformDefinitionException):
            # duplicate 'a' but invalid (diff parents)
            plat_map = [('R', ''), ('a', 'R'), ('a', 'x')]
            NetworkUtil.create_node_network(plat_map)

    def test_serialization_deserialization(self):
        # create NetworkDefinition object by de-serializing the simulated network:
        ndef = NetworkUtil.deserialize_network_definition(
                file('ion/agents/platform/rsn/simulator/network.yml'))

        # serialize object to string
        serialization = NetworkUtil.serialize_network_definition(ndef)

        # recreate object by de-serializing the string:
        ndef2 = NetworkUtil.deserialize_network_definition(serialization)

        # verify the objects are equal:
        diff = ndef.diff(ndef2)
        self.assertIsNone(diff, "deserialized version must be equal to original."
                                " DIFF=\n%s" % diff)

    def test_compute_checksum(self):
        # create NetworkDefinition object by de-serializing the simulated network:
        ndef = NetworkUtil.deserialize_network_definition(
                file('ion/agents/platform/rsn/simulator/network.yml'))

        checksum = ndef.compute_checksum()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("NetworkDefinition checksum = %s", checksum)

    #
    # Basic tests regarding conversion from CI agent configuration to a
    # corresponding network definition.
    #

    def test_create_network_definition_from_ci_config_bad(self):

        CFG = DotDict({
            'device_type' : "bad_device_type",
        })

        # device_type
        with self.assertRaises(PlatformDefinitionException):
            NetworkUtil.create_network_definition_from_ci_config(CFG)

        CFG = DotDict({
            'device_type' : "PlatformDevice",
        })

        # missing platform_id
        with self.assertRaises(PlatformDefinitionException):
            NetworkUtil.create_network_definition_from_ci_config(CFG)

        CFG = DotDict({
            'device_type' : "PlatformDevice",

            'platform_config': {
                'platform_id': 'Node1D'
            },
        })

        # missing driver_config
        with self.assertRaises(PlatformDefinitionException):
            NetworkUtil.create_network_definition_from_ci_config(CFG)

    def test_create_network_definition_from_ci_config(self):

        CFG = DotDict({
            'device_type' : "PlatformDevice",

            'platform_config': {
                'platform_id': 'Node1D'
            },

            'driver_config': {'attributes': {'MVPC_pressure_1': {'attr_id': 'MVPC_pressure_1',
                                                                 'group': 'pressure',
                                                                 'max_val': 33.8,
                                                                 'min_val': -3.8,
                                                                 'monitor_cycle_seconds': 10,
                                                                 'precision': 0.04,
                                                                 'read_write': 'read',
                                                                 'type': 'float',
                                                                 'units': 'PSI'},
                                             'MVPC_temperature': {'attr_id': 'MVPC_temperature',
                                                                  'group': 'temperature',
                                                                  'max_val': 58.5,
                                                                  'min_val': -1.5,
                                                                  'monitor_cycle_seconds': 10,
                                                                  'precision': 0.06,
                                                                  'read_write': 'read',
                                                                  'type': 'float',
                                                                  'units': 'Degrees C'},
                                             'input_bus_current': {'attr_id': 'input_bus_current',
                                                                   'group': 'power',
                                                                   'max_val': 50,
                                                                   'min_val': -50,
                                                                   'monitor_cycle_seconds': 5,
                                                                   'precision': 0.1,
                                                                   'read_write': 'write',
                                                                   'type': 'float',
                                                                   'units': 'Amps'},
                                             'input_voltage': {'attr_id': 'input_voltage',
                                                               'group': 'power',
                                                               'max_val': 500,
                                                               'min_val': -500,
                                                               'monitor_cycle_seconds': 5,
                                                               'precision': 1,
                                                               'read_write': 'read',
                                                               'type': 'float',
                                                               'units': 'Volts'}},
                              'dvr_cls': 'RSNPlatformDriver',
                              'dvr_mod': 'ion.agents.platform.rsn.rsn_platform_driver',
                              'oms_uri': 'embsimulator',
                              'ports': {'Node1D_port_1': {'port_id': 'Node1D_port_1'},
                                        'Node1D_port_2': {'port_id': 'Node1D_port_2'}},
                              },


            'children': {'d7877d832cf94c388089b141043d60de': {'agent': {'resource_id': 'd7877d832cf94c388089b141043d60de'},
                                                              'device_type': 'PlatformDevice',
                                                              'platform_config': {'platform_id': 'MJ01C'},
                                                              'driver_config': {'attributes': {'MJ01C_attr_1': {'attr_id': 'MJ01C_attr_1',
                                                                                                                'group': 'power',
                                                                                                                'max_val': 10,
                                                                                                                'min_val': -2,
                                                                                                                'monitor_cycle_seconds': 5,
                                                                                                                'read_write': 'read',
                                                                                                                'type': 'int',
                                                                                                                'units': 'xyz'},
                                                                                               'MJ01C_attr_2': {'attr_id': 'MJ01C_attr_2',
                                                                                                                'group': 'power',
                                                                                                                'max_val': 10,
                                                                                                                'min_val': -2,
                                                                                                                'monitor_cycle_seconds': 5,
                                                                                                                'read_write': 'write',
                                                                                                                'type': 'int',
                                                                                                                'units': 'xyz'}},
                                                                                'dvr_cls': 'RSNPlatformDriver',
                                                                                'dvr_mod': 'ion.agents.platform.rsn.rsn_platform_driver',
                                                                                'oms_uri': 'embsimulator',
                                                                                'ports': {'MJ01C_port_1': {'port_id': 'MJ01C_port_1'},
                                                                                          'MJ01C_port_2': {'port_id': 'MJ01C_port_2'}}},

                                                              'children': {'d0203cb9eb844727b7a8eea77db78e89': {'agent': {'resource_id': 'd0203cb9eb844727b7a8eea77db78e89'},
                                                                                                                'platform_config': {'platform_id': 'LJ01D'},
                                                                                                                'device_type': 'PlatformDevice',
                                                                                                                'driver_config': {'attributes': {'MVPC_pressure_1': {'attr_id': 'MVPC_pressure_1',
                                                                                                                                                                     'group': 'pressure',
                                                                                                                                                                     'max_val': 33.8,
                                                                                                                                                                     'min_val': -3.8,
                                                                                                                                                                     'monitor_cycle_seconds': 10,
                                                                                                                                                                     'precision': 0.04,
                                                                                                                                                                     'read_write': 'read',
                                                                                                                                                                     'type': 'float',
                                                                                                                                                                     'units': 'PSI'},
                                                                                                                                                 'MVPC_temperature': {'attr_id': 'MVPC_temperature',
                                                                                                                                                                      'group': 'temperature',
                                                                                                                                                                      'max_val': 58.5,
                                                                                                                                                                      'min_val': -1.5,
                                                                                                                                                                      'monitor_cycle_seconds': 10,
                                                                                                                                                                      'precision': 0.06,
                                                                                                                                                                      'read_write': 'read',
                                                                                                                                                                      'type': 'float',
                                                                                                                                                                      'units': 'Degrees C'},
                                                                                                                                                 'input_bus_current': {'attr_id': 'input_bus_current',
                                                                                                                                                                       'group': 'power',
                                                                                                                                                                       'max_val': 50,
                                                                                                                                                                       'min_val': -50,
                                                                                                                                                                       'monitor_cycle_seconds': 5,
                                                                                                                                                                       'precision': 0.1,
                                                                                                                                                                       'read_write': 'write',
                                                                                                                                                                       'type': 'float',
                                                                                                                                                                       'units': 'Amps'},
                                                                                                                                                 'input_voltage': {'attr_id': 'input_voltage',
                                                                                                                                                                   'group': 'power',
                                                                                                                                                                   'max_val': 500,
                                                                                                                                                                   'min_val': -500,
                                                                                                                                                                   'monitor_cycle_seconds': 5,
                                                                                                                                                                   'precision': 1,
                                                                                                                                                                   'read_write': 'read',
                                                                                                                                                                   'type': 'float',
                                                                                                                                                                   'units': 'Volts'}},
                                                                                                                                  'dvr_cls': 'RSNPlatformDriver',
                                                                                                                                  'dvr_mod': 'ion.agents.platform.rsn.rsn_platform_driver',
                                                                                                                                  'oms_uri': 'embsimulator',
                                                                                                                                  'ports': {'LJ01D_port_1': {'port_id': '1'},
                                                                                                                                            'LJ01D_port_2': {'port_id': '2'}}},
                                                                                                                'children': {},
                                                                                                                }
                                                              }
                                                             }
            }
        })

        ndef = NetworkUtil.create_network_definition_from_ci_config(CFG)

        if log.isEnabledFor(logging.TRACE):
            serialization = NetworkUtil.serialize_network_definition(ndef)
            log.trace("serialization = \n%s", serialization)

        self.assertIn('Node1D', ndef.pnodes)
        Node1D = ndef.pnodes['Node1D']

        common_attr_names = ['MVPC_pressure_1|0', 'MVPC_temperature|0',
                             'input_bus_current|0',  'input_voltage|0', ]

        for attr_name in common_attr_names:
            self.assertIn(attr_name, Node1D.attrs)

        #todo complete the network definition: align ports defintion with internal representation.
        #for port_name in ['Node1D_port_1', 'Node1D_port_2']:
        #    self.assertIn(port_name, Node1D.ports)

        for subplat_name in ['MJ01C', ]:
            self.assertIn(subplat_name, Node1D.subplatforms)

        MJ01C = Node1D.subplatforms['MJ01C']

        for subplat_name in ['LJ01D', ]:
            self.assertIn(subplat_name, MJ01C.subplatforms)

        LJ01D = MJ01C.subplatforms['LJ01D']

        for attr_name in common_attr_names:
            self.assertIn(attr_name, LJ01D.attrs)

