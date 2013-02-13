#!/usr/bin/env python

"""
@package ion.agents.platform.util.test.test_network_util
@file    ion/agents/platform/util/test/test_network_util.py
@author  Carlos Rueda
@brief   Test cases for network_util.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from pyon.public import log
import logging

from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.platform.exceptions import PlatformDefinitionException

from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr


@attr('UNIT', group='sa')
class Test(IonUnitTestCase):

    def test_create_node_network(self):

        # small valid map:
        plat_map = [('R', ''), ('a', 'R'), ]
        nodes = NetworkUtil.create_node_network(plat_map)
        for p, q in plat_map: self.assertTrue(p in nodes and q in nodes)

        # duplicate 'a' but valid (same parent)
        plat_map = [('R', ''), ('a', 'R'), ('a', 'R')]
        NetworkUtil.create_node_network(plat_map)
        for p, q in plat_map: self.assertTrue(p in nodes and q in nodes)

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
                file('ion/agents/platform/oms/simulator/network.yml'))

        # serialize object to string
        serialization = NetworkUtil.serialize_network_definition(ndef)

        # recreate object by de-serializing the string:
        ndef2 = NetworkUtil.deserialize_network_definition(serialization)

        # verify the objects are equal:
        self.assertEquals(ndef.diff(ndef2), None)

    def test_compute_checksum(self):
        # create NetworkDefinition object by de-serializing the simulated network:
        ndef = NetworkUtil.deserialize_network_definition(
                file('ion/agents/platform/oms/simulator/network.yml'))

        checksum = ndef.compute_checksum()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("NetworkDefinition checksum = %s", checksum)
