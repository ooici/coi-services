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

from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr


@attr('UNIT', group='sa')
class Test(IonUnitTestCase):

    def test_serialization_deserialization(self):
        # create NetworkDefinition object by de-serializing the simulated network:
        ndef = NetworkUtil.deserialize_network_definition(
                file('ion/agents/platform/oms/simulator/network.yml'))

        # serialize object to string
        serialization = NetworkUtil.serialize_network_definition(ndef)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("NetworkDefinition serialization:\n%s", serialization)

        # recreate object by de-serializing the string:
        ndef2 = NetworkUtil.deserialize_network_definition(serialization)

        # verify the objects are equal:
        self.assertEquals(ndef.diff(ndef2), None)
