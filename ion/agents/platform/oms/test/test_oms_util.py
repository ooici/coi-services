#!/usr/bin/env python

"""
@package ion.agents.platform.oms.test.test_oms_util
@file    ion/agents/platform/oms/test/test_oms_util.py
@author  Carlos Rueda
@brief   Tests for oms_util
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


# NOTE: If this test is run against the embedded simulator (the default),
# make sure to keep the verifications in sync upon any changes in
# ion/agents/platform/oms/simulator/network.yml, which is the file used by
# the simulator.

from pyon.public import log
import logging

from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.platform.oms.oms_client_factory import OmsClientFactory
from ion.agents.platform.oms.oms_util import RsnOmsUtil
from ion.agents.platform.oms.simulator.oms_simulator import OmsSimulator

from pyon.util.int_test import IonIntegrationTestCase

from nose.plugins.attrib import attr


@attr('INT', group='sa')
class Test(IonIntegrationTestCase):

    def setUp(self):
        # Note that OmsClientFactory will create an "embedded" RSN OMS
        # simulator object by default.
        self._rsn_oms = OmsClientFactory.create_instance()

    def test_build_network_definition(self):
        ndef = RsnOmsUtil.build_network_definition(self._rsn_oms)

        if log.isEnabledFor(logging.DEBUG):
            # serialize object to string
            serialization = NetworkUtil.serialize_network_definition(ndef)
            log.debug("NetworkDefinition serialization:\n%s", serialization)

        if not isinstance(self._rsn_oms, OmsSimulator):
            # OK, no more tests if we are not using the embedded simulator
            return

        # Else: do some verifications against network.yml (the spec used by
        # the simulator):

        self.assertTrue("UPS" in ndef.platform_types)

        nnode = ndef.root

        self.assertEqual(nnode.platform_id, "ShoreStation")
        self.assertTrue("ShoreStation_attr_1" in nnode.attrs)
        self.assertTrue("ShoreStation_port_1" in nnode.ports)

        sub_nnodes = nnode.subplatforms
        self.assertTrue("L3-UPS1" in sub_nnodes)
        self.assertTrue("Node1A" in sub_nnodes)
        self.assertTrue("input_voltage" in sub_nnodes["Node1A"].attrs)
        self.assertTrue("Node1A_port_1" in sub_nnodes["Node1A"].ports)


