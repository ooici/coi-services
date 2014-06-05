#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.test.test_oms_util
@file    ion/agents/platform/rsn/test/test_oms_util.py
@author  Carlos Rueda
@brief   Tests for oms_util
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


# NOTE: If this test is run against the embedded simulator (the default),
# make sure to keep the verifications in sync upon any changes in
# ion/agents/platform/rsn/simulator/network.yml, which is the file used by
# the simulator.

from pyon.public import log
import logging

from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.oms_util import RsnOmsUtil
from ion.agents.platform.rsn.simulator.oms_simulator import CIOMSSimulator

from ion.agents.platform.test.helper import HelperTestMixin
from pyon.util.int_test import IonIntegrationTestCase

from nose.plugins.attrib import attr


@attr('INT', group='sa')
class Test(IonIntegrationTestCase, HelperTestMixin):

    @classmethod
    def setUpClass(cls):
        HelperTestMixin.setUpClass()

    def setUp(self):
        # Note that CIOMSClientFactory will create an "embedded" RSN OMS
        # simulator object by default.
        self._rsn_oms = CIOMSClientFactory.create_instance()

    def tearDown(self):
        CIOMSClientFactory.destroy_instance(self._rsn_oms)

    def test_build_network_definition(self):
        ndef = RsnOmsUtil.build_network_definition(self._rsn_oms)

        if log.isEnabledFor(logging.TRACE):
            # serialize object to string
            serialization = NetworkUtil.serialize_network_definition(ndef)
            log.trace("NetworkDefinition serialization:\n%s", serialization)

        if not isinstance(self._rsn_oms, CIOMSSimulator):
            # OK, no more tests if we are not using the embedded simulator
            return

        # Else: do some verifications against network.yml (the spec used by
        # the simulator):

        pnode = ndef.root

        self.assertEqual(pnode.platform_id, "ShoreStation")
        self.assertIn("ShoreStation_attr_1|0", pnode.attrs)
        self.assertIn("ShoreStation_port_1", pnode.ports)

        sub_pnodes = pnode.subplatforms
        self.assertIn("L3-UPS1",       sub_pnodes)
        self.assertIn("Node1A",        sub_pnodes)
        self.assertIn("input_voltage|0", sub_pnodes["Node1A"].attrs)
        self.assertIn("Node1A_port_1", sub_pnodes["Node1A"].ports)
