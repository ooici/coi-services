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

        self.assertTrue("UPS" in ndef.platform_types)

        pnode = ndef.root

        self.assertEqual(pnode.platform_id, "ShoreStation")
        self.assertIn("ShoreStation_attr_1|0", pnode.attrs)
        self.assertIn("ShoreStation_port_1", pnode.ports)

        sub_pnodes = pnode.subplatforms
        self.assertIn("L3-UPS1",       sub_pnodes)
        self.assertIn("Node1A",        sub_pnodes)
        self.assertIn("input_voltage|0", sub_pnodes["Node1A"].attrs)
        self.assertIn("Node1A_port_1", sub_pnodes["Node1A"].ports)

    def _get_checksum(self, platform_id):
        # get checksum from RSN OMS:
        res = self._rsn_oms.config.get_checksum(platform_id)
        checksum = res[platform_id]
        if log.isEnabledFor(logging.DEBUG):
            log.debug("_rsn_oms: checksum: %s", checksum)
        return checksum

    def _connect_instrument(self):
        platform_id = self.PLATFORM_ID
        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID
        attributes = {'maxCurrentDraw': 1, 'initCurrent': 2,
                      'dataThroughput': 3, 'instrumentType': 'FOO'}
        retval = self._rsn_oms.instr.connect_instrument(platform_id, port_id,
                                                        instrument_id, attributes)
        log.info("connect_instrument = %s" % retval)
        ports = self._verify_valid_platform_id(platform_id, retval)
        self._verify_valid_port_id(port_id, ports)

    def test_checksum(self):
        platform_id = self.PLATFORM_ID

        checksum = self._get_checksum(platform_id)

        # build network definition using RSN OMS and get checksum for the
        # corresponding PlatformNode, and verify that the checksums match:
        ndef = RsnOmsUtil.build_network_definition(self._rsn_oms)
        pnode = ndef.pnodes[platform_id]
        self.assertEquals(pnode.compute_checksum(), checksum)

        # ok; now connect an instrument and verify the new checksums again:
        self._connect_instrument()

        checksum = self._get_checksum(platform_id)
        ndef = RsnOmsUtil.build_network_definition(self._rsn_oms)
        pnode = ndef.pnodes[platform_id]
        self.assertEquals(pnode.compute_checksum(), checksum)
