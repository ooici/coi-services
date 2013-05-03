#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.test.test_oms_platform_driver
@file    ion/agents/platform/rsn/test/test_rsn_platform_driver.py
@author  Carlos Rueda
@brief   Some basic and direct tests to RSNPlatformDriver.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


# bin/nosetests -sv ion/agents/platform/rsn/test/test_rsn_platform_driver.py

from pyon.public import log
import logging

from pyon.util.containers import get_ion_ts

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.oms_util import RsnOmsUtil
from ion.agents.platform.util.network_util import NetworkUtil

from ion.agents.platform.rsn.rsn_platform_driver import RSNPlatformDriver

from pyon.util.int_test import IonIntegrationTestCase

from nose.plugins.attrib import attr

from gevent import sleep
import os

from ion.agents.platform.test.helper import HelperTestMixin


# see related comments in base_test_platform_agent_with_rsn
oms_uri = os.getenv('OMS', 'launchsimulator')

DVR_CONFIG = {
    'oms_uri': oms_uri  # see setUp for possible update of this entry
}

DVR_CONFIG = {
    'oms_uri': 'launchsimulator',
}


@attr('INT', group='sa')
class TestRsnPlatformDriver(IonIntegrationTestCase, HelperTestMixin):

    @classmethod
    def setUpClass(cls):
        HelperTestMixin.setUpClass()

    def setUp(self):

        DVR_CONFIG['oms_uri'] = self._dispatch_simulator(oms_uri)
        log.debug("DVR_CONFIG['oms_uri'] = %s", DVR_CONFIG['oms_uri'])

        # Use the network definition provided by RSN OMS directly.
        rsn_oms = CIOMSClientFactory.create_instance(DVR_CONFIG['oms_uri'])
        network_definition = RsnOmsUtil.build_network_definition(rsn_oms)
        CIOMSClientFactory.destroy_instance(rsn_oms)

        if log.isEnabledFor(logging.DEBUG):
            network_definition_ser = NetworkUtil.serialize_network_definition(network_definition)
            log.debug("NetworkDefinition serialization:\n%s", network_definition_ser)

        platform_id = self.PLATFORM_ID
        pnode = network_definition.pnodes[platform_id]
        self._plat_driver = RSNPlatformDriver(pnode, self.evt_recv)

    def evt_recv(self, driver_event):
        log.debug('GOT driver_event=%s', str(driver_event))

    def tearDown(self):
        self._plat_driver.destroy()

    def _configure(self):
        self._plat_driver.configure(DVR_CONFIG)

    def _connect(self):
        self._plat_driver.connect()

    def _ping(self):
        result = self._plat_driver.ping()
        self.assertEquals("PONG", result)

    def _get_attribute_values(self):
        attrNames = self.ATTR_NAMES

        # see OOIION-631 note in test_platform_agent_with_rsn
        from_time = str(int(get_ion_ts()) - 50000)  # a 50-sec time window
        req_attrs = [(attr_id, from_time) for attr_id in attrNames]
        attr_values = self._plat_driver.get_attribute_values(req_attrs)
        log.info("attr_values = %s" % str(attr_values))
        self.assertIsInstance(attr_values, dict)
        for attr_name in attrNames:
            self.assertTrue(attr_name in attr_values)

    def test(self):

        self._configure()
        self._connect()
        self._ping()

        self._get_attribute_values()

        log.info("sleeping to eventually see some events...")
        sleep(15)