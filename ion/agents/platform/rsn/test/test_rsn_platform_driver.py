#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.test.test_rsn_platform_driver
@file    ion/agents/platform/rsn/test/test_rsn_platform_driver.py
@author  Carlos Rueda
@brief   Some basic and direct tests to RSNPlatformDriver.
"""

__author__ = 'Carlos Rueda'


# bin/nosetests -sv ion/agents/platform/rsn/test/test_rsn_platform_driver.py

from pyon.util.unit_test import IonUnitTestCase
from pyon.public import log
from pyon.util.containers import get_ion_ts

from ion.agents.platform.test.helper import HelperTestMixin
from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.platform.rsn.rsn_platform_driver import RSNPlatformDriver
from ion.agents.platform.rsn.rsn_platform_driver import RSNPlatformDriverState
from ion.agents.platform.rsn.rsn_platform_driver import RSNPlatformDriverEvent
from ion.agents.platform.responses import NormalResponse

import logging
from nose.plugins.attrib import attr
from mock import Mock


@attr('UNIT', group='sa')
class TestRsnPlatformDriver(IonUnitTestCase, HelperTestMixin):
    """
    Some basic and direct tests to RSNPlatformDriver.
    All operations done through the driver's FSM, so as to also
    verify state transitions.
    """

    @classmethod
    def setUpClass(cls):
        HelperTestMixin.setUpClass()

    def setUp(self):
        self.DVR_CONFIG = {
            'oms_uri':    self._dispatch_simulator('launchsimulator'),
            'attributes': {},
            'ports':      {}
        }
        log.debug("DVR_CONFIG['oms_uri'] = %s", self.DVR_CONFIG['oms_uri'])

        yaml_filename = 'ion/agents/platform/rsn/simulator/network.yml'
        log.debug("retrieving network definition from %s", yaml_filename)
        network_definition = NetworkUtil.deserialize_network_definition(file(yaml_filename))

        if log.isEnabledFor(logging.DEBUG):
            network_definition_ser = NetworkUtil.serialize_network_definition(network_definition)
            log.debug("NetworkDefinition serialization:\n%s", network_definition_ser)

        platform_id = self.PLATFORM_ID
        pnode = network_definition.pnodes[platform_id]

        def evt_recv(driver_event):
            log.debug('GOT driver_event=%s', str(driver_event))

        self._plat_driver = RSNPlatformDriver(pnode, evt_recv, Mock(), Mock())
        res_state = self._plat_driver.get_resource_state()
        self.assertEqual(res_state, RSNPlatformDriverState.UNCONFIGURED)

    def tearDown(self):
        self._plat_driver.destroy()

    ###################
    # auxiliary methods
    ###################

    def _on_event(self, event, *args, **kwargs):
        return self._plat_driver._fsm.on_event(event, *args, **kwargs)

    def _assert_state(self, state):
        res_state = self._plat_driver.get_resource_state()
        self.assertEqual(res_state, state)

    def _ping(self):
        result = self._on_event(RSNPlatformDriverEvent.PING)
        self.assertEquals("PONG", result)

    def _configure(self):
        self._on_event(RSNPlatformDriverEvent.CONFIGURE, driver_config=self.DVR_CONFIG)
        self._assert_state(RSNPlatformDriverState.DISCONNECTED)

    def _connect(self):
        self._on_event(RSNPlatformDriverEvent.CONNECT)
        self._assert_state(RSNPlatformDriverState.CONNECTED)

    def _disconnect(self):
        self._on_event(RSNPlatformDriverEvent.DISCONNECT)
        self._assert_state(RSNPlatformDriverState.DISCONNECTED)

    def _get_attribute_values(self):
        attrNames = self.ATTR_NAMES

        # see OOIION-631 note in test_platform_agent
        from_time = str(int(get_ion_ts()) - 50000)  # a 50-sec time window
        req_attrs = [(attr_id, from_time) for attr_id in attrNames]
        result = self._on_event(RSNPlatformDriverEvent.GET, attrs=req_attrs)
        log.info("_get_attribute_values result = %s", result)
        self.assertIsInstance(result, dict)
        for attr_name in attrNames:
            self.assertTrue(attr_name in result)

    def _get_ports(self):
        result = self._on_event(RSNPlatformDriverEvent.GET, ports=None)
        log.info("_get_ports result = %s", result)
        self.assertIsInstance(result, dict)

    def _turn_on_port(self, port_id=None):
        port_id = port_id or self.PORT_ID
        log.info("turn_on_port: port_id=%r", port_id)
        kwargs = dict(port_id=port_id, src=self.__class__.__name__)
        result = self._on_event(RSNPlatformDriverEvent.TURN_ON_PORT, **kwargs)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertEquals(result[port_id], NormalResponse.PORT_TURNED_ON)

    def _turn_off_port(self, port_id=None):
        port_id = port_id or self.PORT_ID
        log.info("turn_off_port: port_id=%r", port_id)
        kwargs = dict(port_id=port_id, src=self.__class__.__name__)
        result = self._on_event(RSNPlatformDriverEvent.TURN_OFF_PORT, **kwargs)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertEquals(result[port_id], NormalResponse.PORT_TURNED_OFF)

    def _set_over_current(self, port_id=None, ma=0, us=0):
        port_id = port_id or self.PORT_ID
        kwargs = dict(port_id=port_id, ma=ma, us=us, src=self.__class__.__name__)
        result = self._on_event(RSNPlatformDriverEvent.SET_OVER_CURRENT, **kwargs)
        log.info("set_over_current = %s", result)
        self.assertIsInstance(result, dict)

    ###################
    # tests
    ###################

    def test(self):
        self._configure()
        self._connect()
        self._ping()
        self._get_attribute_values()
        self._get_ports()
        self._turn_on_port()
        self._turn_off_port()
        self._set_over_current()
        self._disconnect()
