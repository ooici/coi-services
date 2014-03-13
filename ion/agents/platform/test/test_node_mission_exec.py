#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_node_mission_exec
@file    ion/agents/platform/test/test_node_mission_exec.py
@author  Edward Hunter
@brief   Test cases for RSN node missions.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_node_mission_exec.py:TestNodeMissionExec
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_node_mission_exec.py:TestNodeMissionExec.test_resource_monitoring
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_node_mission_exec.py:TestNodeMissionExec.test_some_commands

# Import base test class first.
from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

# Standard library imports.
import unittest
import os

# Pyon imports.
from pyon.public import log, CFG
from pyon.util.containers import get_ion_ts
from pyon.core.exception import Conflict
from pyon.event.event import EventSubscriber
from pyon.public import IonObject
from pyon.util.containers import current_time_millis

# Ion imports.
from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent
from ion.agents.platform.responses import NormalResponse
from ion.agents.platform.rsn.rsn_platform_driver import RSNPlatformDriverState
from ion.agents.platform.rsn.rsn_platform_driver import RSNPlatformDriverEvent
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.agents.platform.util import ntp_2_ion_ts

# Interface imports.
from interface.objects import AgentCommand
from interface.objects import CapabilityType
from interface.objects import AgentCapability
from interface.objects import StreamAlertType, AggregateStatusType

# Gevent imports.
from gevent import sleep
from gevent.event import AsyncResult

# Mock imports.
from mock import patch

@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestNodeMissionExec(BaseIntTestPlatform):


    def _create_network_and_start_root_platform(self, clean_up=None):
        """
        Call this at the beginning of each test. We need to make sure that
        the patched timeout is in effect for the actions performed here.

        @note this used to be done in setUp, but the patch.dict mechanism does
        *not* take effect in setUp!

        An addCleanup function is added to reset/shutdown the network and stop the
        root platform. Should avoid leaked processes/greenlet upon failing tests
        (except perhaps if they happen during the launch of the root platform).

        @param clean_up    Not None to override default pre-cleanUp calls.
        """
        self._set_receive_timeout()

        # NOTE The tests expect to use values set up by HelperTestMixin for
        # the following networks (see ion/agents/platform/test/helper.py)
        self.p_root = self._create_single_platform()
        self._start_platform(self.p_root)

        def done():
            if self.p_root:
                try:
                    if clean_up:
                        clean_up()
                    else:
                        # default "done" sequence for most tests
                        try:
                            self._go_inactive()
                            self._reset()
                        finally:  # attempt shutdown anyway
                            self._shutdown()
                finally:
                    self._stop_platform(self.p_root)
                    self.p_root = None
        self.addCleanup(done)


    def _connect_instrument(self):
        #
        # TODO more realistic settings for the connection
        #
        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID
        instrument_attributes = self.INSTRUMENT_ATTRIBUTES_AND_VALUES

        kwargs = dict(
            port_id = port_id,
            instrument_id = instrument_id,
            attributes = instrument_attributes
        )
        result = self._execute_resource(RSNPlatformDriverEvent.CONNECT_INSTRUMENT, **kwargs)
        log.info("CONNECT_INSTRUMENT = %s", result)
        self.assertIsInstance(result, dict)
        self.assertIn(port_id, result)
        self.assertIsInstance(result[port_id], dict)
        returned_attrs = self._verify_valid_instrument_id(instrument_id, result[port_id])
        if isinstance(returned_attrs, dict):
            for attrName in instrument_attributes:
                self.assertIn(attrName, returned_attrs)


    def _disconnect_instrument(self):
        # TODO real settings and corresp verification

        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID

        kwargs = dict(
            port_id = port_id,
            instrument_id = instrument_id
        )
        result = self._execute_resource(RSNPlatformDriverEvent.DISCONNECT_INSTRUMENT, **kwargs)
        log.info("DISCONNECT_INSTRUMENT = %s", result)
        self.assertIsInstance(result, dict)
        self.assertIn(port_id, result)
        self.assertIsInstance(result[port_id], dict)
        self.assertIn(instrument_id, result[port_id])
        self._verify_instrument_disconnected(instrument_id, result[port_id][instrument_id])


    def _turn_on_port(self):
        # TODO real settings and corresp verification

        port_id = self.PORT_ID

        kwargs = dict(
            port_id = port_id
        )
        result = self._execute_resource(RSNPlatformDriverEvent.TURN_ON_PORT, **kwargs)
        log.info("TURN_ON_PORT = %s", result)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertEquals(result[port_id], NormalResponse.PORT_TURNED_ON)


    def _turn_off_port(self):
        # TODO real settings and corresp verification

        port_id = self.PORT_ID

        kwargs = dict(
            port_id = port_id
        )
        result = self._execute_resource(RSNPlatformDriverEvent.TURN_OFF_PORT, **kwargs)
        log.info("TURN_OFF_PORT = %s", result)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertEquals(result[port_id], NormalResponse.PORT_TURNED_OFF)


    def _get_resource(self):
        """
        Gets platform attribute values/
        """
        attrNames = self.ATTR_NAMES
        #
        # OOIION-631: use get_ion_ts() as a basis for using system time, which is
        # a string.
        #
        cur_time = get_ion_ts()
        from_time = str(int(cur_time) - 50000)  # a 50-sec time window
        attrs = [(attr_id, from_time) for attr_id in attrNames]
        kwargs = dict(attrs=attrs)
        cmd = AgentCommand(command=PlatformAgentEvent.GET_RESOURCE, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        attr_values = retval.result
        self.assertIsInstance(attr_values, dict)
        for attr_name in attrNames:
            self._verify_valid_attribute_id(attr_name, attr_values)


    def _set_resource(self):
        attrNames = self.ATTR_NAMES
        writ_attrNames = self.WRITABLE_ATTR_NAMES

        # do valid settings:

        # TODO more realistic value depending on attribute's type
        attrs = [(attrName, self.VALID_ATTR_VALUE) for attrName in attrNames]
        log.info("%r: setting attributes=%s", self.PLATFORM_ID, attrs)
        kwargs = dict(attrs=attrs)
        cmd = AgentCommand(command=PlatformAgentEvent.SET_RESOURCE, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        attr_values = retval.result
        self.assertIsInstance(attr_values, dict)
        for attrName in attrNames:
            if attrName in writ_attrNames:
                self._verify_valid_attribute_id(attrName, attr_values)
            else:
                self._verify_not_writable_attribute_id(attrName, attr_values)

        # try invalid settings:

        # set invalid values to writable attributes:
        attrs = [(attrName, self.INVALID_ATTR_VALUE) for attrName in writ_attrNames]
        log.info("%r: setting attributes=%s", self.PLATFORM_ID, attrs)
        kwargs = dict(attrs=attrs)
        cmd = AgentCommand(command=PlatformAgentEvent.SET_RESOURCE, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        attr_values = retval.result
        self.assertIsInstance(attr_values, dict)
        for attrName in writ_attrNames:
            self._verify_attribute_value_out_of_range(attrName, attr_values)


    def _get_subplatform_ids(self):
        kwargs = dict(subplatform_ids=None)
        cmd = AgentCommand(command=PlatformAgentEvent.GET_RESOURCE, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        subplatform_ids = retval.result
        self.assertIsInstance(subplatform_ids, (list, tuple))
        return subplatform_ids


    def test_some_commands(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._ping_agent()
        self._ping_resource()

        self._get_metadata()
        self._get_subplatform_ids()

        ports = self._get_ports()
        for port_id in ports:
            self._get_connected_instruments(port_id)


    def test_resource_monitoring(self):
        #
        # Basic test for resource monitoring: starts monitoring, waits for
        # a sample to be published, and stops resource monitoring.
        #
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._start_resource_monitoring()
        try:
            self._wait_for_a_data_sample()
        finally:
            self._stop_resource_monitoring()
