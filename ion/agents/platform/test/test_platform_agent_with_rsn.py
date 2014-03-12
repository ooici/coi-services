#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_platform_agent_with_rsn
@file    ion/agents/platform/test/test_platform_agent_with_rsn.py
@author  Carlos Rueda
@brief   Test cases for platform agent interacting with RSN
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

# The following can be prefixed with PLAT_NETWORK=single to exercise the tests
# with a single platform (with no sub-platforms). Otherwise a small network is
# used. See HelperTestMixin.
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_resource_monitoring
# bin/nosetests -sv --nologcapture ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_capabilities
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_some_state_transitions
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_get_set_resources
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_some_commands
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_resource_monitoring
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_resource_monitoring_recent
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_external_event_dispatch
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_connect_disconnect_instrument
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_check_sync
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_execute_resource
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_resource_states
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_lost_connection_and_reconnect
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_alerts
#


from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform
from pyon.public import log, CFG

from pyon.util.containers import get_ion_ts

from interface.objects import AgentCommand
from interface.objects import CapabilityType
from interface.objects import AgentCapability

from interface.objects import StreamAlertType, AggregateStatusType

from pyon.core.exception import Conflict

from pyon.event.event import EventSubscriber

from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent
from ion.agents.platform.responses import NormalResponse
from ion.agents.platform.rsn.rsn_platform_driver import RSNPlatformDriverState
from ion.agents.platform.rsn.rsn_platform_driver import RSNPlatformDriverEvent

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.public import IonObject
from pyon.util.containers import current_time_millis
from ion.agents.platform.util import ntp_2_ion_ts

from gevent import sleep
from gevent.event import AsyncResult
from mock import patch
from pyon.public import CFG
import unittest
import os

@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestPlatformAgent(BaseIntTestPlatform):

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

        self.p_root = None

        # NOTE The tests expect to use values set up by HelperTestMixin for
        # the following networks (see ion/agents/platform/test/helper.py)
        if self.PLATFORM_ID == 'Node1D':
            #self.p_root = self._create_small_hierarchy()
            instr_keys = ["SBE37_SIM_01", ]
            self.p_root = self._set_up_small_hierarchy_with_some_instruments(instr_keys)

        elif self.PLATFORM_ID == 'LJ01D':
            self.p_root = self._create_single_platform()

        else:
            self.fail("self.PLATFORM_ID expected to be one of: 'Node1D', 'LJ01D'")

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

    def test_capabilities(self):
        self._create_network_and_start_root_platform()

        agt_cmds_all = [
            PlatformAgentEvent.INITIALIZE,
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,
            PlatformAgentEvent.GO_ACTIVE,
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RUN,

            PlatformAgentEvent.CLEAR,
            PlatformAgentEvent.PAUSE,
            PlatformAgentEvent.RESUME,
            #PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            #PlatformAgentEvent.PING_RESOURCE,
            #PlatformAgentEvent.GET_RESOURCE,
            #PlatformAgentEvent.SET_RESOURCE,
            #PlatformAgentEvent.EXECUTE_RESOURCE,
            #PlatformAgentEvent.GET_RESOURCE_STATE,

            PlatformAgentEvent.START_MONITORING,
            PlatformAgentEvent.STOP_MONITORING,

            PlatformAgentEvent.RUN_MISSION,
            PlatformAgentEvent.ABORT_MISSION,
            PlatformAgentEvent.KILL_MISSION,
        ]

        def sort_caps(caps_list):
            agt_cmds = []
            agt_pars = []
            res_cmds = []
            res_iface = []
            res_pars = []

            if len(caps_list)>0 and isinstance(caps_list[0], AgentCapability):
                agt_cmds = [x.name for x in caps_list if x.cap_type==CapabilityType.AGT_CMD]
                agt_pars = [x.name for x in caps_list if x.cap_type==CapabilityType.AGT_PAR]
                res_cmds = [x.name for x in caps_list if x.cap_type==CapabilityType.RES_CMD]
                res_iface = [x.name for x in caps_list if x.cap_type==CapabilityType.RES_IFACE]
                res_pars = [x.name for x in caps_list if x.cap_type==CapabilityType.RES_PAR]

            elif len(caps_list)>0 and isinstance(caps_list[0], dict):
                agt_cmds = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.AGT_CMD]
                agt_pars = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.AGT_PAR]
                res_cmds = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.RES_CMD]
                res_iface = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.RES_IFACE]
                res_pars = [x['name'] for x in caps_list if x['cap_type']==CapabilityType.RES_PAR]

            state = self._pa_client.get_agent_state()
            log.debug("sort_caps: in agent state=%s\n"
                      "agt_cmds  => %s\n"
                      "agt_pars  => %s\n"
                      "res_cmds  => %s\n"
                      "res_iface => %s\n"
                      "res_pars  => %s\n",
                      state, agt_cmds, agt_pars, res_cmds, res_iface, res_pars)

            return agt_cmds, agt_pars, res_cmds, res_iface, res_pars

        def verify_schema(caps_list):

            dd_list = ['display_name','description']
            ddt_list = ['display_name','description','type']
            ddvt_list = ['display_name','description','visibility','type']
            ddak_list = ['display_name','description','args','kwargs']
            kkvt_res_list = ['display_name', 'description', 'visibility',
                             'type, monitor_cycle_seconds', 'precision',
                             'min_val', 'max_val', 'units', 'group']
            stream_list = ['tdb', 'tdbtdb']

            for x in caps_list:
                if isinstance(x,dict):
                    x.pop('type_')
                    x = IonObject('AgentCapability', **x)

                try:
                    if x.cap_type == CapabilityType.AGT_CMD:
                        if x['name'] == 'example':
                            pass
                        keys = x.schema.keys()
                        for y in ddak_list:
                            self.assertIn(y, keys)

                    elif x.cap_type == CapabilityType.AGT_PAR:
                            if x.name != 'example':
                                keys = x.schema.keys()
                                for y in ddvt_list:
                                    self.assertIn(y, keys)

                    elif x.cap_type == CapabilityType.RES_CMD:
                        keys = x.schema.keys()
                        for y in ddak_list:
                            self.assertIn(y, keys)

                    elif x.cap_type == CapabilityType.RES_IFACE:
                        pass

                    elif x.cap_type == CapabilityType.RES_PAR:
                        pass
                        #keys = x.schema.keys()
                        #for y in kkvt_res_list:
                        #    self.assertIn(y, keys)

                    elif x.cap_type == CapabilityType.AGT_STATES:
                        for (k,v) in x.schema.iteritems():
                            keys = v.keys()
                            for y in dd_list:
                                self.assertIn(y, keys)

                    elif x.cap_type == CapabilityType.ALERT_DEFS:
                        for (k,v) in x.schema.iteritems():
                            keys = v.keys()
                            for y in ddt_list:
                                self.assertIn(y, keys)

                    elif x.cap_type == CapabilityType.AGT_CMD_ARGS:
                        pass
                        """
                        for (k,v) in x.schema.iteritems():
                            keys = v.keys()
                            for y in ddt_list:
                                self.assertIn(y, keys)
                        """

                    elif x.cap_type == CapabilityType.AGT_STREAMS:
                        pass
                        #keys = x.schema.keys()
                        #for y in stream_list:
                        #    self.assertIn(y, keys)

                except Exception:
                    print '### ERROR verifying schema for'
                    print x['name']
                    raise

        agt_pars_all = [
            'example',
            'child_agg_status',
            'alerts',
            'aggstatus',
            'rollup_status',
        ]
        res_pars_all = []
        res_cmds_all = [
            RSNPlatformDriverEvent.CONNECT_INSTRUMENT,
            RSNPlatformDriverEvent.DISCONNECT_INSTRUMENT,
            RSNPlatformDriverEvent.TURN_ON_PORT,
            RSNPlatformDriverEvent.TURN_OFF_PORT,
#            RSNPlatformDriverEvent.CHECK_SYNC           #OOIION-1623 Remove until Check Sync requirements fully defined
        ]

        ##################################################################
        # UNINITIALIZED
        ##################################################################

        self._assert_state(PlatformAgentState.UNINITIALIZED)

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

        # Validate capabilities for state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        agt_cmds_uninitialized = [
            PlatformAgentEvent.INITIALIZE,
            PlatformAgentEvent.SHUTDOWN,
        ]
        self.assertItemsEqual(agt_cmds, agt_cmds_uninitialized)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states.
        retval = self._pa_client.get_capabilities(current_state=False)

        # Validate all capabilities as read from state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        #self.assertItemsEqual(res_pars, [])

        verify_schema(retval)

        ##################################################################
        # INACTIVE
        ##################################################################
        self._initialize()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

        # Validate capabilities for state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        agt_cmds_inactive = [
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,
            PlatformAgentEvent.GO_ACTIVE,
            #PlatformAgentEvent.PING_RESOURCE,
            #PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            #PlatformAgentEvent.GET_RESOURCE_STATE,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_inactive)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        #self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states.
        retval = self._pa_client.get_capabilities(False)

         # Validate all capabilities as read from state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        #self.assertItemsEqual(res_pars, [])

        verify_schema(retval)

        print '############### resource params'
        for x in res_pars:
            print str(x)

        ##################################################################
        # IDLE
        ##################################################################
        self._go_active()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities for state IDLE.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        agt_cmds_idle = [
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RUN,
            #PlatformAgentEvent.PING_RESOURCE,
            #PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            #PlatformAgentEvent.GET_RESOURCE_STATE,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_idle)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        #self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states as read from IDLE.
        retval = self._pa_client.get_capabilities(False)

         # Validate all capabilities as read from state IDLE.
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        #self.assertItemsEqual(res_pars, [])

        verify_schema(retval)

        ##################################################################
        # COMMAND
        ##################################################################
        self._run()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state COMMAND
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        agt_cmds_command = [
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,
            PlatformAgentEvent.PAUSE,
            PlatformAgentEvent.CLEAR,

            #PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            #PlatformAgentEvent.PING_RESOURCE,
            #PlatformAgentEvent.GET_RESOURCE,
            #PlatformAgentEvent.SET_RESOURCE,
            #PlatformAgentEvent.EXECUTE_RESOURCE,
            #PlatformAgentEvent.GET_RESOURCE_STATE,

            PlatformAgentEvent.START_MONITORING,

            PlatformAgentEvent.RUN_MISSION,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_command)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        #self.assertItemsEqual(res_pars, res_pars_all)

        verify_schema(retval)

        ##################################################################
        # STOPPED
        ##################################################################
        self._pause()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state STOPPED
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        agt_cmds_stopped = [
            PlatformAgentEvent.RESUME,
            PlatformAgentEvent.CLEAR,
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,
            #PlatformAgentEvent.PING_RESOURCE,
            #PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            #PlatformAgentEvent.GET_RESOURCE_STATE,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_stopped)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        #self.assertItemsEqual(res_pars, res_pars_all)

        verify_schema(retval)

        # back to COMMAND:
        self._resume()

        ##################################################################
        # MONITORING
        ##################################################################
        self._start_resource_monitoring()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state MONITORING
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        agt_cmds_monitoring = [
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,

            #PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            #PlatformAgentEvent.PING_RESOURCE,
            #PlatformAgentEvent.GET_RESOURCE,
            #PlatformAgentEvent.SET_RESOURCE,
            #PlatformAgentEvent.EXECUTE_RESOURCE,
            #PlatformAgentEvent.GET_RESOURCE_STATE,

            PlatformAgentEvent.STOP_MONITORING,

            PlatformAgentEvent.RUN_MISSION,

        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_monitoring)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        #self.assertItemsEqual(res_pars, res_pars_all)

        verify_schema(retval)

        # return to COMMAND state:
        self._stop_resource_monitoring()


        ###################
        # ALL CAPABILITIES
        ###################

        # Get exposed capabilities in all states as read from state COMMAND.
        retval = self._pa_client.get_capabilities(False)

         # Validate all capabilities as read from state COMMAND
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        #self.assertItemsEqual(res_pars, res_pars_all)

        verify_schema(retval)

    def test_some_state_transitions(self):
        self._create_network_and_start_root_platform(self._shutdown)

        self._assert_state(PlatformAgentState.UNINITIALIZED)

        self._initialize()   # -> INACTIVE
        self._reset()        # -> UNINITIALIZED

        self._initialize()   # -> INACTIVE
        self._go_active()    # -> IDLE
        self._reset()        # -> UNINITIALIZED        """

        self._initialize()   # -> INACTIVE
        self._go_active()    # -> IDLE
        self._run()          # -> COMMAND
        self._pause()        # -> STOPPED
        self._resume()       # -> COMMAND
        self._clear()        # -> IDLE
        self._reset()        # -> UNINITIALIZED

    def test_get_set_resources(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._get_resource()
        self._set_resource()

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

    def test_resource_monitoring_recent(self):
        #
        # https://jira.oceanobservatories.org/tasks/browse/OOIION-1372
        #
        # Verifies that the requests for attribute values are always for
        # the most recent ones, meaning that the retrieved values should *not*
        # be older than a small multiple of the nominal monitoring rate, even
        # after a long period in non-monitoring state.
        # See ResourceMonitor._retrieve_attribute_values
        #

        # start this test as in test_resource_monitoring()
        self.test_resource_monitoring()
        # which completes right after stopping monitoring. We want that initial
        # start/stop-monitoring phase to make this test more comprehensive.
        self._assert_state(PlatformAgentState.COMMAND)

        # now, the rest of this test does the following:
        # - pick an attribute to use as a basis for the time parameters to
        #   be used in the test
        # - wait for a while in the current non-monitoring mode
        # - re-enable monitoring
        # - wait for a sample to be published
        # - verify that new received data sample is "recent"
        # - stop monitoring

        # first, use an attribute (from the root platform being tested) with
        # a minimal monitoring rate, since that attribute should be reported
        # in a first sample received after re-enabling the monitoring.
        attr = None
        for attr_id, plat_attr in self._platform_attributes[self.PLATFORM_ID].iteritems():
            if attr is None or \
               float(plat_attr['monitor_cycle_seconds']) < float(attr['monitor_cycle_seconds']):
                attr = plat_attr

        self.assertIsNotNone(attr,
                             "some attribute expected to be defined for %r to "
                             "actually proceed with this test" % self.PLATFORM_ID)

        attr_id = attr['attr_id']
        monitor_cycle_seconds = attr['monitor_cycle_seconds']
        log.info("test_resource_monitoring_recent: using attr_id=%r: monitor_cycle_seconds=%s",
                 attr_id, monitor_cycle_seconds)

        # sleep for twice the interval defining "recent":
        from ion.agents.platform.resource_monitor import _MULT_INTERVAL
        time_to_sleep = 2 * (_MULT_INTERVAL * monitor_cycle_seconds)
        log.info("test_resource_monitoring_recent: sleeping for %s secs "
                 "before resuming monitoring", time_to_sleep)
        sleep(time_to_sleep)

        # reset the variables associated with the _wait_for_a_data_sample call below:
        self._samples_received = []
        self._async_data_result = AsyncResult()

        #################################################
        # re-start monitoring and wait for new sample:
        log.info("test_resource_monitoring_recent: re-starting monitoring")
        self._start_resource_monitoring(recursion=False)
        # should also work with recursion to children but set recursion=False
        # to avoid wasting the extra time in this test.

        try:
            self._wait_for_a_data_sample()

            # get current time here (right after receiving sample) for comparison below:
            curr_time_millis = current_time_millis()

            # verify that the timestamp of the received sample is not too old.
            # For this, use the minimum of the reported timestamps:
            rdt = RecordDictionaryTool.load_from_granule(self._samples_received[0])
            log.trace("test_resource_monitoring_recent: rdt:\n%s", rdt.pretty_print())
            temporal_parameter_name = rdt.temporal_parameter
            times = rdt[temporal_parameter_name]
            log.trace("test_resource_monitoring_recent: times:\n%s", self._pp.pformat(times))

            # minimum reported timestamp (note the NTP -> ION_time conversion):
            min_reported_time_ntp = min(times)
            min_reported_time_millis = float(ntp_2_ion_ts(min_reported_time_ntp))
            log.info("test_resource_monitoring_recent: sample received, min_reported_time_millis=%s",
                     int(min_reported_time_millis))

            # finally verify that it is actually not older than the small multiple
            # of monitor_cycle_seconds plus some additional tolerance (which is
            # arbitrarily set here to 10 secs):
            lower_limit_millis = \
                curr_time_millis - 1000 * (_MULT_INTERVAL * monitor_cycle_seconds + 10)

            self.assertGreaterEqual(
                min_reported_time_millis, lower_limit_millis,
                "min_reported_time_millis=%s must be >= %s. Diff=%s millis" % (
                min_reported_time_millis, lower_limit_millis,
                min_reported_time_millis - lower_limit_millis))

        finally:
            self._stop_resource_monitoring(recursion=False)

    def test_external_event_dispatch(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()

        # according to process_oms_event() (in service_gateway_service.py)
        # https://github.com/ooici/coi-services/blob/999c4315259082a9e50d6f4f96f8dd606073fda8/ion/services/coi/service_gateway_service.py#L339-370
        async_event_result, events_received = self._start_event_subscriber2(
            count=1,
            event_type="OMSDeviceStatusEvent",
            origin_type='OMS Platform'
        )

        self._go_active()
        self._run()

        # verify reception of the external event:
        log.info("waiting for external event notification... (timeout=%s)", self._receive_timeout)
        async_event_result.get(timeout=self._receive_timeout)
        self.assertEquals(len(events_received), 1)
        log.info("external events received: (%d): %s", len(events_received), events_received)

    def test_connect_disconnect_instrument(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._connect_instrument()
        self._turn_on_port()

        self._turn_off_port()
        self._disconnect_instrument()

    def test_check_sync(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._check_sync()

        self._connect_instrument()
        self._check_sync()

        self._disconnect_instrument()
        self._check_sync()

    def test_execute_resource(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)

        self._initialize()
        self._go_active()
        self._run()

        self._execute_resource(RSNPlatformDriverEvent.CHECK_SYNC)

    def test_resource_states(self):
        self._create_network_and_start_root_platform(self._shutdown)

        self._assert_state(PlatformAgentState.UNINITIALIZED)

        with self.assertRaises(Conflict):
            self._pa_client.get_resource_state()

        self._initialize()

        self._start_event_subscriber(event_type="ResourceAgentResourceStateEvent",
                                     count=2)

        res_state = self._pa_client.get_resource_state()
        self.assertEqual(res_state, RSNPlatformDriverState.DISCONNECTED)

        self._go_active()

        res_state = self._pa_client.get_resource_state()
        self.assertEqual(res_state, RSNPlatformDriverState.CONNECTED)

        self._run()

        res_state = self._pa_client.get_resource_state()
        self.assertEqual(res_state, RSNPlatformDriverState.CONNECTED)

        self._go_inactive()

        res_state = self._pa_client.get_resource_state()
        self.assertEqual(res_state, RSNPlatformDriverState.DISCONNECTED)

        self._reset()

        with self.assertRaises(Conflict):
            self._pa_client.get_resource_state()

        self._async_event_result.get(timeout=self._receive_timeout)
        self.assertGreaterEqual(len(self._events_received), 2)

    def test_lost_connection_and_reconnect(self):
        #
        # Starts up the network and puts the root platform in the MONITORING
        # state; then makes the simulator generate synthetic exceptions for
        # any call, which are handled by the driver as "connection lost"
        # situations; then it verifies the publication of the associated event
        # from the agent, and the LOST_CONNECTION state for the agent.
        # Finally, it instructs the simulator to resume working normally,
        # which should make the reconnect logic in the agent to recover the
        # connection and go back to the state where it was at connection lost.
        #

        ######################################################
        # set up network and put root in MONITORING state

        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._initialize()

        async_event_result, events_received = self._start_event_subscriber2(
            count=1,
            event_type="ResourceAgentConnectionLostErrorEvent",
            origin=self.p_root.platform_device_id)

        self._go_active()
        self._run()

        self._start_resource_monitoring()

        # let normal activity go on for a while:
        sleep(15)

        ######################################################
        # disable simulator to trigger lost connection:
        log.debug("disabling simulator")
        self._simulator_disable()

        # verify a ResourceAgentConnectionLostErrorEvent was published:
        async_event_result.get(timeout=self._receive_timeout)
        self.assertEquals(len(events_received), 1)

        # verify the platform is now in LOST_CONNECTION:
        self._assert_state(PlatformAgentState.LOST_CONNECTION)

        ######################################################
        # reconnect phase

        # re-enable simulator so connection is re-established:
        log.debug("re-enabling simulator")
        self._simulator_enable()

        # wait for a bit for the reconnection to take effect:
        sleep(15)

        # verify the platform is now back in MONITORING
        self._assert_state(PlatformAgentState.MONITORING)

        self._stop_resource_monitoring()

    def test_alerts(self):
        #
        # Tests alert processing/publication from the platform agent. Both
        # alert definitions passed via configuration and alert definitions
        # passed via the agent's set_agent({'alerts' : alert_defs}) method
        # are tested.
        #

        def start_DeviceStatusAlertEvent_subscriber(value_id, sub_type):
            """
            @return async_event_result  Use it to wait for the expected event
            """
            event_type = "DeviceStatusAlertEvent"

            async_event_result = AsyncResult()

            def consume_event(evt, *args, **kwargs):
                log.info('DeviceStatusAlertEvent_subscriber received evt: %s', str(evt))
                if evt.type_ != event_type or \
                   evt.value_id != value_id or \
                   evt.sub_type != sub_type:
                    return

                async_event_result.set(evt)

            kwargs = dict(event_type=event_type,
                          callback=consume_event,
                          origin=self.p_root.platform_device_id,
                          sub_type=sub_type)

            sub = EventSubscriber(**kwargs)
            sub.start()
            log.info("registered DeviceStatusAlertEvent subscriber: %s", kwargs)

            self._event_subscribers.append(sub)
            sub._ready_event.wait(timeout=self._receive_timeout)

            return async_event_result

        # before the creation of the network, set some alert defs for the
        # configuration of the root platform we are testing:
        alerts_for_config = [
            {
                'name'           : 'input_bus_current_warning_interval',
                'stream_name'    : 'parsed',
                'value_id'       : 'input_bus_current',
                'description'    : 'input_bus_current is above normal range.',
                'alert_type'     : StreamAlertType.WARNING,
                'aggregate_type' : AggregateStatusType.AGGREGATE_DATA,
                'lower_bound'    : None,
                'lower_rel_op'   : None,
                'upper_bound'    : 200.0,
                'upper_rel_op'   : '<',
                'alert_class'    : 'IntervalAlert'
            }]
        self._set_additional_extra_fields_for_platform_configuration(
            self.PLATFORM_ID, {'alerts': alerts_for_config})

        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()

        # verify we get reported the configured alerts:
        configed_alerts = self._pa_client.get_agent(['alerts'])['alerts']
        self.assertEquals(len(alerts_for_config), len(configed_alerts),
                          "must have %d alerts defined from configuration but got %d" % (
                          len(alerts_for_config), len(configed_alerts)))

        # define some additional alerts:
        # NOTE: see ion/agents/platform/rsn/simulator/oms_values.py for the
        # sinusoidal waveforms that are generated; here we depend on those
        # ranges to indicate the upper_bounds for these alarms; for example,
        # input_voltage fluctuates within -500.0 to +500, so we specify
        # upper_bound = 400.0 to see the alert being published.
        new_alert_defs = [
            {
                'name'           : 'input_voltage_warning_interval',
                'stream_name'    : 'parsed',
                'value_id'       : 'input_voltage',
                'description'    : 'input_voltage is above normal range.',
                'alert_type'     : StreamAlertType.WARNING,
                'aggregate_type' : AggregateStatusType.AGGREGATE_DATA,
                'lower_bound'    : None,
                'lower_rel_op'   : None,
                'upper_bound'    : 400.0,
                'upper_rel_op'   : '<',
                'alert_class'    : 'IntervalAlert'
            }]

        # All the alerts to be set: the configured ones plus the new ones above:
        alert_defs = configed_alerts + new_alert_defs

        self._pa_client.set_agent({'alerts' : alert_defs})

        retval = self._pa_client.get_agent(['alerts'])['alerts']
        log.debug('alerts: %s', self._pp.pformat(retval))
        self.assertEquals(len(alert_defs), len(retval),
                          "must have %d alerts defined here but got %d" % (
                          len(alert_defs), len(retval)))

        self._go_active()
        self._run()

        #################################################################
        # prepare to receive alert publications:
        # note: as the values for the above streams fluctuate we should get
        # both WARNING and ALL_CLEAR events:

        # NOTE that the verifications below are for both the configured
        # alerts and the additional alerts set via set_agent.

        async_event_result1 = start_DeviceStatusAlertEvent_subscriber(
            value_id="input_voltage",
            sub_type=StreamAlertType._str_map[StreamAlertType.WARNING])

        async_event_result2 = start_DeviceStatusAlertEvent_subscriber(
            value_id="input_bus_current",
            sub_type=StreamAlertType._str_map[StreamAlertType.WARNING])

        async_event_result3 = start_DeviceStatusAlertEvent_subscriber(
            value_id="input_voltage",
            sub_type=StreamAlertType._str_map[StreamAlertType.ALL_CLEAR])

        async_event_result4 = start_DeviceStatusAlertEvent_subscriber(
            value_id="input_bus_current",
            sub_type=StreamAlertType._str_map[StreamAlertType.ALL_CLEAR])

        self._start_resource_monitoring()

        # wait for the expected DeviceStatusAlertEvent events:
        # (60sec timeout enough for the sine periods associated to the streams)
        async_event_result1.get(timeout=60)
        async_event_result2.get(timeout=60)
        async_event_result3.get(timeout=60)
        async_event_result4.get(timeout=60)

        self._stop_resource_monitoring()
