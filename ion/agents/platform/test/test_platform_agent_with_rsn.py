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
#
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_capabilities
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_some_state_transitions
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_get_set_resources
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_some_commands
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_resource_monitoring
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_external_event_dispatch
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_connect_disconnect_instrument
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_check_sync
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_execute_resource
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_resource_states
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_lost_connection_and_reconnect
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_rsn.py:TestPlatformAgent.test_alerts
#


from pyon.public import log

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

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

from gevent import sleep
from gevent.event import AsyncResult
from mock import patch
from pyon.public import CFG


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class TestPlatformAgent(BaseIntTestPlatform):

    def _create_network_and_start_root_platform(self):
        """
        Call this at the beginning of each test. We need to make sure that
        the patched timeout is in effect for the actions performed here.

        @note this used to be done in setUp, but the patch.dict mechanism does
        *not* take effect in setUp!

        An addCleanup function is added here to make sure the root platform
        is stopped even if the test fails.
        """
        self.p_root = None

        # NOTE The tests expect to use values set up by HelperTestMixin for
        # the following networks (see ion/agents/platform/test/helper.py)
        if self.PLATFORM_ID == 'Node1D':
            self.p_root = self._create_small_hierarchy()

        elif self.PLATFORM_ID == 'LJ01D':
            self.p_root = self._create_single_platform()

        else:
            self.fail("self.PLATFORM_ID expected to be one of: 'Node1D', 'LJ01D'")

        self._start_platform(self.p_root)

        def stop_root():
            # check p_root to avoid generating one more exception if the
            # creation/launch of the network fails for some reason
            if self.p_root:
                self._stop_platform(self.p_root)
                self.p_root = None
        self.addCleanup(stop_root)

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
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE,
            PlatformAgentEvent.SET_RESOURCE,
            PlatformAgentEvent.EXECUTE_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE_STATE,

            PlatformAgentEvent.START_MONITORING,
            PlatformAgentEvent.STOP_MONITORING,
        ]


        def sort_caps(caps):
            agt_cmds = []
            agt_pars = []
            res_cmds = []
            res_pars = []

            if len(caps)>0 and isinstance(caps[0], AgentCapability):
                agt_cmds = [x.name for x in caps if x.cap_type==CapabilityType.AGT_CMD]
                agt_pars = [x.name for x in caps if x.cap_type==CapabilityType.AGT_PAR]
                res_cmds = [x.name for x in caps if x.cap_type==CapabilityType.RES_CMD]
                res_pars = [x.name for x in caps if x.cap_type==CapabilityType.RES_PAR]

            elif len(caps)>0 and isinstance(caps[0], dict):
                agt_cmds = [x['name'] for x in caps if x['cap_type']==CapabilityType.AGT_CMD]
                agt_pars = [x['name'] for x in caps if x['cap_type']==CapabilityType.AGT_PAR]
                res_cmds = [x['name'] for x in caps if x['cap_type']==CapabilityType.RES_CMD]
                res_pars = [x['name'] for x in caps if x['cap_type']==CapabilityType.RES_PAR]

            return agt_cmds, agt_pars, res_cmds, res_pars

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
            RSNPlatformDriverEvent.CHECK_SYNC
        ]

        ##################################################################
        # UNINITIALIZED
        ##################################################################

        self._assert_state(PlatformAgentState.UNINITIALIZED)

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

        # Validate capabilities for state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

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
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])


        ##################################################################
        # INACTIVE
        ##################################################################
        self._initialize()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

        # Validate capabilities for state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_inactive = [
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,
            PlatformAgentEvent.GO_ACTIVE,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.GET_RESOURCE_STATE,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_inactive)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states.
        retval = self._pa_client.get_capabilities(False)

         # Validate all capabilities as read from state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertEqual(set(res_cmds), set(res_cmds_all))
        self.assertItemsEqual(res_pars, [])

        ##################################################################
        # IDLE
        ##################################################################
        self._go_active()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities for state IDLE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_idle = [
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RUN,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.GET_RESOURCE_STATE,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_idle)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states as read from IDLE.
        retval = self._pa_client.get_capabilities(False)

         # Validate all capabilities as read from state IDLE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, [])


        ##################################################################
        # COMMAND
        ##################################################################
        self._run()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_command = [
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,
            PlatformAgentEvent.PAUSE,
            PlatformAgentEvent.CLEAR,

            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE,
            PlatformAgentEvent.SET_RESOURCE,
            PlatformAgentEvent.EXECUTE_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE_STATE,

            PlatformAgentEvent.START_MONITORING,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_command)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, res_pars_all)


        ##################################################################
        # STOPPED
        ##################################################################
        self._pause()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state STOPPED
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_stopped = [
            PlatformAgentEvent.RESUME,
            PlatformAgentEvent.CLEAR,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.GET_RESOURCE_STATE,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_stopped)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, res_pars_all)


        # back to COMMAND:
        self._resume()

        ##################################################################
        # MONITORING
        ##################################################################
        self._start_resource_monitoring()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state MONITORING
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_monitoring = [
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.SHUTDOWN,

            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE,
            PlatformAgentEvent.SET_RESOURCE,
            PlatformAgentEvent.EXECUTE_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE_STATE,

            PlatformAgentEvent.STOP_MONITORING,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_monitoring)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, res_pars_all)

        # return to COMMAND state:
        self._stop_resource_monitoring()


        ###################
        # ALL CAPABILITIES
        ###################

        # Get exposed capabilities in all states as read from state COMMAND.
        retval = self._pa_client.get_capabilities(False)

         # Validate all capabilities as read from state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, res_pars_all)

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

    def test_some_state_transitions(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)

        self._initialize()   # -> INACTIVE
        self._reset()        # -> UNINITIALIZED

        self._initialize()   # -> INACTIVE
        self._go_active()    # -> IDLE
        self._reset()        # -> UNINITIALIZED

        self._initialize()   # -> INACTIVE
        self._go_active()    # -> IDLE
        self._run()          # -> COMMAND
        self._pause()        # -> STOPPED
        self._resume()       # -> COMMAND
        self._clear()        # -> IDLE
        self._reset()        # -> UNINITIALIZED

        self._shutdown()     # -> UNINITIALIZED

    def test_get_set_resources(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._get_resource()
        self._set_resource()

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

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

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

    def test_resource_monitoring(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._start_resource_monitoring()
        self._wait_for_a_data_sample()
        self._stop_resource_monitoring()

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

    def test_external_event_dispatch(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._wait_for_external_event()

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

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

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

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

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

    def test_execute_resource(self):
        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)

        self._initialize()
        self._go_active()
        self._run()

        self._execute_resource(RSNPlatformDriverEvent.CHECK_SYNC)

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

    def test_resource_states(self):
        self._create_network_and_start_root_platform()

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

        self._async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertGreaterEqual(len(self._events_received), 2)

        #####################
        # done
        self._shutdown()

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

        # let normal activity go on for a while - note, the sleep here should
        # be large enough to allow for some retrievals during the monitoring
        # so that the lost connection is detected.
        sleep(15)

        ######################################################
        # disable simulator to trigger lost connection:
        log.debug("disabling simulator")
        self._simulator_disable()

        # verify a ResourceAgentConnectionLostErrorEvent was published:
        async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertEquals(len(events_received), 1)

        # verify the platform is now in LOST_CONNECTION:
        self._assert_state(PlatformAgentState.LOST_CONNECTION)

        ######################################################
        # reconnect phase

        # launch simulator again so connection is re-established:
        log.debug("re-enabling simulator")
        self._simulator_enable()

        # wait for a bit for the reconnection to take effect:
        sleep(15)

        # verify the platform is now back in MONITORING
        self._assert_state(PlatformAgentState.MONITORING)

        #####################
        # done
        self._stop_resource_monitoring()
        self._go_inactive()
        self._reset()
        self._shutdown()

    def test_alerts(self):

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
            sub._ready_event.wait(timeout=CFG.endpoint.receive.timeout)

            return async_event_result

        self._create_network_and_start_root_platform()

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()

        retval = self._pa_client.get_agent(['alerts'])['alerts']
        self.assertEquals([], retval, "No alerts must have been defined here")

        # define some alerts:
        # NOTE: see ion/agents/platform/rsn/simulator/oms_values.py for the
        # sinusoidal waveforms that are generated; here we depend on those
        # ranges to indicate the upper_bounds for these alarms; for example,
        # input_voltage fluctuates within -500.0 to +500, so we specify
        # upper_bound = 400.0 to see the alert being published.
        alert_defs = [
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
            }, {
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

        self._pa_client.set_agent({'alerts' : alert_defs})

        retval = self._pa_client.get_agent(['alerts'])['alerts']
        log.debug('alerts: %s', self._pp.pformat(retval))
        self.assertEquals(2, len(retval), "must have 2 alerts defined here")

        self._go_active()
        self._run()

        #################################################################
        # prepare to receive alert publications:
        # note: as the values for the above streams fluctuate we should get
        # both WARNING and ALL_CLEAR events:

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
        async_event_result1.get(timeout=30)
        async_event_result2.get(timeout=30)
        async_event_result3.get(timeout=30)
        async_event_result4.get(timeout=30)

        self._stop_resource_monitoring()

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

