#!/usr/bin/env python

"""Additional tests focused on robustness testing"""

__author__ = 'Carlos Rueda'

# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_instrument_reset_externally
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_adverse_activation_sequence
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_with_instrument_directly_put_into_streaming
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_with_instrument_directly_stopped
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_with_leaf_subplatform_directly_stopped
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_with_intermediate_subplatform_directly_stopped
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_robustness.py:TestPlatformRobustness.test_with_intermediate_subplatform_directly_stopped_then_restarted

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform
from ion.agents.platform.status_manager import publish_event_for_diagnostics
from pyon.public import log, CFG
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentState, ResourceAgentEvent
from interface.objects import AgentCommand
from interface.objects import ProcessStateEnum

from gevent.timeout import Timeout
import gevent
from mock import patch
import unittest
import os


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestPlatformRobustness(BaseIntTestPlatform):

    ###################
    # auxiliary methods
    ###################

    def _launch_network(self, p_root, recursion=True):
        def shutdown():
            try:
                self._go_inactive(recursion)
                self._reset(recursion)
            finally:  # attempt shutdown anyway
                self._shutdown(True)  # NOTE: shutdown always with recursion=True

        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(shutdown)

    def _start_device_failed_command_event_subscriber(self, p_root, count=1):
        return self._start_event_subscriber2(
            count=count,
            event_type="DeviceStatusEvent",
            origin_type="PlatformDevice",
            origin=p_root.platform_device_id,
            sub_type="device_failed_command")

    def _start_ProcessLifecycleEvent_subscriber(self, origin, count=1):
        return self._start_event_subscriber2(
            count=count,
            event_type="ProcessLifecycleEvent",
            origin_type="DispatchedProcess",
            origin=origin)

    def _instrument_initialize(self, instr_key, ia_client):
        self._instrument_execute_agent(instr_key, ia_client, ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)

    def _instrument_go_active(self, instr_key, ia_client):
        self._instrument_execute_agent(instr_key, ia_client, ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)

    def _instrument_run(self, instr_key, ia_client):
        self._instrument_execute_agent(instr_key, ia_client, ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

    def _instrument_start_autosample(self, instr_key, ia_client):
        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
        command = SBE37ProtocolEvent.START_AUTOSAMPLE
        self._instrument_execute_resource(instr_key, ia_client, command, ResourceAgentState.STREAMING)

    def _instrument_stop_autosample(self, instr_key, ia_client):
        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
        command = SBE37ProtocolEvent.STOP_AUTOSAMPLE
        self._instrument_execute_resource(instr_key, ia_client, command, ResourceAgentState.COMMAND)

    def _instrument_reset(self, instr_key, ia_client):
        self._instrument_execute_agent(instr_key, ia_client, ResourceAgentEvent.RESET, ResourceAgentState.UNINITIALIZED)

    def _instrument_execute_agent(self, instr_key, ia_client, command, expected_state=None):
        log.debug("execute_agent %r on instrument %r", command, instr_key)
        cmd = AgentCommand(command=command)
        retval = ia_client.execute_agent(cmd, timeout=self._receive_timeout)
        log.debug("execute_agent of %r on instrument %r returned: %s", command, instr_key, retval)
        if expected_state:
            self.assertEqual(expected_state, ia_client.get_agent_state())

    def _instrument_execute_resource(self, instr_key, ia_client, command, expected_state=None):
        log.debug("execute_resource %r on instrument %r", command, instr_key)
        cmd = AgentCommand(command=command)
        retval = ia_client.execute_resource(cmd, timeout=self._receive_timeout)
        log.debug("execute_resource of %r on instrument %r returned: %s", command, instr_key, retval)
        if expected_state:
            self.assertEqual(expected_state, ia_client.get_agent_state())

    ###################
    # tests
    ###################

    def test_instrument_reset_externally(self):
        #
        # - a network of one platform with 2 instruments is launched and put in command state
        # - one of the instruments is directly reset here simulating some external situation
        # - network is shutdown following regular sequence
        # - test should complete fine: the already reset child instrument should not
        #   cause issue during the reset sequence from parent platform agent.
        #
        # During the shutdown sequence the following should be logged out by the platform prior to trying
        # the GO_INACTIVE command to the (already reset) instrument:
        # ion.agents.platform.platform_agent:2195 'LJ01D': instrument '8761366c8d734a4ba886606c3a47b621':
        #       current_state='RESOURCE_AGENT_STATE_UNINITIALIZED'
        #       expected_state='RESOURCE_AGENT_STATE_INACTIVE'
        #       command='RESOURCE_AGENT_EVENT_GO_INACTIVE', comp=-1, actl=decr, acceptable_state=True
        #
        self._set_receive_timeout()
        recursion = True

        instr_keys = ['SBE37_SIM_01', 'SBE37_SIM_02']
        p_root = self._set_up_single_platform_with_some_instruments(instr_keys)
        self._launch_network(p_root, recursion)

        i_obj = self._get_instrument(instr_keys[0])
        ia_client = self._create_resource_agent_client(i_obj.instrument_device_id)

        # command sequence:
        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)

        self.assertEqual(ia_client.get_agent_state(), ResourceAgentState.COMMAND)

        # reset the instrument
        self._instrument_reset(instr_keys[0], ia_client)

        # and let the test complete with regular shutdown sequence.

    def test_adverse_activation_sequence(self):
        #
        # - initialize network (so all agents get INITIALIZED)
        # - externally reset an instrument (so that instrument gets to UNINITIALIZED)
        # - activate network (so a "device_failed_command" event should be published because
        #   instrument is not INITIALIZED)
        # - verify publication of the "device_failed_command" event
        # - similarly, "run" the network
        # - verify publication of 2nd "device_failed_command" event
        #
        # note: the platform successfully completes the GO_ACTIVE and RUN commands even with the failed child;
        # see https://confluence.oceanobservatories.org/display/CIDev/Platform+commands
        #
        self._set_receive_timeout()
        recursion = True

        instr_keys = ['SBE37_SIM_01']
        p_root = self._set_up_single_platform_with_some_instruments(instr_keys)
        self._launch_network(p_root, recursion)

        i_obj = self._get_instrument(instr_keys[0])
        ia_client = self._create_resource_agent_client(i_obj.instrument_device_id)

        # initialize the network
        self._ping_agent()
        self._initialize(recursion)

        self.assertEqual(ia_client.get_agent_state(), ResourceAgentState.INACTIVE)

        # reset the instrument before we continue the activation of the network
        self._instrument_reset(instr_keys[0], ia_client)

        # prepare to receive 2 "device_failed_command" events, one during GO_ACTIVE and one during RUN:
        # (See StatusManager.publish_device_failed_command_event)
        async_event_result, events_received = self._start_device_failed_command_event_subscriber(p_root, 2)

        # activate and run network:
        self._go_active(recursion)
        self._run(recursion)

        # verify publication of "device_failed_command" events
        async_event_result.get(timeout=self._receive_timeout)
        self.assertEquals(len(events_received), 2)
        log.info("DeviceStatusEvents received (%d): %s", len(events_received), events_received)
        for event_received in events_received:
            self.assertGreaterEqual(len(event_received.values), 1)
            failed_resource_id = event_received.values[0]
            self.assertEquals(i_obj.instrument_device_id, failed_resource_id)

    def test_with_instrument_directly_put_into_streaming(self):
        #
        # - network (of a platform with an instrument) is launched until COMMAND state
        # - instrument is directly moved to STREAMING
        # - network is moved to STREAMING
        # - instrument is directly stopped STREAMING
        # - network is moved back to COMMAND
        #
        # All of this should be handled without errors, in particular no DeviceStatusEvent's should be published.
        #
        self._set_receive_timeout()
        recursion = True

        instr_key = 'SBE37_SIM_01'
        p_root = self._set_up_single_platform_with_some_instruments([instr_key])
        self._launch_network(p_root, recursion)

        i_obj = self._get_instrument(instr_key)
        ia_client = self._create_resource_agent_client(i_obj.instrument_device_id)

        # initialize the network
        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)
        self._assert_agent_client_state(ia_client, ResourceAgentState.COMMAND)

        async_event_result, events_received = self._start_device_failed_command_event_subscriber(p_root, 1)

        # move instrument directly to STREAMING
        self._instrument_start_autosample(instr_key, ia_client)

        # continue moving network to streaming
        self._start_resource_monitoring(recursion)

        # directly stop streaming in instrument
        self._instrument_stop_autosample(instr_key, ia_client)

        # stop streaming through the platform
        self._stop_resource_monitoring(recursion)

        # verify no device_failed_command events were published
        with self.assertRaises(Timeout):
            async_event_result.get(timeout=10)

    def test_with_instrument_directly_stopped(self):
        #
        # - network (of a platform with an instrument) is launched until COMMAND state
        # - instrument is directly stopped
        # - TERMINATED lifecycle event from instrument when stopped should be published
        # - shutdown sequence of the test should complete without issues
        #
        self._set_receive_timeout()
        recursion = True

        instr_key = 'SBE37_SIM_01'
        p_root = self._set_up_single_platform_with_some_instruments([instr_key])
        self._launch_network(p_root, recursion)

        i_obj = self._get_instrument(instr_key)
        ia_client = self._create_resource_agent_client(i_obj.instrument_device_id)

        # initialize the network
        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)
        self._assert_agent_client_state(ia_client, ResourceAgentState.COMMAND)

        # use associated process ID for the subscription:
        instrument_pid = ia_client.get_agent_process_id()
        async_event_result, events_received = self._start_ProcessLifecycleEvent_subscriber(instrument_pid)

        # directly stop instrument
        log.info("stopping instrument %r", i_obj.instrument_device_id)
        self._stop_instrument(i_obj)

        # verify publication of TERMINATED lifecycle event from instrument when stopped
        async_event_result.get(timeout=self._receive_timeout)
        self.assertEquals(len(events_received), 1)
        event_received = events_received[0]
        log.info("ProcessLifecycleEvent received: %s", event_received)
        self.assertEquals(instrument_pid, event_received.origin)
        self.assertEquals(ProcessStateEnum.TERMINATED, event_received.state)

    def test_with_leaf_subplatform_directly_stopped(self):
        #
        # - small network of platforms (no instruments) is launched and put in COMMAND state
        # - leaf sub-platform is directly stopped
        # - TERMINATED lifecycle event from leaf sub-platform when stopped should be published
        # - shutdown sequence of the test should complete without issues
        #
        self._set_receive_timeout()
        recursion = True

        p_root = self._create_small_hierarchy()  # Node1D -> MJ01C -> LJ01D
        self._launch_network(p_root, recursion)

        log.info('platforms in the launched network (%d): %s', len(self._setup_platforms), self._setup_platforms.keys())
        p_obj = self._get_platform('LJ01D')
        pa_client = self._create_resource_agent_client(p_obj.platform_device_id)

        # initialize the network
        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)
        self._assert_agent_client_state(pa_client, ResourceAgentState.COMMAND)

        # use associated process ID for the subscription:
        platform_pid = pa_client.get_agent_process_id()
        async_event_result, events_received = self._start_ProcessLifecycleEvent_subscriber(platform_pid)

        # directly stop sub-platform
        log.info("stopping sub-platform %r", p_obj.platform_device_id)
        self.IMS.stop_platform_agent_instance(p_obj.platform_agent_instance_id)

        # verify publication of TERMINATED lifecycle event from sub-platform when stopped
        async_event_result.get(timeout=self._receive_timeout)
        self.assertEquals(len(events_received), 1)
        event_received = events_received[0]
        log.info("ProcessLifecycleEvent received: %s", event_received)
        self.assertEquals(platform_pid, event_received.origin)
        self.assertEquals(ProcessStateEnum.TERMINATED, event_received.state)

    def test_with_intermediate_subplatform_directly_stopped(self):
        #
        # - network of 13 platforms (no instruments) is launched and put in COMMAND state
        # - one non-leaf sub-platform (LV01B) is directly stopped
        # - TERMINATED lifecycle event from sub-platform when stopped should be published
        # - shutdown sequence of the test should complete without issues.
        #
        # NOTE: we explicitly stop the processes corresponding to the orphaned
        # sub-platforms of LV01B (LJ01B and MJ01B), so they don't get reported as leaked.
        #
        self._set_receive_timeout()
        recursion = True

        p_root = self._set_up_platform_hierarchy_with_some_instruments([])
        self._launch_network(p_root, recursion)

        log.info('platforms in the launched network (%d): %s', len(self._setup_platforms), self._setup_platforms.keys())
        p_obj = self._get_platform('LV01B')
        pa_client = self._create_resource_agent_client(p_obj.platform_device_id)

        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)
        self._assert_agent_client_state(pa_client, ResourceAgentState.COMMAND)

        # use associated process ID for the subscription:
        platform_pid = pa_client.get_agent_process_id()
        async_event_result, events_received = self._start_ProcessLifecycleEvent_subscriber(platform_pid)

        # directly stop sub-platform
        log.info("stopping sub-platform %r", p_obj.platform_device_id)
        self.IMS.stop_platform_agent_instance(p_obj.platform_agent_instance_id)

        # verify publication of TERMINATED lifecycle event from sub-platform when stopped
        async_event_result.get(timeout=self._receive_timeout)
        self.assertEquals(len(events_received), 1)
        event_received = events_received[0]
        log.info("ProcessLifecycleEvent received: %s", event_received)
        self.assertEquals(platform_pid, event_received.origin)
        self.assertEquals(ProcessStateEnum.TERMINATED, event_received.state)

        # we know there would be two orphaned processes (corresponding to the sub-platforms of LV01B),
        # so, explicitly stop them here:
        for orphaned in ['LJ01B', 'MJ01B']:
            o_obj = self._get_platform(orphaned)
            log.info("stopping orphaned sub-platform %r platform_agent_instance_id=%r", orphaned, o_obj.platform_agent_instance_id)
            try:
                self.IMS.stop_platform_agent_instance(o_obj.platform_agent_instance_id)
            except Exception as ex:
                log.warn("Error while trying IMS.stop_platform_agent_instance(%r)", o_obj.platform_agent_instance_id, ex)

    def test_with_intermediate_subplatform_directly_stopped_then_restarted(self):
        #
        # Similar to test_with_intermediate_subplatform_directly_stopped but the sub-platform is then
        # relaunched to verify that it is "revalidated" for subsequent processing.
        # We can visually verify this via the publish_event_for_diagnostics utility.
        # The test should complete without any issues.
        #
        self._set_receive_timeout()
        recursion = True

        p_root = self._set_up_platform_hierarchy_with_some_instruments([])
        self._launch_network(p_root, recursion)

        log.info('platforms in the launched network (%d): %s', len(self._setup_platforms), self._setup_platforms.keys())
        p_obj = self._get_platform('LV01B')
        pa_client = self._create_resource_agent_client(p_obj.platform_device_id)

        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)
        self._assert_agent_client_state(pa_client, ResourceAgentState.COMMAND)

        # use associated process ID for the subscription:
        platform_pid = pa_client.get_agent_process_id()
        async_event_result, events_received = self._start_ProcessLifecycleEvent_subscriber(platform_pid)

        # directly stop sub-platform
        log.info("stopping sub-platform %r", p_obj.platform_device_id)
        self.IMS.stop_platform_agent_instance(p_obj.platform_agent_instance_id)

        # verify publication of TERMINATED lifecycle event from sub-platform when stopped
        async_event_result.get(timeout=self._receive_timeout)
        self.assertEquals(len(events_received), 1)
        event_received = events_received[0]
        log.info("ProcessLifecycleEvent received: %s", event_received)
        self.assertEquals(platform_pid, event_received.origin)
        self.assertEquals(ProcessStateEnum.TERMINATED, event_received.state)

        gevent.sleep(3)

        publish_event_for_diagnostics()  # should show the invalidated child for parent Node1B:
        # INFO ... ion.agents.platform.status_manager:1019 'Node1B'/RESOURCE_AGENT_STATE_COMMAND: (a7f865c34f534e60a14e5f0f8ef2fd53) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         26215ffcf7c94260a99e9c9d103f22f9 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         0e07bc623af64a3a8f61465329451de7 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         a914714894b844a8b42724fe9208fde4 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         02d5a770fba8405c868cc8d55bbbb8d3 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         b19f89585e7c43789b60beac5ddec43c : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         a2f81525ab1e425da808191f9bbe945d : STATUS_UNKNOWN      STATUS_UNKNOWN      STATUS_UNKNOWN      STATUS_UNKNOWN
        #         6ff02a90e34643fe87ecf262a33437cd : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         83b4f74ab1db4c70ae63072336083ac3 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         7639e530740a48a8b299d0d19dcf7abe : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         88b143e311514121adc544c5933f92a6 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         b1453122a5a64ac6868cfc39e12e4e50 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         c996ff0478a6449da62955859020ee50 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #
        #                     invalidated_children : ['a2f81525ab1e425da808191f9bbe945d']

        gevent.sleep(3)

        ############################################
        # relaunch the intermediate sub-platform:
        log.info("relaunching sub-platform 'LV01B': %r", p_obj.platform_device_id)
        pa_client = self._start_a_platform(p_obj)
        self._ping_agent(pa_client)
        # recursion=False because LV01B's children are already in COMMAND
        self._initialize(recursion=False, pa_client=pa_client)
        self._go_active(recursion=False, pa_client=pa_client)
        self._run(recursion=False, pa_client=pa_client)

        # wait for a bit to allow ancestors to re-validate the child, in particular for the parent Node1B:
        gevent.sleep(10)

        publish_event_for_diagnostics()  # should show the child re-validated:
        # INFO ... ion.agents.platform.status_manager:1019 'Node1B'/RESOURCE_AGENT_STATE_COMMAND: (a7f865c34f534e60a14e5f0f8ef2fd53) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         26215ffcf7c94260a99e9c9d103f22f9 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         0e07bc623af64a3a8f61465329451de7 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         a914714894b844a8b42724fe9208fde4 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         02d5a770fba8405c868cc8d55bbbb8d3 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         b19f89585e7c43789b60beac5ddec43c : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         a2f81525ab1e425da808191f9bbe945d : STATUS_UNKNOWN      STATUS_UNKNOWN      STATUS_UNKNOWN      STATUS_UNKNOWN
        #         6ff02a90e34643fe87ecf262a33437cd : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         83b4f74ab1db4c70ae63072336083ac3 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         7639e530740a48a8b299d0d19dcf7abe : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         88b143e311514121adc544c5933f92a6 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         b1453122a5a64ac6868cfc39e12e4e50 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         c996ff0478a6449da62955859020ee50 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #
        #                     invalidated_children : []
