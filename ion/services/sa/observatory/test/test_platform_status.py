#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_status
@file    ion/services/sa/observatory/test/test_platform_status.py
@author  Carlos Rueda, Maurice Manning
@brief   Platform device status and aggregate status tests
"""

__author__ = 'Carlos Rueda, Maurice Manning'
__license__ = 'Apache 2.0'

#
# Base preparations and construction of the platform topology are provided by
# the base class BaseTestPlatform.
#

# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_status.py:Test.test_platform_status_small_network_3
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_status.py:Test.test_platform_status_small_network_5
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_status.py:Test.test_platform_status_small_network_5_1
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_status.py:Test.test_platform_status_launch_instruments_first_2_3
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_status.py:Test.test_platform_status_terminate_and_restart_instrument_1_1

from pyon.public import log

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform
from ion.agents.platform.status_manager import formatted_statuses
from ion.agents.platform.status_manager import publish_event_for_diagnostics

from ion.agents.instrument.instrument_agent import InstrumentAgentEvent
from ion.agents.instrument.instrument_agent import InstrumentAgentState
from interface.objects import AgentCommand

from pyon.event.event import EventPublisher
from pyon.event.event import EventSubscriber

from interface.objects import AggregateStatusType
from interface.objects import DeviceStatusType
from interface.objects import ProcessStateEnum

from gevent.event import AsyncResult
from gevent import sleep

from mock import patch
from pyon.public import CFG


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class Test(BaseIntTestPlatform):

    def setUp(self):
        super(Test, self).setUp()
        self._event_publisher = EventPublisher()
        self._expected_events = 1
        self._received_events = []
        self._last_checked_status = None

    def _done(self):
        try:
            self._go_inactive()
            self._reset()
        finally:  # attempt shutdown anyway
            self._shutdown()

    def _start_agg_status_event_subscriber(self, p_root):
        """
        Start the event subscriber to the given root platform. Upon reception
        of event, the callback only sets the async result if the number of
        expected event has been reached.
        """

        event_type = "DeviceAggregateStatusEvent"

        def consume_event(evt, *args, **kwargs):
            self._received_events.append(evt)
            assert len(self._received_events) <= self._expected_events
            if len(self._received_events) == self._expected_events:
                self._async_result.set(evt)

        sub = EventSubscriber(event_type=event_type,
                              origin=p_root.platform_device_id,
                              callback=consume_event)

        sub.start()
        self._data_subscribers.append(sub)
        sub._ready_event.wait(timeout=CFG.endpoint.receive.timeout)

        log.debug("registered for DeviceAggregateStatusEvent")

    def _expect_from_root(self, number_of_events):
        """
        Sets the number of expected events for the subscriber. To be called
        before any action that triggers publications from the root platform.
        """
        self._expected_events = number_of_events
        self._received_events = []
        self._async_result = AsyncResult()

    def _publish_for_child(self, child_obj, status_name, status):
        """
        Publishes a DeviceAggregateStatusEvent from the given platform or
        instrument object.

        NOTE that we just publish on behalf of the child, but the statuses in
        the child itself are *not* set. This is OK for these tests; we just
        need that child's ancestors to react to the event.
        """

        if 'platform_device_id' in child_obj:
            origin = child_obj.platform_device_id
            origin_type = "PlatformDevice"
        else:
            origin = child_obj.instrument_device_id
            origin_type = "InstrumentDevice"

        # create and publish event from the given origin and type:
        evt = dict(event_type='DeviceAggregateStatusEvent',
                   origin_type=origin_type,
                   origin=origin,
                   description="Fake event for testing",
                   status_name=status_name,
                   status=status)

        log.debug("publishing for child %r: evt=%s", origin, evt)
        self._event_publisher.publish_event(**evt)

    def _wait_root_event(self):
        """
        waits for the expected number of events.
        """
        root_evt = self._async_result.get(timeout=CFG.endpoint.receive.timeout)
        return root_evt

    def _wait_root_event_and_verify(self, status_name, status):
        """
        Waits for the expected event from root and verifies that the root
        status, as indicated in the received event, has been updated as expected.

        @param status_name   Entry in AggregateStatusType
        @param status        Entry in DeviceStatusType
        """

        # verify we are not checking the same status twice in a row:
        self.assertNotEquals(self._last_checked_status, (status_name, status),
                             "The same status cannot be checked twice in a row "
                             "because there won't be any event going to be "
                             "published in the second case. Fix the test!")
        self._last_checked_status = (status_name, status)

        root_evt = self._wait_root_event()

        self.assertEquals(root_evt.origin, self.p_root.platform_device_id)
        self.assertEquals(root_evt.type_, 'DeviceAggregateStatusEvent')

        log.debug("Got event from root platform: %s = %s",
                  AggregateStatusType._str_map[root_evt.status_name],
                  DeviceStatusType._str_map[root_evt.status])

        # verify the status name:
        self.assertEquals(status_name, root_evt.status_name,
                          "Expected: %s, Got: %s" % (
                          AggregateStatusType._str_map[status_name],
                          AggregateStatusType._str_map[root_evt.status_name]))

        # verify the status value:
        self.assertEquals(status, root_evt.status,
                          "Expected: %s, Got: %s" % (
                          DeviceStatusType._str_map[status],
                          DeviceStatusType._str_map[root_evt.status]))

    def _verify_with_get_agent(self, status_name, status):
        """
        Verifies the expected rollup_status against the reported status from the
        agent using get_agent.
        """
        self._last_checked_status = (status_name, status)

        rollup_status = self._get_all_root_statuses()[2]

        retrieved_status = rollup_status[status_name]
        self.assertEquals(status, retrieved_status,
                          "Expected: %s, Got: %s" % (
                          DeviceStatusType._str_map[status],
                          DeviceStatusType._str_map[retrieved_status]))

    def _get_all_root_statuses(self):
        resp = self._pa_client.get_agent(['aggstatus', 'child_agg_status', 'rollup_status'])

        aggstatus        = resp['aggstatus']
        child_agg_status = resp['child_agg_status']
        rollup_status    = resp['rollup_status']

        log.debug("All root statuses:\n%s",
                  formatted_statuses(aggstatus, child_agg_status, rollup_status))

        return aggstatus, child_agg_status, rollup_status

    def _verify_statuses(self, statuses, status_values):
        """
        Verifies that each given status is equal any of the given status_values.
        """
        for status_name in AggregateStatusType._str_map.keys():
            retrieved_status = statuses[status_name]
            self.assertIn(retrieved_status, status_values,
                          "For %s, expected one of: %s, got: %s" % (
                          AggregateStatusType._str_map[status_name],
                          [DeviceStatusType._str_map[sv] for sv in status_values],
                          DeviceStatusType._str_map[retrieved_status]))

    def _verify_children_statuses(self, child_agg_status, status_values):
        """
        verifies that all child agg status are equal to any of the given values.
        """
        for one_child_agg_status in child_agg_status.itervalues():
            self._verify_statuses(one_child_agg_status, status_values)

    def _verify_initial_statuses(self, aggstatus, child_agg_status, rollup_status):
        """
        verifies:
        - all aggstatus OK
        - all child agg status OK or UNKNOWN
        - all rollup_status are OK
        """
        self._verify_statuses(aggstatus, [DeviceStatusType.STATUS_OK])
        self._verify_children_statuses(child_agg_status,
                                       [DeviceStatusType.STATUS_OK, DeviceStatusType.STATUS_UNKNOWN])
        self._verify_statuses(rollup_status, [DeviceStatusType.STATUS_OK])

    def _verify_all_statuses_OK(self, aggstatus, child_agg_status, rollup_status):
        """
        verifies that all statues are OK.
        """
        self._verify_statuses(aggstatus, [DeviceStatusType.STATUS_OK])
        self._verify_children_statuses(child_agg_status, [DeviceStatusType.STATUS_OK])
        self._verify_statuses(rollup_status, [DeviceStatusType.STATUS_OK])

    def test_platform_status_small_network_3(self):
        #
        # Test of status propagation in a small platform network of 3
        # platforms (one parent and two direct children). No instruments.
        #
        #   LV01B
        #       LJ01B
        #       MJ01B
        #
        # The updates are triggered from direct event publications done on
        # behalf of the leaf platforms.
        #
        self._set_receive_timeout()

        # create the network:
        p_objs = {}
        self.p_root = p_root = self._create_hierarchy("LV01B", p_objs)

        self.assertEquals(3, len(p_objs))
        for platform_id in ["LV01B", "LJ01B", "MJ01B"]:
            self.assertIn(platform_id, p_objs)

        # the two children
        p_LJ01B = p_objs["LJ01B"]
        p_MJ01B = p_objs["MJ01B"]

        #####################################################################
        # start up the network
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._done)
        self._initialize()
        self._go_active()
        self._run()

        #####################################################################
        # get all root statuses
        aggstatus, child_agg_status, rollup_status = self._get_all_root_statuses()

        #####################################################################
        # before any updates in this test verify initial statuses:
        self._verify_initial_statuses(aggstatus, child_agg_status, rollup_status)

        #####################################################################
        # verify the root platform has set its aparam_child_agg_status with
        # all its descendant nodes:
        all_origins = [p_obj.platform_device_id for p_obj in p_objs.values()]
        all_origins.remove(p_root.platform_device_id)
        all_origins = sorted(all_origins)
        child_agg_status_keys = sorted(child_agg_status.keys())
        self.assertEquals(all_origins, child_agg_status_keys)

        #####################################################################
        # do the actual stuff and verifications: we "set" a particular status
        # in a child (that is, via publishing an event on behalf of that
        # child) and then confirm that the event has been propagated
        # to the root to have the corresponding status updated:

        # Note:
        #  - at this point every device in the network has status STATUS_OK.
        #  - we only test cases that trigger an actual change in the root (so
        #    we get the corresponding events for confirmation), so make sure
        #    there are NO consecutive calls to _wait_root_event_and_verify with
        #    the same expected status!
        #  - the root statuses are updated *ONLY* because of status updates
        #    in their two children.

        # -------------------------------------------------------------------
        # start the only event subscriber for this test:
        self._start_agg_status_event_subscriber(p_root)

        # -------------------------------------------------------------------
        # LJ01B publishes a STATUS_WARNING for AGGREGATE_COMMS
        self._expect_from_root(1)
        self._publish_for_child(p_LJ01B,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_WARNING)

        # confirm root gets updated to STATUS_WARNING
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_WARNING)

        # -------------------------------------------------------------------
        # MJ01B publishes a STATUS_CRITICAL for AGGREGATE_COMMS
        self._expect_from_root(1)
        self._publish_for_child(p_MJ01B,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_CRITICAL)

        # confirm root gets updated to STATUS_CRITICAL
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_CRITICAL)

        # -------------------------------------------------------------------
        # MJ01B publishes a STATUS_OK for AGGREGATE_COMMS
        self._expect_from_root(1)
        self._publish_for_child(p_MJ01B,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_OK)

        # confirm root gets updated to STATUS_WARNING because of LJ01B
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_WARNING)

        # -------------------------------------------------------------------
        # LJ01B publishes a STATUS_OK for AGGREGATE_COMMS
        self._expect_from_root(1)
        self._publish_for_child(p_LJ01B,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_OK)

        # confirm root gets updated to STATUS_OK because both children are OK
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_OK)

        # -------------------------------------------------------------------
        # LJ01B publishes a STATUS_UNKNOWN for AGGREGATE_COMMS
        self._expect_from_root(0)
        self._publish_for_child(p_LJ01B,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_UNKNOWN)

        # Note that the root platform should continue in STATUS_OK, but we are
        # not verifying that via reception of event because there's no
        # such event to be published. We verify this with explicit call to
        # the agent to get its rollup_status dict:
        self._verify_with_get_agent(AggregateStatusType.AGGREGATE_COMMS,
                                    DeviceStatusType.STATUS_OK)

        # -------------------------------------------------------------------
        # MJ01B publishes a STATUS_UNKNOWN for AGGREGATE_COMMS
        self._expect_from_root(0)
        self._publish_for_child(p_MJ01B,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_UNKNOWN)

        # now, both children are in STATUS_UNKNOWN (from point of view of the
        # root); but the platform itself remains in OK, so we again verify
        # this via get_agent as there is no event to be published:
        self._verify_with_get_agent(AggregateStatusType.AGGREGATE_COMMS,
                                    DeviceStatusType.STATUS_OK)

    def test_platform_status_small_network_5(self):
        #
        # Test of status propagation in a small network of 5 platforms with
        # multiple levels. No instruments.
        #
        #   LV01A
        #       LJ01A
        #       PC01A
        #           SC01A
        #               SF01A
        #
        # This test is similar to test_platform_status_small_network_3 but
        # here we verify that the multiple level case is handled properly.
        # In particular, note that the root platform will get multiple
        # notifications arising from a single update in a device that
        # is *not* a direct child. This test uses the leaf SF01A, which is 3
        # levels below the root, to trigger the status updates.
        #
        # So, for each status update in that leaf, the root platform should get
        # 3 event notifications:
        #  - one from the leaf itself, SF01A
        #  - one from SC01A
        #  - one from PC01A
        #
        # However, only one of those will actually generate a change in the
        # rollup status of the root, so only one publication will come from it.
        #
        # BTW Note that the order in which the root platform gets those 3
        # events is in general unpredictable.
        #
        # In the tests below, we can verify the expected root status with
        # either the received event or via explicit request via get_agent.
        #
        #
        self._set_receive_timeout()

        # create the network:
        p_objs = {}
        self.p_root = p_root = self._create_hierarchy("LV01A", p_objs)

        self.assertEquals(5, len(p_objs))
        for platform_id in ["LV01A", "LJ01A", "PC01A", "SC01A", "SF01A"]:
            self.assertIn(platform_id, p_objs)

        # the leaf that is 3 levels below the root:
        p_SF01A = p_objs["SF01A"]

        #####################################################################
        # start up the network
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._done)
        self._initialize()
        self._go_active()
        self._run()

        #####################################################################
        # get all root statuses
        aggstatus, child_agg_status, rollup_status = self._get_all_root_statuses()

        #####################################################################
        # before any updates in this test verify initial statuses:
        self._verify_initial_statuses(aggstatus, child_agg_status, rollup_status)

        #####################################################################
        # verify the root platform has set its aparam_child_agg_status with
        # all its descendant nodes:
        all_origins = [p_obj.platform_device_id for p_obj in p_objs.values()]
        all_origins.remove(p_root.platform_device_id)
        all_origins = sorted(all_origins)
        child_agg_status_keys = sorted(child_agg_status.keys())
        self.assertEquals(all_origins, child_agg_status_keys)

        #####################################################################
        # trigger status updates

        # -------------------------------------------------------------------
        # start the only event subscriber for this test:
        self._start_agg_status_event_subscriber(p_root)

        # -------------------------------------------------------------------
        # SF01A publishes a STATUS_CRITICAL for AGGREGATE_COMMS
        self._expect_from_root(1)
        self._publish_for_child(p_SF01A,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_CRITICAL)

        # confirm root gets updated to STATUS_CRITICAL
        self._wait_root_event()
        self._verify_with_get_agent(AggregateStatusType.AGGREGATE_COMMS,
                                    DeviceStatusType.STATUS_CRITICAL)

        # -------------------------------------------------------------------
        # SF01A publishes a STATUS_OK for AGGREGATE_COMMS
        self._expect_from_root(1)
        self._publish_for_child(p_SF01A,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_OK)

        # confirm root gets updated to STATUS_OK
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_OK)

    def test_platform_status_small_network_5_1(self):
        #
        # Test of status propagation in a small network of 5 platforms with
        # multiple levels, and 1 instrument, with all updates triggered from
        # the instrument.
        #
        #   LV01A
        #       LJ01A
        #       PC01A
        #           SC01A
        #               SF01A*
        #
        # This test is similar to test_platform_status_small_network_5 but
        # here also assign an instrument to a platform and trigger all
        # updates from that instrument.
        #
        #
        self._set_receive_timeout()

        # create the network:
        p_objs = {}
        self.p_root = p_root = self._create_hierarchy("LV01A", p_objs)

        self.assertEquals(5, len(p_objs))
        for platform_id in ["LV01A", "LJ01A", "PC01A", "SC01A", "SF01A"]:
            self.assertIn(platform_id, p_objs)

        # the leaf platform that is 3 levels below the root:
        p_SF01A = p_objs["SF01A"]

        # create and assign an instrument to SF01A
        # (the instrument will be 4 levels below the root).
        i_obj = self._create_instrument("SBE37_SIM_01")
        self._assign_instrument_to_platform(i_obj, p_SF01A)

        #####################################################################
        # start up the network
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._done)
        self._initialize()
        self._go_active()
        self._run()

        #####################################################################
        # get all root statuses
        aggstatus, child_agg_status, rollup_status = self._get_all_root_statuses()

        #####################################################################
        # before any updates in this test verify initial statuses:
        self._verify_initial_statuses(aggstatus, child_agg_status, rollup_status)

        #####################################################################
        # verify the root platform has set its aparam_child_agg_status with
        # all its descendant nodes (including the instrument):
        all_origins = [p_obj.platform_device_id for p_obj in p_objs.values()]
        all_origins.remove(p_root.platform_device_id)
        all_origins.append(i_obj.instrument_device_id)
        all_origins = sorted(all_origins)
        child_agg_status_keys = sorted(child_agg_status.keys())
        self.assertEquals(all_origins, child_agg_status_keys)

        #####################################################################
        # trigger status updates from the instrument

        # -------------------------------------------------------------------
        # start the only event subscriber for this test:
        self._start_agg_status_event_subscriber(p_root)

        # -------------------------------------------------------------------
        # instrument publishes a STATUS_CRITICAL for AGGREGATE_COMMS
        self._expect_from_root(1)
        self._publish_for_child(i_obj,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_CRITICAL)

        # confirm root gets updated to STATUS_CRITICAL
        self._wait_root_event()
        self._verify_with_get_agent(AggregateStatusType.AGGREGATE_COMMS,
                                    DeviceStatusType.STATUS_CRITICAL)

        # -------------------------------------------------------------------
        # instrument publishes a STATUS_OK for AGGREGATE_COMMS
        self._expect_from_root(1)
        self._publish_for_child(i_obj,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_OK)

        # confirm root gets updated to STATUS_OK
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_OK)

    def test_platform_status_launch_instruments_first_2_3(self):
        #
        # Test of status propagation in a small network of 2 platforms and
        # 3 instruments, with the instruments launched (including port
        # agents) before the root platform.
        #
        #   MJ01C (with 2 instruments)
        #       LJ01D (with 1 instrument)
        #
        # Right after the root platform is launched, it verifies that all its
        # statuses are updated to OK. Note that this is a scenario in which
        # the updates are not triggered by the event publications done by the
        # instruments because those publications happen at a time when the
        # platform have not been launched yet. Rather, during the launch of
        # the platforms, they retrieve the statuses of their children to
        # update the corresponding statuses. This capability was initially
        # added to support UI testing with instruments whose port agents need
        # to be manually launched.
        #
        # The test also includes some explicitly triggered updates via
        # publication on behalf of the instruments.
        #
        #
        self._set_receive_timeout()

        # create the network:
        p_objs = {}
        self.p_root = p_root = self._create_hierarchy("MJ01C", p_objs)

        self.assertEquals(2, len(p_objs))
        for platform_id in ["MJ01C", "LJ01D"]:
            self.assertIn(platform_id, p_objs)

        # the sub-platform:
        p_LJ01D = p_objs["LJ01D"]

        #####################################################################
        # create and launch instruments/port_agents:
        instrs = []
        for instr_key in ["SBE37_SIM_01", "SBE37_SIM_02", "SBE37_SIM_03"]:
            i_obj = self._create_instrument(instr_key, start_port_agent=False)
            ia_client = self._start_instrument(i_obj)
            self.addCleanup(self._stop_instrument, i_obj)
            instrs.append(i_obj)
            log.debug("started instrument %s", instr_key)

        #####################################################################
        # assign instruments to platforms:
        # 2 instruments to root:
        self._assign_instrument_to_platform(instrs[0], p_root)
        self._assign_instrument_to_platform(instrs[1], p_root)
        # 1 instrument to sub-platform LJ01D:
        self._assign_instrument_to_platform(instrs[2], p_LJ01D)

        #####################################################################
        # start up the root platform
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._shutdown)
        log.debug("started root platform")

        #####################################################################
        # get all root statuses
        aggstatus, child_agg_status, rollup_status = self._get_all_root_statuses()
        # this logs out:
        #                                    AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        # 3bac82f2fe2f4136a0ebbe4207ab3747 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        # 14c6d93a196d4d2fa4a4ee19ff945888 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        # 51e37e7c8a684f2dbb5dcc0e9cc758a4 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        # 04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                        aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                    rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK

        publish_event_for_diagnostics()
        # this makes the status manager log out:
        # 2013-05-18 09:17:06,043 INFO Dummy-218 ion.agents.platform.status_manager:792 'MJ01C': (38389854b6664da08178b3b0f1d33797) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         3bac82f2fe2f4136a0ebbe4207ab3747 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         14c6d93a196d4d2fa4a4ee19ff945888 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         51e37e7c8a684f2dbb5dcc0e9cc758a4 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #
        #
        # 2013-05-18 09:17:06,045 INFO Dummy-219 ion.agents.platform.status_manager:792 'LJ01D': (14c6d93a196d4d2fa4a4ee19ff945888) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK

        #####################################################################
        # verify the root platform has set its aparam_child_agg_status with
        # all its descendant nodes (including all instruments):
        all_origins = [p_obj.platform_device_id for p_obj in p_objs.values()]
        all_origins.remove(p_root.platform_device_id)
        all_origins.extend(i_obj.instrument_device_id for i_obj in instrs)
        all_origins = sorted(all_origins)
        child_agg_status_keys = sorted(child_agg_status.keys())
        self.assertEquals(all_origins, child_agg_status_keys)

        #####################################################################
        # all statuses must be OK (in particular for the instrument children
        self._verify_all_statuses_OK(aggstatus, child_agg_status, rollup_status)

        #####################################################################
        # trigger some status updates from the instruments and do
        # corresponding verifications against the root platform.
        # Note that the sub-platform also should get properly updated but
        # this test doesn't do these verifications.

        self._start_agg_status_event_subscriber(p_root)

        # -------------------------------------------------------------------
        # instrs[0] publishes a STATUS_CRITICAL for AGGREGATE_COMMS
        self._expect_from_root(1)
        self._publish_for_child(instrs[0],
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_CRITICAL)

        # confirm root gets updated to STATUS_CRITICAL
        self._wait_root_event()
        self._verify_with_get_agent(AggregateStatusType.AGGREGATE_COMMS,
                                    DeviceStatusType.STATUS_CRITICAL)
        log.debug("after AGGREGATE_COMMS <- STATUS_CRITICAL on behalf of instr[0]")
        publish_event_for_diagnostics()
        # 2013-05-18 09:17:06,111 INFO Dummy-218 ion.agents.platform.status_manager:792 'MJ01C': (38389854b6664da08178b3b0f1d33797) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         3bac82f2fe2f4136a0ebbe4207ab3747 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         14c6d93a196d4d2fa4a4ee19ff945888 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         51e37e7c8a684f2dbb5dcc0e9cc758a4 : STATUS_CRITICAL     STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_CRITICAL     STATUS_OK           STATUS_OK           STATUS_OK
        #
        #
        # 2013-05-18 09:17:06,112 INFO Dummy-219 ion.agents.platform.status_manager:792 'LJ01D': (14c6d93a196d4d2fa4a4ee19ff945888) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK

        # -------------------------------------------------------------------
        # instrs[0] publishes a STATUS_WARNING for AGGREGATE_DATA
        self._expect_from_root(1)
        self._publish_for_child(instrs[0],
                                AggregateStatusType.AGGREGATE_DATA,
                                DeviceStatusType.STATUS_WARNING)

        # confirm root gets updated to STATUS_WARNING
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_DATA,
                                         DeviceStatusType.STATUS_WARNING)
        log.debug("after AGGREGATE_DATA <- STATUS_WARNING on behalf of instr[0]")
        publish_event_for_diagnostics()
        # 2013-05-18 09:17:06,149 INFO Dummy-218 ion.agents.platform.status_manager:792 'MJ01C': (38389854b6664da08178b3b0f1d33797) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         3bac82f2fe2f4136a0ebbe4207ab3747 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         14c6d93a196d4d2fa4a4ee19ff945888 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         51e37e7c8a684f2dbb5dcc0e9cc758a4 : STATUS_CRITICAL     STATUS_WARNING      STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_CRITICAL     STATUS_WARNING      STATUS_OK           STATUS_OK
        #
        #
        # 2013-05-18 09:17:06,150 INFO Dummy-219 ion.agents.platform.status_manager:792 'LJ01D': (14c6d93a196d4d2fa4a4ee19ff945888) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK

        # -------------------------------------------------------------------
        # instrs[1] publishes a STATUS_WARNING for AGGREGATE_POWER
        self._expect_from_root(1)
        self._publish_for_child(instrs[1],
                                AggregateStatusType.AGGREGATE_POWER,
                                DeviceStatusType.STATUS_WARNING)

        # confirm root gets updated to STATUS_WARNING
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_POWER,
                                         DeviceStatusType.STATUS_WARNING)
        log.debug("after AGGREGATE_POWER <- STATUS_WARNING on behalf of instr[1]")
        publish_event_for_diagnostics()
        # 2013-05-18 09:17:06,186 INFO Dummy-219 ion.agents.platform.status_manager:792 'LJ01D': (14c6d93a196d4d2fa4a4ee19ff945888) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #
        #
        # 2013-05-18 09:17:06,187 INFO Dummy-218 ion.agents.platform.status_manager:792 'MJ01C': (38389854b6664da08178b3b0f1d33797) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         3bac82f2fe2f4136a0ebbe4207ab3747 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_WARNING
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         14c6d93a196d4d2fa4a4ee19ff945888 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #         51e37e7c8a684f2dbb5dcc0e9cc758a4 : STATUS_CRITICAL     STATUS_WARNING      STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_CRITICAL     STATUS_WARNING      STATUS_OK           STATUS_WARNING

        # -------------------------------------------------------------------
        # instrs[2] publishes a AGGREGATE_LOCATION for STATUS_CRITICAL
        self._expect_from_root(1)
        self._publish_for_child(instrs[2],
                                AggregateStatusType.AGGREGATE_LOCATION,
                                DeviceStatusType.STATUS_CRITICAL)

        # confirm root gets updated to STATUS_WARNING
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_LOCATION,
                                         DeviceStatusType.STATUS_CRITICAL)
        log.debug("after AGGREGATE_LOCATION <- STATUS_CRITICAL on behalf of instr[2]")
        publish_event_for_diagnostics()
        # 2013-05-18 09:17:06,233 INFO Dummy-219 ion.agents.platform.status_manager:792 'LJ01D': (14c6d93a196d4d2fa4a4ee19ff945888) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_CRITICAL     STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_CRITICAL     STATUS_OK
        #
        #
        # 2013-05-18 09:17:06,234 INFO Dummy-218 ion.agents.platform.status_manager:792 'MJ01C': (38389854b6664da08178b3b0f1d33797) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         3bac82f2fe2f4136a0ebbe4207ab3747 : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_WARNING
        #         04322d41ca9a414e8ef41729fec539b0 : STATUS_OK           STATUS_OK           STATUS_CRITICAL     STATUS_OK
        #         14c6d93a196d4d2fa4a4ee19ff945888 : STATUS_OK           STATUS_OK           STATUS_CRITICAL     STATUS_OK
        #         51e37e7c8a684f2dbb5dcc0e9cc758a4 : STATUS_CRITICAL     STATUS_WARNING      STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_CRITICAL     STATUS_WARNING      STATUS_CRITICAL     STATUS_WARNING

        #####################################################################
        # Done.
        # For orchestration with instruments that have been launched before
        # the platform, see test_platform_launch.py, in particular
        # test_instrument_first_then_platform.

    def _start_subscriber_process_lifecycle_event(self, resource_id, state):
        """
        The resource_id is mapped to corresponding process ID,
        which is used as the origin for the subscription.
        """
        async_event_result, events_received = AsyncResult(), []

        def consume_event(evt, *args, **kwargs):

            if evt.type_ != "ProcessLifecycleEvent":
                log.trace("ignoring event type %r. Only handle ProcessLifecycleEvent directly.",
                          evt.type_)
                return

            log.debug("[.]Event subscriber received %r: origin=%r origin_type=%r state=%r(%s)",
                      evt.type_,
                      evt.origin,
                      evt.origin_type,
                      ProcessStateEnum._str_map[evt.state], evt.state)

            if evt.state != state:
                return

            if not resource_id in evt.origin:
                log.trace("ignoring event from origin %r. Expecting an origin "
                          "containing %r.",
                          evt.origin, resource_id)
                return

            events_received.append(evt)
            async_event_result.set(evt)

        # note: cannot indicate:
        #     origin = ResourceAgentClient._get_agent_process_id(resource_id)
        # because the process hasn't been launched yet, so we don't know the
        # process_id at this point.
        # Instead, subscribe without indicating origin, but check that the
        # resource_id is contained in the received origin.

        sub = EventSubscriber(event_type="ProcessLifecycleEvent",
                              origin_type='DispatchedProcess',
                              callback=consume_event)
        sub.start()
        log.info("[.]registered ProcessLifecycleEvent subscriber: resource_id=%r", resource_id)

        self._event_subscribers.append(sub)
        sub._ready_event.wait(timeout=30)

        return async_event_result, events_received, sub

    def test_platform_status_terminate_and_restart_instrument_1_1(self):
        #
        # Tests reaction of a platform upon termination and re-start of its
        # associated instrument.
        #
        #   platform='LJ01D'
        #   instrument=SBE37_SIM_01
        #
        self._set_receive_timeout()

        # create the network:
        self.p_root = p_root = self._create_platform('LJ01D')

        # create and assign an instrument to LJ01D
        i_obj = self._create_instrument("SBE37_SIM_01")
        self._assign_instrument_to_platform(i_obj, p_root)
        log.debug("OOIION-1077 instrument assigned: %s", i_obj)

        #####################################################################
        # prepare to verify expected ProcessLifecycleEvent is generated when
        # the instrument process gets running for the very first time:
        async_event_result, events_received, sub = \
            self._start_subscriber_process_lifecycle_event(
                i_obj.instrument_device_id,
                ProcessStateEnum.RUNNING)

        #####################################################################
        # start up the network
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._done)
        self._initialize()
        self._go_active()
        self._run()

        log.debug("OOIION-1077 waiting for ProcessLifecycleEvent RUNNING")
        async_event_result.get(timeout=30) #CFG.endpoint.receive.timeout)
        self.assertEquals(len(events_received), 1)
        log.debug("OOIION-1077 waiting for ProcessLifecycleEvent RUNNING - Got it!")
        sub.stop()

        #####################################################################
        # get all root statuses
        aggstatus, child_agg_status, rollup_status = self._get_all_root_statuses()

        log.debug("OOIION-1077 publish_event_for_diagnostics")
        publish_event_for_diagnostics()
        # log shows:
        # ion.agents.platform.status_manager:952 'LJ01D'/RESOURCE_AGENT_STATE_COMMAND: (09029e6423d345fa972f0d03c74b1424) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         7147082cd48c405ea8536327d2f97d3f : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #
        #                     invalidated_children : []

        #####################################################################
        # before any updates in this test verify initial statuses are all OK:
        self._verify_all_statuses_OK(aggstatus, child_agg_status, rollup_status)

        #####################################################################
        # verify the root platform has set its aparam_child_agg_status with
        # the assigned instrument:
        all_origins = [i_obj.instrument_device_id]
        child_agg_status_keys = sorted(child_agg_status.keys())
        self.assertEquals(all_origins, child_agg_status_keys)

        # Now, the core of this test follows.

        #####################################################################
        # terminate instrument
        #####################################################################

        # before the termination of the instrument:
        # - prepare to verify the expected ProcessLifecycleEvent is generated:
        async_event_result, events_received, sub = \
            self._start_subscriber_process_lifecycle_event(
                i_obj.instrument_device_id,
                ProcessStateEnum.TERMINATED)

        # now terminate the instrument:
        log.debug("OOIION-1077 terminating instrument: %s", i_obj)

        self._stop_instrument(i_obj, use_ims=False)

        log.debug("OOIION-1077 waiting for ProcessLifecycleEvent TERMINATED")
        async_event_result.get(timeout=CFG.endpoint.receive.timeout)
        self.assertEquals(len(events_received), 1)
        log.debug("OOIION-1077 waiting for ProcessLifecycleEvent TERMINATED - Got it!")
        sub.stop()

        # log shows:
        # ion.agents.platform.status_manager:409 'LJ01D': OOIION-1077  _got_process_lifecycle_event: pid='InstrumentAgent_7147082cd48c405ea8536327d2f97d3f8715261278b94871bdf88b153d6e8df6' origin='7147082cd48c405ea8536327d2f97d3f' state='TERMINATED'(6)

        # verify the root's child_status are all UNKNOWN
        # Note: no event is going to be generated from the platform because
        # its rollup_status is *not* changing.
        # So, we have to wait for a bit to let the updates propagate:
        sleep(15)
        log.debug("OOIION-1077 publish_event_for_diagnostics after instrument termination")
        publish_event_for_diagnostics()
        # log shows:
        # ion.agents.platform.status_manager:952 'LJ01D'/RESOURCE_AGENT_STATE_COMMAND: (09029e6423d345fa972f0d03c74b1424) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         7147082cd48c405ea8536327d2f97d3f : STATUS_UNKNOWN      STATUS_UNKNOWN      STATUS_UNKNOWN      STATUS_UNKNOWN
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #
        #                     invalidated_children : ['7147082cd48c405ea8536327d2f97d3f']

        # and do verification that the child_agg_status are all UNKNOWN:
        _, child_agg_status, _ = self._get_all_root_statuses()
        self._verify_statuses(child_agg_status[i_obj.instrument_device_id],
                              [DeviceStatusType.STATUS_UNKNOWN])

        #####################################################################
        # re-start instrument
        #####################################################################

        # NOTE: platform agents don't rely on ProcessLifecycleEvent
        # RUNNING events to restore information about re-started children;
        # rather they just react to regular status updates to eventually
        # re-validate them.

        log.debug("OOIION-1077 re-starting instrument: %s", i_obj)

        ia_client = self._start_instrument(i_obj, use_ims=False)

        from pyon.agent.agent import ResourceAgentClient
        pid = ResourceAgentClient._get_agent_process_id(i_obj.instrument_device_id)
        log.debug("OOIION-1077 instrument re-started: rid=%r", i_obj.instrument_device_id)
        log.debug("OOIION-1077 instrument re-started: pid=%r", pid)

        # again, have to wait for a bit to let the updates propagate:
        sleep(15)

        # log shows:
        # ion.agents.platform.platform_agent:1114 'LJ01D': OOIION-1077 _child_running: revalidated child with resource_id='7147082cd48c405ea8536327d2f97d3f', new pid='processa78a63ea70c649809cc3441d6bfddf3d'

        log.debug("OOIION-1077 publish_event_for_diagnostics after instrument re-start")
        publish_event_for_diagnostics()
        # log shows:
        # ion.agents.platform.status_manager:952 'LJ01D'/RESOURCE_AGENT_STATE_COMMAND: (09029e6423d345fa972f0d03c74b1424) status report triggered by diagnostic event:
        #                                            AGGREGATE_COMMS     AGGREGATE_DATA      AGGREGATE_LOCATION  AGGREGATE_POWER
        #         7147082cd48c405ea8536327d2f97d3f : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                                aggstatus : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #                            rollup_status : STATUS_OK           STATUS_OK           STATUS_OK           STATUS_OK
        #
        #                     invalidated_children : []

        # And do verification that the child_agg_status are all OK again:
        # NOTE: this assumes that, once running again, the instrument's
        # aggstatus are in turn back to OK.
        _, child_agg_status, _ = self._get_all_root_statuses()
        self._verify_statuses(child_agg_status[i_obj.instrument_device_id],
                              [DeviceStatusType.STATUS_OK])

        #####################################################################
        # move the instrument to the COMMAND state where it was when terminated
        # so the shutdown sequence in the root platform completes fine.
        # This also verifies that we are able to continue interacting with
        # the instrument after the re-start.
        #####################################################################

        cmd = AgentCommand(command=InstrumentAgentEvent.INITIALIZE)
        retval = ia_client.execute_agent(cmd, timeout=CFG.endpoint.receive.timeout)
        log.debug("OOIION-1077 INITIALIZE to instrument returned: %s", retval)

        cmd = AgentCommand(command=InstrumentAgentEvent.GO_ACTIVE)
        retval = ia_client.execute_agent(cmd, timeout=CFG.endpoint.receive.timeout)
        log.debug("OOIION-1077 GO_ACTIVE to instrument returned: %s", retval)

        cmd = AgentCommand(command=InstrumentAgentEvent.RUN)
        retval = ia_client.execute_agent(cmd, timeout=CFG.endpoint.receive.timeout)
        log.debug("OOIION-1077 RUN to instrument returned: %s", retval)

        # verify instrument is in COMMAND:
        instr_state = ia_client.get_agent_state()
        log.debug("instrument state: %s", instr_state)
        self.assertEquals(InstrumentAgentState.COMMAND, instr_state)

        # (we can also move the last _verify_statuses call above to this point.)
