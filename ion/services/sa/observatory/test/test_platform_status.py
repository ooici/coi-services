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

# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_status.py:Test.test_platform_status_small_network

from pyon.public import log

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

from pyon.event.event import EventPublisher
from pyon.event.event import EventSubscriber

from interface.objects import AggregateStatusType
from interface.objects import DeviceStatusType

from gevent.event import AsyncResult

from mock import patch
from pyon.public import CFG


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class Test(BaseIntTestPlatform):

    def setUp(self):
        super(Test, self).setUp()
        self._event_publisher = EventPublisher()
        self._last_checked_status = None

    def _start_agg_status_event_subscriber(self, p_root):
        """
        Start the event subscriber to the given root platform. Upon reception
        of event, the callback only sets the async result with the event.
        """

        event_type = "DeviceAggregateStatusEvent"

        def consume_event(evt, *args, **kwargs):
            self._async_result.set(evt)

        sub = EventSubscriber(event_type=event_type,
                              origin=p_root.platform_device_id,
                              callback=consume_event)

        sub.start()
        self._data_subscribers.append(sub)
        sub._ready_event.wait(timeout=CFG.endpoint.receive.timeout)

        log.debug("registered for DeviceAggregateStatusEvent")

    def _publish_for_child(self, origin, status_name, status):
        """
        Publishes a DeviceAggregateStatusEvent from the given origin

        NOTE that we just publish on behalf of the child, but the statuses in
        the child itself are *not* set. This is OK for these tests; we just
        need that child's parent to react to the event.
        """

        # create new AsyncResult for the subsequent event reception:
        self._async_result = AsyncResult()

        # create and publish event from the given origin:
        evt = dict(event_type='DeviceAggregateStatusEvent',
                   origin_type="PlatformDevice",
                   origin=origin,
                   description="Fake event",
                   status_name=status_name,
                   status=status)

        log.debug("publishing for child %r: evt=%s", origin, evt)
        self._event_publisher.publish_event(**evt)

    def _wait_root_event_and_verify(self, status_name, status):
        """
        Waits for event from root and verifies that the root status has been
        updated as expected.

        @param status_name   Entry in AggregateStatusType
        @param status        Entry in DeviceStatusType
        """

        # verify we are not checking the same status twice in a row:
        self.assertNotEquals(self._last_checked_status, (status_name, status),
                             "The same status cannot be checked twice in a row "
                             "because there won't be any event going to be "
                             "published in the second case. Fix the test!")
        self._last_checked_status = (status_name, status)

        # wait for event from root:
        root_evt = self._async_result.get(timeout=CFG.endpoint.receive.timeout)

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

    def test_platform_status_small_network(self):
        #
        # Test of status propagation in a small platform network (no instruments)
        #

        # create the network:
        p_objs = {}
        self.p_root = p_root = self._create_hierarchy("LV01B", p_objs)
        #   LV01B
        #       LJ01B
        #       MJ01B

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
        self._initialize()
        self._go_active()
        self._run()

        #####################################################################
        # do the actual stuff and verifications: we "set" a particular status
        # in a child (that is, via publishing an event on behalf of that
        # child) and then confirm that the event has been propagated
        # to the root to have the corresponding status updated:

        # Note:
        #  - every device in the network starts in STATUS_UNKNOWN
        #  - we only test cases that trigger an actual change in the root (so
        #    we get the corresponding events for confirmation), so make sure
        #    there are NO consecutive calls to _wait_root_event_and_verify with
        #    the same expected status!
        #  - the root statuses are updated *ONLY* because of status updates
        #    in their two children. When other elements are considered (in
        #    particular, platform attributes), then these tests will need to be
        #    adjusted.

        # -------------------------------------------------------------------
        # start the only event subscriber for this test:
        self._start_agg_status_event_subscriber(p_root)

        # -------------------------------------------------------------------
        # LJ01B publishes a STATUS_WARNING for AGGREGATE_COMMS
        self._publish_for_child(p_LJ01B.platform_device_id,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_WARNING)

        # confirm root gets updated to STATUS_WARNING
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_WARNING)

        # -------------------------------------------------------------------
        # MJ01B publishes a STATUS_CRITICAL for AGGREGATE_COMMS
        self._publish_for_child(p_MJ01B.platform_device_id,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_CRITICAL)

        # confirm root gets updated to STATUS_CRITICAL
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_CRITICAL)

        # -------------------------------------------------------------------
        # MJ01B publishes a STATUS_OK for AGGREGATE_COMMS
        self._publish_for_child(p_MJ01B.platform_device_id,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_OK)

        # confirm root gets updated to STATUS_WARNING because of LJ01B
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_WARNING)

        # -------------------------------------------------------------------
        # LJ01B publishes a STATUS_OK for AGGREGATE_COMMS
        self._publish_for_child(p_LJ01B.platform_device_id,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_OK)

        # confirm root gets updated to STATUS_OK because both children are OK
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_OK)

        # -------------------------------------------------------------------
        # LJ01B publishes a STATUS_UNKNOWN for AGGREGATE_COMMS
        self._publish_for_child(p_LJ01B.platform_device_id,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_UNKNOWN)

        # Note that the root platform should continue in STATUS_OK, but we are
        # not verifying that via reception of event because there's no
        # such event to be published. We verify this with explicit call to
        # the agent to get its aggstatus dict:
        aggstatus = self._pa_client.get_agent(['aggstatus'])['aggstatus']
        self.assertEquals(aggstatus[AggregateStatusType.AGGREGATE_COMMS],
                          DeviceStatusType.STATUS_OK)

        # -------------------------------------------------------------------
        # MJ01B publishes a STATUS_UNKNOWN for AGGREGATE_COMMS
        self._publish_for_child(p_MJ01B.platform_device_id,
                                AggregateStatusType.AGGREGATE_COMMS,
                                DeviceStatusType.STATUS_UNKNOWN)

        # now, both children are in STATUS_UNKNOWN (from point of view of the
        # root), so confirm root gets updated to STATUS_UNKNOWN;
        self._wait_root_event_and_verify(AggregateStatusType.AGGREGATE_COMMS,
                                         DeviceStatusType.STATUS_UNKNOWN)

        #####################################################################
        # done
        self._go_inactive()
        self._reset()
