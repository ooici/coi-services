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

from pyon.agent.agent import ResourceAgentClient
from ion.agents.platform.test.base_test_platform_agent_with_rsn import FakeProcess

from pyon.event.event import EventPublisher

from interface.objects import AggregateStatusType
from interface.objects import DeviceStatusType

import gevent

from mock import patch
from pyon.public import CFG


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class Test(BaseIntTestPlatform):

    def setUp(self):
        super(Test, self).setUp()
        self._event_publisher = EventPublisher()

    def _create_resource_client(self, resource_id):
        ra_client = ResourceAgentClient(resource_id, process=FakeProcess())
        return ra_client

    def _set_rollup_statuses(self, ra_client, rollup_status):

        log.debug("TRS setting rollup_status in child: %s", rollup_status)
        ra_client.set_agent({'rollup_status': rollup_status})

        # sanity check: verify the statues were actually set:
        ret_rollup_status = ra_client.get_agent(['rollup_status'])['rollup_status']
        self.assertEquals(ret_rollup_status, rollup_status)

    def test_platform_status_small_network(self):

        p_objs = {}
        p_root = self._create_hierarchy("LV01B", p_objs)
        #   LV01B
        #       LJ01B
        #       MJ01B

        self.assertEquals(3, len(p_objs))
        for platform_id in ["LV01B", "LJ01B", "MJ01B"]:
            self.assertIn(platform_id, p_objs)

        #####################################################################
        # todo assign some instruments
        # ...

        #####################################################################
        # start up the network
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self._initialize()
        self._go_active()
        self._run()

        #####################################################################
        # specific preparations:

        pa_client_root = self._create_resource_client(p_root.platform_device_id)
        p_LJ01B = p_objs["LJ01B"]
        pa_client_LJ01B = self._create_resource_client(p_LJ01B.platform_device_id)

        p_MJ01B = p_objs["MJ01B"]
        pa_client_MJ01B = self._create_resource_client(p_MJ01B.platform_device_id)

        #####################################################################
        # set rollup_status for the children, initially all OK

        rollup_status = {
            AggregateStatusType.AGGREGATE_COMMS:    DeviceStatusType.STATUS_OK,
            AggregateStatusType.AGGREGATE_DATA:     DeviceStatusType.STATUS_OK,
            AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_OK,
            AggregateStatusType.AGGREGATE_POWER:    DeviceStatusType.STATUS_OK
        }

        self._set_rollup_statuses(pa_client_LJ01B, rollup_status)
        self._set_rollup_statuses(pa_client_MJ01B, rollup_status)

        # Note that the above does NOT trigger any event publications. It
        # would be interesting to see whether the PA should react to those
        # settings to do the publications.
        # The actual publication in this test happens below,
        # which simulates the PA doing that publication.

        #####################################################################
        # do the actual stuff and verifications

        def publish_and_verify_root_status(origin, expected_rollup_status):
            """
            publishes a DeviceStatusEvent with a gioven child as the originator
            to trigger the updates, and then verifies the expected rollup
            statuses in the root.
            """
            evt = dict(event_type='DeviceStatusEvent',
                       origin_type="PlatformDevice",
                       origin=origin,
                       description="TRS Fake event to test rollup status")
            log.debug("TRS publishing for child 'LJ01B': %s", evt)
            self._event_publisher.publish_event(**evt)

            # TODO set a subscriber to get publication from root.
            # for now, just sleeping for a bit, which is a weak mechanism of course.
            gevent.sleep(5)

            # retrieve root status
            root_rollup_status = pa_client_root.get_agent(['rollup_status'])['rollup_status']
            log.debug("TRS root rollup_status: %s", root_rollup_status)

            # verify expected statuses at the root:
            for key in AggregateStatusType._value_map.values():
                self.assertEquals(root_rollup_status[key],
                                  expected_rollup_status[key])

        # ----------------------------------
        # verify OKs at the root
        publish_and_verify_root_status(p_LJ01B.platform_device_id, rollup_status)

        # ----------------------------------
        # verify with some variations
        rollup_status = {
            AggregateStatusType.AGGREGATE_COMMS:    DeviceStatusType.STATUS_OK,
            AggregateStatusType.AGGREGATE_DATA:     DeviceStatusType.STATUS_WARNING,
            AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_CRITICAL,
            AggregateStatusType.AGGREGATE_POWER:    DeviceStatusType.STATUS_OK
        }
        self._set_rollup_statuses(pa_client_LJ01B, rollup_status)
        publish_and_verify_root_status(p_LJ01B.platform_device_id, rollup_status)

        # ----------------------------------
        # verify with all back to OK
        rollup_status = {
            AggregateStatusType.AGGREGATE_COMMS:    DeviceStatusType.STATUS_OK,
            AggregateStatusType.AGGREGATE_DATA:     DeviceStatusType.STATUS_OK,
            AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_OK,
            AggregateStatusType.AGGREGATE_POWER:    DeviceStatusType.STATUS_OK
        }
        self._set_rollup_statuses(pa_client_LJ01B, rollup_status)
        publish_and_verify_root_status(p_LJ01B.platform_device_id, rollup_status)

        # ----------------------------------
        # verify with all STATUS_CRITICAL
        # In this case, use the other child p_MJ01B
        rollup_status = {
            AggregateStatusType.AGGREGATE_COMMS:    DeviceStatusType.STATUS_CRITICAL,
            AggregateStatusType.AGGREGATE_DATA:     DeviceStatusType.STATUS_CRITICAL,
            AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_CRITICAL,
            AggregateStatusType.AGGREGATE_POWER:    DeviceStatusType.STATUS_CRITICAL
        }
        self._set_rollup_statuses(pa_client_LJ01B, rollup_status)
        publish_and_verify_root_status(p_MJ01B.platform_device_id, rollup_status)

        #####################################################################
        # done
        self._go_inactive()
        self._reset()
