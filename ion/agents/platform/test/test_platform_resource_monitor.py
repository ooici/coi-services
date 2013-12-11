#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_platform_resource_monitor
@file    ion/agents/platform/test/test_platform_resource_monitor.py
@author  Carlos Rueda
@brief   Unit test cases related with platform resource monitoring
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

#
# bin/nosetests -v ion/agents/platform/test/test_platform_resource_monitor.py:Test.test_attr_grouping_by_similar_rate
# bin/nosetests -v ion/agents/platform/test/test_platform_resource_monitor.py:Test.test_aggregation_for_granule


from pyon.public import log
from nose.plugins.attrib import attr
from pyon.util.unit_test import IonUnitTestCase

from ion.agents.platform.platform_resource_monitor import PlatformResourceMonitor
from ion.agents.platform.util.network_util import NetworkUtil

import pprint


@attr('UNIT', group='sa')
class Test(IonUnitTestCase):
    """
    @note These tests depend on definitions in network.yml
    """

    def setUp(self):
        self._pp = pprint.PrettyPrinter()
        yaml_filename = 'ion/agents/platform/rsn/simulator/network.yml'
        self._ndef = NetworkUtil.deserialize_network_definition(file(yaml_filename))

    def _get_attribute_values_dummy(self, attr_names, from_time):
        return {}

    def evt_recv(self, driver_event):
        log.debug('evt_recv: %s', driver_event)

        self._driver_event = driver_event

    def _get_attrs(self, platform_id):
        pnode = self._ndef.pnodes[platform_id]

        platform_attributes = dict((attr.attr_id, attr.defn) for attr
                                   in pnode.attrs.itervalues())
        log.debug("%r: platform_attributes: %s",
                  platform_id, self._pp.pformat(platform_attributes))
        return platform_attributes

    def _verify_attr_grouping(self, platform_id, expected_groups):
        attrs = self._get_attrs(platform_id)

        prm = PlatformResourceMonitor(
            platform_id, attrs,
            self._get_attribute_values_dummy, self.evt_recv)

        groups = prm._group_by_monitoring_rate()

        log.debug("groups=\n%s", self._pp.pformat(groups))

        self.assertEquals(len(expected_groups), len(groups))

        for k in expected_groups:
            self.assertIn(k, groups)
            attr_ids = set(d['attr_id'] for d in groups[k])
            self.assertEquals(set(expected_groups[k]), attr_ids)

    def test_attr_grouping_by_similar_rate(self):

        self._verify_attr_grouping(
            platform_id="LJ01D",
            expected_groups={
                2.5 : ["input_voltage|0"],
                4.0 : ["MVPC_pressure_1|0", "MVPC_temperature|0"],
                5.0 : ["input_bus_current|0"],
            }
        )

        self._verify_attr_grouping(
            platform_id="Node1D",
            expected_groups={
                5.0 : ["input_voltage|0", "input_bus_current|0"],
                10.0 : ["MVPC_pressure_1|0", "MVPC_temperature|0"],
            }
        )

        self._verify_attr_grouping(
            platform_id="MJ01C",
            expected_groups={
                2.5 : ["input_voltage|0"],
                5.0 : ["input_bus_current|0"],
                4.0 : ["MVPC_pressure_1|0", "MVPC_temperature|0"],
            }
        )

        self._verify_attr_grouping(
            platform_id="ShoreStation",
            expected_groups={
                5.0 : ["ShoreStation_attr_1|0", "ShoreStation_attr_2|0"],
            }
        )

    def test_aggregation_for_granule(self):
        platform_id = "LJ01D"
        attrs = self._get_attrs(platform_id)

        prm = PlatformResourceMonitor(
            platform_id, attrs,
            self._get_attribute_values_dummy, self.evt_recv)

        # set the buffers simulating retrieved data:
        prm._init_buffers()
        bufs = prm._buffers
        # each entry is a list of (val, ts) pairs:
        # note the missing pairs, which should be filled with (None, ts), see below
        bufs["input_voltage"]     = [(1000, 9000), (1001, 9001), (1002, 9002)]
        bufs["input_bus_current"] = [(2000, 9000),               (2002, 9002)]
        bufs["MVPC_temperature"]  = [              (3000, 9001)]

        # run the key method under testing:
        prm._dispatch_publication()

        self.assertTrue(self._driver_event, "evt_recv callback must have been called")
        driver_event = self._driver_event

        # buffers must have been re-init'ed:
        for attr_id in attrs:
            self.assertEquals([], prm._buffers[attr_id])

        # this vals_dict is used by PlatformAgent to do the final creation of
        # the granule to be published
        vals_dict = driver_event.vals_dict

        # verify the attributes that must be present:
        self.assertIn("input_voltage", vals_dict)
        self.assertIn("input_bus_current", vals_dict)
        self.assertIn("MVPC_temperature", vals_dict)

        # verify the attribute that must *not* be present:
        # self.assertNotIn("MVPC_pressure_1", vals_dict)

        # verify the expected aligned values so they are on a common set of
        # timestamps:

        input_voltage     = vals_dict["input_voltage"]
        input_bus_current = vals_dict["input_bus_current"]
        MVPC_temperature  = vals_dict["MVPC_temperature"]

        self.assertEquals(
            [(1000, 9000), (1001, 9001), (1002, 9002)],
            input_voltage
        )

        # note the None entries that must have been created

        self.assertEquals(
            [(2000, 9000),  (None, 9001), (2002, 9002)],
            input_bus_current
        )

        self.assertEquals(
            [(None, 9000),  (3000, 9001), (None, 9002)],
            MVPC_temperature
        )
