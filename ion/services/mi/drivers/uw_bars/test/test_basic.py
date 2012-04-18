#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

"""
Unit tests for the basic bars module.
"""


import ion.services.mi.drivers.uw_bars.bars as bars


from unittest import TestCase
from nose.plugins.attrib import attr


@unittest.skip('Need to align.')
@attr('UNIT', group='mi')
class BasicBarsTest(TestCase):
    """
    Unit tests for the basic bars module.
    """

    def test_get_cycle_time(self):
        val = bars.get_cycle_time(bars.SYSTEM_PARAMETER_MENU)
        self.assertEqual(val, '20 Seconds')

    def test_get_verbose_vs_data_only(self):
        val = bars.get_verbose_vs_data_only(bars.SYSTEM_PARAMETER_MENU)
        self.assertEqual(val, 'Data Only')

    def test_get_system_date_and_time(self):
        res = bars.get_system_date_and_time(bars.ADJUST_SYSTEM_CLOCK_MENU)
        self.assertEqual(res, ('02/09/12', '14:21:06'))

    def test_get_power_statuses(self):
        res = bars.get_power_statuses(bars.SENSOR_POWER_CONTROL_MENU)
        self.assertEqual(res, ('On', 'On', 'On', 'On', 'On'))
