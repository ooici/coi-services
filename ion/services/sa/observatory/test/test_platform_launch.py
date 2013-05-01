#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_launch
@file    ion/services/sa/observatory/test/test_platform_launch.py
@author  Carlos Rueda, Maurice Manning, Ian Katz
@brief   Test cases for launching and shutting down a platform agent network
"""

__author__ = 'Carlos Rueda, Maurice Manning, Ian Katz'
__license__ = 'Apache 2.0'

#
# Base preparations and construction of the platform topology are provided by
# the base class BaseTestPlatform. The focus here is to complement the
# verifications in terms of state transitions at a finer granularity during
# launch and shutdown of the various platforms in the hierarchy.
#

# developer conveniences:
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_single_platform
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_hierarchy
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_single_platform_with_an_instrument
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_13_platforms_and_2_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_13_platforms_and_8_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_platform_device_extended_attributes

from interface.objects import ComputedIntValue
from ion.services.sa.test.helpers import any_old
from pyon.ion.resource import RT

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform
from ion.agents.platform.test.base_test_platform_agent_with_rsn import instruments_dict

from unittest import skip
from mock import patch
from pyon.public import CFG


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class TestPlatformLaunch(BaseIntTestPlatform):

    def _run_startup_commands(self):
        self._ping_agent()
        self._initialize()
        self._go_active()
        self._run()

    def _run_shutdown_commands(self):
        self._go_inactive()
        self._reset()
        self._shutdown()

    def _run_commands(self):
        """
        A common sequence of commands for the root platform in some of the
        tests below.
        """
        self._run_startup_commands()

        #####################
        # done
        self._run_shutdown_commands()

    def test_single_platform(self):
        #
        # Tests the launch and shutdown of a single platform (no instruments).
        #
        p_root = self._create_single_platform()
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)

        self._run_commands()

    def test_hierarchy(self):
        #
        # Tests the launch and shutdown of a small platform topology (no instruments).
        #
        p_root = self._create_small_hierarchy()
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)

        self._run_commands()

    def test_single_platform_with_an_instrument(self):
        #
        # basic test of launching a single platform with an instrument
        #

        p_root = self._set_up_single_platform_with_some_instruments(['SBE37_SIM_01'])
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)

        self._run_commands()

    def test_13_platforms_and_2_instruments(self):
        #
        # Test with network of 13 platforms and 2 instruments.
        #
        instr_keys = ["SBE37_SIM_01", "SBE37_SIM_02", ]

        p_root = self._set_up_platform_hierarchy_with_some_instruments(instr_keys)
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)

        self._run_commands()

    @skip("Runs fine but waiting for comments to enable in general")
    @patch.dict(CFG, {'endpoint': {'receive': {'timeout': 420}}})
    def test_13_platforms_and_8_instruments(self):
        #
        # Test with network of 13 platforms and 8 instruments (the current
        # number of enabled instrument simulator instances).
        #
        instr_keys = sorted(instruments_dict.keys())
        p_root = self._set_up_platform_hierarchy_with_some_instruments(instr_keys)
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)

        self._run_commands()

    def test_platform_device_extended_attributes(self):
        p_root = self._create_small_hierarchy()
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)

        self._run_startup_commands()

        pdevice_id = p_root["platform_device_id"]
        p_extended = self.IMS.get_platform_device_extension(pdevice_id)

        for retval in [p_extended.computed.communications_status_roll_up,
                       p_extended.computed.data_status_roll_up,
                       p_extended.computed.power_status_roll_up,
                       p_extended.computed.power_status_roll_up,
                       #p_extended.computed.rsn_network_child_device_status,
                       #p_extended.computed.rsn_network_rollup,
                       ]:
            self.assertIsInstance(retval, ComputedIntValue)


        print "aggregated status:", p_extended.aggregated_status
        print "communications_status_roll_up", p_extended.computed.communications_status_roll_up
        print "data_status_roll_up", p_extended.computed.data_status_roll_up
        print "location_status_roll_up", p_extended.computed.location_status_roll_up
        print "power_status_roll_up", p_extended.computed.power_status_roll_up
        print "rsn_network_child_device_status", p_extended.computed.rsn_network_child_device_status
        print "rsn_network_rollup", p_extended.computed.rsn_network_rollup

        psite_id = self.RR2.create(any_old(RT.PlatformSite))
        self.RR2.assign_device_to_site_with_has_device(pdevice_id, psite_id)

        ps_extended = self.OMS.get_site_extension(psite_id)

        for retval in [ps_extended.computed.communications_status_roll_up,
                       ps_extended.computed.data_status_roll_up,
                       ps_extended.computed.power_status_roll_up,
                       ps_extended.computed.power_status_roll_up,
                       #ps_extended.computed.rsn_network_child_device_status,
                       #ps_extended.computed.rsn_network_rollup,
        ]:
            self.assertIsInstance(retval, ComputedIntValue)

        self._run_shutdown_commands()
