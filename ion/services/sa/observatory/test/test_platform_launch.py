#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_launch
@file    ion/services/sa/observatory/test/test_platform_launch.py
@author  Carlos Rueda, Maurice Manning, Ian Katz
@brief   Test cases for launching and shutting down a platform agent network
"""
from interface.objects import ComputedIntValue, ComputedValueAvailability, ComputedListValue, ComputedDictValue
from ion.services.sa.test.helpers import any_old
from pyon.ion.resource import RT

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
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_instrument_first_then_platform
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_13_platforms_and_1_instrument
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_13_platforms_and_2_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_13_platforms_and_8_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_platform_device_extended_attributes



from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform
from ion.agents.platform.test.base_test_platform_agent_with_rsn import instruments_dict

from pyon.agent.agent import ResourceAgentState

from unittest import skip
from mock import patch
from pyon.public import log, CFG


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class TestPlatformLaunch(BaseIntTestPlatform):

    def _run_startup_commands(self):
        self._ping_agent()
        self._initialize()
        self._go_active()
        self._run()

    def _run_shutdown_commands(self):
        try:
            self._go_inactive()
            self._reset()
        finally:  # attempt shutdown anyway
            self._shutdown()

    def test_single_platform(self):
        #
        # Tests the launch and shutdown of a single platform (no instruments).
        #
        p_root = self._create_single_platform()
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    def test_hierarchy(self):
        #
        # Tests the launch and shutdown of a small platform topology (no instruments).
        #
        p_root = self._create_small_hierarchy()
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    def test_single_platform_with_an_instrument(self):
        #
        # basic test of launching a single platform with an instrument
        #

        p_root = self._set_up_single_platform_with_some_instruments(['SBE37_SIM_01'])
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    def test_instrument_first_then_platform(self):
        #
        # An instrument is launched first (including its associated port
        # agent), then its parent platform is launched. This test verifies
        # that the platform is able to detect the child is already running to
        # continue operating normally.
        # See PlatformAgent._launch_instrument_agent
        #

        # create instrument but do not start associated port agent ...
        instr_key = 'SBE37_SIM_01'
        i_obj = self._create_instrument(instr_key, start_port_agent=False)

        # ... because the following also starts the port agent:
        ia_client = self._start_instrument(i_obj)
        self.addCleanup(self._stop_instrument, i_obj)

        # verify instrument is in UNINITIALIZED:
        instr_state = ia_client.get_agent_state()
        log.debug("instrument state: %s", instr_state)
        self.assertEquals(ResourceAgentState.UNINITIALIZED, instr_state)

        # set up platform with that instrument:
        p_root = self._set_up_single_platform_with_some_instruments([instr_key])

        # start the platform:
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

        # verify the instrument has moved to COMMAND by the platform:
        instr_state = ia_client.get_agent_state()
        log.debug("instrument state: %s", instr_state)
        self.assertEquals(ResourceAgentState.COMMAND, instr_state)

    def test_13_platforms_and_1_instrument(self):
        #
        # Test with network of 13 platforms and 1 instrument.
        # This test added while investigating OOIION-1095.
        # With one instrument the test runs fine even --with-pycc.
        #
        instr_keys = ["SBE37_SIM_02", ]

        p_root = self._set_up_platform_hierarchy_with_some_instruments(instr_keys)
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    @skip("Used to pass in general, but now not --with-pycc when more "
          "than one instrument is associated. Skipped while investigating. OOIION-1095")
    # Note: it's the same test as above but with more than one instrument.
    # TODO reenable. what has changed in the launching of port/instrument agents?
    def test_13_platforms_and_2_instruments(self):
        #
        # Test with network of 13 platforms and 2 instruments.
        #
        instr_keys = ["SBE37_SIM_01", "SBE37_SIM_02", ]

        p_root = self._set_up_platform_hierarchy_with_some_instruments(instr_keys)
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

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
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    def test_platform_device_extended_attributes(self):

        instr_keys = ["SBE37_SIM_01", "SBE37_SIM_02", ]
        p_root = self._set_up_platform_hierarchy_with_some_instruments(instr_keys)

        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

        pdevice_id = p_root["platform_device_id"]
        p_extended = self.IMS.get_platform_device_extension(pdevice_id)

        extended_device_datatypes = {
            "communications_status_roll_up": ComputedIntValue,
            "power_status_roll_up": ComputedIntValue,
            "data_status_roll_up": ComputedIntValue,
            "location_status_roll_up": ComputedIntValue,
            "platform_status": ComputedListValue,
            "instrument_status": ComputedListValue,
            "rsn_network_child_device_status": ComputedDictValue,
            "rsn_network_rollup": ComputedDictValue,
        }

        for attr, thetype in extended_device_datatypes.iteritems():
            self.assertIn(attr, p_extended.computed)
            self.assertIsInstance(getattr(p_extended.computed, attr),
                                  thetype,
                                  "Computed attribute %s is not %s" % (attr, thetype))




        for attr in ["communications_status_roll_up",
                     "data_status_roll_up",
                     "power_status_roll_up",
                     "power_status_roll_up",
                     ]:
            retval = getattr(p_extended.computed, attr)
            self.assertEqual(ComputedValueAvailability.PROVIDED,
                             retval.status,
                             "platform computed.%s was not PROVIDED: %s" % (attr, retval.reason))


        print "communications_status_roll_up", p_extended.computed.communications_status_roll_up
        print "data_status_roll_up", p_extended.computed.data_status_roll_up
        print "location_status_roll_up", p_extended.computed.location_status_roll_up
        print "power_status_roll_up", p_extended.computed.power_status_roll_up
        print "rsn_network_child_device_status", p_extended.computed.rsn_network_child_device_status
        print "rsn_network_rollup", p_extended.computed.rsn_network_rollup


        # test extended attributes of site

        psite_id = self.RR2.create(any_old(RT.PlatformSite))
        self.RR2.assign_device_to_site_with_has_device(pdevice_id, psite_id)

        ps_extended = self.OMS.get_site_extension(psite_id)

        extended_site_datatypes = {
            "number_data_sets": ComputedIntValue,
            "number_instruments_deployed": ComputedIntValue,
            "number_instruments_operational": ComputedIntValue,
            "number_instruments": ComputedIntValue,
            "number_platforms": ComputedIntValue,
            "number_platforms_deployed": ComputedIntValue,
            "communications_status_roll_up": ComputedIntValue,
            "power_status_roll_up": ComputedIntValue,
            "data_status_roll_up": ComputedIntValue,
            "location_status_roll_up": ComputedIntValue,
            "platform_status": ComputedListValue,
            "instrument_status": ComputedListValue,
            "site_status": ComputedListValue
        }
        for attr, thetype in extended_site_datatypes.iteritems():
            self.assertIn(attr, ps_extended.computed)
            self.assertIsInstance(getattr(ps_extended.computed, attr),
                                  thetype,
                                  "Computed attribute %s is not %s" % (attr, thetype))



        for attr in ["communications_status_roll_up",
                     "data_status_roll_up",
                     "power_status_roll_up",
                     "power_status_roll_up",
        ]:
            retval = getattr(ps_extended.computed, attr)
            self.assertEqual(ComputedValueAvailability.PROVIDED, retval.status, "site computed.%s was not PROVIDED" % attr)

        all_vals = ""
        for attr, computedvalue in ps_extended.computed.__dict__.iteritems():
            all_vals = "%s: " % all_vals

            if hasattr(computedvalue, "__dict__"):
                all_vals = "%s {" % all_vals
                for k, v in computedvalue.__dict__.iteritems():
                    all_vals = "%s%s: %s, " % (all_vals, k, v)
                all_vals = "%s}, " % all_vals
            else:
                all_vals = "%s"

            all_vals = "%s}, " % all_vals

        print "\n%s\n" % all_vals
        #if True: self.fail(all_vals)
