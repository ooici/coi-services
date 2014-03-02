#!/usr/bin/env python

"""Test cases for launching and shutting down a platform agent network"""

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
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_single_deployed_platform
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_hierarchy
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_hierarchy_recursion_false
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_single_platform_with_an_instrument
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_ims_instrument_status
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_ims_platform_status
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_single_platform_with_an_instrument_and_deployments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_single_platform_with_instruments_streaming
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_instrument_first_then_platform
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_13_platforms_and_1_instrument
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_13_platforms_and_2_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_13_platforms_and_8_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_platform_device_extended_attributes

from unittest import skip
from mock import patch
import gevent
import unittest
import os

from pyon.agent.agent import ResourceAgentState
from pyon.util.context import LocalContextMixin
from pyon.public import log, CFG, RT
from ion.services.sa.test.helpers import any_old
from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform, instruments_dict

from interface.objects import ComputedIntValue, ComputedValueAvailability, ComputedListValue, ComputedDictValue
from interface.objects import AggregateStatusType, DeviceStatusType


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestPlatformLaunch(BaseIntTestPlatform):

    def _run_startup_commands(self, recursion=True):
        self._ping_agent()
        self._initialize(recursion)
        self._go_active(recursion)
        self._run(recursion)

    def _run_shutdown_commands(self, recursion=True):
        try:
            self._go_inactive(recursion)
            self._reset(recursion)
        finally:  # attempt shutdown anyway
            self._shutdown(True)  # NOTE: shutdown always with recursion=True

    def test_single_platform(self):
        #
        # Tests the launch and shutdown of a single platform (no instruments).
        #
        self._set_receive_timeout()

        p_root = self._create_single_platform()
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    def test_single_deployed_platform(self):
        #
        # Tests the launch and shutdown of a single platform (no instruments).
        #
        self._set_receive_timeout()

        p_root = self._create_single_platform()
        platform_site_id, platform_deployment_id = self._create_platform_site_and_deployment(p_root.platform_device_id )
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

        # verify the instrument has moved to COMMAND by the platform:
        _pa_client = self._create_resource_agent_client(p_root.platform_device_id)
        state = _pa_client.get_agent_state()
        log.debug("platform state: %s", state)
        self.assertEquals(ResourceAgentState.COMMAND, state)

        ports = self._get_ports()
        log.info("_get_ports = %s", ports)
        for dev_id, state_dict in ports.iteritems():
            self.assertEquals(state_dict['state'], None)

    def test_hierarchy(self):
        #
        # Tests the launch and shutdown of a small platform topology (no instruments).
        #
        self._set_receive_timeout()

        p_root = self._create_small_hierarchy()
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    def test_hierarchy_recursion_false(self):
        #
        # As with test_hierarchy but with recursion=False.
        # There are no particular asserts here, but the test should complete
        # fine and the logs should show that the recursion parameter is properly
        # reflected with value false in the root platform, except for the
        # final shutdown as we want a graceful shutdown of the whole hierarchy.
        #
        self._set_receive_timeout()

        p_root = self._create_small_hierarchy()
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)

        def shutdown_commands():
            self._run_shutdown_commands(recursion=False)

        self.addCleanup(shutdown_commands)

        self._run_startup_commands(recursion=False)

    def test_single_platform_with_an_instrument(self):
        #
        # basic test of launching a single platform with an instrument
        #
        self._set_receive_timeout()

        p_root = self._set_up_single_platform_with_some_instruments(['SBE37_SIM_01'])
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    def test_ims_instrument_status(self):
        #
        # test the access of instrument aggstatus via ims and the object store
        #
        #bin/nosetests -s -v --nologcapture ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_ims_instrument_status

        self._set_receive_timeout()

        p_root = self._set_up_single_platform_with_some_instruments(['SBE37_SIM_01'])
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

        i_obj1 = self._get_instrument('SBE37_SIM_01')
        #check that the instrument is in streaming mode.
        _ia_client1 = self._create_resource_agent_client(i_obj1.instrument_device_id)
        state1 = _ia_client1.get_agent_state()
        self.assertEqual(state1, 'RESOURCE_AGENT_STATE_COMMAND')

        # Grab instrument aggstatus aparam.
        retval = _ia_client1.get_agent(['aggstatus'])['aggstatus']

        # Assert all status types are OK.
        self.assertEqual(retval, {AggregateStatusType.AGGREGATE_COMMS: DeviceStatusType.STATUS_OK,
                                  AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_OK,
                                  AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_OK,
                                  AggregateStatusType.AGGREGATE_POWER: DeviceStatusType.STATUS_OK})

        # Assert we have a valid device id.
        device_id = i_obj1['instrument_device_id']
        self.assertIsInstance(device_id, str)
        self.assertTrue(len(device_id)>0)

        # Need to wait for device state (event) persister - every second
        gevent.sleep(1.1)

        # Get the device extension and construct a rollup dictionary.
        dev_ext = self.IMS.get_instrument_device_extension(instrument_device_id=device_id)

        dev_ext_rollup_dict = {
            AggregateStatusType.AGGREGATE_COMMS : dev_ext.computed.communications_status_roll_up['value'],
            AggregateStatusType.AGGREGATE_DATA : dev_ext.computed.power_status_roll_up['value'],
            AggregateStatusType.AGGREGATE_LOCATION : dev_ext.computed.data_status_roll_up['value'],
            AggregateStatusType.AGGREGATE_POWER : dev_ext.computed.location_status_roll_up['value']
        }

        # Assert the rollup dictionary is same as the aparam.
        self.assertEqual(retval, dev_ext_rollup_dict)

        # Assert state equals agent value.
        self.assertEqual(dev_ext.computed.operational_state.value, 'COMMAND')


    def test_ims_platform_status(self):
        #
        # test the access of instrument aggstatus via ims and the object store
        #
        #bin/nosetests -s -v --nologcapture ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_ims_platform_status


        p_root = self._set_up_single_platform_with_some_instruments(['SBE37_SIM_01', 'SBE37_SIM_02'])
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

        platform_device_id = p_root['platform_device_id']

        dev_ext = self.IMS.get_platform_device_extension(platform_device_id=platform_device_id)

        self.assertEqual(dev_ext.computed.communications_status_roll_up.value, DeviceStatusType.STATUS_OK)
        self.assertEqual(dev_ext.computed.data_status_roll_up.value, DeviceStatusType.STATUS_OK)
        self.assertEqual(dev_ext.computed.location_status_roll_up.value, DeviceStatusType.STATUS_OK)
        self.assertEqual(dev_ext.computed.power_status_roll_up.value, DeviceStatusType.STATUS_OK)

        self.assertEqual(dev_ext.computed.instrument_status.value, [DeviceStatusType.STATUS_OK,DeviceStatusType.STATUS_OK])
        self.assertEqual(dev_ext.computed.platform_status.value, [])
        self.assertEqual(dev_ext.computed.portal_status.value, [])

    def test_single_platform_with_an_instrument_and_deployments(self):
        #
        # basic test of launching a single platform with an instrument
        #
        self._set_receive_timeout()

        p_root = self._set_up_single_platform_with_some_instruments(['SBE37_SIM_01'])

        i_obj = self._setup_instruments['SBE37_SIM_01']

        platform_site_id, platform_deployment_id = self._create_platform_site_and_deployment(p_root.platform_device_id )

        instrument_site_id, instrument_deployment_id = self._create_instrument_site_and_deployment(platform_site_id=platform_site_id, instrument_device_id=i_obj.instrument_device_id)

        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

        # TODO(OOIION-1495) review the following. It's commented out because
        # the handling of the platform ports needs further revision as there
        # seems to be a confusion of definitions related with the CI-OMS
        # interface vs. the definitions handled on RR.
        # With the recent code reverts the returned ports below would be all
        # with state=None:
        # INFO: _get_ports after startup= {'1': {'state': None}, '2': {'state': None}}
        #
        # Also, better make sure that the only possible values for a port
        # state are "ON" and "OFF" (and perhaps also "UNKNOWN"), but not None.
        """
        ports = self._get_ports()
        log.info("_get_ports after startup= %s", ports)
        # the deployment should have turned port 1 on, but port 2 should still be off
        self.assertEquals(ports['1']['state'], 'ON')
        self.assertEquals(ports['2']['state'], None)
        """

    def test_single_platform_with_instruments_streaming(self):
        #
        # basic test of launching a single platform with an instrument
        #
        self._set_receive_timeout()

        p_root = self._set_up_single_platform_with_some_instruments(['SBE37_SIM_01', 'SBE37_SIM_02'])
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

        self._start_resource_monitoring()

        self._wait_for_a_data_sample()

        i_obj1 = self._get_instrument('SBE37_SIM_01')
        #check that the instrument is in streaming mode.
        _ia_client1 = self._create_resource_agent_client(i_obj1.instrument_device_id)
        state1 = _ia_client1.get_agent_state()
        self.assertEquals(state1, ResourceAgentState.STREAMING)

        i_obj2 = self._get_instrument('SBE37_SIM_02')
        #check that the instrument is in streaming mode.
        _ia_client2 = self._create_resource_agent_client(i_obj2.instrument_device_id)
        state2 = _ia_client2.get_agent_state()
        self.assertEquals(state2, ResourceAgentState.STREAMING)

        self._stop_resource_monitoring()

        #check that the instrument is NOT in streaming mode.
        state1 = _ia_client1.get_agent_state()
        self.assertEquals(state1, ResourceAgentState.COMMAND)

        state2 = _ia_client2.get_agent_state()
        self.assertEquals(state2, ResourceAgentState.COMMAND)


    def test_instrument_first_then_platform(self):
        #
        # An instrument is launched first (including its associated port
        # agent), then its parent platform is launched. This test verifies
        # that the platform is able to detect the child is already running to
        # continue operating normally.
        # See PlatformAgent._launch_instrument_agent
        #
        self._set_receive_timeout()

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
        self._set_receive_timeout()

        instr_keys = ["SBE37_SIM_02", ]

        p_root = self._set_up_platform_hierarchy_with_some_instruments(instr_keys)
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    def test_13_platforms_and_2_instruments(self):
        #
        # Test with network of 13 platforms and 2 instruments.
        #
        self._set_receive_timeout()

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
        self._set_receive_timeout()

        instr_keys = sorted(instruments_dict.keys())
        p_root = self._set_up_platform_hierarchy_with_some_instruments(instr_keys)
        self._start_platform(p_root)
        self.addCleanup(self._stop_platform, p_root)
        self.addCleanup(self._run_shutdown_commands)

        self._run_startup_commands()

    def test_platform_device_extended_attributes(self):
        self._set_receive_timeout()

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

        """
        print "communications_status_roll_up", p_extended.computed.communications_status_roll_up
        print "data_status_roll_up", p_extended.computed.data_status_roll_up
        print "location_status_roll_up", p_extended.computed.location_status_roll_up
        print "power_status_roll_up", p_extended.computed.power_status_roll_up
        print "rsn_network_child_device_status", p_extended.computed.rsn_network_child_device_status
        print "rsn_network_rollup", p_extended.computed.rsn_network_rollup
        """

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
