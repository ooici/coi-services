#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_launch
@file    ion/services/sa/observatory/test/test_platform_launch.py
@author  Carlos Rueda, Maurice Manning, Ian Katz
@brief   Test cases for launching and shutting down a platform agent network
"""
from interface.objects import ComputedIntValue, ComputedValueAvailability, ComputedListValue
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
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_platform_hierarchy_with_some_instruments

from pyon.public import log
import time

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

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

    def test_platform_hierarchy_with_some_instruments(self):
        #
        # TODO for some reason assigning more than 2 or 3 instruments triggers
        # exceptions in the port_agent toward the finalization of the test:
        #
        # 2013-03-31 20:47:21,509 ERROR    build/bdist.macosx-10.8-intel/egg/mi/core/instrument/port_agent_client.py _init_comms(): Exception initializing comms for localhost: 5001: error(61, 'Connection refused')
        # Traceback (most recent call last):
        #   File "build/bdist.macosx-10.8-intel/egg/mi/core/instrument/port_agent_client.py", line 281, in _init_comms
        #     self._create_connection()
        #   File "build/bdist.macosx-10.8-intel/egg/mi/core/instrument/port_agent_client.py", line 327, in _create_connection
        #     self.sock.connect((self.host, self.port))
        #   File "/usr/local/Cellar/python/2.7.3/Frameworks/Python.framework/Versions/2.7/lib/python2.7/socket.py", line 224, in meth
        #     return getattr(self._sock,name)(*args)
        # error: [Errno 61] Connection refused
        #
        # And there are lots of:
        #
        # ## attempting reconnect...
        #
        # generated from instrument_agent ... perhaps the finalization of the
        # instr agent is conflicting somehow with attempts to re-connect (?)
        #
        # So, while this is investigated, setting number of assignments to 2
        # as it seems to be consistently stable:
        #instr_keys = sorted(instruments_dict.keys())

        instr_keys = ["SBE37_SIM_01", "SBE37_SIM_02", ]

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

        extended_device_datatypes = {
            "communications_status_roll_up": ComputedIntValue,
            "power_status_roll_up": ComputedIntValue,
            "data_status_roll_up": ComputedIntValue,
            "location_status_roll_up": ComputedIntValue,
            "aggregated_status": ComputedIntValue,
        }

        for attr, thetype in extended_device_datatypes.iteritems():
            self.assertIsInstance(getattr(p_extended.computed, attr),
                                  thetype,
                                  "Computed attribute %s is not %s" % (attr, thetype))



        for retval in [p_extended.computed.communications_status_roll_up,
                       p_extended.computed.data_status_roll_up,
                       p_extended.computed.power_status_roll_up,
                       p_extended.computed.power_status_roll_up,
                       #p_extended.computed.rsn_network_child_device_status,
                       #p_extended.computed.rsn_network_rollup,
                       ]:
            self.assertEqual(ComputedValueAvailability.PROVIDED, retval.status)


        print "aggregated status:", p_extended.aggregated_status
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
            "aggregated_status": ComputedIntValue,
            "platform_status": ComputedListValue,
            "instrument_status": ComputedListValue,
            "platform_station_sites": ComputedListValue,
            "platform_assembly_sites": ComputedListValue,
            "platform_component_sites": ComputedListValue,
            "instrument_sites": ComputedListValue,
        }
        for attr, thetype in extended_site_datatypes.iteritems():
            self.assertIsInstance(getattr(ps_extended.computed, attr),
                                  thetype,
                                  "Computed attribute %s is not %s" % (attr, thetype))



        for retval in [ps_extended.computed.communications_status_roll_up,
                       ps_extended.computed.data_status_roll_up,
                       ps_extended.computed.power_status_roll_up,
                       ps_extended.computed.power_status_roll_up,
                       #ps_extended.computed.rsn_network_child_device_status,
                       #ps_extended.computed.rsn_network_rollup,
        ]:
            self.assertEqual(ComputedValueAvailability.PROVIDED, retval.status)

        self._run_shutdown_commands()
