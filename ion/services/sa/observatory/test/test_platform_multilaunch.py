#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_multilaunch
@file    ion/services/sa/observatory/test/test_platform_multilaunch.py
@author  Carlos Rueda
@brief   Test multiple startup-shutdown cycles in various platform topologies
@see     https://jira.oceanobservatories.org/tasks/browse/OOIION-1297
"""

__author__ = 'Carlos Rueda'


# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_01_platforms_and_00_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_01_platforms_and_01_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_01_platforms_and_02_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_03_platforms_and_00_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_03_platforms_and_01_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_03_platforms_and_02_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_03_platforms_and_03_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_13_platforms_and_00_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_13_platforms_and_01_instruments
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_multilaunch.py:TestMultiLaunch.test_13_platforms_and_02_instruments


from ion.agents.platform.test.base_test_platform_agent import BaseIntTestPlatform

from mock import patch
from pyon.public import log, CFG
import unittest
import os
import time

# the number of startup-shutdown cycles for each test:
NO_CYCLES = 2


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
@unittest.skipIf((not os.getenv('PYCC_MODE', False)) and os.getenv('CEI_LAUNCH_TEST', False), 'Skip until tests support launch port agent configurations.')
class TestMultiLaunch(BaseIntTestPlatform):

    def _do_one_cycle(self, create_network):
        """
        Does a complete setup/startup/monitoring/shutdown cycle.

        The monitoring includes verifying the reception of data samples.

        @param create_network   function that creates the desired network.
        """

        p_root = create_network()
        try:
            self._start_platform(p_root)
            self._ping_agent()
            self._initialize()
            self._go_active()
            self._run()
            self._start_resource_monitoring()

            self._wait_for_a_data_sample()

            self._stop_resource_monitoring()
            self._go_inactive()
            self._reset()

        finally:
            try:
                self._shutdown()
            finally:
                self._stop_platform(p_root)

    def _do_cycles(self, create_network):
        start = time.time()

        for i in range(NO_CYCLES):
            log.info("cycle %d starting...", i)
            cycle_start = time.time()
            self._do_one_cycle(create_network)
            log.info("cycle %d completed in %.1f secs", i, time.time() - cycle_start)

        log.info("%d total cycles completed in %.1f secs", NO_CYCLES, time.time() - start)

    def test_01_platforms_and_00_instruments(self):
        #
        # single platform (no instruments).
        #
        self._set_receive_timeout()
        self._do_cycles(self._create_single_platform)

    def test_01_platforms_and_01_instruments(self):
        #
        # single platform with an instrument
        #
        def create_network():
            instr_keys = ["SBE37_SIM_01"]
            return self._set_up_single_platform_with_some_instruments(instr_keys)

        self._set_receive_timeout()
        self._do_cycles(create_network)

    def test_01_platforms_and_02_instruments(self):
        #
        # single platform with 2 instruments
        #
        def create_network():
            instr_keys = ["SBE37_SIM_01", "SBE37_SIM_02"]
            return self._set_up_single_platform_with_some_instruments(instr_keys)

        self._set_receive_timeout()
        self._do_cycles(create_network)

    def test_03_platforms_and_00_instruments(self):
        #
        # small platform topology (no instruments).
        #
        self._set_receive_timeout()
        self._do_cycles(self._create_small_hierarchy)

    def test_03_platforms_and_01_instruments(self):
        #
        # small platform topology and 1 instrument assigned to leaf platform)
        #
        def create_network():
            instr_keys = ["SBE37_SIM_01"]
            return self._set_up_small_hierarchy_with_some_instruments(instr_keys)

        self._set_receive_timeout()
        self._do_cycles(create_network)

    def test_03_platforms_and_02_instruments(self):
        #
        # small platform topology and 2 instruments, all assigned to leaf platform
        #
        def create_network():
            instr_keys = ["SBE37_SIM_01", "SBE37_SIM_02"]
            return self._set_up_small_hierarchy_with_some_instruments(instr_keys)

        self._set_receive_timeout()
        self._do_cycles(create_network)

    def test_03_platforms_and_03_instruments(self):
        #
        # small platform topology and 3 instruments, all assigned to leaf platform
        #
        def create_network():
            instr_keys = ["SBE37_SIM_01", "SBE37_SIM_02", "SBE37_SIM_03"]
            return self._set_up_small_hierarchy_with_some_instruments(instr_keys)

        self._set_receive_timeout()
        self._do_cycles(create_network)

    def test_13_platforms_and_00_instruments(self):
        #
        # Test with network of 13 platforms (no instruments).
        #
        def create_network():
            instr_keys = []
            return self._set_up_platform_hierarchy_with_some_instruments(instr_keys)

        self._set_receive_timeout()
        self._do_cycles(create_network)

    def test_13_platforms_and_01_instruments(self):
        #
        # Test with network of 13 platforms and 1 instrument.
        #
        def create_network():
            instr_keys = ["SBE37_SIM_02"]
            return self._set_up_platform_hierarchy_with_some_instruments(instr_keys)

        self._set_receive_timeout()
        self._do_cycles(create_network)

    def test_13_platforms_and_02_instruments(self):
        #
        # Test with network of 13 platforms and 2 instruments.
        #
        def create_network():
            instr_keys = ["SBE37_SIM_01", "SBE37_SIM_02"]
            return self._set_up_platform_hierarchy_with_some_instruments(instr_keys)

        self._set_receive_timeout()
        self._do_cycles(create_network)
