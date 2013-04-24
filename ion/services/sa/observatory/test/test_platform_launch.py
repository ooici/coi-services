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
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_platform_hierarchy_with_some_instruments

from pyon.public import log
import time

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

from mock import patch
from pyon.public import CFG


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class TestPlatformLaunch(BaseIntTestPlatform):

    def _run_commands(self):
        """
        A common sequence of commands for the root platform in some of the
        tests below.
        """
        self._ping_agent()
        self._initialize()
        self._go_active()
        self._run()

        #####################
        # done
        self._go_inactive()
        self._reset()
        self._shutdown()

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
