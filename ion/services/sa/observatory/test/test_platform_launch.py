#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_launch.py
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

from pyon.public import log
import logging

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

from nose.plugins.attrib import attr


@attr('INT', group='sa')
class TestPlatformLaunch(BaseIntTestPlatform):

    def _run_commands(self):

        self._ping_agent()
        self._initialize()
        self._go_active()
        self._run()

        self._wait_for_external_event()

        self._go_inactive()
        self._reset()

    def test_single_platform(self):
        #
        # Tests the launch and shutdown of a single platform.
        #
        p_root = self._create_single_platform()

        self._start_platform(p_root.platform_agent_instance_id)
        self._run_commands()
        self._stop_platform(p_root.platform_agent_instance_id)

    def test_hierarchy(self):
        #
        # Tests the launch and shutdown of a small platform topology.
        #
        p_root = self._create_small_hierarchy()

        self._start_platform(p_root.platform_agent_instance_id)
        self._run_commands()
        self._stop_platform(p_root.platform_agent_instance_id)
