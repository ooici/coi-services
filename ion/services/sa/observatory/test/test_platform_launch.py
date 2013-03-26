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
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_single_platform_with_an_instrument
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_launch.py:TestPlatformLaunch.test_platform_hierarchy_with_some_instruments

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
        # Tests the launch and shutdown of a single platform (no instruments).
        #
        p_root = self._create_single_platform()

        self._start_platform(p_root.platform_agent_instance_id)
        self._run_commands()
        self._stop_platform(p_root.platform_agent_instance_id)

    def test_hierarchy(self):
        #
        # Tests the launch and shutdown of a small platform topology (no instruments).
        #
        p_root = self._create_small_hierarchy()

        self._start_platform(p_root.platform_agent_instance_id)
        self._run_commands()
        self._stop_platform(p_root.platform_agent_instance_id)

    def test_single_platform_with_an_instrument(self):
        #
        # basic test of launching a single platform with an instrument
        #

        p_root = self._create_single_platform()
        i_obj = self._create_instrument()
        self._assign_instrument_to_platform(i_obj, p_root)

        self._start_platform(p_root.platform_agent_instance_id)
        self._run_commands()
        self._stop_platform(p_root.platform_agent_instance_id)

    def test_platform_hierarchy_with_some_instruments(self):
        #
        # test of launching a multiple-level platform hierarchy with
        # instruments associated to some of the platforms.
        #
        # The platform hierarchy corresponds to the sub-network in the
        # simulated topology rooted at 'Node1B', which at time of writing
        # looks like this:
        #
        # Node1B
        #     Node1C
        #         Node1D
        #             MJ01C
        #                 LJ01D
        #         LV01C
        #             PC01B
        #                 SC01B
        #                     SF01B
        #             LJ01C
        #     LV01B
        #         LJ01B
        #         MJ01B
        #
        # In DEBUG logging level for the relevant modules (in particular, the
        # parent class of this test, and PlatformAgent), the following files are
        # generated under logs/:
        #    platform_CFG_generated_Node1B_complete.txt
        #    platform_CFG_received_Node1B.txt
        #    platform_CFG_received_Node1C.txt
        #    platform_CFG_received_Node1D.txt
        #    platform_CFG_received_MJ01C.txt
        #    platform_CFG_received_LJ01D.txt
        #    platform_CFG_received_LV01C.txt
        #    platform_CFG_received_PC01B.txt
        #    platform_CFG_received_SC01B.txt
        #    platform_CFG_received_SF01B.txt
        #    platform_CFG_received_LJ01C.txt
        #    platform_CFG_received_LV01B.txt
        #    platform_CFG_received_LJ01B.txt
        #    platform_CFG_received_MJ01B.txt

        # disable the generation of config files (to only generate the
        # complete one below)
        self._debug_config_enabled = False

        root_platform_id = 'Node1B'
        p_objs = {}
        p_root = self._create_hierarchy(root_platform_id, p_objs)

        log.debug("platform hierarchy built. Root platform=%r, number of platforms=%d: %s",
                  root_platform_id, len(p_objs), p_objs.keys())

        self.assertIn(root_platform_id, p_objs)
        self.assertEquals(13, len(p_objs))

        # create and assign some instruments

        # TODO just creating/assigning a single instrument at the moment.

        i_obj = self._create_instrument()

        log.debug("instrument created = %r", i_obj.instrument_agent_instance_id)

        pid_LV01C = 'LV01C'
        self.assertIn(pid_LV01C, p_objs)
        self._assign_instrument_to_platform(i_obj, p_objs[pid_LV01C])

        log.debug("instrument assigned to = %r", pid_LV01C)

        # generate the config for the whole hierarchy including instruments:
        self._debug_config_enabled = True
        self._generate_config(p_root.platform_agent_instance_obj,
                              root_platform_id, "_complete")

        # start the root platform and run the commands:
        self._start_platform(p_root.platform_agent_instance_id)
        self._run_commands()
        self._stop_platform(p_root.platform_agent_instance_id)
