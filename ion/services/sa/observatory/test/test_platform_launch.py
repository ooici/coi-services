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
from ion.agents.platform.test.base_test_platform_agent_with_rsn import instruments_dict

from mock import patch
from pyon.public import CFG


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class TestPlatformLaunch(BaseIntTestPlatform):

    def _run_commands(self):

        self._ping_agent()
        self._initialize()
        self._go_active()
        self._run()

        self._go_inactive()
        self._reset()

    def test_single_platform(self):
        #
        # Tests the launch and shutdown of a single platform (no instruments).
        #
        p_root = self._create_single_platform()

        self._start_platform(p_root)
        self._run_commands()
        self._stop_platform(p_root)

    def test_hierarchy(self):
        #
        # Tests the launch and shutdown of a small platform topology (no instruments).
        #
        p_root = self._create_small_hierarchy()

        self._start_platform(p_root)
        self._run_commands()
        self._stop_platform(p_root)

    def test_single_platform_with_an_instrument(self):
        #
        # basic test of launching a single platform with an instrument
        #

        p_root = self._create_single_platform()
        i_obj = self._create_instrument('SBE37_SIM_01')
        self._assign_instrument_to_platform(i_obj, p_root)

        #####################################
        # start the root platform:
        #####################################
        log.info("will start the root platform ...")
        start_time = time.time()
        self._start_platform(p_root)
        log.info("root platform started. Took %.3f secs.", time.time() - start_time)

        self._run_commands()
        self._stop_platform(p_root)

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
        # In DEBUG logging level for the platform agent, files like the following
        # are generated under logs/:
        #    platform_CFG_received_Node1B.txt
        #    platform_CFG_received_MJ01C.txt
        #    platform_CFG_received_LJ01D.txt

        #####################################
        # create platform hierarchy
        #####################################
        log.info("will create platform hierarchy ...")
        start_time = time.time()

        root_platform_id = 'Node1B'
        p_objs = {}
        p_root = self._create_hierarchy(root_platform_id, p_objs)

        log.info("platform hierarchy built. Took %.3f secs. "
                  "Root platform=%r, number of platforms=%d: %s",
                  time.time() - start_time,
                  root_platform_id, len(p_objs), p_objs.keys())

        self.assertIn(root_platform_id, p_objs)
        self.assertEquals(13, len(p_objs))

        #####################################
        # create some instruments
        #####################################
        instr_keys = sorted(instruments_dict.keys())
        log.info("will create %d instruments: %s", len(instr_keys), instr_keys)
        start_time = time.time()

        i_objs = []
        for instr_key in instr_keys:
            i_obj = self._create_instrument(instr_key)
            i_objs.append(i_obj)
            log.debug("instrument created = %r (%s)",
                      i_obj.instrument_agent_instance_id, instr_key)

        log.info("%d instruments created. Took %.3f secs.", len(instr_keys), time.time() - start_time)

        #####################################
        # assign the instruments
        #####################################
        log.info("will assign instruments ...")
        start_time = time.time()

        plats_to_assign_instrs = [
            'LJ01D', 'SF01B', 'LJ01B', 'MJ01B',  # leaves
            'MJ01C', 'Node1D', 'LV01B',          # intermediate
        ]

        # assign one available instrument to a platform
        num_assigns = min(len(instr_keys), len(plats_to_assign_instrs))

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
        # Ah! it seems there're similar exceptions in coi_pycc with other tests
        # eg.,
        # test_connect_failed (ion.agents.instrument.test.test_instrument_agent.TestInstrumentAgent)
        #
        # So, while this is investigated setting number of assignments to 2
        # as it seems to be consistently stable:
        num_assigns = 2

        for ii in range(num_assigns):
            platform_id = plats_to_assign_instrs[ii]
            self.assertIn(platform_id, p_objs)
            p_obj = p_objs[platform_id]
            i_obj = i_objs[ii]
            self._assign_instrument_to_platform(i_obj, p_obj)
            log.debug("instrument %r (%s) assigned to platform %r",
                      i_obj.instrument_agent_instance_id,
                      instr_keys[ii],
                      platform_id)

        log.info("%d instruments assigned. Took %.3f secs.",
                 num_assigns, time.time() - start_time)

        #####################################
        # start the root platform:
        #####################################
        log.info("will start the root platform ...")
        start_time = time.time()

        self._start_platform(p_root)

        log.info("root platform started. Took %.3f secs.", time.time() - start_time)

        #####################################
        # run the commands:
        #####################################
        log.info("will run commands ...")
        start_time = time.time()
        self._run_commands()

        log.info("commands run. Took %.3f secs.", time.time() - start_time)

        #####################################
        # stop the root platform
        #####################################
        log.info("will stop the root platform ...")
        start_time = time.time()
        self._stop_platform(p_root)

        log.info("root platform stopped. Took %.3f secs.", time.time() - start_time)
