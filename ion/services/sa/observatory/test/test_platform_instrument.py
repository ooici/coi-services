#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_instrument
@file    ion/services/sa/observatory/test/test_platform_instrument.py
@author  Carlos Rueda, Maurice Manning
@brief   Tests involving some more detailed platform-instrument interations
"""

__author__ = 'Carlos Rueda, Maurice Manning'
__license__ = 'Apache 2.0'

#
# Base preparations and construction of the platform topology are provided by
# the base class BaseTestPlatform.
#

# developer conveniences:
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_instrument.py:Test.test_platform_with_instrument_streaming

from pyon.public import log

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

from pyon.agent.agent import ResourceAgentClient
from ion.agents.platform.test.base_test_platform_agent_with_rsn import FakeProcess
from pyon.agent.agent import ResourceAgentState

from interface.objects import AgentCommand
import unittest

import gevent

from mock import patch
from pyon.public import CFG

# -------------------------------- MI ----------------------------
# the following adapted from test_instrument_agent to be able to import from
# the MI repo, using egg directly.

import sys
from ion.agents.instrument.driver_process import ZMQEggDriverProcess

# A seabird driver.
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.1.0-py2.7.egg'
DRV_MOD = 'mi.instrument.seabird.sbe37smb.ooicore.driver'
DRV_CLS = 'SBE37Driver'

WORK_DIR = '/tmp/'

DVR_CONFIG = {
    'dvr_egg' : DRV_URI,
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : None
}

# Dynamically load the egg into the test path
launcher = ZMQEggDriverProcess(DVR_CONFIG)
egg = launcher._get_egg(DRV_URI)
if not egg in sys.path:
    sys.path.insert(0, egg)

# now we can import SBE37ProtocolEvent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent

# ------------------------------------------------------------------------


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class Test(BaseIntTestPlatform):

    @unittest.skip('This test is immature.')
    def test_platform_with_instrument_streaming(self):
        #
        # The following is with just a single platform and the single
        # instrument "SBE37_SIM_08", which corresponds to the one on port 4008.
        #
        instr_key = "SBE37_SIM_08"

        p_root = self._set_up_single_platform_with_some_instruments([instr_key])

        self._start_platform(p_root)

        # get everything in command mode:
        self._ping_agent()
        self._initialize()
        self._go_active()
        self._run()
        # note that this includes the instrument also getting to the command state

        # get client to the instrument:
        # the i_obj is a DotDict with various pieces captured during the
        # set-up of the instrument, in particular instrument_device_id
        i_obj = self._get_instrument(instr_key)

        log.debug("KK creating ResourceAgentClient")
        ia_client = ResourceAgentClient(i_obj.instrument_device_id,
                                        process=FakeProcess())
        log.debug("KK got ResourceAgentClient: %s", ia_client)

        # verify the instrument is command state:
        state = ia_client.get_agent_state()
        log.debug("KK instrument state: %s", state)
        self.assertEqual(state, ResourceAgentState.COMMAND)

        # start streaming:
        log.debug("KK starting instrument streaming: %s", state)
        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)

        # NOTE: commented out because of error (see other #!! lines)
        #!! self._ia_client.execute_resource(cmd)
        """
2013-04-03 14:17:22,018 DEBUG Dummy-7 ion.services.sa.observatory.test.test_platform_instrument:121 KK starting instrument streaming: RESOURCE_AGENT_STATE_COMMAND
ERROR
2013-04-03 14:17:22,020 INFO Dummy-7 mi_logger:98 Stopping pagent pid 53267
Exception AttributeError: AttributeError("'_DummyThread' object has no attribute '_Thread__block'",) in <module 'threading' from '/usr/local/Cellar/python/2.7.3/Frameworks/Python.framework/Versions/2.7/lib/python2.7/threading.pyc'> ignored
2013-04-03 14:17:22,092 ERROR    build/bdist.macosx-10.8-intel/egg/mi/core/instrument/port_agent_client.py Zero bytes received from port_agent socket
2013-04-03 14:17:22,098 ERROR    build/bdist.macosx-10.8-intel/egg/mi/core/instrument/port_agent_client.py fn_local_callback_error, Connection error: Zero bytes received from port_agent socket
2013-04-03 14:17:22,102 ERROR    build/bdist.macosx-10.8-intel/egg/mi/core/instrument/port_agent_client.py Attempting connection_level recovery; attempt number 1
2013-04-03 14:17:22,113 ERROR    build/bdist.macosx-10.8-intel/egg/mi/core/instrument/port_agent_client.py _init_comms(): Exception initializing comms for localhost: 5008: error(61, 'Connection refused')
Traceback (most recent call last):
  File "build/bdist.macosx-10.8-intel/egg/mi/core/instrument/port_agent_client.py", line 281, in _init_comms
    self._create_connection()
  File "build/bdist.macosx-10.8-intel/egg/mi/core/instrument/port_agent_client.py", line 327, in _create_connection
    self.sock.connect((self.host, self.port))
  File "/usr/local/Cellar/python/2.7.3/Frameworks/Python.framework/Versions/2.7/lib/python2.7/socket.py", line 224, in meth
    return getattr(self._sock,name)(*args)
error: [Errno 61] Connection refused
2013
        """

        # TODO set up listeners to verify things ...
        # ...

        # sleep to let the streaming run for a while
        #!! log.debug("KK sleeping ...")
        #!! gevent.sleep(15)

        # stop streaming:
        #!! log.debug("KK stopping instrument streaming: %s", state)
        #!! cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        #!! self._ia_client.execute_resource(cmd)

        # TODO verifications ...
        # ...

        # then shutdown the network:
        self._go_inactive()
        self._reset()

        self._stop_platform(p_root)
