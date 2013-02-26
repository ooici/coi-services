#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.test.test_cgsn_client
@file    ion/agents/platform/cgsn/test/test_cgsn_client.py
@author  Carlos Rueda
@brief   preliminary tests
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from pyon.util.unit_test import IonUnitTestCase
from unittest import skipIf
import os

from gevent import sleep

from ion.agents.platform.cgsn.cgsn_client_factory import CGSNClientFactory
from ion.agents.platform.cgsn.cgsn_state import CgsnState
from ion.agents.platform.cgsn.defs import DclIds, MessageIds


@skipIf(os.getenv('CGSN', None) is None, "CGSN env var not defined")
class Test(IonUnitTestCase):

    def setUp(self):
        self._cgsn_state = CgsnState()
        self._cc = CGSNClientFactory.create_instance()
        self._cc.set_listener(self._cgsn_state.listener)
        self._cc.start()

    def tearDown(self):
        self._cc.end_reception()

    def test_get_dcl_status(self):

        dcl_id = DclIds.DCL11
        cmd = MessageIds.DCL_STATUS
        self._cc.send_command(dcl_id, cmd)

        # TODO instead of sleep, use AsyncResult or similar mechanism
        sleep(2)

        ack_nack = self._cgsn_state.get_ack_nack(dcl_id, cmd)
        self.assertTrue(MessageIds.ACK, ack_nack)
