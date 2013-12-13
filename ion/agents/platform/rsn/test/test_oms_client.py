#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.test.test_oms_client
@file    ion/agents/platform/rsn/test/test_oms_client.py
@author  Carlos Rueda
@brief   Test cases for CIOMSClient. The OMS enviroment variable can be used
         to indicate which CIOMSClient will be tested.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from pyon.public import log
from ion.agents.platform.rsn.simulator.logger import Logger
Logger.set_logger(log)

from pyon.util.int_test import IonIntegrationTestCase

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.test.oms_test_mixin import OmsTestMixin

from nose.plugins.attrib import attr

import os


@attr('INT', group='sa')
class Test(IonIntegrationTestCase, OmsTestMixin):
    """
    The OMS enviroment variable can be used to indicate which CIOMSClient will
    be tested. By default, it tests against the simulator, which is launched
    as an external process.
    """

    @classmethod
    def setUpClass(cls):
        OmsTestMixin.setUpClass()
        if cls.using_actual_rsn_oms_endpoint():
            # use FQDM for local host if testing against actual RSN OMS:
            cls._use_fqdn_for_event_listener = True

    def setUp(self):
        oms_uri = os.getenv('OMS', "launchsimulator")
        oms_uri = self._dispatch_simulator(oms_uri)
        log.debug("oms_uri = %s", oms_uri)
        self.oms = CIOMSClientFactory.create_instance(oms_uri)

        def done():
            CIOMSClientFactory.destroy_instance(self.oms)

        self.addCleanup(done)
