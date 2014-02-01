#!/usr/bin/env python

"""
@package ion.services.sa.test.test_register_and_activate
@file ion/services/sa/test/test_register_and_activate.py
@author Edward Hunter
@brief Integration test cases to confirm registration and activation services
for marine device resources.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon imports
from pyon.public import IonObject, log, RT, PRED, LCS, OT, CFG

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase


"""
--with-queueblame   report leftover queues
--with-pycc         run in seperate container
--with-greenletleak ewpoer leftover greenlets
bin/nosetests -s -v --nologcapture ion/services/sa/test/test_register_and_activate.py:TestRegisterAndActivate
"""

class TestRegisterAndActivate(IonIntegrationTestCase):
    """
    Integration test cases to confirm registration and activation services
    for marine device resources.
    """

    def _setup(self):
        """
        Test setup.
        """

        # Start container.
        log.info('Staring capability container.')
        self._start_container()

        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')



    def test_instrument_device(self):
        """
        Test instrument device registration and activation.
        """
        pass
