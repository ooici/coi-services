#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.test.test_trhph_driver
@file    ion/services/mi/drivers/uw_trhph/test/test_trhph_driver.py
@author Carlos Rueda
@brief Direct tests to the TrhphInstrumentDriver class.
"""

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'


from ion.services.mi.drivers.uw_trhph.trhph_driver import TrhphInstrumentDriver

from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase
from ion.services.mi.drivers.uw_trhph.test.driver_test_mixin import DriverTestMixin
from nose.plugins.attrib import attr

from ion.services.mi.mi_logger import mi_logger
log = mi_logger

import unittest
import os


@unittest.skipIf(os.getenv('run_it') is None,
'''Not run by default because of mixed monkey-patching issues. \
Define environment variable run_it to force execution.''')
@attr('UNIT', group='mi')
class DriverTest(TrhphTestCase, DriverTestMixin):
    """
    Direct tests to the TrhphInstrumentDriver class. The actual set of tests
    is provided by DriverTestMixin.
    """

    def setUp(self):
        """
        Calls TrhphTestCase.setUp(self),creates and assigns the
        TrhphDriverProxy, and assign the comm_config object.
        """

        TrhphTestCase.setUp(self)

        def evt_callback(event):
            log.info("CALLBACK: %s" % str(event))

        # needed by DriverTestMixin
        self.driver = TrhphInstrumentDriver(evt_callback)
        self.comms_config = {
            'addr': self.device_address,
            'port': self.device_port}
