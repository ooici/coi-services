#!/usr/bin/env python

"""
@package ion.services.mi.drivers.vadcp.test
@file    ion/services/mi/drivers/vadcp/test/__init__.py
@author Carlos Rueda

@brief Supporting stuff for tests
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

import os
import unittest
from ion.services.mi.mi_logger import mi_logger
log = mi_logger


@unittest.skipIf(os.getenv('VADCP') is None,
                 'VADCP environment variable undefined')
class VadcpTestCase(unittest.TestCase):
    """
    """

    @classmethod
    def setUpClass(cls):
        """
        Sets up _vadcp, _timeout, according to corresponding
        environment variables.
        """
        cls._vadcp = os.getenv('VADCP')

        cls._timeout = 30
        timeout_str = os.getenv('timeout')
        if timeout_str:
            try:
                cls._timeout = int(timeout_str)
            except:
                log.warn("Malformed timeout environment variable value '%s'",
                         timeout_str)
        log.info("Generic timeout set to: %d" % cls._timeout)

    def setUp(self):
        """
        """

        if self._vadcp is None:
            # should not happen, but anyway just skip here:
            self.skipTest("Environment variable VADCP undefined")

        try:
            a, p = self._vadcp.split(':')
            port = int(p)
        except:
            self.skipTest("Malformed VADCP value")

        log.info("==Assuming VADCP is listening on %s:%s==" % (a, p))
        self.device_address = a
        self.device_port = port

        self.config = {
            'method': 'ethernet',
            'device_addr': self.device_address,
            'device_port': self.device_port,
            'server_addr': 'localhost',
            'server_port': 8888
        }
