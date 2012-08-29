#!/usr/bin/env python

"""
@package ion.agents.platform.oms.test.test_oms_platform_driver
@file    ion/agents/platform/oms/test/test_oms_platform_driver.py
@author  Carlos Rueda
@brief   Basic OmsPlatformDriver tests
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.oms.oms_platform_driver import OmsPlatformDriver

import unittest
import os
from nose.plugins.attrib import attr


DVR_CONFIG = {
    'oms_uri': os.getenv('OMS', 'embsimulator'),
}


@unittest.skipIf(os.getenv('OMS') is None, "Define OMS to include this test")
@attr('UNIT', group='sa')
class TestOmsPlatformDriver(unittest.TestCase):

    def test(self):
        platform_id = 'platA'
        self._plat_driver = OmsPlatformDriver(platform_id, DVR_CONFIG)
        self._plat_driver.start_resource_monitoring()
