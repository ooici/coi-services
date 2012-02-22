#!/usr/bin/env python

"""
@package ion.services.mi.test.test_instrument_driver
@file ion/services/mi/test_instrument_driver.py
@author Steve Foley
@brief Test cases for R2 instrument driver
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

import time
import unittest
import logging
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from mock import Mock
from ion.services.mi.instrument_driver import DriverChannel
from ion.services.mi.common import InstErrorCode
from ion.services.mi.exceptions import RequiredParameterException
from ion.services.mi.drivers.satlantic_par.satlantic_par import Channel
from ion.services.mi.drivers.satlantic_par.satlantic_par import SatlanticPARInstrumentDriver


mi_logger = logging.getLogger('mi_logger')

# bin/nosetests -s -v ion/services/mi/test/test_instrument_driver.py:InstrumentDriverTest

@attr('UNIT', group='mi')
class InstrumentDriverTest(PyonTestCase):
    """Test the instrument driver class"""
    def setUp(self):
        self.callback = Mock()
        # pick an instantiated driver, but only poke base class methods
        self.driver = SatlanticPARInstrumentDriver(self.callback) 
    
    
    def test_check_channel(self):
        """Test the routines to check the channel arguments"""
        self.assertRaises(RequiredParameterException,
                          self.driver._check_channel_args, DriverChannel.ALL)
        self.assertRaises(RequiredParameterException,
                          self.driver._check_channel_args, [])
        self.assertRaises(RequiredParameterException,
                          self.driver._check_channel_args, None)
        
        (bad, good) = self.driver._check_channel_args([DriverChannel.INSTRUMENT])
        self.assertEquals(bad, {})
        self.assertEquals(good, [DriverChannel.INSTRUMENT])
        
        (bad, good) = self.driver._check_channel_args(["BAD_CHANNEL"])
        self.assertEquals(bad, {"BAD_CHANNEL":InstErrorCode.INVALID_CHANNEL})
        self.assertEquals(good, [])
        
        (bad, good) = self.driver._check_channel_args([Channel.PAR])
        self.assertEquals(bad, {})
        self.assertEquals(good, [Channel.PAR])
        
        (bad, good) = self.driver._check_channel_args([Channel.PAR,
                                                       Channel.PAR])
        self.assertEquals(bad, {})
        self.assertEquals(good, [Channel.PAR])

        # @todo Need a better test...something with more channels
        (bad, good) = self.driver._check_channel_args([Channel.PAR,
                                                       Channel.ALL])
        self.assertEquals(bad, {})
        self.assertEquals(good, [Channel.PAR])

        (bad, good) = self.driver._check_channel_args([Channel.PAR,
                                                       Channel.INSTRUMENT])
        self.assertEquals(bad, {})
        self.assertEquals(good.count(Channel.PAR), 1)
        self.assertEquals(good.count(Channel.INSTRUMENT), 1)
        self.assertEquals(len(good), 2)
        

        (bad, good) = self.driver._check_channel_args([Channel.PAR,
                                                       "BAD_CHANNEL"])
        self.assertEquals(bad, {"BAD_CHANNEL":InstErrorCode.INVALID_CHANNEL})
        self.assertEquals(good, [Channel.PAR])

        
        
        
        
    
