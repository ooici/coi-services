#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, sentinel, patch
from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase
from pyon.core.exception import NotFound

from ion.services.mi.instrument_agent import InstrumentAgent


@attr('UNIT', group='sa')
class InstrumentAgentTest(PyonTestCase):

    def setUp(self):
        """
        """
        pass
    
    def test_instrument_agent(self):
        """
        """
        ia = InstrumentAgent()
        self.assertIsInstance(ia, InstrumentAgent)