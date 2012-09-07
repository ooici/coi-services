#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'


from pyon.public import log
from pyon.public import CFG
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from unittest import SkipTest
from nose.plugins.attrib import attr

import time
import unittest

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_r3pc.py
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_r3pc.py:TestR3PCSocket.test_something


@attr('INT', group='sa')
class TestR3PCSocket(PyonTestCase):

    def setUp(self):
        log.info('I am testing this setup thing now.')
        print 'In the setup'

    
    def test_something(self):
        """
        """
        log.info('I am testing this thing now.')
        print 'This is a print statement!'




