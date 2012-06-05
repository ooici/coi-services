#!/usr/bin/env python

"""
@package ion.idk.test.test_metadata
@file ion/idk/test/test_metadata.py
@author Bill French
@brief test metadata object
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

from os.path import basename, dirname
from os import makedirs
from os.path import exists
import sys

from nose.plugins.attrib import attr
from mock import Mock
import unittest

from pyon.util.log import log
from ion.idk.metadata import Metadata

from ion.idk.exceptions import InvalidParameters

@unittest.skip('Skip until moved to MI repo')
@attr('UNIT', group='mi')
class TestMetadata(unittest.TestCase):
    """
    Test the metadata object
    """    
    def setUp(self):
        """
        Setup the test case
        """
        
    def test_constructor(self):
        """
        Test object creation
        """
        default_metadata = Metadata()
        self.assertTrue(default_metadata)
        
        specific_metadata = Metadata('seabird','sbe37smb','ooicore');
        self.assertTrue(specific_metadata)
        
        failure_metadata = None;
        try:
            failure_metadata = Metadata('seabird');
        except InvalidParameters, e:
            self.assertTrue(e)
        self.assertFalse(failure_metadata)
