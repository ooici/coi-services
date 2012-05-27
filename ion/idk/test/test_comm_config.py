#!/usr/bin/env python

"""
@package ion.idk.test.test_comm_config
@file ion/idk/test/test_comm_config.py
@author Bill French
@brief test metadata object
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

from os.path import basename, dirname
from os import makedirs
from os import remove
from os.path import exists
import sys

from nose.plugins.attrib import attr
from mock import Mock
import unittest

from pyon.util.log import log
from ion.idk.metadata import Metadata
from ion.idk.comm_config import CommConfig

from ion.idk.exceptions import DriverParameterUndefined
from ion.idk.exceptions import NoConfigFileSpecified
from ion.idk.exceptions import CommConfigReadFail
from ion.idk.exceptions import InvalidCommType

ROOTDIR="/tmp/test_config.idk_test"
# /tmp is a link on OS X
if exists("/private/tmp"):
    ROOTDIR = "/private%s" % ROOTDIR
    
CONFIG_FILE="comm_config.yml"
    
@attr('UNIT', group='mi')
class TestCommConfig(unittest.TestCase):
    """
    Test the comm config object.  
    """    
    def setUp(self):
        """
        Setup the test case
        """
        if not exists(ROOTDIR):
            makedirs(ROOTDIR)
            
        self.write_config()
        
    def config_file(self):
        return "%s/comm_config.yml" % ROOTDIR
    
    def config_content(self):
        return "comm:\n" + \
               "  device_addr: localhost\n" + \
               "  device_port: 1000\n" + \
               "  method: ethernet\n" + \
               "  server_addr: localhost\n" + \
               "  server_port: 2000\n"
               
    def write_config(self):
        ofile = open(self.config_file(), "w");
        ofile.write(self.config_content())
        ofile.close()
               
    def read_config(self):
        infile = open(self.config_file(), "r")
        result = infile.read()
        infile.close()
        return result
               
    def test_constructor(self):
        """
        Test object creation
        """
        config = CommConfig()
        self.assertTrue(config)
    
    def test_exceptions(self):
        """
        Test exceptions raised by the CommConfig object
        """
        ## No exception thrown if file doesn't exist
        error = None
        try:
            config = CommConfig("this_file_does_not_exist.foo")
        except CommConfigReadFail, e:
            error = e
        self.assertFalse(error)
        
        error = None
        try:
            config = CommConfig()
            config.read_from_file("/tmp");
        except CommConfigReadFail, e:
            log.debug("caught error %s" % e)
            error = e
        self.assertTrue(error)
        
        error = None
        try:
            config = CommConfig()
            config.store_to_file();
        except NoConfigFileSpecified, e:
            log.debug("caught error %s" % e)
            error = e
        self.assertTrue(error)
        
        error = None
        try:
            config = CommConfig.get_config_from_type(self.config_file(), "foo")
        except InvalidCommType, e:
            log.debug("caught error %s" % e)
            error = e
        self.assertTrue(error)
    
    def test_comm_config_type_list(self):
        types = CommConfig.valid_type_list()
        log.debug( "types: %s" % types)
        
        known_types = ['ethernet']
        
        self.assertEqual(sorted(types), sorted(known_types))
        
    def test_config_write_ethernet(self):
        log.debug("Config File: %s" % self.config_file())
        if exists(self.config_file()):
            log.debug(" -- remove %s" % self.config_file())
            remove(self.config_file())
            
        self.assertFalse(exists(self.config_file()))
        
        config = CommConfig.get_config_from_type(self.config_file(), "ethernet")
        config.device_addr = 'localhost'
        config.device_port = 1000
        config.server_addr = 'localhost'
        config.server_port = 2000
        
        log.debug("CONFIG: %s" % config.serialize())
        
        config.store_to_file()
        
        self.assertEqual(self.config_content(), self.read_config())
        
    def test_config_read_ethernet(self):
        config = CommConfig.get_config_from_type(self.config_file(), "ethernet")
        
        self.assertEqual(config.device_addr, 'localhost')
        self.assertEqual(config.device_port, 1000)
        self.assertEqual(config.server_addr, 'localhost')
        self.assertEqual(config.server_port, 2000)
        
    