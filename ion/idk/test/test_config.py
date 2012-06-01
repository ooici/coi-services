#!/usr/bin/env python

"""
@package ion.idk.test.test_config
@file ion/idk/test/test_config.py
@author Bill French
@brief test metadata object
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

from os import remove, makedirs
from os.path import exists
from shutil import rmtree
import os

from nose.plugins.attrib import attr
from mock import Mock
import unittest

from pyon.util.log import log
from ion.idk.metadata import Metadata
from ion.idk.config import Config

from ion.idk.exceptions import DriverParameterUndefined
from ion.idk.exceptions import NoConfigFileSpecified
from ion.idk.exceptions import CommConfigReadFail
from ion.idk.exceptions import InvalidCommType

ROOTDIR="/tmp/test_config.idk_test"
# /tmp is a link on OS X
if exists("/private/tmp"):
    ROOTDIR = "/private%s" % ROOTDIR

@unittest.skip('Skip until moved to MI repo')
@attr('UNIT', group='mi')
class TestConfig(unittest.TestCase):
    """
    Test the config object.  
    """    
    def setUp(self):
        """
        Setup the test case
        """
        Config().cm.destroy()

        if not exists(ROOTDIR):
            makedirs(ROOTDIR)
            
        if exists(self.config_file()):
            log.debug("remove test dir %s" % self.config_file())
            remove(self.config_file())
        self.assertFalse(exists(self.config_file()))


    def config_file(self):
        return "%s/idk.yml" % ROOTDIR

    def read_config(self):
        infile = open(self.config_file(), "r")
        result = infile.read()
        infile.close()
        return result

    def write_config(self):
        outfile = open(self.config_file(), "a")
        outfile.write("  couchdb: couchdb")
        outfile.close()

    def test_default_config(self):
        """Test that the default configuration is created"""
        config = Config(ROOTDIR)
        self.assertTrue(config)

        expected_string = "idk:\n  working_repo: %s\n" % config.get("working_repo")

        self.assertEqual(expected_string, self.read_config())
        self.assertTrue(config.get("working_repo"))
        self.assertTrue(config.get("template_dir"))
        self.assertTrue(config.get("couchdb"))
        self.assertTrue(config.get("rabbit-server"))

    def test_overloaded_config(self):
        """Test that the overloaded configuration"""
        # Build the default config and add a line
        config = Config(ROOTDIR)
        self.write_config()
        self.assertTrue(config)

        # reload the configuration
        config.cm.init(ROOTDIR)

        expected_string = "idk:\n  working_repo: %s\n  couchdb: %s" % (config.get("working_repo"), config.get("couchdb"))

        self.assertEqual(expected_string, self.read_config())
        self.assertEqual(config.get("couchdb"), "couchdb")
        self.assertTrue(config.get("working_repo"))
        self.assertTrue(config.get("template_dir"))
        self.assertTrue(config.get("rabbit-server"))






