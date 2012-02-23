#!/usr/bin/env python

"""
@package ion.processes.idk.test.driver_qualification
@file ion/processes/idk/test/driver_qualification.py
@author Bill French
@brief Qualification tests for all drivers.  Ensure the driver has minimal instruction set to integrate into ION
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'


import sys
import unittest
import pprint

from nose.plugins.attrib import attr

import ion.services.mi.mi_logger
from pyon.util.log import log

from ion.processes.idk.metadata import Metadata
from ion.processes.idk.comm_config import CommConfig


@attr('QUAL', group='mi')
class TestDriverQualification():
    def setUp(self):
        self.comm_config = self._get_comm_config()

    def tearDown(self):
        pass

    def _get_comm_config(self):
        raise Exception( "TODO: update this test so it can be run from the command line with nosetest" )

    def test_a(self):
        assert 'a' == 'a'


class RunFromIDK(TestDriverQualification):
    """
    This class overloads the default test class so that comm configurations can be overloaded.  This is the test class
    called from the IDK test_driver program
    """
    def _get_comm_config(self):
        return CommConfig(Metadata())

