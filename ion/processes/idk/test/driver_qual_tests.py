#!/usr/bin/env python

"""
@package ion.processes.idk.test.driver_qual_tests
@file ion/processes/idk/test/driver_qual_tests.py
@author Bill French
@brief Qualification tests for all drivers.  Ensure the driver has minimal instruction set to integrate into ION
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'


from nose.plugins.attrib import attr

import ion.services.mi.mi_logger

from ion.processes.idk.metadata import Metadata
from ion.processes.idk.comm_config import CommConfig


@attr('QUAL', group='mi')
def test_b():
    assert 'a' == 'a'

