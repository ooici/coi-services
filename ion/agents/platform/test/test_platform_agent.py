#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_platform_agent
@file    ion/agents/platform/test/test_platform_agent.py
@author  Carlos Rueda
@brief   Unit test cases for R2 platform agent.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import unittest
from nose.plugins.attrib import attr


@attr('UNIT', group='sa')
class TestPlatformAgent(unittest.TestCase):
    """
    Class intended for *unit* tests; currently none as the current focus is
    on the more integrated behavior, see test_platform_agent_with_oms.
    """
    # TODO define appropriate unit tests for the platform agent class.
    pass
