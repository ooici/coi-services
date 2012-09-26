#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.test.test_terrestrial_endpoint
@file ion/services/sa/tcaa/test/test_terrestrial_endpoint.py
@author Edward Hunter
@brief Test cases for 2CAA terrestrial endpoint.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.
import time
import os
import signal
import time
import unittest
from datetime import datetime
import uuid
import socket
import re
import random

# 3rd party imports.
import gevent
from gevent import spawn
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase

from pyon.public import IonObject
from pyon.util.context import LocalContextMixin
from interface.services.icontainer_agent import ContainerAgentClient
from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_endpoint.py:TestRemoteEndpoint.test_xxx

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''
        
@attr('INT', group='sa')
class TestRemoteEndpoint(IonIntegrationTestCase):
    """
    Test cases for 2CAA terrestrial endpoint.
    """
    def setUp(self):
        """
        """
        pass
    
    def test_xxx(self):
        """
        """
        pass

