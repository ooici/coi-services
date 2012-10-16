#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.test.test_remote_client
@file ion/services/sa/tcaa/test/test_remote_client.py
@author Edward Hunter
@brief Test cases for 2CAA remote clients.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.

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
from pyon.event.event import EventPublisher, EventSubscriber
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpoint
from interface.services.icontainer_agent import ContainerAgentClient
from ion.services.sa.tcaa.remote_client import RemoteClient

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_client.py:TestRemoteClient
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_remote_client.py:TestRemoteClient.test_xxx

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''
    
@attr('INT', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestRemoteClient(IonIntegrationTestCase):
    """
    Test cases for 2CAA remote clients.
    """
    def setUp(self):
        """
        """
        pass
    
    def test_xxx(self):
        """
        """
        pass
