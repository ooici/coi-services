#!/usr/bin/env python
"""
@package ion.services.sa.tcaa.test.test_2caa
@file ion/services/sa/tcaa/test/test_2caa.py
@author Edward Hunter
@brief Test cases combined 2CAA endpoints.
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
from pyon.util.context import LocalContextMixin

# Pyon exceptions.
from pyon.core.exception import IonException
from pyon.core.exception import BadRequest
from pyon.core.exception import ServerError
from pyon.core.exception import NotFound

# Endpoints.
from ion.services.sa.tcaa.remote_endpoint import RemoteEndpoint
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpoint
from ion.services.sa.tcaa.remote_endpoint import RemoteEndpointClient
from ion.services.sa.tcaa.terrestrial_endpoint import TerrestrialEndpointClient
from interface.services.icontainer_agent import ContainerAgentClient

# Publishers, subscribers.
from pyon.event.event import EventPublisher, EventSubscriber
from interface.objects import TelemetryStatusType

# Objects.
from pyon.public import IonObject
from interface.objects import UserInfo

# Agent imports.
from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from interface.objects import AgentCommand
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent

# IA config imports.
from ion.agents.instrument.test.test_instrument_agent import DRV_MOD
from ion.agents.instrument.test.test_instrument_agent import DRV_CLS
from ion.agents.instrument.test.test_instrument_agent import DVR_CONFIG
from ion.agents.instrument.test.test_instrument_agent import DEV_ADDR
from ion.agents.instrument.test.test_instrument_agent import DEV_PORT
from ion.agents.instrument.test.test_instrument_agent import DATA_PORT
from ion.agents.instrument.test.test_instrument_agent import CMD_PORT
from ion.agents.instrument.test.test_instrument_agent import PA_BINARY
from ion.agents.instrument.test.test_instrument_agent import DELIM
from ion.agents.instrument.test.test_instrument_agent import WORK_DIR
from ion.agents.instrument.test.test_instrument_agent import IA_RESOURCE_ID
from ion.agents.instrument.test.test_instrument_agent import IA_NAME
from ion.agents.instrument.test.test_instrument_agent import IA_MOD
from ion.agents.instrument.test.test_instrument_agent import IA_CLS

# IA launch imports.
from ion.agents.instrument.test.test_instrument_agent import start_instrument_agent_process
from ion.agents.instrument.driver_int_test_support import DriverIntegrationTestSupport

# bin/nosetests -s -v ion/services/sa/tcaa/test/test_2caa.py:Test2CAA
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_2caa.py:Test2CAA.test_xxx


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''
        
@attr('INT', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class Test2CAA(IonIntegrationTestCase):
    """
    Test cases for 2CAA terrestrial endpoint.
    """
    def setUp(self):
        """
        """
        pass
    
    def test_xxx(sefl):
        """
        """
        pass
