#!/usr/bin/env python

"""
@package ion.services.mi.test.test_instrument_agent
@file ion/services/mi/test_instrument_agent.py
@author Edward Hunter
@brief Test cases for R2 instrument agent.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import time
import unittest

from nose.plugins.attrib import attr
from interface.services.icontainer_agent import ContainerAgentClient
from pyon.agent.agent import ResouceAgentClient
from pyon.util.int_test import IonIntegrationTestCase
import unittest

# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_x

"""
now regarding the client
the client has arguments (see the agents.py and the agents.yml example)
rac = ResourceAgentClient(resource_id=rid, name=the agent exchange name, process=self)
the idea is that name= represents the inbox of the agent (or service for many agents)
in the example it is easy: it is the string of the process_id of the agent process
so 1. agent_id = container.spawn_process(agent_code, config)
2. rac = ResourceAgentClient(name=agent_id etc)
3. rac.execute(agent_cmd)
if you want to use the client in a different container, you need the process id of the agent process
you can find this out from 2 places:
1: resource registry: read agentinstance object
4:01
2: directory service: find agent by id (not fully implemented, but existing)
"""

"""
conainter.spawn_process(agent_code, config)
rac=ResourceAgentClient(name=agent_id etc)
rac.execute()
"""

@attr('INT', group='sa')
#unittest.skip('coi/dm/sa services not working yet for integration tests to pass')
class TestInstrumentAgent(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')


    def test_x(self):
        """
        """
        pass