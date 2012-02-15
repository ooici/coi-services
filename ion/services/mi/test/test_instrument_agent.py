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

from pyon.public import log
from nose.plugins.attrib import attr
from interface.services.icontainer_agent import ContainerAgentClient
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from ion.services.mi.drivers.sbe37_driver import SBE37Channel


# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_initialize
# bin/nosetests -s -v ion/services/mi/test/test_instrument_agent.py:TestInstrumentAgent.test_go_active


class FakeProcess(LocalContextMixin):
    name = ''


#@unittest.skip('Do not run hardware test.')
@attr('INT', group='sa')
class TestInstrumentAgent(IonIntegrationTestCase):

    def setUp(self):
        
        
        # Driver module parameters.
        self.driver_config = {
            'svr_addr': 'localhost',
            'cmd_port': 5556,
            'evt_port': 5557,
            'dvr_mod': 'ion.services.mi.drivers.sbe37_driver',
            'dvr_cls': 'SBE37Driver'
        }

        # Comms config.
        self.comms_config = {
            SBE37Channel.CTD: {
                'method':'ethernet',
                'device_addr': '137.110.112.119',
                'device_port': 4001,
                'server_addr': 'localhost',
                'server_port': 8888
            }
        }
        
        # Start container
        self._start_container()

        # Establish endpoint with container
        self._container_client = ContainerAgentClient(node=self.container.node,
                                                      name=self.container.name)
        
        # Bring up services in a deploy file.        
        self._container_client.start_rel_from_url('res/deploy/r2sa.yml')

        # Launch an instrument agent process.
        self._ia_name = 'agent007'
        self._ia_mod = 'ion.services.mi.instrument_agent'
        self._ia_class = 'InstrumentAgent'
        self._ia_pid = self._container_client.spawn_process(name=self._ia_name,
                                       module=self._ia_mod, cls=self._ia_class)      
        log.info('got pid=%s', str(self._ia_pid))
        
        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient('a resource id', name=self._ia_pid,
                                              process=FakeProcess())
        log.info('got ia client %s', str(self._ia_client))

    def test_initialize(self):
        """
        """
        args = [
            self.driver_config,
            self.comms_config
        ]
        cmd = AgentCommand(command='initialize', args=args)
        retval = self._ia_client.execute_agent(cmd)        
        time.sleep(2)
        
        caps = self._ia_client.get_capabilities()
        print 'got caps: %s' % str(caps)
        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)

    def test_go_active(self):
        """
        """
        args = [
            self.driver_config,
            self.comms_config
        ]
        cmd = AgentCommand(command='initialize', args=args)
        retval = self._ia_client.execute_agent(cmd)        
        time.sleep(2)
        
        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='go_inactive')
        retval = self._ia_client.execute_agent(cmd)
        time.sleep(2)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        time.sleep(2)


