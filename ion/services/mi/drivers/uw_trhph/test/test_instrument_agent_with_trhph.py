#!/usr/bin/env python

"""
@package ion.services.mi.test.test_instrument_agent_with_trhph
@file    ion/services/mi/test/test_instrument_agent_with_trhph.py
@author Carlos Rueda
@brief R2 instrument agent tests with the TRHPH driver.
    directly adapted from ion.services.mi.test.test_instrument_agent, which is
    for the SBE37 driver. TODO complete alignment with mainlne as well as the
    set of tests.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from pyon.public import log

import os

from gevent import spawn
from gevent.event import AsyncResult
import gevent
from nose.plugins.attrib import attr
from mock import patch

from interface.objects import StreamQuery
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.public import CFG
from pyon.event.event import EventSubscriber, EventPublisher

from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase

from ion.services.mi.logger_process import EthernetDeviceLogger
from ion.services.mi.instrument_agent import InstrumentAgentState


# Work dir and logger delimiter.
WORK_DIR = '/tmp/'
DELIM = ['<<','>>']

# Driver config.
# DVR_CONFIG['comms_config']['port'] is set by the setup.

#from ion.services.mi.drivers.sbe37_driver import PACKET_CONFIG
PACKET_CONFIG = {}

DVR_CONFIG = {
    'dvr_mod' : 'ion.services.mi.drivers.uw_trhph.trhph_driver',
    'dvr_cls' : 'TrhphInstrumentDriver',
    'workdir' : '/tmp/',
}

# Agent parameters.
IA_RESOURCE_ID = '123xyz'
IA_NAME = 'Agent007'
IA_MOD = 'ion.services.mi.instrument_agent'
IA_CLS = 'InstrumentAgent'



class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestInstrumentAgent(TrhphTestCase, IonIntegrationTestCase):
    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
    """
    def setUp(self):
        """
        Initialize test members.
        Start port agent.
        Start container and client.
        Start streams and subscribers.
        Start agent, client.
        """

        TrhphTestCase.setUp(self)

        # Start port agent, add stop to cleanup.
        self._pagent = None
        self._start_pagent()
        self.addCleanup(self._stop_pagent)

        # Start container.
        self._start_container()

        # Bring up services in a deploy file (no need to message)
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        # Start data suscribers, add stop to cleanup.
        # Define stream_config.
        self._no_samples = None
        self._async_data_result = AsyncResult()
        self._data_greenlets = []
        self._stream_config = {}
        self._samples_received = []
        self._data_subscribers = []
        self._start_data_subscribers()
        self.addCleanup(self._stop_data_subscribers)

        # Start event subscribers, add stop to cleanup.
        self._no_events = None
        self._async_event_result = AsyncResult()
        self._events_received = []
        self._event_subscribers = []
        self._start_event_subscribers()
        self.addCleanup(self._stop_event_subscribers)

        # Create agent config.
        agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : self._stream_config,
            'agent'         : {'resource_id': IA_RESOURCE_ID},
            'test_mode' : True
        }

        # Start instrument agent.
        self._ia_pid = None
        log.debug("TestInstrumentAgent.setup(): starting IA.")
        container_client = ContainerAgentClient(node=self.container.node,
                                                      name=self.container.name)
        self._ia_pid = container_client.spawn_process(name=IA_NAME,
                                module=IA_MOD, cls=IA_CLS, config=agent_config)
        log.info('Agent pid=%s.', str(self._ia_pid))

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = None
        self._ia_client = ResourceAgentClient(IA_RESOURCE_ID, process=FakeProcess())
        log.info('Got ia client %s.', str(self._ia_client))

    def _start_pagent(self):
        """
        Construct and start the port agent.
        """

        # Create port agent object.
        this_pid = os.getpid()
        self._pagent = EthernetDeviceLogger.launch_process(self.device_address,
                                                           self.device_port,
                                                           WORK_DIR,
                                                           DELIM,
                                                           this_pid)

        # Get the pid and port agent server port number.
        pid = self._pagent.get_pid()
        while not pid:
            gevent.sleep(.1)
            pid = self._pagent.get_pid()
        port = self._pagent.get_port()
        while not port:
            gevent.sleep(.1)
            port = self._pagent.get_port()

        # Configure driver to use port agent port number.
        DVR_CONFIG['comms_config'] = {
            'addr' : 'localhost',
            'port' : port
        }

        # Report.
        log.info('Started port agent pid %d listening at port %d.', pid, port)

    def _stop_pagent(self):
        """
        Stop the port agent.
        """
        if self._pagent:
            pid = self._pagent.get_pid()
            if pid:
                log.info('Stopping pagent pid %i.', pid)
                self._pagent.stop()
            else:
                log.warning('No port agent running.')

    def _start_data_subscribers(self):
        """
        """
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)

        # A callback for processing subscribed-to data.
        def consume_data(message, headers):
            log.info('Subscriber received data message: %s.', str(message))
            self._samples_received.append(message)
            if self._no_samples and self._no_samples == len(self._samples_received):
                self._async_data_result.set()

        # Create a stream subscriber registrar to create subscribers.
        subscriber_registrar = StreamSubscriberRegistrar(process=self.container,
                                                node=self.container.node)

        # Create streams and subscriptions for each stream named in driver.
        self._stream_config = {}
        self._data_subscribers = []
        for (stream_name, val) in PACKET_CONFIG.iteritems():
            stream_def = ctd_stream_definition(stream_id=None)
            stream_def_id = pubsub_client.create_stream_definition(
                                                    container=stream_def)
            stream_id = pubsub_client.create_stream(
                        name=stream_name,
                        stream_definition_id=stream_def_id,
                        original=True,
                        encoding='ION R2')
            self._stream_config[stream_name] = stream_id

            # Create subscriptions for each stream.
            exchange_name = '%s_queue' % stream_name
            sub = subscriber_registrar.create_subscriber(exchange_name=exchange_name,
                                                         callback=consume_data)
            self._listen(sub)
            self._data_subscribers.append(sub)
            query = StreamQuery(stream_ids=[stream_id])
            sub_id = pubsub_client.create_subscription(\
                                query=query, exchange_name=exchange_name)
            pubsub_client.activate_subscription(sub_id)

    def _listen(self, sub):
        """
        Pass in a subscriber here, this will make it listen in a background greenlet.
        """
        gl = spawn(sub.listen)
        self._data_greenlets.append(gl)
        sub._ready_event.wait(timeout=5)
        return gl

    def _stop_data_subscribers(self):
        """
        Stop the data subscribers on cleanup.
        """
        for sub in self._data_subscribers:
            sub.stop()
        for gl in self._data_greenlets:
            gl.kill()

    def _start_event_subscribers(self):
        """
        Create subscribers for agent and driver events.
        """
        def consume_event(*args, **kwargs):
            log.info('Test recieved ION event: args=%s  kwargs=%s.', str(args), str(kwargs))
            self._events_received.append(args[0])
            if self._no_events and self._no_events == len(self._event_received):
                self._async_event_result.set()

        event_sub = EventSubscriber(event_type="DeviceEvent", callback=consume_event)
        event_sub.activate()
        self._event_subscribers.append(event_sub)

    def _stop_event_subscribers(self):
        """
        Stop event subscribers on cleanup.
        """
        for sub in self._event_subscribers:
            sub.deactivate()

    def test_initialize(self):
        """
        Test agent initialize command.
        """

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_states(self):
        """
        Test agent state transitions.
        """
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        print 'AAAA %s' % str(retval)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        print 'BBBB %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        print 'CCCC %s' % str(retval)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        print 'DDDD %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        print 'EEEE %s' % str(retval)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        print 'FFFF %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        print 'GGGG %s' % str(retval)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._ia_client.execute_agent(cmd)
        print 'HHHH %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='resume')
        retval = self._ia_client.execute_agent(cmd)
        print 'IIII %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='clear')
        retval = self._ia_client.execute_agent(cmd)
        print 'JJJJ %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        print 'KKKK %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._ia_client.execute_agent(cmd)
        print 'LLLL %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='clear')
        retval = self._ia_client.execute_agent(cmd)
        print 'MMMM %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        print 'NNNN %s' % str(retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        print 'NNNN2 %s' % str(retval)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)
