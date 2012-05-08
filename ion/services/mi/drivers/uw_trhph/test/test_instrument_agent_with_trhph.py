#!/usr/bin/env python

"""
@package ion.services.mi.test.test_instrument_agent_with_trhph
@file    ion/services/mi/test/test_instrument_agent_with_trhph.py
@author Carlos Rueda
@brief R2 instrument agent tests with the TRHPH driver.
    Adapted from ion.services.mi.test.test_instrument_agent, which is
    for the SBE37 driver.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from pyon.public import log

import os
import random

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

from pyon.core.exception import InstParameterError


from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase
from ion.services.mi.drivers.uw_trhph.common import TrhphParameter
from ion.services.mi.drivers.uw_trhph.common import TrhphMetadataParameter

from ion.services.mi.logger_process import EthernetDeviceLogger
from ion.services.mi.instrument_agent import InstrumentAgentState
from ion.services.mi.instrument_driver import DriverParameter



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
# TODO any rules to set this ID and name for the agent?
IA_RESOURCE_ID = '123trhph'
IA_NAME = 'InstrAgent_TRHPH'
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
class TestInstrumentAgentWithTrhph(TrhphTestCase, IonIntegrationTestCase):
    """
    R2 instrument agent tests with the TRHPH driver.
    """

    def addCleanup(self, f):
        IonIntegrationTestCase.addCleanup(self, f)

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
        log.debug("TestInstrumentAgentWithTrhph.setup(): starting IA.")
        container_client = ContainerAgentClient(node=self.container.node,
                                                      name=self.container.name)
        self._ia_pid = container_client.spawn_process(name=IA_NAME,
                                module=IA_MOD, cls=IA_CLS, config=agent_config)
        log.info('Agent pid=%s.', str(self._ia_pid))

        # Start a resource agent client to talk with the instrument agent.
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

    def test_01_initialize(self):
        """
        -- INSTR-AGENT/TRHPH: initialize
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

    def test_10_states(self):
        """
        -- INSTR-AGENT/TRHPH: state transitions
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

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='resume')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='clear')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='clear')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def _prepare_and_connect(self):
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

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

    def _get_params(self, params):

        result = self._ia_client.get_param(params)
        log.info('_get_params result: %s' % str(result))

        self.assertTrue(isinstance(result, dict))

        if params == DriverParameter.ALL:
            all_requested_params = TrhphParameter.list()
        else:
            all_requested_params = params

        # check all requested params are in the result
        for p in all_requested_params:
            self.assertTrue(p in result)

            if TrhphParameter.TIME_BETWEEN_BURSTS == p:
                seconds = result.get(p)
                self.assertTrue(isinstance(seconds, int))
            elif TrhphParameter.VERBOSE_MODE == p:
                is_data_only = result.get(p)
                self.assertTrue(isinstance(is_data_only, bool))

        return result

    def test_15_get_params(self):
        """
        -- INSTR-AGENT/TRHPH: get valid and invalid params
        """
        self._prepare_and_connect()

        self._get_params(DriverParameter.ALL)

        p1 = TrhphParameter.TIME_BETWEEN_BURSTS
        p2 = TrhphParameter.VERBOSE_MODE
        self._get_params([p1, p2])

        with self.assertRaises(InstParameterError):
            self._get_params(['bad-param'])

    def _set_params(self, params):
        """
        Sets the given parameters, which are assumed to be all valid.
        """
        result = self._ia_client.set_param(params)
        log.info("set result = %s" % str(result))

        if result is None:
            # TODO check why self._ia_client.set_param returns None
            return

        assert isinstance(result, dict)

        # check all requested params are in the result
        for (p, v) in params.items():
            self.assertTrue(p in result)

        return result

    def _get_verbose_flag_for_set_test(self):
        """
        Gets the value to use for the verbose flag in the "set" operations.
        If we are testing against the real instrument (self._is_real_instrument
        is true), this always returns False because the associated
        interfaces with verbose=True are not implemented yet.
        Otherwise it returns a random boolean value. Note, in this case, which
        means we are testing against the simulator, the actual verbose value
        does not have any effect of the other interface elements, so it is ok
        to set any value here.
        TODO align this when the verbose flag is handled completely,
        both in the driver and in the simulator.
        """
        if self._is_real_instrument:
            log.info("setting verbose=False because _is_real_instrument")
            return False
        else:
            return 0 == random.randint(0, 1)

    def test_20_set_params_valid(self):
        """
        -- INSTR-AGENT/TRHPH: set valid params
        """
        self._prepare_and_connect()

        p1 = TrhphParameter.TIME_BETWEEN_BURSTS
        new_seconds = random.randint(15, 60)

        p2 = TrhphParameter.VERBOSE_MODE
        verbose = self._get_verbose_flag_for_set_test()

        valid_params = {p1: new_seconds, p2: verbose}

        self._set_params(valid_params)

    def test_21_set_params_invalid(self):
        """
        -- INSTR-AGENT/TRHPH: set invalid params
        """
        self._prepare_and_connect()

        p1 = TrhphParameter.TIME_BETWEEN_BURSTS
        new_seconds = random.randint(15, 60)

        invalid_params = {p1: new_seconds, "bad-param": "dummy-value"}

        with self.assertRaises(InstParameterError):
            self._set_params(invalid_params)

    def test_25_get_set_params(self):
        """
        -- INSTR-AGENT/TRHPH: get and set params
        """
        self._prepare_and_connect()

        p1 = TrhphParameter.TIME_BETWEEN_BURSTS
        result = self._get_params([p1])
        seconds = result[p1]
        new_seconds = seconds + 5
        if new_seconds > 30 or new_seconds < 15:
            new_seconds = 15

        p2 = TrhphParameter.VERBOSE_MODE
        new_verbose = self._get_verbose_flag_for_set_test()

        valid_params = {p1: new_seconds, p2: new_verbose}

        log.info("setting: %s" % str(valid_params))

        self._set_params(valid_params)

        result = self._get_params([p1, p2])

        seconds = result[p1]
        self.assertEqual(seconds, new_seconds)

        verbose = result[p2]
        self.assertEqual(verbose, new_verbose)

    def test_60_execute_stop_autosample(self):
        """
        -- INSTR-AGENT/TRHPH: execute stop autosample
        """
        self._prepare_and_connect()

        log.info("stopping autosample")
        cmd = AgentCommand(command='stop_autosample',
                           kwargs=dict(timeout=self._timeout))
        reply = self._ia_client.execute(cmd)
        log.info("stop_autosample reply = %s" % str(reply))

    def test_70_execute_get_metadata(self):
        """
        -- INSTR-AGENT/TRHPH: execute get metadata
        """
        self._prepare_and_connect()

        log.info("getting metadata")
        cmd = AgentCommand(command='get_metadata',
                           kwargs=dict(timeout=self._timeout))
        reply = self._ia_client.execute(cmd)
        log.info("get_metadata reply = %s" % str(reply))
        self.assertTrue(isinstance(reply.result, dict))

    def test_80_execute_diagnostics(self):
        """
        -- INSTR-AGENT/TRHPH: execute diagnostics
        """
        self._prepare_and_connect()

        log.info("executing diagnostics")
        num_scans = 11
        cmd = AgentCommand(command='diagnostics',
                           kwargs=dict(num_scans=num_scans, timeout=self._timeout))
        reply = self._ia_client.execute(cmd)
        log.info("diagnostics reply = %s" % str(reply))
        self.assertTrue(isinstance(reply.result, list))
        self.assertEqual(len(reply.result), num_scans)

    def test_90_execute_get_power_statuses(self):
        """
        -- INSTR-AGENT/TRHPH: execute get power statuses
        """
        self._prepare_and_connect()

        log.info("executing get_power_statuses")
        cmd = AgentCommand(command='get_power_statuses',
                           kwargs=dict(timeout=self._timeout))
        reply = self._ia_client.execute(cmd)
        log.info("get_power_statuses reply = %s" % str(reply))
        self.assertTrue(isinstance(reply.result, dict))

    def test_99_execute_start_autosample(self):
        """
        -- INSTR-AGENT/TRHPH: execute start autosample
        """
        self._prepare_and_connect()

        log.info("executing start_autosample")
        cmd = AgentCommand(command='start_autosample',
                           kwargs=dict(timeout=self._timeout))
        reply = self._ia_client.execute(cmd)
        log.info("start_autosample reply = %s" % str(reply))
