#!/usr/bin/env python

"""
@package ion.services.mi.instrument_agent Instrument resource agent
@file ion/services/mi/instrument_agent.py
@author Edward Hunter
@brief Resource agent derived class providing an instrument agent as a resource.
This resource fronts instruments and instrument drivers one-to-one in ION.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon imports
from pyon.public import IonObject, log
from pyon.agent.agent import ResourceAgent
from pyon.core import exception as iex
from pyon.util.containers import get_ion_ts
from pyon.ion.stream import StreamPublisherRegistrar
from pyon.event.event import EventPublisher
from pyon.util.containers import get_safe

# Pyon exceptions
from pyon.core.exception import exception_map
from pyon.core.exception import IonException
from pyon.core.exception import BadRequest
from pyon.core.exception import Conflict
from pyon.core.exception import Timeout
from pyon.core.exception import NotFound
from pyon.core.exception import IonInstrumentError
from pyon.core.exception import InstTimeoutError
from pyon.core.exception import InstConnectionError
from pyon.core.exception import InstNotImplementedError
from pyon.core.exception import InstParameterError
from pyon.core.exception import InstProtocolError
from pyon.core.exception import InstSampleError
from pyon.core.exception import InstStateError
from pyon.core.exception import InstUnknownCommandError
from pyon.core.exception import InstDriverError

# Standard imports.
import time
import socket
import os
import traceback

# ION service imports.
from ion.services.mi.instrument_fsm import InstrumentFSM
from ion.services.mi.common import BaseEnum
from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess
from ion.services.sa.direct_access.direct_access_server import DirectAccessServer, DirectAccessTypes

# MI imports.
from ion.services.mi.exceptions import ConnectionError
from ion.services.mi.exceptions import InstrumentException
from ion.services.mi.exceptions import NotImplementedError
from ion.services.mi.exceptions import ParameterError
from ion.services.mi.exceptions import ProtocolError
from ion.services.mi.exceptions import SampleError
from ion.services.mi.exceptions import StateError
from ion.services.mi.exceptions import TimeoutError
from ion.services.mi.exceptions import UnknownCommandError
from ion.services.mi.instrument_driver import DriverConnectionState
from ion.services.mi.instrument_driver import DriverProtocolState
from ion.services.mi.instrument_driver import DriverAsyncEvent

class InstrumentAgentState(BaseEnum):
    """
    Instrument agent state enum.
    """
    POWERED_DOWN = 'INSTRUMENT_AGENT_STATE_POWERED_DOWN'
    UNINITIALIZED = 'INSTRUMENT_AGENT_STATE_UNINITIALIZED'
    INACTIVE = 'INSTRUMENT_AGENT_STATE_INACTIVE'
    IDLE = 'INSTRUMENT_AGENT_STATE_IDLE'
    STOPPED = 'INSTRUMENT_AGENT_STATE_STOPPED'
    OBSERVATORY = 'INSTRUMENT_AGENT_STATE_OBSERVATORY'
    STREAMING = 'INSTRUMENT_AGENT_STATE_STREAMING'
    TEST = 'INSTRUMENT_AGENT_STATE_TEST'
    CALIBRATE = 'INSTRUMENT_AGENT_STATE_CALIBRATE'
    DIRECT_ACCESS = 'INSTRUMENT_AGENT_STATE_DIRECT_ACCESS'
        
class InstrumentAgentEvent(BaseEnum):
    """
    Instrument agent event enum.
    """
    ENTER = 'INSTRUMENT_AGENT_EVENT_ENTER'
    EXIT = 'INSTRUMENT_AGENT_EVENT_EXIT'
    POWER_UP = 'INSTRUMENT_AGENT_EVENT_POWER_UP'
    POWER_DOWN = 'INSTRUMENT_AGENT_EVENT_POWER_DOWN'
    INITIALIZE = 'INSTRUMENT_AGENT_EVENT_INITIALIZE'
    RESET = 'INSTRUMENT_AGENT_EVENT_RESET'
    GO_ACTIVE = 'INSTRUMENT_AGENT_EVENT_GO_ACTIVE'
    GO_INACTIVE = 'INSTRUMENT_AGENT_EVENT_GO_INACTIVE'
    RUN = 'INSTRUMENT_AGENT_EVENT_RUN'
    CLEAR = 'INSTRUMENT_AGENT_EVENT_CLEAR'
    PAUSE = 'INSTRUMENT_AGENT_EVENT_PAUSE'
    RESUME = 'INSTRUMENT_AGENT_EVENT_RESUME'
    GO_OBSERVATORY = 'INSTRUMENT_AGENT_EVENT_GO_OBSERVATORY'
    GO_DIRECT_ACCESS = 'INSTRUMENT_AGENT_EVENT_GO_DIRECT_ACCESS'
    GO_STREAMING = 'INSTRUMENT_AGENT_EVENT_GO_STREAMING'
    GET_RESOURCE_PARAMS = 'INSTRUMENT_AGENT_EVENT_GET_RESOURCE_PARAMS'
    GET_RESOURCE_COMMANDS = 'INSTRUMENT_AGENT_EVENT_GET_RESOURCE_COMMANDS'
    GET_PARAMS = 'INSTRUMENT_AGENT_EVENT_GET_PARAMS'
    SET_PARAMS = 'INSTRUMENT_AGENT_EVENT_SET_PARAMS'
    EXECUTE_RESOURCE = 'INSTRUMENT_AGENT_EVENT_EXECUTE_RESOURCE'

class InstrumentAgent(ResourceAgent):
    """
    ResourceAgent derived class for the instrument agent. This class
    logically abstracts instruments as taskable resources in the ION
    system. It directly provides common functionality (common state model,
    common resource interface, point of publication) and creates
    a driver process to specialize for particular hardware.
    """

    # Override to publish specific types of events
    COMMAND_EVENT_TYPE = "DeviceCommandEvent"
    # Override to set specific origin type
    ORIGIN_TYPE = "InstrumentDevice"

    def __init__(self, initial_state=InstrumentAgentState.UNINITIALIZED):
        """
        Initialize instrument agent prior to pyon process initialization.
        Define state machine, initialize member variables.
        """
        log.debug("InstrumentAgent.__init__: initial_state = <" + str(initial_state) + ">" )
        ResourceAgent.__init__(self)
                
        # Instrument agent state machine.
        self._fsm = InstrumentFSM(InstrumentAgentState, InstrumentAgentEvent, InstrumentAgentEvent.ENTER,
                                  InstrumentAgentEvent.EXIT)
        
        # Populate state machine for all state-events.
        self._fsm.add_handler(InstrumentAgentState.POWERED_DOWN, InstrumentAgentEvent.ENTER, self._handler_powered_down_enter)
        self._fsm.add_handler(InstrumentAgentState.POWERED_DOWN, InstrumentAgentEvent.EXIT, self._handler_powered_down_exit)
        self._fsm.add_handler(InstrumentAgentState.POWERED_DOWN, InstrumentAgentEvent.POWER_UP, self._handler_powered_down_power_up)
        
        self._fsm.add_handler(InstrumentAgentState.UNINITIALIZED, InstrumentAgentEvent.ENTER, self._handler_uninitialized_enter)
        self._fsm.add_handler(InstrumentAgentState.UNINITIALIZED, InstrumentAgentEvent.EXIT, self._handler_uninitialized_exit)
        self._fsm.add_handler(InstrumentAgentState.UNINITIALIZED, InstrumentAgentEvent.POWER_DOWN, self._handler_uninitialized_power_down)
        self._fsm.add_handler(InstrumentAgentState.UNINITIALIZED, InstrumentAgentEvent.INITIALIZE, self._handler_uninitialized_initialize)

        self._fsm.add_handler(InstrumentAgentState.INACTIVE, InstrumentAgentEvent.ENTER, self._handler_inactive_enter)
        self._fsm.add_handler(InstrumentAgentState.INACTIVE, InstrumentAgentEvent.EXIT, self._handler_inactive_exit)
        self._fsm.add_handler(InstrumentAgentState.INACTIVE, InstrumentAgentEvent.RESET, self._handler_inactive_reset)
        self._fsm.add_handler(InstrumentAgentState.INACTIVE, InstrumentAgentEvent.GO_ACTIVE, self._handler_inactive_go_active)
        self._fsm.add_handler(InstrumentAgentState.INACTIVE, InstrumentAgentEvent.GET_RESOURCE_COMMANDS, self._handler_get_resource_commands)
        self._fsm.add_handler(InstrumentAgentState.INACTIVE, InstrumentAgentEvent.GET_RESOURCE_PARAMS, self._handler_get_resource_params)

        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.ENTER, self._handler_idle_enter)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.EXIT, self._handler_idle_exit)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.RUN, self._handler_idle_run)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.GET_RESOURCE_COMMANDS, self._handler_get_resource_commands)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.GET_RESOURCE_PARAMS, self._handler_get_resource_params)

        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.ENTER, self._handler_stopped_enter)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.EXIT, self._handler_stopped_exit)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.GO_INACTIVE, self._handler_stopped_go_inactive)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.RESET, self._handler_stopped_reset)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.CLEAR, self._handler_stopped_clear)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.RESUME, self._handler_stopped_resume)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.GET_RESOURCE_COMMANDS, self._handler_get_resource_commands)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.GET_RESOURCE_PARAMS, self._handler_get_resource_params)

        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.ENTER, self._handler_observatory_enter)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.EXIT, self._handler_observatory_exit)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.GO_INACTIVE, self._handler_observatory_go_inactive)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.RESET, self._handler_observatory_reset)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.CLEAR, self._handler_observatory_clear)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.PAUSE, self._handler_observatory_pause)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.GO_STREAMING, self._handler_observatory_go_streaming)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.GO_DIRECT_ACCESS, self._handler_observatory_go_direct_access)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.GET_RESOURCE_COMMANDS, self._handler_get_resource_commands)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.GET_RESOURCE_PARAMS, self._handler_get_resource_params)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.GET_PARAMS, self._handler_get_params)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.SET_PARAMS, self._handler_observatory_set_params)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.EXECUTE_RESOURCE, self._handler_observatory_execute_resource)

        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.ENTER, self._handler_streaming_enter)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.EXIT, self._handler_streaming_exit)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.GO_INACTIVE, self._handler_streaming_go_inactive)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.RESET, self._handler_streaming_reset)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.GO_OBSERVATORY, self._handler_streaming_go_observatory)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.GET_RESOURCE_COMMANDS, self._handler_get_resource_commands)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.GET_RESOURCE_PARAMS, self._handler_get_resource_params)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.GET_PARAMS, self._handler_get_params)

        self._fsm.add_handler(InstrumentAgentState.DIRECT_ACCESS, InstrumentAgentEvent.ENTER, self._handler_direct_access_enter)
        self._fsm.add_handler(InstrumentAgentState.DIRECT_ACCESS, InstrumentAgentEvent.EXIT, self._handler_direct_access_exit)
        self._fsm.add_handler(InstrumentAgentState.DIRECT_ACCESS, InstrumentAgentEvent.GO_OBSERVATORY, self._handler_direct_access_go_observatory)
        self._fsm.add_handler(InstrumentAgentState.DIRECT_ACCESS, InstrumentAgentEvent.GET_RESOURCE_COMMANDS, self._handler_get_resource_commands)
        self._fsm.add_handler(InstrumentAgentState.DIRECT_ACCESS, InstrumentAgentEvent.GET_RESOURCE_PARAMS, self._handler_get_resource_params)

        ###############################################################################
        # Instrument agent internal parameters.
        ###############################################################################

        # State machine start state, defaults to unconfigured.
        self._initial_state = initial_state

        # Driver configuration. Passed as part of the spawn configuration
        # or with an initialize command. Sets driver specific
        # context.
        self._dvr_config = None
                                
        # The driver process popen object. To terminate, signal, wait on,
        # or otherwise interact with the driver process via subprocess.
        # Set by transition to inactive.
        self._dvr_proc = None
        
        # The driver client for communicating to the driver process in
        # request-response or event publication. Set by transition to
        # inactive.
        self._dvr_client = None
                
        # UUID of the current transaction.
        self.transaction_id = None
        
        # List of pending transactions.
        self._pending_transactions = []
                                        
        # Dictionary of data stream IDs for data publishing. Constructed
        # by stream_config agent config member during process on_init.
        self._data_streams = {}
        
        # Dictionary of data stream publishers. Constructed by
        # stream_config agent config member during process on_init.
        self._data_publishers = {}

        # Factories for stream packets. Constructed by driver
        # configuration information on transition to inactive.
        self._packet_factories = {}
        
        # Stream registrar to create publishers. Used to create
        # stream publishers, set during process on_init.
        self._stream_registrar = None

        # Latitude value. Set by subscription to platform. Used to
        # append data packets prior to publication.
        self._lat = 0
        
        # Longitude value. Set by subscription to platform. Used to
        # append data packets prior to publication.
        self._lon = 0

        # Flag indicates if the agent is running in a test so that it
        # can instruct drivers to self destruct if it disappears.
        self._test_mode = False
        
        ###############################################################################
        # Instrument agent parameter capabilities.
        ###############################################################################
        
        self.aparam_ia_param = None

    def on_init(self):
        """
        Instrument agent pyon process initialization.
        Init objects that depend on the container services and start state
        machine.
        """
        resource_id = get_safe(self.CFG, "agent.resource_id")
        if not self.resource_id:
            log.warn("InstrumentAgent.on_init(): agent has no resource_id in configuration")
                
        # The registrar to create publishers.
        self._stream_registrar = StreamPublisherRegistrar(process=self,
                                                    node=self.container.node)
        
        # Set the driver config from the agent config if present.
        self._dvr_config = self.CFG.get('driver_config', None)
        
        # Set the test mode.
        self._test_mode = self.CFG.get('test_mode', False)
        
        # Construct stream publishers.
        self._construct_data_publishers()

        # Start state machine.
        self._fsm.start(self._initial_state)


    ###############################################################################
    # Event callback and handling for direct access.
    ###############################################################################
    
    def telnet_input_processor(self, data):
        # callback passed to DA Server for receiving input from server       
        if isinstance(data, int):
            # not character data, so check for lost connection
            if data == -1:
                log.warning("InstAgent.telnetInputProcessor: connection lost")
                self._fsm.on_event(InstrumentAgentEvent.GO_OBSERVATORY)
            else:
                log.error("InstAgent.telnetInputProcessor: got unexpected integer " + str(data))
            return
        log.debug("InstAgent.telnetInputProcessor: data = <" + str(data) + "> len=" + str(len(data)))
        # send the data to the driver
        self._dvr_client.cmd_dvr('execute_direct_access', data + chr(13) + chr(10))
            

    ###############################################################################
    # Event callback and handling.
    ###############################################################################

    def evt_recv(self, evt):
        """
        Callback to receive asynchronous driver events.
        @param evt The driver event received.
        """
        log.info('Instrument agent %s received driver event %s', self._proc_name,
                 str(evt))
        
        try:
            type = evt['type']
            value = evt.get('value', None)
            if type == DriverAsyncEvent.SAMPLE:
                stream_name = value.pop('stream_name')
                value['lat'] = [self._lat]
                value['lon'] = [self._lon]
                value['stream_id'] = self._data_streams[stream_name]
                packet = self._packet_factories[stream_name](**value)
                self._data_publishers[stream_name].publish(packet)
                log.info('Instrument agent %s published data packet.', self._proc_name)

            elif type == DriverAsyncEvent.CONFIG_CHANGE:
                # Needs a specific event type.
                desc_str = 'New driver configuration: %s' % str(value)
                pub = EventPublisher('DeviceEvent')
                pub.publish_event(origin=self.resource_id ,description=desc_str)
                
            elif type == DriverAsyncEvent.STATE_CHANGE:
                desc_str = 'New driver state: %s' % str(value)
                pub = EventPublisher('DeviceSpecificLifecycleEvent')
                pub.publish_event(origin=self.resource_id ,description=desc_str)
            
            elif type == DriverAsyncEvent.TEST_RESULT:
                # Needs a specific event type.
                desc_str = 'Driver test result: %s' % str(value)
                pub = EventPublisher('DeviceEvent')
                pub.publish_event(origin=self.resource_id ,description=desc_str)
            
            elif type == DriverAsyncEvent.ERROR:
                desc_str = 'Driver error: %s' % str(value)
                pub = EventPublisher('DeviceEvent')
                pub.publish_event(origin=self.resource_id ,description=desc_str)
            
            elif type == DriverAsyncEvent.DIRECT_ACCESS:
                if (self.da_server):
                    if (value):
                        self.da_server.send(value)
                    else:
                        log.error('Instrument agent %s error %s processing driver event %s',
                                  self._proc_name, '<no value present in event>', str(evt))
                else:
                    log.error('Instrument agent %s error %s processing driver event %s',
                              self._proc_name, '<no DA server>', str(evt))
            
            else:
                log.warning('Instrument agent %s recieved unhandled driver event %s',
                            self._proc_name, str(evt))
                    
        except Exception as e:
            log.error('Instrument agent %s error %s processing driver event %s',
                      self._proc_name, str(e), str(evt))
        

    ###############################################################################
    # Instrument agent state transition interface.
    # All the following commands are forwarded as a eponymous event to
    # the agent state machine and return the state handler result.
    ###############################################################################

    def acmd_power_up(self, *args, **kwargs):
        """
        Agent power_up command. Forward with args to state machine.
        """
        
        try:
            return self._fsm.on_event(InstrumentAgentEvent.POWER_UP, *args, **kwargs)
            
        except StateError:
            raise InstStateError('power_up not allowed in state %s.', self._fsm.get_current_state()) 
    
    def acmd_power_down(self, *args, **kwargs):
        """
        Agent power_down command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.POWER_DOWN, *args, **kwargs)

        except StateError:
            raise InstStateError('power_down not allowed in state %s.', self._fsm.get_current_state()) 

    
    def acmd_initialize(self, *args, **kwargs):
        """
        Agent initialize command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.INITIALIZE, *args, **kwargs)
        
        except StateError:
            raise InstStateError('initialize not allowed in state %s.', self._fsm.get_current_state()) 

    def acmd_reset(self, *args, **kwargs):
        """
        Agent reset command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.RESET, *args, **kwargs)

        except StateError:
            raise InstStateError('reset not allowed in state %s.', self._fsm.get_current_state()) 
    
    def acmd_go_active(self, *args, **kwargs):
        """
        Agent go_active command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.GO_ACTIVE, *args, **kwargs)

        except StateError:
            raise InstStateError('go_active not allowed in state %s.', self._fsm.get_current_state()) 

    def acmd_go_inactive(self, *args, **kwargs):
        """
        Agent go_inactive command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.GO_INACTIVE, *args, **kwargs)

        except StateError:
            raise InstStateError('go_inactive not allowed in state %s.', self._fsm.get_current_state()) 

    def acmd_run(self, *args, **kwargs):
        """
        Agent run command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.RUN, *args, **kwargs)

        except StateError:
            raise InstStateError('run not allowed in state %s.', self._fsm.get_current_state()) 

    def acmd_clear(self, *args, **kwargs):
        """
        Agent clear command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.CLEAR, *args, **kwargs)
            
        except StateError:
            raise InstStateError('clear not allowed in state %s.', self._fsm.get_current_state()) 

    def acmd_pause(self, *args, **kwargs):
        """
        Agent pause command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.PAUSE, *args, **kwargs)
        
        except StateError:
            raise InstStateError('pause not allowed in state %s.', self._fsm.get_current_state()) 

    def acmd_resume(self, *args, **kwargs):
        """
        Agent resume command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.RESUME, *args, **kwargs)

        except StateError:
            raise InstStateError('resume not allowed in state %s.', self._fsm.get_current_state()) 


    def acmd_go_streaming(self, *args, **kwargs):
        """
        Agent go_streaming command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.GO_STREAMING, *args, **kwargs)
        
        except StateError:
            raise InstStateError('go_streaming not allowed in state %s.', self._fsm.get_current_state()) 

    def acmd_go_direct_access(self, *args, **kwargs):
        """
        Agent go_direct_access command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.GO_DIRECT_ACCESS, *args, **kwargs)
        
        except StateError:
            raise InstStateError('go_direct_access not allowed in state %s.', self._fsm.get_current_state())         

    def acmd_go_observatory(self, *args, **kwargs):
        """
        Agent go_observatory command. Forward with args to state machine.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.GO_OBSERVATORY, *args, **kwargs)

        except StateError:
            raise InstStateError('go_observatory not allowed in state %s.', self._fsm.get_current_state()) 

    def acmd_get_current_state(self, *args, **kwargs):
        """
        Query the agent current state.
        """
        return self._fsm.get_current_state()

    ###############################################################################
    # Instrument agent capabilities interface. These functions override base
    # class helper functinos for specialized instrument agent behavior.
    ###############################################################################

    def _get_resource_commands(self):
        """
        Get driver resource commands. Send event to state machine and return
        response or empty list if none.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.GET_RESOURCE_COMMANDS)
        
        except StateError:
            return []
    
    def _get_resource_params(self):
        """
        Get driver resource parameters. Send event to state machine and return
        response or empty list if none.
        """
        try:    
            return self._fsm.on_event(InstrumentAgentEvent.GET_RESOURCE_PARAMS)
    
        except StateError:
            return []
    
            
    ###############################################################################
    # Instrument agent resource interface. These functions override ResourceAgent
    # base class functions to specialize behavior for instrument driver resources.
    ###############################################################################
    
    def get_param(self, resource_id="", name=''):
        """
        Get driver resource parameters. Send get_params event and args to agent
        state machine to handle request.
        NOTE: Need to adjust the ResourceAgent class and client for instrument
        interface needs.
        @param resource_id
        @param name A list of (channel, name) tuples of driver parameter
        to retrieve
        @retval Dict of (channel, name) : value parameter values if handled.
        """
        
        try:
            return self._fsm.on_event(InstrumentAgentEvent.GET_PARAMS, name)
        
        except StateError:
            raise InstStateError('get_params not allowed in state %s.', self._fsm.get_current_state())        
        
    def set_param(self, resource_id="", name='', value=''):
        """
        Set driver resource parameters. Send set_params event and args to agent
        state machine to handle set driver resource parameters request.
        NOTE: Need to adjust the ResourceAgent class and client for instrument
        interface needs.
        @param resource_id
        @param name a Dict of (channel, name) : value for driver parameters
        to be set.
        @retval Dict of (channel, name) : None or Error if handled.
        """
        try:
            return self._fsm.on_event(InstrumentAgentEvent.SET_PARAMS, name)
        
        except StateError:
            raise InstStateError('set_params not allowed in state %s.', self._fsm.get_current_state())        
                
    def execute(self, resource_id="", command=None):
        """
        Execute driver resource command. Send execute_resource event and args
        to agent state machine to handle resource execute request.
        @param resource_id
        @param command agent command object containing the driver command
        to execute
        @retval Resrouce agent command response object if handled.
        """
        
        
        if not command:
            raise iex.BadRequest("execute argument 'command' not present")
        if not command.command:
            raise iex.BadRequest("command not set")

        cmd_res = IonObject("AgentCommandResult", command_id=command.command_id,
                            command=command.command)
        cmd_res.ts_execute = get_ion_ts()
        command.command = 'execute_' + command.command
            
        try:
            ex = None
            res = self._fsm.on_event(InstrumentAgentEvent.EXECUTE_RESOURCE,
                                        command.command, *command.args,
                                        **command.kwargs)            
            cmd_res.status = 0
            cmd_res.result = res
            result = cmd_res
           
        except TimeoutError:
            ex = InstTimeoutError('Instrument timed out attempting %s.',str(command.command))
        
        except ProtocolError:
            ex = InstProtocolError('Instrument protocol error attempting %s.', str(command.command))
        
        except UnknownCommandError:
            ex = InstUnknownCommandError('Command %s unknown.', str(command.command))

        except ParameterError:
            ex = InstParameterError('Instrument parameter error attempting %s: args=%s, kwargs=%s.',
                                     str(command.command), str(command.args), str(command.kwargs))

        except StateError:
            ex = InstStateError('execute not allowed in state %s.', self._fsm.get_current_state())        

        except InstrumentException:
            ex = IonInstrumentError('Unknown driver error.')

        except:
            ex = IonException('Unknown error.')
        
        if ex:
            # TODO: Distinguish application vs. uncaught exception
            cmd_res.status = getattr(ex, 'status_code', -1)
            cmd_res.result = str(ex)
            log.info("Agent command %s failed with trace=%s" % (command.command, traceback.format_exc()))

        return cmd_res
        
    ###############################################################################
    # Instrument agent transaction interface.
    ###############################################################################

    def acmd_start_transaction(self):
        """
        """
        pass
    
    def acmd_end_transaction(self):
        """
        """
        pass

    ###############################################################################
    # Powered down state handlers.
    # TBD. This state requires clarification of use.
    ###############################################################################

    def _handler_powered_down_enter(self, *args, **kwargs):
        """
        Handler upon entry to powered_down state.
        """
        self._common_state_enter()
    
    def _handler_powered_down_exit(self, *args, **kwargs):
        """
        Handler upon exit from powered_down state.
        """
        pass

    def _handler_powered_down_power_up(self,  *args, **kwargs):
        """
        Handler for power_down agent command in uninitialized state.
        """
        result = None
        next_state = InstrumentAgentState.UNINITIALIZED
        
        return (next_state, result)

    ###############################################################################
    # Uninitialized state handlers.
    # Default start state. The driver has not been configured or started.
    ###############################################################################

    def _handler_uninitialized_enter(self, *args, **kwargs):
        """
        Handler upon entry to uninitialized state.
        """
        self._common_state_enter()
    
    def _handler_uninitialized_exit(self,  *args, **kwargs):
        """
        Handler upon exit from uninitialized state.
        """
        pass

    def _handler_uninitialized_power_down(self,  *args, **kwargs):
        """
        Handler for power_down agent command in uninitialized state.
        """
        result = none
        next_state = InstrumentAgentState.POWERED_DOWN

        return (next_state, result)

    def _handler_uninitialized_initialize(self, *args, **kwargs):
        """
        Handler for initialize agent command in uninitialized state.
        Attempt to start driver process with driver config supplied as
        argument or in agent configuration. Switch to inactive state if
        successful.
        @raises InstDriverError if the driver configuration is missing or
        invalid, or if the driver or client faild to start.
        """
        result = None
        next_state = None

        # If a config is passed, update member.
        try:
            self._dvr_config = args[0]
        
        except IndexError:
            pass
        
        # If config not valid, fail.
        if not self._validate_driver_config():
            raise InstDriverError('The driver configuration is missing or invalid.')

        # Start the driver and switch to inactive.
        self._start_driver(self._dvr_config)
        next_state = InstrumentAgentState.INACTIVE
            
        return (next_state, result)

    ###############################################################################
    # Inactive state handlers.
    # The driver is configured and started, but not connected.
    ###############################################################################

    def _handler_inactive_enter(self,  *args, **kwargs):
        """
        Handler upon entry to inactive state.
        """
        self._common_state_enter()
    
    def _handler_inactive_exit(self,  *args, **kwargs):
        """
        Handler upon exit from inactive state.
        """
        pass

    def _handler_inactive_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command in inactive state.
        Stop the driver process and switch to unitinitalized state if
        successful.
        """
        result = self._stop_driver()
        next_state = InstrumentAgentState.UNINITIALIZED
        
        return (next_state, result)

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        Handler for go_active agent command in inactive state.
        Attempt to establsih communications with all device channels.
        Switch to active state if any channels activated.
        @raises InstDriverError if the comms config is not valid.
        @raises InstConnectionError if the driver connection failed.
        """
        result = None
        next_state = None
                    
        # Set the driver config if passed as a parameter.
        try:
            self._dvr_config['comms_config'] = args[0]
        
        except IndexError:
            pass
        
        # Configure the driver, driver checks if config is valid.
        dvr_comms = self._dvr_config.get('comms_config', None)   
        try:
            self._dvr_client.cmd_dvr('configure', dvr_comms)
        
        except ParameterError:
            raise InstParameterError('The driver comms configuration is invalid.')
        
        # Connect to the device, propagating connection errors.
        try:
            self._dvr_client.cmd_dvr('connect')
        
        except ConnectionError:
            raise InstConnectionError('Driver could not connect to %s', str(dvr_comms))
        
        # If the device state is unknown, send the discover command.
        # Disconnect and raise if the state cannot be determined.
        # If state discoveered, switch into autosample state if driver there,
        # else switch into idle. Agent assumes a non autosample driver state
        # is observatory friendly. Drivers should implement discover to
        # affect the necessary internal state changes if necessary.
        dvr_state = self._dvr_client.cmd_dvr('get_current_state')
        if dvr_state == DriverProtocolState.UNKNOWN:
            max_tries = kwargs.get('max_tries', 5)
            if not isinstance(max_tries, int) or max_tries < 1:
                max_tries = 5
            no_tries = 0
            while True: 
                try:    
                    dvr_state = self._dvr_client.cmd_dvr('discover')
                    if dvr_state == DriverProtocolState.AUTOSAMPLE:
                        next_state = InstrumentAgentState.STREAMING
                    else:
                        next_state = InstrumentAgentState.IDLE
                    break
                
                except TimeoutError, ProtocolError:
                    no_tries += 1
                    if no_tries >= max_tries:
                        self._dvr_client.cmd_dvr('disconnect')
                        raise InstProtocolError('Could not discover instrument state.')
        
        else:
            next_state = InstrumentAgentState.IDLE
        
        return (next_state, result)

    ###############################################################################
    # Idle state handlers.
    ###############################################################################

    def _handler_idle_enter(self,  *args, **kwargs):
        """
        Handler upon entry to idle state.
        """
        self._common_state_enter()
                
    def _handler_idle_exit(self,  *args, **kwargs):
        """
        Handler upon exit from idle state.
        """
        pass

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        Handler for go_inactive agent command in idle state.
        Attempt to disconnect and initialize all active driver channels.
        Swtich to inactive state if successful.
        """
        result = None
        next_state = None
        
        # Disconnect, initialize and go to inactive.
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')
        next_state = InstrumentAgentState.INACTIVE
            
        return (next_state, result)

    def _handler_idle_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command in idle state.
        """
        result = None
        next_state = None
        
        # Disconnect, initialize, stop driver and go to uninitialized.
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        next_state = InstrumentAgentState.UNINITIALIZED
        
        return (next_state, result)

    def _handler_idle_run(self,  *args, **kwargs):
        """
        Handler for run agent command in idle state.
        Switch to observatory state.
        """
        result = None
        next_state = InstrumentAgentState.OBSERVATORY
        
        return (next_state, result)

    ###############################################################################
    # Stopped state handlers.
    # @todo Determine and implement behavior for stopped state.
    ###############################################################################

    def _handler_stopped_enter(self,  *args, **kwargs):
        """
        Handler for entry into stopped state.
        """
        self._common_state_enter()
    
    def _handler_stopped_exit(self,  *args, **kwargs):
        """
        Handler for exit from stopped state.
        """
        pass

    def _handler_stopped_go_inactive(self,  *args, **kwargs):
        """
        Handler for go_inactive agent command in stopped state.
        """
        result = None
        next_state = None

        # Disconnect, initialize and go to inactive.
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')
        next_state = InstrumentAgentState.INACTIVE
         
        return (next_state, result)

    def _handler_stopped_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command in stopped state.
        """
        result = None
        next_state = None

        # Disconnect, initialize, stop driver and go to uninitialized.        
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        next_state = InstrumentAgentState.UNINITIALIZED        
        
        return (next_state, result)

    def _handler_stopped_clear(self,  *args, **kwargs):
        """
        Handler for clear agent command in stopped state.
        """
        result = None
        next_state = InstrumentAgentState.IDLE
        
        return (next_state, result)

    def _handler_stopped_resume(self,  *args, **kwargs):
        """
        Handler for resume agent command in stopped state.
        """
        result = None
        next_state = InstrumentAgentState.OBSERVATORY
        
        return (next_state, result)

    ###############################################################################
    # Observatory state handlers.
    ###############################################################################

    def _handler_observatory_enter(self,  *args, **kwargs):
        """
        Handler upon entry to observatory state.
        """
        self._common_state_enter()
    
    def _handler_observatory_exit(self,  *args, **kwargs):
        """
        Handler upon exit from observatory state.
        """
        pass

    def _handler_observatory_go_inactive(self,  *args, **kwargs):
        """
        Handler for go_inactive agent command in observatory state.
        Attempt to disconnect and initialize all active driver channels.
        Switch to inactive state if successful.
        """
        result = None
        next_state = None
        
        # Disconnect, initialize and go to inactive.
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')
        next_state = InstrumentAgentState.INACTIVE
            
        return (next_state, result)

    def _handler_observatory_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command in observatory state.
        """
        result = None
        next_state = None

        # Disconnect, initialize, stop driver and go to uninitialized.
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        next_state = InstrumentAgentState.UNINITIALIZED        

        return (next_state, result)

    def _handler_observatory_clear(self,  *args, **kwargs):
        """
        Handler for clear agent command in observatory state.
        """
        result = None
        next_state = InstrumentAgentState.IDLE
        
        return (next_state, result)

    def _handler_observatory_pause(self,  *args, **kwargs):
        """
        Handler for pause agent command in observatory state.
        """
        result = None
        next_state = InstrumentAgentState.STOPPED
        
        return (next_state, result)

    def _handler_observatory_go_streaming(self,  *args, **kwargs):
        """
        Handler for go_streaming agent command in observatory state.
        Send start autosample command to driver and switch to streaming
        state if successful.
        @todo Add logic to switch to streaming mode.
        """
        result = None
        next_state = None
        
        try:
            self._dvr_client.cmd_dvr('execute_start_autosample', *args, **kwargs)

        except TimeoutError:
            raise InstTimeoutError('Instrument timed out attempting autosample.')
        
        except ProtocolError:
            raise InstProtocolError('Instrument protocol error attempting autosample.')
        
        except NotImplementedError:
            raise InstNotImplementedError('Autosample not implemented.')

        except ParameterError:
            raise InstParameterError('Instrument parameter error attempting autosample: args=%s, kwargs=%s.', str(args), str(kwargs))

        next_state = InstrumentAgentState.STREAMING

        return (next_state, result)

    def _handler_observatory_go_direct_access(self,  *args, **kwargs):
        """
        Handler for go_direct_access agent command in observatory state.
        """
        result = None
        next_state = None
        
        log.info("Instrument agent requested to start direct access mode")
        
        # get 'address' of host
        hostname = socket.gethostname()
        log.debug("hostname = " + hostname)        
        ip_addresses = socket.gethostbyname_ex(hostname)
        log.debug("ip_address=" + str(ip_addresses))
        ip_address = ip_addresses[2][0]
        ip_address = hostname
        # create a DA server instance (TODO: just telnet for now) and pass in callback method
        try:
            self.da_server = DirectAccessServer(DirectAccessTypes.telnet, self.telnet_input_processor, ip_address)
        except Exception as ex:
            log.warning("InstrumentAgent: failed to start DA Server <%s>" %str(ex))
            raise ex

        # get the connection info from the DA server to return to the user
        port, token = self.da_server.get_connection_info()
        result = {'ip_address':ip_address, 'port':port, 'token':token}
        next_state = InstrumentAgentState.DIRECT_ACCESS

        # tell driver to start direct access mode
        self._dvr_client.cmd_dvr('execute_start_direct_access')

        return (next_state, result)

    def _handler_get_params(self, *args, **kwargs):
        """
        Handler for get_params resource command in observatory state.
        Send get command to driver and return result.
        """
        
        next_state = None
        
        try:
            result = self._dvr_client.cmd_dvr('get', *args, **kwargs)
        
        except TimeoutError:
            raise InstTimeoutError('Instrument timed out attempting get.')
        
        except ProtocolError:
            raise InstProtocolError('Instrument protocol error attempting get.')
        
        except NotImplementedError:
            raise InstNotImplementedError('Get not implemented.')

        except ParameterError:
            raise InstParameterError('Instrument parameter error attempting get: args=%s, kwargs=%s.', str(args), str(kwargs))
        
        
        return (next_state, result)

    def _handler_observatory_set_params(self, *args, **kwargs):
        """
        Handler for set_params resource command in observatory state.
        Send the set command to the driver and return result.
        """
        next_state = None
        result = None
        
        try:        
            self._dvr_client.cmd_dvr('set', *args, **kwargs)

        except TimeoutError:
            raise InstTimeoutError('Instrument timed out attempting set.')
        
        except ProtocolError:
            raise InstProtocolError('Instrument protocol error attempting set.')
        
        except NotImplementedError:
            raise InstNotImplementedError('Get not implemented.')

        except ParameterError:
            raise InstParameterError('Instrument parameter error attempting set: args=%s, kwargs=%s.', str(args), str(kwargs))
        
        return (next_state, result)

    def _handler_observatory_execute_resource(self, command, *args, **kwargs):
        """
        Handler for execute_resource command in observatory state.
        Issue driver command and return the result.
        """
        result = None
        next_state = None
        
        result = self._dvr_client.cmd_dvr(command, *args, **kwargs)

        return (next_state, result)

    ###############################################################################
    # Streaming state handlers.
    ###############################################################################

    def _handler_streaming_enter(self,  *args, **kwargs):
        """
        Handler for entry to streaming state.
        """
        self._common_state_enter()
    
    def _handler_streaming_exit(self,  *args, **kwargs):
        """
        Handler upon exit from streaming state.
        """
        pass

    def _handler_streaming_go_inactive(self,  *args, **kwargs):
        """
        Handler for go_inactive agent command within streaming state.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_streaming_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command within streaming state.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_streaming_go_observatory(self,  *args, **kwargs):
        """
        Handler for go_observatory agent command within streaming state. Command
        driver to stop autosampling, and switch to observatory mode if
        successful.
        """
        result = None
        next_state = None

        max_tries = kwargs.get('max_tries', 5)
        if not isinstance(max_tries, int) or max_tries < 1:
            max_tries = 5
            
        no_tries = 0
        while True:
            try:
                self._dvr_client.cmd_dvr('execute_stop_autosample', *args, **kwargs)
                break
            
            except TimeoutError:
                no_tries += 1
                if no_tries >= max_tries:
                    raise InstTimeoutError('Instrument timed out attempting stop autosample.')
            
            except ProtocolError:
                raise InstProtocolError('Instrument protocol error attempting stop autosample.')
            
            except NotImplementedError:
                raise InstNotImplementedError('Stop autosample not implemented.')
    
            except ParameterError:
                raise InstParameterError('Instrument parameter error attempting stop autosample: args=%s, kwargs=%.', str(args), str(kwargs))

        next_state = InstrumentAgentState.OBSERVATORY

        return (next_state, result)

    ###############################################################################
    # Direct access state handlers.
    ###############################################################################

    def _handler_direct_access_enter(self,  *args, **kwargs):
        """
        Handler upon direct access entry.
        """
        self._common_state_enter()
    
    def _handler_direct_access_exit(self,  *args, **kwargs):
        """
        Handler upon direct access exit.
        """
        pass

    def _handler_direct_access_go_observatory(self,  *args, **kwargs):
        """
        Handler for go_observatory agent command within direct access state.
        """
        result = None
        next_state = None
        
        log.info("Instrument agent requested to stop direct access mode")
        
        # tell driver to stop direct access mode
        result = self._dvr_client.cmd_dvr('execute_stop_direct_access')
        # stop and delete DA server
        if (self.da_server):
            self.da_server.stop()
            del self.da_server
        next_state = InstrumentAgentState.OBSERVATORY

        return (next_state, result)

    ###############################################################################
    # Get resource state handlers.
    # Available for all states with a valid driver process. 
    ###############################################################################

    def _handler_get_resource_params(self,  *args, **kwargs):
        """
        Handler for get_resource_params resource command. Send
        get_resource_params and args to driver and return result.
        """
        result = self._dvr_client.cmd_dvr('get_resource_params')
        next_state = None

        return (next_state, result)

    def _handler_get_resource_commands(self,  *args, **kwargs):
        """
        Handler for get_resource_commands resource command. Send
        get_resource_commands and args to driver and return result.
        """
        result = self._dvr_client.cmd_dvr('get_resource_commands')
        next_state = None

        return (next_state, result)

    ###############################################################################
    # Private helpers.
    ###############################################################################

    def _start_driver(self, dvr_config):
        """
        Start the driver process and driver client.
        @param dvr_config The driver configuration.
        @raises InstDriverError If the driver or client failed to start properly.
        """

        # Get driver configuration and pid for test case.        
        dvr_mod = self._dvr_config['dvr_mod']
        dvr_cls = self._dvr_config['dvr_cls']
        workdir = self._dvr_config.get('workdir','/tmp/')
        this_pid = os.getpid() if self._test_mode else None

        (self._dvr_proc, cmd_port, evt_port) = ZmqDriverProcess.launch_process(dvr_mod, dvr_cls, workdir, this_pid)
            
        # Verify the driver has started.
        if not self._dvr_proc or self._dvr_proc.poll():
            log.error('Instrument agent %s rror starting driver process.', self._proc_name)
            raise InstDriverError('Error starting driver process.')
            
        log.info('Started driver process for %d %d %s %s', cmd_port,
            evt_port, dvr_mod, dvr_cls)
        log.info('Instrument agent %s driver process pid %d', self._proc_name, self._dvr_proc.pid)

        # Start client messaging and verify messaging.
        try:
            self._dvr_client = ZmqDriverClient('localhost', cmd_port, evt_port)
            self._dvr_client.start_messaging(self.evt_recv)
            retval = self._dvr_client.cmd_dvr('process_echo', 'Test.')
        
        except Exception:
            self._dvr_proc.kill()
            self._dvr_proc.wait()
            self._dvr_proc = None
            self._dvr_client = None
            log.error('Instrument agent %s rror starting driver client.', self._proc_name)
            raise InstDriverError('Error starting driver client.')            

        self._construct_packet_factories(dvr_mod)

        log.info('Instrument agent %s started its driver.', self._proc_name)
        
    def _stop_driver(self):
        """
        Stop the driver process and driver client.
        """
        if self._dvr_proc:
            if self._dvr_client:
                self._dvr_client.done()
                self._dvr_proc.wait()
                self._dvr_proc = None
                self._dvr_client = None
                self._clear_packet_factories()
                log.info('Instrument agent %s stopped its driver.', self._proc_name)
                
            else:
                try:
                    self._dvr_proc.kill()
                    self._dvr_proc.wait()
                    self._dvr_proc = None
                    log.info('Instrument agent %s killed its driver.', self._proc_name)
                                
                except OSError:
                    pass
            
    def _validate_driver_config(self):
        """
        Test the driver config for validity.
        @retval True if the current config is valid, False otherwise.
        """
        try:
            dvr_mod = self._dvr_config['dvr_mod']
            dvr_cls = self._dvr_config['dvr_cls']
            
        except TypeError, KeyError:
            return False
        
        if not isinstance(dvr_mod, str) or not isinstance(dvr_cls, str):
            return False
        
        return True
                
    def _construct_data_publishers(self):
        """
        Construct the stream publishers from the stream_config agent
        config variable.
        @retval None
        """
        stream_config = self.CFG.stream_config

        for (name, stream_id) in stream_config.iteritems():
            self._data_streams[name] = stream_id
            publisher = self._stream_registrar.create_publisher(stream_id=stream_id)
            self._data_publishers[name] = publisher
            log.info('Instrumen agent %s created publisher for stream %s',
                     self._proc_name, name)        
        
    def _construct_packet_factories(self, dvr_mod):
        """
        Construct packet factories from packet_config member of the
        driver_config.
        @retval None
        """

        import_str = 'from %s import PACKET_CONFIG' % dvr_mod
        try:
            exec import_str
            log.info('Instrument agent %s imported packet config.', self._proc_name)
            for (name, val) in PACKET_CONFIG.iteritems():
                if val:
                    try:
                        mod = val[0]
                        cls = val[1]
                        import_str = 'from %s import %s' % (mod, cls)
                        ctor_str = 'ctor = %s' % cls
                        exec import_str
                        exec ctor_str
                        self._packet_factories[name] = ctor
                    
                    except Exception:
                        log.error('Instrument agent %s had error creating packet factory for stream %s',
                                 self._proc_name, name)
                    
                    else:
                        log.info('Instrument agent %s created packet factory for stream %s',
                                 self._proc_name, name)

        except Exception:
            log.error('Instrument agent %s had error creating packet factories.',
                      self._proc_name)
                                
    def _clear_packet_factories(self):
        """
        Delete packet factories.
        @retval None
        """
        self._packet_factories.clear()
        log.info('Instrument agent %s deleted packet factories.', self._proc_name)
        
    def _common_state_enter(self):
        """
        Common work upon every state entry.
        """
        state = self._fsm.get_current_state()
        log.info('Instrument agent entered state %s', state)
        desc_str = 'Agent entered state: %s' % state
        pub = EventPublisher('DeviceCommonLifecycleEvent')
        pub.publish_event(origin=self.resource_id, description=desc_str)
                    
    ###############################################################################
    # Misc and test.
    ###############################################################################

    def test_ia(self):
        log.info('Hello from the instrument agent!')



