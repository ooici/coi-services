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

from pyon.core.exception import BadRequest, NotFound
from pyon.public import IonObject, log
from pyon.agent.agent import ResourceAgent
from pyon.core import exception as iex
from pyon.util.containers import get_ion_ts
from pyon.ion.endpoint import StreamPublisherRegistrar

import time

from ion.services.mi.instrument_fsm_args import InstrumentFSM
from ion.services.mi.common import BaseEnum
from ion.services.mi.common import InstErrorCode
from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess
from ion.services.sa.direct_access.direct_access_server import DirectAccessServer, DirectAccessTypes


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
    def __init__(self, initial_state=InstrumentAgentState.UNINITIALIZED):
        """
        Initialize instrument agent prior to pyon process initialization.
        Define state machine, initialize member variables.
        """
        ResourceAgent.__init__(self)
                
        # Instrument agent state machine.
        self._fsm = InstrumentFSM(InstrumentAgentState, InstrumentAgentEvent, InstrumentAgentEvent.ENTER,
                            InstrumentAgentEvent.EXIT, InstErrorCode.UNHANDLED_EVENT)
        
        # Populate state machine for all state-events.
        self._fsm.add_handler(InstrumentAgentState.POWERED_DOWN, InstrumentAgentEvent.ENTER, self._handler_powered_down_enter)
        self._fsm.add_handler(InstrumentAgentState.POWERED_DOWN, InstrumentAgentEvent.EXIT, self._handler_powered_down_exit)
        
        self._fsm.add_handler(InstrumentAgentState.UNINITIALIZED, InstrumentAgentEvent.ENTER, self._handler_uninitialized_enter)
        self._fsm.add_handler(InstrumentAgentState.UNINITIALIZED, InstrumentAgentEvent.EXIT, self._handler_uninitialized_exit)
        self._fsm.add_handler(InstrumentAgentState.UNINITIALIZED, InstrumentAgentEvent.POWER_DOWN, self._handler_uninitialized_power_down)
        self._fsm.add_handler(InstrumentAgentState.UNINITIALIZED, InstrumentAgentEvent.INITIALIZE, self._handler_uninitialized_initialize)
        self._fsm.add_handler(InstrumentAgentState.UNINITIALIZED, InstrumentAgentEvent.RESET, self._handler_uninitialized_reset)

        self._fsm.add_handler(InstrumentAgentState.INACTIVE, InstrumentAgentEvent.ENTER, self._handler_inactive_enter)
        self._fsm.add_handler(InstrumentAgentState.INACTIVE, InstrumentAgentEvent.EXIT, self._handler_inactive_exit)
        self._fsm.add_handler(InstrumentAgentState.INACTIVE, InstrumentAgentEvent.INITIALIZE, self._handler_inactive_initialize)
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
                
        # Process ID of the driver process. Useful to identify and signal
        # the process if necessary. Set by transition to inactive.
        self._dvr_pid = None
                
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
        # The registrar to create publishers.
        self._stream_registrar = StreamPublisherRegistrar(process=self,
                                                    node=self.container.node)
        
        # Set the driver config from the agent config if present.
        self._dvr_config = self.CFG.get('driver_config', None)
        
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
                log.info("InstAgent.telnetInputProcessor: connection lost")
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
            if evt['type'] == 'sample':
                name = evt['name']
                value = evt['value']
                value['lat'] = [self._lat]
                value['lon'] = [self._lon]
                value['stream_id'] = self._data_streams[name]
                if isinstance(value, dict):
                    packet = self._packet_factories[name](**value)
                    self._data_publishers[name].publish(packet)        
                    log.info('Instrument agent %s published data packet.',
                             self._proc_name)
            if evt['type'] == 'direct_access':
                self.da_server.send(evt['value'])
                    
        except (KeyError, TypeError) as e:
            pass
        
        except Exception as e:
            log.info('Instrument agent %s error %s', self._proc_name, str(e))

    ###############################################################################
    # Instrument agent state transition interface.
    # All the following commands are forwarded as a eponymous event to
    # the agent state machine and return the state handler result.
    ###############################################################################

    def acmd_power_up(self, *args, **kwargs):
        """
        Agent power_up command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.POWER_UP, *args, **kwargs)
    
    def acmd_power_down(self, *args, **kwargs):
        """
        Agent power_down command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.POWER_DOWN, *args, **kwargs)
    
    def acmd_initialize(self, *args, **kwargs):
        """
        Agent initialize command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.INITIALIZE, *args, **kwargs)

    def acmd_reset(self, *args, **kwargs):
        """
        Agent reset command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.RESET, *args, **kwargs)
    
    def acmd_go_active(self, *args, **kwargs):
        """
        Agent go_active command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_ACTIVE, *args, **kwargs)

    def acmd_go_inactive(self, *args, **kwargs):
        """
        Agent go_inactive command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_INACTIVE, *args, **kwargs)

    def acmd_run(self, *args, **kwargs):
        """
        Agent run command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.RUN, *args, **kwargs)

    def acmd_clear(self, *args, **kwargs):
        """
        Agent clear command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.CLEAR, *args, **kwargs)

    def acmd_pause(self, *args, **kwargs):
        """
        Agent pause command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.PAUSE, *args, **kwargs)

    def acmd_resume(self, *args, **kwargs):
        """
        Agent resume command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.RESUME, *args, **kwargs)

    def acmd_go_streaming(self, *args, **kwargs):
        """
        Agent go_streaming command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_STREAMING, *args, **kwargs)

    def acmd_go_direct_access(self, *args, **kwargs):
        """
        Agent go_direct_access command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_DIRECT_ACCESS, *args, **kwargs)

    def acmd_go_observatory(self, *args, **kwargs):
        """
        Agent go_observatory command. Forward with args to state machine.
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_OBSERVATORY, *args, **kwargs)

    ###############################################################################
    # Misc instrument agent command interface.
    ###############################################################################

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
        return self._fsm.on_event(InstrumentAgentEvent.GET_RESOURCE_COMMANDS) or []
    
    def _get_resource_params(self):
        """
        Get driver resource parameters. Send event to state machine and return
        response or empty list if none.
        """
        return self._fsm.on_event(InstrumentAgentEvent.GET_RESOURCE_PARAMS) or []

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
        params = name
        return self._fsm.on_event(InstrumentAgentEvent.GET_PARAMS, params) or {}
        
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
        params = name
        return self._fsm.on_event(InstrumentAgentEvent.SET_PARAMS, params) or {}
                
    def execute(self, resource_id="", command=None):
        """
        Execute driver resource command. Send execute_resource event and args
        to agent state machine to handle resource execute request.
        @param resource_id
        @param command agent command object containing the driver command
        to execute
        @retval Resrouce agent command response object if handled.
        """
        return self._fsm.on_event(InstrumentAgentEvent.EXECUTE_RESOURCE, command)
                
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
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_powered_down_exit(self, *args, **kwargs):
        """
        Handler upon exit from powered_down state.
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    ###############################################################################
    # Uninitialized state handlers.
    # Default start state. The driver has not been configured or started.
    ###############################################################################

    def _handler_uninitialized_enter(self, *args, **kwargs):
        """
        Handler upon entry to uninitialized state.
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_uninitialized_exit(self,  *args, **kwargs):
        """
        Handler upon exit from uninitialized state.
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_uninitialized_power_down(self,  *args, **kwargs):
        """
        Handler for power_down agent command in uninitialized state.
        """
        result = InstErrorCode.NOT_IMPLEMENTED
        next_state = None
        
        return (next_state, result)

    def _handler_uninitialized_initialize(self,  dvr_config=None, *args, **kwargs):
        """
        Handler for initialize agent command in uninitialized state.
        Attempt to start driver process with driver config supplied as
        argument or in agent configuration. Switch to inactive state if
        successful.
        """
        result = None
        next_state = None

        self._dvr_config = dvr_config or self._dvr_config
        result = self._start_driver(self._dvr_config)
        if not result:
            next_state = InstrumentAgentState.INACTIVE
            
        return (next_state, result)

    def _handler_uninitialized_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command in uninitialized state.
        Exit and reenter uninitializeds state.
        """
        result = None
        next_state = InstrumentAgentState.UNINITIALIZED
        
        return (next_state, result)

    ###############################################################################
    # Inactive state handlers.
    # The driver is configured and started, but not connected.
    ###############################################################################

    def _handler_inactive_enter(self,  *args, **kwargs):
        """
        Handler upon entry to inactive state.
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())

    
    def _handler_inactive_exit(self,  *args, **kwargs):
        """
        Handler upon exit from inactive state.
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())


    def _handler_inactive_initialize(self,  dvr_config=None, *args, **kwargs):
        """
        Handler for initialize command in inactive state. Stop and restart
        driver process using new driver config if supplied.
        """
        result = None
        next_state = None
        
        result = self._stop_driver()
        if result:
            return (next_state, result)
        
        self._dvr_config = dvr_config or self._dvr_config
        result = self._start_driver(self._dvr_config)
        if not result:
            next_state = InstrumentAgentState.INACTIVE
                
        return (next_state, result)

    def _handler_inactive_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command in inactive state.
        Stop the driver process and switch to unitinitalized state if
        successful.
        """
        result = None
        next_state = None
        result = self._stop_driver()
        if not result:
            next_state = InstrumentAgentState.UNINITIALIZED
        
        return (next_state, result)

    def _handler_inactive_go_active(self, dvr_comms=None, *args, **kwargs):
        """
        Handler for go_active agent command in inactive state.
        Attempt to establsih communications with all device channels.
        Switch to active state if any channels activated.
        """
        result = None
        next_state = None
        
        if not dvr_comms:
            dvr_comms = self._dvr_config.get('comms_config', None)
            
        cfg_result = self._dvr_client.cmd_dvr('configure', dvr_comms)
        
        channels = [key for (key, val) in cfg_result.iteritems() if not
            InstErrorCode.is_error(val)]
        
        con_result = self._dvr_client.cmd_dvr('connect', channels)

        result = cfg_result.copy()

        try:
            for (key, val) in con_result.iteritems():
                result[key] = val
        except:
            log.error("Instrument agent connection failure: " + str(con_result))

        self._active_channels = self._dvr_client.cmd_dvr('get_active_channels')

        if len(self._active_channels)>0:
                next_state = InstrumentAgentState.IDLE

        return (next_state, result)

    ###############################################################################
    # Idle state handlers.
    ###############################################################################

    def _handler_idle_enter(self,  *args, **kwargs):
        """
        Handler upon entry to idle state.
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
                
    def _handler_idle_exit(self,  *args, **kwargs):
        """
        Handler upon exit from idle state.
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        Handler for go_inactive agent command in idle state.
        Attempt to disconnect and initialize all active driver channels.
        Swtich to inactive state if successful.
        """
        result = None
        next_state = None
        
        channels = self._dvr_client.cmd_dvr('get_active_channels')
        dis_result = self._dvr_client.cmd_dvr('disconnect', channels)
        
        [key for (key, val) in dis_result.iteritems() if not
            InstErrorCode.is_error(val)]
        
        init_result = self._dvr_client.cmd_dvr('initialize', channels)

        result = dis_result.copy()
        for (key, val) in init_result.iteritems():
            result[key] = val
            
        self._active_channels = self._dvr_client.cmd_dvr('get_active_channels')
            
        if len(self._active_channels)==0:
            next_state = InstrumentAgentState.INACTIVE
            
        return (next_state, result)

    def _handler_idle_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command in idle state.
        """
        result = None
        next_state = None
        
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
    ###############################################################################

    def _handler_stopped_enter(self,  *args, **kwargs):
        """
        Handler for entry into stopped state.
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_stopped_exit(self,  *args, **kwargs):
        """
        Handler for exit from stopped state.
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_stopped_go_inactive(self,  *args, **kwargs):
        """
        Handler for go_inactive agent command in stopped state.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_stopped_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command in stopped state.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_stopped_clear(self,  *args, **kwargs):
        """
        Handler for clear agent command in stopped state.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_stopped_resume(self,  *args, **kwargs):
        """
        Handler for resume agent command in stopped state.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    ###############################################################################
    # Observatory state handlers.
    ###############################################################################

    def _handler_observatory_enter(self,  *args, **kwargs):
        """
        Handler upon entry to observatory state.
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_observatory_exit(self,  *args, **kwargs):
        """
        Handler upon exit from observatory state.
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_observatory_go_inactive(self,  *args, **kwargs):
        """
        Handler for go_inactive agent command in observatory state.
        Attempt to disconnect and initialize all active driver channels.
        Switch to inactive state if successful.
        """
        result = None
        next_state = None
        
        channels = self._dvr_client.cmd_dvr('get_active_channels')
        dis_result = self._dvr_client.cmd_dvr('disconnect', channels)
        
        [key for (key, val) in dis_result.iteritems() if not
            InstErrorCode.is_error(val)]
        
        init_result = self._dvr_client.cmd_dvr('initialize', channels)

        result = dis_result.copy()
        for (key, val) in init_result.iteritems():
            result[key] = val
            
        self._active_channels = self._dvr_client.cmd_dvr('get_active_channels')
            
        if len(self._active_channels)==0:
            next_state = InstrumentAgentState.INACTIVE
            
        return (next_state, result)

    def _handler_observatory_reset(self,  *args, **kwargs):
        """
        Handler for reset agent command in observatory state.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_clear(self,  *args, **kwargs):
        """
        Handler for clear agent command in observatory state.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_pause(self,  *args, **kwargs):
        """
        Handler for pause agent command in observatory state.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_go_streaming(self,  *args, **kwargs):
        """
        Handler for go_streaming agent command in observatory state.
        Send start autosample command to driver and switch to streaming
        state if successful.
        """
        result = None
        next_state = None

        result = self._dvr_client.cmd_dvr('start_autosample', *args, **kwargs)
    
        if isinstance(result, dict):
            if any([val == None for val in result.values()]):
                next_state = InstrumentAgentState.STREAMING

        return (next_state, result)

    def _handler_observatory_go_direct_access(self,  *args, **kwargs):
        """
        Handler for go_direct_access agent command in observatory state.
        """
        result = None
        next_state = None
        
        log.info("Instrument agent requested to go to direct access mode")
        # tell driver to start direct access mode
        result = self._dvr_client.cmd_dvr('start_direct_access')
        # create a DA server instance (TODO: just telnet for now) and pass in callback method
        self.da_server = DirectAccessServer(DirectAccessTypes.telnet, self.telnet_input_processor)
        # get the connection info from the DA server
        addr, port, name, password = self.da_server.get_connection_info()
        result = {'ip_address':addr, 'port':port, 'username':name, 'password':password}
        next_state = InstrumentAgentState.DIRECT_ACCESS
        return (next_state, result)

    def _handler_get_params(self, params, *args, **kwargs):
        """
        Handler for get_params resource command in observatory state.
        Send get command to driver and return result.
        """
        result = self._dvr_client.cmd_dvr('get', params)
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_set_params(self, params, *args, **kwargs):
        """
        Handler for set_params resource command in observatory state.
        Send the set command to the driver and return result.
        """
        result = self._dvr_client.cmd_dvr('set', params)
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_execute_resource(self, command, *args, **kwargs):
        """
        Handler for execute_resource command in observatory state.
        Issue driver command and return the result.
        """
        result = None
        next_state = None

        if not command:
            raise iex.BadRequest("execute argument 'command' not present")
        if not command.command:
            raise iex.BadRequest("command not set")

        cmd_res = IonObject("AgentCommandResult", command_id=command.command_id,
                            command=command.command)
        cmd_res.ts_execute = get_ion_ts()
        command.command = 'execute_' + command.command
        res = self._dvr_client.cmd_dvr(command.command, *command.args,
                                           **command.kwargs)
        cmd_res.status = 0
        cmd_res.result = res
        result = cmd_res
        
        return (next_state, result)

    ###############################################################################
    # Streaming state handlers.
    ###############################################################################

    def _handler_streaming_enter(self,  *args, **kwargs):
        """
        Handler for entry to streaming state.
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_streaming_exit(self,  *args, **kwargs):
        """
        Handler upon exit from streaming state.
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

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
        
        result = self._dvr_client.cmd_dvr('stop_autosample', *args, **kwargs)
        
        if isinstance(result, dict):
            if all([val == None for val in result.values()]):
                next_state = InstrumentAgentState.OBSERVATORY
            
        return (next_state, result)

    ###############################################################################
    # Direct access state handlers.
    ###############################################################################

    def _handler_direct_access_enter(self,  *args, **kwargs):
        """
        Handler upon direct access entry.
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_direct_access_exit(self,  *args, **kwargs):
        """
        Handler upon direct access exit.
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_direct_access_go_observatory(self,  *args, **kwargs):
        """
        Handler for go_observatory agent command within direct access state.
        """
        result = None
        next_state = None
        
        # tell driver to stop direct access mode
        result = self._dvr_client.cmd_dvr('stop_direct_access')
        # stop and delete DA server
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
        @param comms_config The driver communications configuration.
        @retval None or error.
        """
        try:        
            cmd_port = dvr_config['cmd_port']
            evt_port = dvr_config['evt_port']
            dvr_mod = dvr_config['dvr_mod']
            dvr_cls = dvr_config['dvr_cls']
            svr_addr = dvr_config['svr_addr']
            
        except (TypeError, KeyError):
            # Not a dict. or missing required parameter.
            log.error('Insturment agent %s missing required parameter in start_driver.',
                      self._proc_name)            
            return InstErrorCode.REQUIRED_PARAMETER
                
        # Launch driver process.
        self._dvr_proc = ZmqDriverProcess.launch_process(cmd_port, evt_port,
                                                         dvr_mod,  dvr_cls)

        self._dvr_proc.poll()
        if self._dvr_proc.returncode:
            # Error proc didn't start.
            log.error('Insturment agent %s driver process did not launch.',
                      self._proc_name)
            return InstErrorCode.AGENT_INIT_FAILED

        log.info('Insturment agent %s launched driver process.', self._proc_name)
        
        # Create client and start messaging.
        self._dvr_client = ZmqDriverClient(svr_addr, cmd_port, evt_port)
        self._dvr_client.start_messaging(self.evt_recv)
        log.info('Insturment agent %s driver process client started.',
                 self._proc_name)
        time.sleep(1)

        try:        
            retval = self._dvr_client.cmd_dvr('process_echo', 'Test.')
            log.info('Insturment agent %s driver process echo test: %s.',
                     self._proc_name, str(retval))
            
        except Exception:
            self._dvr_proc.terminate()
            self._dvr_proc.wait()
            self._dvr_proc = None
            self._dvr_client = None
            log.error('Insturment agent %s error commanding driver process.',
                      self._proc_name)            
            return InstErrorCode.AGENT_INIT_FAILED

        else:
            log.info('Insturment agent %s started its driver.', self._proc_name)
            self._construct_packet_factories(dvr_mod)

    def _stop_driver(self):
        """
        Stop the driver process and driver client.
        @retval None.
        """
        if self._dvr_client:
            self._dvr_client.done()
            self._dvr_proc.wait()
            self._dvr_proc = None
            self._dvr_client = None
            self._clear_packet_factories()
            log.info('Insturment agent %s stopped its driver.', self._proc_name)
            
        time.sleep(1)

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
            
    ###############################################################################
    # Misc and test.
    ###############################################################################

    def test_ia(self):
        log.info('Hello from the instrument agent!')



