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

import time

from ion.services.mi.instrument_fsm_args import InstrumentFSM
from ion.services.mi.common import BaseEnum
from ion.services.mi.common import InstErrorCode
from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess


class InstrumentAgentState(BaseEnum):
    """
    """
    POWERED_DOWN = 'INSTRUMENT_AGENT_STATE_POWERED_DOWN'
    UNINITIALIZED = 'INSTRUMENT_AGENT_STATE_UNINITIALIZED'
    INACTIVE = 'INSTRUMENT_AGENT_STATE_INACTIVE'
    IDLE = 'INSTRUMENT_AGENT_STATE_IDLE'
    STOPPED = 'INSTRUMENT_AGENT_STATE_STOPPED'
    OBSERVATORY = 'INSTRUMENT_AGENT_STATE_OBSERVATORY'
    STREAMING = 'INSTRUMENT_AGENT_STATE_STREAMING'
    DIRECT_ACCESS = 'INSTRUMENT_AGENT_STATE_DIRECT_ACCESS'
    
ACTIVE_OBSERVATORY_STATES = [
    InstrumentAgentState.OBSERVATORY,
    InstrumentAgentState.STREAMING
    ]    
    
class InstrumentAgentEvent(BaseEnum):
    """
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

class InstrumentAgent(ResourceAgent):
    """
    """
    def __init__(self, initial_state=InstrumentAgentState.UNINITIALIZED):
        """
        """
        ResourceAgent.__init__(self)
        
        self._fsm = InstrumentFSM(InstrumentAgentState, InstrumentAgentEvent, InstrumentAgentEvent.ENTER,
                            InstrumentAgentEvent.EXIT, InstErrorCode.UNHANDLED_EVENT)
        
        # Add handlers for all events.
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

        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.ENTER, self._handler_idle_enter)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.EXIT, self._handler_idle_exit)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(InstrumentAgentState.IDLE, InstrumentAgentEvent.RUN, self._handler_idle_run)

        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.ENTER, self._handler_stopped_enter)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.EXIT, self._handler_stopped_exit)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.GO_INACTIVE, self._handler_stopped_go_inactive)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.RESET, self._handler_stopped_reset)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.CLEAR, self._handler_stopped_clear)
        self._fsm.add_handler(InstrumentAgentState.STOPPED, InstrumentAgentEvent.RESUME, self._handler_stopped_resume)

        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.ENTER, self._handler_observatory_enter)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.EXIT, self._handler_observatory_exit)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.GO_INACTIVE, self._handler_observatory_go_inactive)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.RESET, self._handler_observatory_reset)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.CLEAR, self._handler_observatory_clear)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.PAUSE, self._handler_observatory_pause)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.GO_STREAMING, self._handler_observatory_go_streaming)
        self._fsm.add_handler(InstrumentAgentState.OBSERVATORY, InstrumentAgentEvent.GO_DIRECT_ACCESS, self._handler_observatory_go_direct_access)

        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.ENTER, self._handler_streaming_enter)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.EXIT, self._handler_streaming_exit)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.GO_INACTIVE, self._handler_streaming_go_inactive)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.RESET, self._handler_streaming_reset)
        self._fsm.add_handler(InstrumentAgentState.STREAMING, InstrumentAgentEvent.GO_OBSERVATORY, self._handler_streaming_go_observatory)

        self._fsm.add_handler(InstrumentAgentState.DIRECT_ACCESS, InstrumentAgentEvent.ENTER, self._handler_direct_access_enter)
        self._fsm.add_handler(InstrumentAgentState.DIRECT_ACCESS, InstrumentAgentEvent.EXIT, self._handler_direct_access_exit)
        self._fsm.add_handler(InstrumentAgentState.DIRECT_ACCESS, InstrumentAgentEvent.GO_OBSERVATORY, self._handler_direct_access_go_observatory)

        self._fsm.start(initial_state)

        ###############################################################################
        # Instrument agent internal parameters.
        ###############################################################################

        # Driver configuration. Passed with initialize command.
        # Configures driver process with driver module and class.
        self._dvr_config = None
        
        # Process ID of the driver process. Useful to identify and signal
        # the process if necessary. Set by transition to inactive.
        self._dvr_pid = None
        
        # Process ID of the device broker daemon. Useful to identify and signal
        # the broker if necessary. Set by transition to active.
        self._conn_pid = None
        
        # The driver process popen object. To terminate, signal, wait on,
        # or otherwise interact with the driver process via subprocess.
        # Set by transition to inactive.
        self._dvr_proc = None
        
        # The driver client for communicating to the driver process in
        # request-response or event publication. Set by transition to
        # inactive.
        self._dvr_client = None
        
        # The comms configuration. Passed with initialize or go_active
        # commands and forwarded to driver during connect.
        self._comms_config = None
        
        # UUID of the current transaction.
        self.transaction_id = None
        
        # List of pending transactions.
        self._pending_transactions = []
                
        # Origin for agent publications.
        self._publisher_origin = None
        
        ###############################################################################
        # Instrument agent parameter capabilities.
        ###############################################################################
        
        self.aparam_ia_param = None

    ###############################################################################
    # Event callback and handling.
    ###############################################################################

    def evt_recv(self, evt):
        """
        Callback to receive asynchronous driver events.
        @param evt The driver event received.
        """
        pass

    ###############################################################################
    # Instrument agent state transition interface.
    # All the following commands are forwarded as a eponymous event to
    # the agent state machine and return the state handler result.
    ###############################################################################

    def acmd_power_up(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.POWER_UP, *args, **kwargs)
    
    def acmd_power_down(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.POWER_DOWN, *args, **kwargs)
    
    def acmd_initialize(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.INITIALIZE, *args, **kwargs)

    def acmd_reset(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.RESET, *args, **kwargs)
    
    def acmd_go_active(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_ACTIVE, *args, **kwargs)

    def acmd_go_inactive(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_INACTIVE, *args, **kwargs)

    def acmd_run(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_INACTIVE, *args, **kwargs)

    def acmd_clear(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.CLEAR, *args, **kwargs)

    def acmd_pause(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.PAUSE, *args, **kwargs)

    def acmd_resume(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.RESUME, *args, **kwargs)

    def acmd_go_streaming(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_STREAMING, *args, **kwargs)

    def acmd_go_direct_access(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_DIRECT_ACCESS, *args, **kwargs)

    def acmd_go_observatory(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(InstrumentAgentEvent.GO_OBSERVATORY, *args, **kwargs)

    ###############################################################################
    # Instrument agent resouce interface.
    ###############################################################################

    def get_capabilities(self, resource_id="", capability_types=[]):
        capability_types = capability_types or ["CONV_TYPE", "AGT_CMD", "AGT_PAR", "RES_CMD", "RES_PAR"]
        cap_list = []
        if "CONV_TYPE" in capability_types:
            cap_list.extend([("CONV_TYPE", cap) for cap in self._get_agent_conv_types()])
        if "AGT_CMD" in capability_types:
            cap_list.extend([("AGT_CMD", cap) for cap in self._get_agent_commands()])
        if "AGT_PAR" in capability_types:
            cap_list.extend([("AGT_PAR", cap) for cap in self._get_agent_params()])
        if "RES_CMD" in capability_types:
            cap_list.extend([("RES_CMD", cap) for cap in self._get_resource_commands()])
        if "RES_PAR" in capability_types:
            cap_list.extend([("RES_PAR", cap) for cap in self._get_resource_params()])
        return cap_list

    def get_param(self, resource_id="", params=None):
        state = self._fsm.get_current_state()
        if state in ACTIVE_OBSERVATORY_STATES:
            return self._dvr_client.cmd_dvr('get', params)
        else:
            raise iex.Conflict('Cannot retrieve device parmeters in this state.')
        
    def set_param(self, resource_id="", params=None):
        state = self._fsm.get_current_state()
        if state in ACTIVE_OBSERVATORY_STATES:
            return self._dvr_client.cmd_dvr('set', params)
        else:
            raise iex.Conflict('Cannot set device parmeters in this state.')
        
    def execute(self, resource_id="", command=None):
        state = self._fsm.get_current_state()
        if state in ACTIVE_OBSERVATORY_STATES:
            return self._execute("execute_", command)
        else:
            raise iex.Conflict('Cannot command device in this state.')

    def _execute(self, cprefix, command):
        if not command:
            raise iex.BadRequest("execute argument 'command' not present")
        if not command.command:
            raise iex.BadRequest("command not set")

        cmd_res = IonObject("AgentCommandResult", command_id=command.command_id,
                            command=command.command)
        cmd_res.ts_execute = get_ion_ts()
        res = self._dvr_client.cmd_dvr(command.command, *command.args,
                                           **command.kwargs)
        cmd_res.status = 0
        cmd_res.result = res

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
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_powered_down_exit(self, *args, **kwargs):
        """
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    ###############################################################################
    # Uninitialized state handlers.
    # Default start state. The driver has not been configured or started.
    ###############################################################################

    def _handler_uninitialized_enter(self, *args, **kwargs):
        """
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_uninitialized_exit(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_uninitialized_power_down(self,  *args, **kwargs):
        """
        Switch to power down state.
        @retval Not implemented.
        """
        result = InstErrorCode.NOT_IMPLEMENTED
        next_state = None
        
        return (next_state, result)

    def _handler_uninitialized_initialize(self,  dvr_config, comms_config,
                                          *args, **kwargs):
        """
        Configure and start the driver. Switch to inactive on success.
        @param dvr_config The driver configuration.
        @param comms_config The communications configuration.
        @retval None or error.
        """
        result = None
        next_state = None
        result = self._start_driver(dvr_config, comms_config)
        if not result:
            next_state = InstrumentAgentState.INACTIVE

        return (next_state, result)

    def _handler_uninitialized_reset(self,  *args, **kwargs):
        """
        Reenter the uninitialized state.
        @retval None or error.
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
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_inactive_exit(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_inactive_initialize(self,  dvr_config, comms_config,
                                     *args, **kwargs):
        """
        Stop and restart the driver. Reenter inactive if successful.
        @param dvr_config The driver configuration.
        @param comms_config The communications configuration.
        @retval None or error.
        """
        result = None
        next_state = None
        result = self._stop_driver()
        if result:
            return (next_state, result)
            
        result = self._start_driver(dvr_config, comms_config)
        if not result:
            next_state = InstrumentAgentState.INACTIVE
                
        return (next_state, result)

    def _handler_inactive_reset(self,  *args, **kwargs):
        """
        Stop the driver and switch to uninitialized if successful.
        @retval None or error.
        """
        result = None
        next_state = None
        result = self._stop_driver()
        if not result:
            next_state = InstrumentAgentState.UNINITIALIZED
        
        return (next_state, result)

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        Establish communications with the device and switch to active if
        successful.
        @param comms_config The communications config.
        @retval Channel:result dictionary giving results of configure
        and connect.
        """
        result = None
        next_state = None
        
        cfg_result = self._dvr_client.cmd_dvr('configure', self._comms_config)
        
        channels = [key for (key, val) in cfg_result.iteritems() if not
            InstErrorCode.is_error(val)]
        
        con_result = self._dvr_client.cmd_dvr('connect', channels)

        result = cfg_result.copy()
        for (key, val) in con_result.iteritems():
            result[key] = val

        self._active_channels = self._dvr_client.cmd_dvr('get_active_channels')

        if len(self._active_channels)>0:
                next_state = InstrumentAgentState.IDLE
        
        return (next_state, result)

    ###############################################################################
    # Idle state handlers.
    ###############################################################################

    def _handler_idle_enter(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_idle_exit(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_idle_go_inactive(self,  *args, **kwargs):
        """
        Disconnect and initialize. Switch to inactive if no active channels
        remain.
        @param channnels (optional) list of channels to deactivate. Defaults
        to all active channels.
        @retval Channel:result dictionary giving success of disconnect
        and initialize.
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
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_idle_run(self,  *args, **kwargs):
        """
        Switch into observatory mode.
        @retval None or error.
        """
        result = None
        next_state = InstrumentAgentState.OBSERVATORY
        
        return (next_state, result)

    ###############################################################################
    # Stopped state handlers.
    ###############################################################################

    def _handler_stopped_enter(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_stopped_exit(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_stopped_go_inactive(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_stopped_reset(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_stopped_clear(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_stopped_resume(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    ###############################################################################
    # Observatory state handlers.
    ###############################################################################

    def _handler_observatory_enter(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_observatory_exit(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_observatory_go_inactive(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_reset(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_clear(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_pause(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_go_streaming(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_observatory_go_direct_access(self,  *args, **kwargs):
        """
        @retval None or error.
        """
        result = None
        next_state = None
        
        return (next_state, result)

    ###############################################################################
    # Streaming state handlers.
    ###############################################################################

    def _handler_streaming_enter(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_streaming_exit(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_streaming_go_inactive(self,  *args, **kwargs):
        """
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_streaming_reset(self,  *args, **kwargs):
        """
        """
        result = None
        next_state = None
        
        return (next_state, result)

    def _handler_streaming_go_observatory(self,  *args, **kwargs):
        """
        """
        result = None
        next_state = None
        
        return (next_state, result)

    ###############################################################################
    # Direct access state handlers.
    ###############################################################################

    def _handler_direct_access_enter(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent entered state %s',
                 self._fsm.get_current_state())
    
    def _handler_direct_access_exit(self,  *args, **kwargs):
        """
        """
        log.info('Instrument agent left state %s',
                 self._fsm.get_current_state())

    def _handler_direct_access_go_observatory(self,  *args, **kwargs):
        """
        """
        result = None
        next_state = None
        
        return (next_state, result)

    ###############################################################################
    # Private helpers.
    ###############################################################################

    def _start_driver(self, dvr_config, comms_config):
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
            return InstErrorCode.REQUIRED_PARAMETER
                
        # Launch driver process.
        self._dvr_proc = ZmqDriverProcess.launch_process(cmd_port, evt_port,
                                                         dvr_mod,  dvr_cls)

        self._dvr_proc.poll()
        if self._dvr_proc.returncode:
            # Error proc didn't start.
            return InstErrorCode.AGENT_INIT_FAILED
        
        # Create client and start messaging.
        self._dvr_client = ZmqDriverClient(svr_addr, cmd_port, evt_port)
        self._dvr_client.start_messaging(self.evt_recv)
        time.sleep(1)

        try:        
            retval = self._dvr_client.cmd_dvr('process_echo', 'Test.')
            log.info('Driver client started, echo test: %s', str(retval))
            
        except Exception:
            self._dvr_proc.terminate()
            self._dvr_proc.wait()
            self._dvr_proc = None
            self._dvr_client = None
            return InstErrorCode.AGENT_INIT_FAILED

        self._dvr_config = dvr_config
        self._comms_config = comms_config        

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
        time.sleep(1)
        
    ###############################################################################
    # Misc and test.
    ###############################################################################

    def test_ia(self):
        log.info('Hello from the instrument agent!')



