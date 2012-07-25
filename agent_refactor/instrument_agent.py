#!/usr/bin/env python

"""
@package ion.agents.instrument.instrument_agent Instrument resource agent
@file ion/agents.instrument/instrument_agent.py
@author Edward Hunter
@brief Resource agent derived class providing an instrument agent as a resource.
This resource fronts instruments and instrument drivers one-to-one in ION.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon imports
from pyon.public import IonObject, log
from pyon.agent.agent import ResourceAgent
from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.agent import ResourceAgentState

# Stancdard imports.
import time

# Pyon exceptions.
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

# ION imports.
from ion.agents.instrument.driver_process import DriverProcess

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

    def __init__(self, *args, **kwargs):
        """
        """
        ResourceAgent.__init__(self, *args, **kwargs)

        ###############################################################################
        # Instrument agent internal parameters.
        ###############################################################################

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
        
        # Flag indicates if the agent is running in a test so that it
        # can instruct drivers to self destruct if it disappears.
        self._test_mode = False
        
        # Set the 'reason' to be the default.
        self.da_session_close_reason = 'due to ION request'                

        # TODO stream publisher setup (mixin)

    def on_init(self):
        """
        Instrument agent pyon process initialization.
        Init objects that depend on the container services and start state
        machine.
        """
        
        # Set the driver config from the agent config if present.
        self._dvr_config = self.CFG.get('driver_config', None)
        
        # Set the test mode.
        self._test_mode = self.CFG.get('test_mode', False)
        
        # TODO stream publisher setup (mixin)

    ##############################################################
    # Capabilities interface and event handlers.
    ##############################################################    


    def _handler_get_resource_capabilities(self, *args, **kwargs):
        """
        """
        pass

    ##############################################################
    # Agent interface.
    ##############################################################    

    # The agent interface is completely defined in the resource agent
    # common base class.    

    ##############################################################
    # Resource interface and common resource event handlers.
    ##############################################################    

    def _handler_get_resource(self, *args, **kwargs):
        """
        """
        pass

    def _handler_set_resource(self, *args, **kwargs):
        """
        """
        pass

    def _handler_execute_resource(self, *args, **kwargs):
        """
        """
        pass

    ##############################################################
    # UNINITIALIZED event handlers.
    ##############################################################    

    def _handler_uninitialized_initialize(self, *args, **kwargs):
        """
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
        next_state = ResourceAgentState.INACTIVE

        return (next_state, result)

    ##############################################################
    # INACTIVE event handlers.
    ##############################################################    

    def _handler_inactive_reset(self, *args, **kwargs):
        """
        """
        result = None
        next_state = None
  
        result = self._stop_driver()
        next_state = ResourceAgentState.UNINITIALIZED
  
        return (next_state, result)

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
                            
        # Set the driver config if passed as a parameter.
        try:
            self._dvr_config['comms_config'] = args[0]
        
        except IndexError:
            pass
        
        # Configure the driver, driver checks if config is valid.
        dvr_comms = self._dvr_config.get('comms_config', None)   
        try:
            self._dvr_client.cmd_dvr('configure', dvr_comms)
        
        except InstrumentParameterException:
            raise InstParameterError('The driver comms configuration is invalid.')
        
        # Connect to the device, propagating connection errors.
        try:
            self._dvr_client.cmd_dvr('connect')
        
        except InstrumentConnectionException:
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
                
                except InstrumentTimeoutException, InstrumentProtocolException:
                    no_tries += 1
                    if no_tries >= max_tries:
                        self._dvr_client.cmd_dvr('disconnect')
                        raise InstProtocolError('Could not discover instrument state.')
        
        else:
            next_state = InstrumentAgentState.IDLE        
        
        return (next_state, result)        

    ##############################################################
    # IDLE event handlers.
    ##############################################################    

    def _handler_idle_reset(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    ##############################################################
    # STOPPED event handlers.
    ##############################################################    

    def _handler_stopped_reset(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    def _handler_stopped_go_inactive(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    def _handler_stopped_resume(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    def _handler_stopped_clear(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    ##############################################################
    # COMMAND event handlers.
    ##############################################################    

    def _handler_command_reset(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        
    
    def _handler_command_go_inactive(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    def _handler_command_clear(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    def _handler_command_pause(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    def _handler_command_go_direct_access(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        

    ##############################################################
    # STREAMING event handlers.
    ##############################################################    

    def _handler_streaming_reset(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        
    
    def _handler_streaming_go_inactive(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        
    
    ##############################################################
    # TEST event handlers.
    ##############################################################    

    ##############################################################
    # CALIBRATE event handlers.
    ##############################################################    

    ##############################################################
    # BUSY event handlers.
    ##############################################################    

    ##############################################################
    # DIRECT_ACCESS event handlers.
    ##############################################################    

    def _handler_direct_access_go_command(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)        


    def evt_recv(self, evt):
        """
        """
        log.info('Instrument agent got an event: %s', str(evt))

    ##############################################################
    # Helpers.
    ##############################################################    

    def _construct_fsm(self):
        """
        """
        
        # Construct default state machine states and handlers.
        ResourceAgent._construct_fsm(self)
        
        # UNINITIALIZED state event handlers.
        self._fsm.add_handler(ResourceAgentState.UNINITIALIZED, ResourceAgentEvent.INITIALIZE, self._handler_uninitialized_initialize)

        # Instrument agents do not currently use POWERED_DOWN.

        # INACTIVE state event handlers.
        self._fsm.add_handler(ResourceAgentState.INACTIVE, ResourceAgentEvent.RESET, self._handler_inactive_reset)
        self._fsm.add_handler(ResourceAgentState.INACTIVE, ResourceAgentEvent.GO_ACTIVE, self._handler_inactive_go_active)
        self._fsm.add_handler(ResourceAgentState.INACTIVE, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # IDLE state event handlers.
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.RUN, self._handler_idle_run)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
 
        # STOPPED state event handlers.
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.RESET, self._handler_stopped_reset)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.GO_INACTIVE, self._handler_stopped_go_inactive)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.RESUME, self._handler_stopped_resume)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.CLEAR, self._handler_stopped_clear)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # COMMAND state event handlers.
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.RESET, self._handler_command_reset)
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.GO_INACTIVE, self._handler_command_go_inactive)
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.CLEAR, self._handler_command_clear)
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.PAUSE, self._handler_command_pause)
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.SET_RESOURCE, self._handler_set_resource)
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.GO_DIRECT_ACCESS, self._handler_command_go_direct_access)
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        
        # STREAMING state event handlers.
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.RESET, self._handler_streaming_reset)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GO_INACTIVE, self._handler_streaming_go_inactive)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        
        # TEST state event handlers.
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        
        # CALIBRATE state event handlers.
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
                
        # BUSY state event handlers.
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # DIRECT_ACCESS state event handlers.
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GO_COMMAND, self._handler_direct_access_go_command)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

    def _start_driver(self, dvr_config):
        """
        Start the driver process and driver client.
        @param dvr_config The driver configuration.
        @raises InstDriverError If the driver or client failed to start properly.
        """


        self._dvr_proc = DriverProcess.get_process(dvr_config, True)
        self._dvr_proc.launch()

        # Verify the driver has started.
        if not self._dvr_proc.getpid():
            log.error('Instrument agent %s error starting driver process.', self._proc_name)
            raise InstDriverError('Error starting driver process.')

        try:
            driver_client = self._dvr_proc.get_client()
            driver_client.start_messaging(self.evt_recv)
            retval = driver_client.cmd_dvr('process_echo', 'Test.')
            self._dvr_client = driver_client

        except Exception, e:
            self._dvr_proc.stop()
            log.error('Instrument agent %s rror starting driver client. %s', self._proc_name, e)
            raise InstDriverError('Error starting driver client.')

        #self._construct_packet_factories()

        log.info('Instrument agent %s started its driver.', self._proc_name)
        
    def _stop_driver(self):
        """
        Stop the driver process and driver client.
        """
        log.info('Instrument agent %s stopped its driver.', self._proc_name)
        self._dvr_proc.stop()

            
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
