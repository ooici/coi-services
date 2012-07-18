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
        
        log.debug('GOT INITIALIZE EVENT')
        return (next_state, result)


    ##############################################################
    # INACTIVE event handlers.
    ##############################################################    

    def _handler_inactive_initialize(self, *args, **kwargs):
        """
        """
        pass

    def _handler_inactive_reset(self, *args, **kwargs):
        """
        """
        pass

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        """
        pass

    ##############################################################
    # IDLE event handlers.
    ##############################################################    

    def _handler_idle_reset(self, *args, **kwargs):
        """
        """
        pass

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        """
        pass

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        pass

    ##############################################################
    # STOPPED event handlers.
    ##############################################################    

    def _handler_stopped_reset(self, *args, **kwargs):
        """
        """
        pass

    def _handler_stopped_go_inactive(self, *args, **kwargs):
        """
        """
        pass

    def _handler_stopped_resume(self, *args, **kwargs):
        """
        """
        pass

    def _handler_stopped_clear(self, *args, **kwargs):
        """
        """
        pass

    ##############################################################
    # COMMAND event handlers.
    ##############################################################    

    def _handler_command_reset(self, *args, **kwargs):
        """
        """
        pass
    
    def _handler_command_go_inactive(self, *args, **kwargs):
        """
        """
        pass

    def _handler_command_clear(self, *args, **kwargs):
        """
        """
        pass

    def _handler_command_pause(self, *args, **kwargs):
        """
        """
        pass

    def _handler_command_go_direct_access(self, *args, **kwargs):
        """
        """
        pass

    ##############################################################
    # STREAMING event handlers.
    ##############################################################    

    def _handler_streaming_reset(self, *args, **kwargs):
        """
        """
        pass
    
    def _handler_streaming_go_inactive(self, *args, **kwargs):
        """
        """
        pass
    
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
        pass

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
        self._fsm.add_handler(ResourceAgentState.INACTIVE, ResourceAgentEvent.INITIALIZE, self._handler_inactive_initialize)
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


