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
from pyon.core.exception import ServerError
from pyon.core.exception import ResourceError

# MI exceptions
from mi.core.exceptions import InstrumentTimeoutException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import SampleException
from mi.core.exceptions import InstrumentStateException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentException

# ION imports.
from ion.agents.instrument.driver_process import DriverProcess
from ion.agents.instrument.common import BaseEnum
from ion.agents.instrument.instrument_fsm import FSMStateError
from ion.agents.instrument.instrument_fsm import FSMCommandUnknownError

# MI imports
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter

class InstrumentAgentState():
    UNINITIALIZED='xxx'

class InstrumentAgentEvent():
    pass

class InstrumentAgentCapability(BaseEnum):
    INITIALIZE = ResourceAgentEvent.INITIALIZE
    RESET = ResourceAgentEvent.RESET
    GO_ACTIVE = ResourceAgentEvent.GO_ACTIVE
    GO_INACTIVE = ResourceAgentEvent.GO_INACTIVE
    RUN = ResourceAgentEvent.RUN
    CLEAR = ResourceAgentEvent.CLEAR
    PAUSE = ResourceAgentEvent.PAUSE
    RESUME = ResourceAgentEvent.RESUME
    GO_COMMAND = ResourceAgentEvent.GO_COMMAND
    GO_DIRECT_ACCESS = ResourceAgentEvent.GO_DIRECT_ACCESS

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
        result = None
        next_state = None

        result = self._dvr_client.cmd_dvr('get_resource_capabilities', *args, **kwargs)
        return (next_state, result)

    def _filter_capabilities(self, events):

        events_out = [x for x in events if InstrumentAgentCapability.has(x)]
        return events_out
    
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
        result = None
        next_state = None
        
        try:
            params = args[0]
        
        # Raise ION BadRequest if required parameters missing.
        except KeyError:
            raise BadRequest('get_resource missing parameters argument.')

        try:
            result = self._dvr_client.cmd_dvr('get_resource', params)
            
        except Exception as ex:
            self._raise_ion_exception(ex)
        
        return (next_state, result)

    def _handler_set_resource(self, *args, **kwargs):
        """
        """
        result = None
        next_state = None
        
        try:
            params = args[0]
        
        except KeyError:
            raise BadRequest('set_resource missing parameters argument.')

        try:
            result = self._dvr_client.cmd_dvr('set_resource', params)
            
        except Exception as ex:
            self._raise_ion_exception(ex)

        return (next_state, result)

    def _handler_execute_resource(self, *args, **kwargs):
        """
        """
        result = None
        next_state = None
        
        try:
            (next_state, result) = self._dvr_client.cmd_dvr(
                'execute_resource', *args, **kwargs)
            
        except Exception as ex:
            self._raise_ion_exception(ex)

        return (next_state, result)

    def _handler_get_resource_state(self, *args, **kwargs):
        """
        """
        result = None
        next_state = None
        
        try:
            result = self._dvr_client.cmd_dvr('get_resource_state',
                                              *args, **kwargs)
            
        except Exception as ex:
            self._raise_ion_exception(ex)

        return (next_state, result)



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
            log.error('Bad or missing driver configuration.')
            raise BadRequest('The driver configuration is missing or invalid.')

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
        
        # Connect to the device.
        dvr_comms = self._dvr_config.get('comms_config', None)   
        self._dvr_client.cmd_dvr('configure', dvr_comms)
        self._dvr_client.cmd_dvr('connect')

        max_tries = kwargs.get('max_tries', 5)
        if not isinstance(max_tries, int) or max_tries < 1:
            max_tries = 5
        no_tries = 0
        while True:
            try:
                next_state = self._dvr_client.cmd_dvr('discover_state')
                break
            except InstrumentTimeoutException, InstrumentProtocolException:
                no_tries += 1
                if no_tries >= max_tries:
                    self._dvr_client.cmd_dvr('disconnect')
                    # fixfix
                    raise ResourceError('Could not discover instrument state.')
        
        return (next_state, result)        

    ##############################################################
    # IDLE event handlers.
    ##############################################################    

    def _handler_idle_reset(self, *args, **kwargs):
        """
        """
        result = None
        next_state = None

        # Disconnect, initialize, stop driver and go to uninitialized.
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        next_state = ResourceAgentState.UNINITIALIZED
  
        return (next_state, result)

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        next_state = ResourceAgentState.INACTIVE
        
        return (next_state, result)        

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        # TODO: need to determine correct obs state to enter (streaming or
        # command, and follow agent transitions as needed.)
        next_state = ResourceAgentState.COMMAND
        
        return (next_state, result)        

    ##############################################################
    # STOPPED event handlers.
    ##############################################################    

    def _handler_stopped_reset(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        next_state = ResourceAgentState.UNINITIALIZED
        
        return (next_state, result)        

    def _handler_stopped_go_inactive(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        next_state = ResourceAgentState.INACTIVE
        
        return (next_state, result)        

    def _handler_stopped_resume(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        next_state = ResourceAgentState.COMMAND
        
        return (next_state, result)        

    def _handler_stopped_clear(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        next_state = ResourceAgentState.IDLE
        
        return (next_state, result)        

    ##############################################################
    # COMMAND event handlers.
    ##############################################################    

    def _handler_command_reset(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        next_state = ResourceAgentState.UNINITIALIZED
        
        return (next_state, result)        
    
    def _handler_command_go_inactive(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        next_state = ResourceAgentState.INACTIVE
        
        return (next_state, result)        

    def _handler_command_clear(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        next_state = ResourceAgentState.IDLE
        
        return (next_state, result)        

    def _handler_command_pause(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        next_state = ResourceAgentState.STOPPED
        
        return (next_state, result)        

    def _handler_command_go_direct_access(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        # TODO add this logic.
        
        return (next_state, result)        

    ##############################################################
    # STREAMING event handlers.
    ##############################################################    

    def _handler_streaming_reset(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        next_state = ResourceAgentState.UNINITIALIZED
          
        return (next_state, result)        
    
    def _handler_streaming_go_inactive(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        next_state = ResourceAgentState.INACTIVE
          
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


    ##############################################################
    # Instrument agent event callback.
    ##############################################################    

    def evt_recv(self, evt):
        """
        """        
        log.info('Instrument agent %s got async driver event %s',
                 self.id, str(evt))
        try:
            type = evt['type']
            val = evt['value']
            ts = evt['time']
            
        except KeyError, ValueError:
            log.error('Instrument agent %s received driver event %s \
                      has missing required fields.', self.id, str(evt))

        else:
            if type == DriverAsyncEvent.STATE_CHANGE:
                event_data = {
                    'state' : val
                }
                self._event_publisher.publish_event(
                    event_type='ResourceAgentResourceStateEvent',
                    origin=self.resource_id, **event_data)
  
            elif type == DriverAsyncEvent.CONFIG_CHANGE:
                # Publsih resource config change event.
                event_data = {
                    'config' : val
                }
                self._event_publisher.publish_event(
                    event_type='ResourceAgentResourceConfigEvent',
                    origin=self.resource_id, **event_data)
            
            elif type == DriverAsyncEvent.SAMPLE:
                # Publish sample on sample data streams.
                pass
            
            elif type == DriverAsyncEvent.ERROR:
                # Publish resource error event.
                if isinstance(val, IonException):
                    event_data = {
                        'error_type' : str(type(val)),
                        'error_msg' : val.message,
                        "error_code" : val.error_code
                    }
                
                elif isinstance(val, Exception):
                    event_data = {
                        'error_type' : str(type(val)),
                        'error_msg' : val.message
                    }
                
                else:
                    event_data = {
                        'error_msg' : str(val)
                    }
                
                self._event_publisher.publish_event(
                    event_type='ResourceAgentErrorEvent',
                    origin=self.resource_id, **event_data)
            
            elif type == DriverAsyncEvent.RESULT:
                # Publsih async result event.
                cmd = val.get('cmd', None)
                desc = val.get('desc', None)
                event_data = {
                    'command' : cmd,
                    'desc' : desc,
                    'result' : val
                }
                self._event_publisher.publish_event(
                    event_type='ResourceAgentAsyncResultEvent',
                    origin=self.resource_id, **event_data)

            elif type == DriverAsyncEvent.DIRECT_ACCESS:
                # Add direct access logic.
                pass

            elif type == DriverAsyncEvent.AGENT_EVENT:
                # File instrument agent FSM event (e.g. to switch agent state).
                try:
                    self._fsm.on_event(val)
                    
                except:
                    log.error('Instrument agent %s error processing \
                              asynchronous agent event %s', self.id, val)

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
        self._fsm.add_handler(ResourceAgentState.INACTIVE, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)

        # IDLE state event handlers.
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.RUN, self._handler_idle_run)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
 
        # STOPPED state event handlers.
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.RESET, self._handler_stopped_reset)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.GO_INACTIVE, self._handler_stopped_go_inactive)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.RESUME, self._handler_stopped_resume)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.CLEAR, self._handler_stopped_clear)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)

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
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        
        # STREAMING state event handlers.
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.RESET, self._handler_streaming_reset)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GO_INACTIVE, self._handler_streaming_go_inactive)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        
        # TEST state event handlers.
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        
        # CALIBRATE state event handlers.
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
                
        # BUSY state event handlers.
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)

        # DIRECT_ACCESS state event handlers.
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GO_COMMAND, self._handler_direct_access_go_command)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)

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
            raise ResourceError('Error starting driver process.')

        try:
            driver_client = self._dvr_proc.get_client()
            driver_client.start_messaging(self.evt_recv)
            retval = driver_client.cmd_dvr('process_echo', 'Test.')
            self._dvr_client = driver_client

        except Exception, e:
            self._dvr_proc.stop()
            log.error('Instrument agent %s rror starting driver client. %s', self._proc_name, e)
            raise ResourceError('Error starting driver client.')

        #self._construct_packet_factories()

        log.info('Instrument agent %s started its driver.', self._proc_name)
        
    def _stop_driver(self):
        """
        Stop the driver process and driver client.
        """
        self._dvr_proc.stop()
        log.info('Instrument agent %s stopped its driver.', self._proc_name)

            
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

    def _raise_ion_exception(self, ex):
        """
        """
        if isinstance(ex, IonException):
            iex = ex
        
        elif isinstance(ex, InstrumentParameterException):
            iex = BadRequest(*(ex.args))

        elif isinstance(ex, InstrumentStateException):
            iex = Conflict(*(ex.args))

        elif isinstance(ex, FSMStateError):
            iex = Conflict(*(ex.args))

        elif isinstance(ex, FSMCommandUnknownError):
            iex = BadRequest(*(ex.args))

        elif isinstance(ex, InstrumentTimeoutException):
            iex = Timeout(*(ex.args))
        
        elif isinstance(ex, InstrumentException):
            iex = ResourceError(*(ex.args))

        elif isinstance(ex, Exception):
            iex = ServerError(*(ex.args))

        if iex:
            raise iex
        
        