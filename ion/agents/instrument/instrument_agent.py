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
from pyon.public import IonObject, log, RT, PRED, LCS, OT
from pyon.ion.stream import StreamPublisher
from pyon.agent.agent import ResourceAgent
from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.agent import ResourceAgentState
from pyon.util.containers import get_ion_ts

# Pyon exceptions.
from pyon.core.exception import IonException
from pyon.core.exception import BadRequest
from pyon.core.exception import Conflict
from pyon.core.exception import Timeout
from pyon.core.exception import NotFound
from pyon.core.exception import ServerError
from pyon.core.exception import ResourceError

# Standard imports.
import socket
import json
import base64

# Packages
import numpy

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
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessServer
from ion.agents.instrument.direct_access.direct_access_server import SessionCloseReasons
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from coverage_model.parameter import ParameterDictionary

# MI imports
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverAsyncEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.data_particle import DataParticle
from interface.objects import StreamRoute

from interface.objects import AgentCommand

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

class ResourceInterfaceCapability(BaseEnum):
    GET_RESOURCE = ResourceAgentEvent.GET_RESOURCE
    SET_RESOURCE = ResourceAgentEvent.SET_RESOURCE
    PING_RESOURCE = ResourceAgentEvent.PING_RESOURCE
    GET_RESOURCE_STATE = ResourceAgentEvent.GET_RESOURCE_STATE
    EXECUTE_RESOURCE = ResourceAgentEvent.EXECUTE_RESOURCE

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

        
        #This is the type of Resource managed by this agent
        self.resource_type = RT.InstrumentDevice
        #This is the id of the resource managed by this agent
        self.resource_id = None
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
        self._da_session_close_reason = 'due to ION request'                

        # The direct accerss server
        self._da_server = None

        # Dictionary of data stream IDs for data publishing. Constructed
        # by stream_config agent config member during process on_init.
        self._data_streams = {}

        self._param_dicts = {}
        self._stream_defs = {}

        # Dictionary of data stream publishers. Constructed by
        # stream_config agent config member during process on_init.
        self._data_publishers = {}

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

        # Construct stream publishers.
        self._construct_data_publishers()


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

    def _get_resource_interface(self, current_state=True):
        """
        """
        agent_cmds = self._fsm.get_events(current_state)
        res_iface_cmds = [x for x in agent_cmds if ResourceInterfaceCapability.has(x)]
    
        # convert agent mediated resource commands into interface names.
        result = []
        for x in res_iface_cmds:
            if x == ResourceAgentEvent.GET_RESOURCE:
                result.append('get_resource')
            elif x == ResourceAgentEvent.SET_RESOURCE:
                result.append('set_resource')
            elif x == ResourceAgentEvent.PING_RESOURCE:
                result.append('ping_resource')
            elif x == ResourceAgentEvent.GET_RESOURCE_STATE:
                result.append('get_resource_state')
            elif x == ResourceAgentEvent.EXECUTE_RESOURCE:
                result.append('execute_resource')

        return result
    
    ##############################################################
    # Agent interface.
    ##############################################################    

    ##############################################################
    # Governance interfaces
    ##############################################################

    def check_set_resource(self, msg,  headers):
        '''
        This function is used for governance validation for the set_resource operation.
        '''
        com = self._get_resource_commitments(headers['ion-actor-id'])
        if com is None:
            return False, '(set_resource) has been denied since the user %s has not acquired the resource %s' % (headers['ion-actor-id'], self.resource_id)

        return True, ''

    def check_execute_resource(self, msg,  headers):
        '''
        This function is used for governance validation for the execute_resource operation.
        '''
        com = self._get_resource_commitments(headers['ion-actor-id'])
        if com is None:
            return False, '(execute_resource) has been denied since the user %s has not acquired the resource %s' % (headers['ion-actor-id'], self.resource_id)

        #if msg['command'].command == ResourceAgentEvent.GO_DIRECT_ACCESS and not com.commitment.exclusive:
        #    return False, 'Direct Access Mode has been denied since the user %s has not acquired the resource %s exclusively' % (headers['ion-actor-id'], self.resource_id)

        return True, ''

    def check_ping_resource(self, msg,  headers):
        '''
        This function is used for governance validation for the ping_resource operation.
        '''
        com = self._get_resource_commitments(headers['ion-actor-id'])
        if com is None:
            return False, '(ping_resource) has been denied since the user %s has not acquired the resource %s' % (headers['ion-actor-id'], self.resource_id)

        return True, ''



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

    def _handler_ping_resource(self, *args, **kwargs):
        """
        """
        result = None
        next_state = None
        
        try:
            result = self._dvr_client.cmd_dvr('process_echo', *args, **kwargs)
            result = result + ', time:%s' % get_ion_ts()
        except Exception as ex:
            self._raise_ion_exception(ex)

        return (next_state, result)

    def _handler_done(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        next_state = ResourceAgentState.COMMAND
          
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

                
        session_timeout = kwargs.get('session_timeout', 10)
        inactivity_timeout = kwargs.get('inactivity_timeout', 5)
        session_type = kwargs.get('session_type', None)

        if not session_type:
            raise BadRequest('Instrument parameter error attempting direct access: session_type not present') 

        log.info("Instrument agent requested to start direct access mode: sessionTO=%d, inactivityTO=%d,  session_type=%s" 
                 %(session_timeout, inactivity_timeout, dir(DirectAccessTypes)[session_type]))
        
        
        # get 'address' of host
        hostname = socket.gethostname()
        log.debug("hostname = " + hostname)        
        ip_addresses = socket.gethostbyname_ex(hostname)
        log.debug("ip_address=" + str(ip_addresses))
        ip_address = ip_addresses[2][0]
        ip_address = hostname
        
        # create a DA server instance (TODO: just telnet for now) and pass in callback method
        try:
            self._da_server = DirectAccessServer(session_type, 
                                                self._da_server_input_processor, 
                                                ip_address,
                                                session_timeout,
                                                inactivity_timeout)
        except Exception as ex:
            log.warning("InstrumentAgent: failed to start DA Server <%s>" %str(ex))
            raise ex
        
        # get the connection info from the DA server to return to the user
        port, token = self._da_server.get_connection_info()
        result = {'ip_address':ip_address, 'port':port, 'token':token}
        #next_state = InstrumentAgentState.DIRECT_ACCESS
        
        # tell driver to start direct access mode
        # (dvr_result, next_state) = self._dvr_client.cmd_dvr('execute_start_direct_access')
        #(next_state, dvr_result) = self.execute_resource(DriverEvent.START_DIRECT)
        (next_state, dvr_result) = self._handler_execute_resource(DriverEvent.START_DIRECT)
        
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

        log.info("Instrument agent requested to stop direct access mode - %s" %self._da_session_close_reason)
        
        # tell driver to stop direct access mode
        (next_state, dvr_result) = self._handler_execute_resource(DriverEvent.STOP_DIRECT)
        
        # stop DA server
        if (self._da_server):
            self._da_server.stop()
            self._da_server = None
            
        # re-set the 'reason' to be the default
        self._da_session_close_reason = 'due to ION request'
        
        return (next_state, result)        

    ##############################################################
    # Asynchronous driver event callback and handlers.
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
            return
        
        if type == DriverAsyncEvent.STATE_CHANGE:
            self._async_driver_event_state_change(val, ts)
                
        elif type == DriverAsyncEvent.CONFIG_CHANGE:
            self._async_driver_event_config_change(val, ts)

        elif type == DriverAsyncEvent.SAMPLE:
            self._async_driver_event_sample(val, ts)

        elif type == DriverAsyncEvent.ERROR:
            self._async_driver_event_error(val, ts)

        elif type == DriverAsyncEvent.RESULT:
            self._async_driver_event_result(val, ts)

        elif type == DriverAsyncEvent.DIRECT_ACCESS:
            self._async_driver_event_direct_access(val, ts)

        elif type == DriverAsyncEvent.AGENT_EVENT:
            self._async_driver_event_agent_event(val, ts)
        
        else:
            log.error('Instrument agent %s received unknown driver event %s.',
                      self._proc_name, str(evt))

    def _async_driver_event_state_change(self, val, ts):
        """
        """
        try:
            event_data = {
                'state' : val
            }
            self._event_publisher.publish_event(
                event_type='ResourceAgentResourceStateEvent',
                origin=self.resource_id, **event_data)
        except:
            log.error('Instrument agent %s could not publish driver state change event.',
                      self._proc_name)
            

    def _async_driver_event_config_change(self, val, ts):
        """
        """
        # Publsih resource config change event.
        try:
            event_data = {
                'config' : val
            }
            self._event_publisher.publish_event(
                event_type='ResourceAgentResourceConfigEvent',
                origin=self.resource_id, **event_data)
        except:
            log.error('Instrument agent %s could not publish driver config change event.',
                      self._proc_name)            

    def _async_driver_event_sample(self, val, ts):
        """
        """
        # Publish sample on sample data streams.
        """
        quality_flag : ok
        preferred_timestamp : driver_timestamp
        stream_name : raw
        pkt_format_id : JSON_Data
        pkt_version : 1
        values : [{u'binary': True, u'value_id': u'raw', u'value':
            u'MTkuMDYxMiwzLjMzNzkxLCA0NDkuMDA1LCAgIDE0Ljg3MjksIDE1MDUuMTQ3L
            CAwMSBGZWIgMjAwMSwgMDE6MDE6MDA='}]

        quality_flag : ok
        preferred_timestamp : driver_timestamp
        stream_name : parsed
        pkt_format_id : JSON_Data
        pkt_version : 1
        values : [{u'value_id': u'temp', u'value': 19.0612},
            {u'value_id': u'conductivity', u'value': 3.33791},
            {u'value_id': u'pressure', u'value': 449.005}]
        """
        
        # If the sample event is encoded, load it back to a dict.
        if isinstance(val, str):
            val = json.loads(val)

        try:
            stream_name = val['stream_name']
            publisher = self._data_publishers[stream_name]
            #param_dict = self._param_dicts[stream_name]
            stream_def = self._stream_defs[stream_name]
            rdt = RecordDictionaryTool(stream_definition_id=stream_def)
            log.info("Stream definition has the followinf fields: %s" % rdt.fields)

            for (k, v) in val.iteritems():
                if k == 'values':
                    for x in v:
                        value_id = x['value_id']
                        if value_id in rdt:
                            value = x['value']
                            if x.get('binary', None):
                                value = base64.b64decode(value)
                            rdt[value_id] = numpy.array([value]) # There might be an issue here, if value is a list...
                    
                elif k in rdt:
                    if k == 'driver_timestamp':
                        rdt['time'] = numpy.array([v])
                    rdt[k] = numpy.array([v]) # There might be an issue here if value is a list

            log.info('Outgoing granule: %s' % ['%s: %s'%(k,v) for k,v in rdt.iteritems()])
            g = rdt.to_granule(data_producer_id=self.resource_id)
            publisher.publish(g)        
            
            log.info('Instrument agent %s published data granule on stream %s.',
                     self._proc_name, stream_name)
            
        except:
            log.exception('Instrument agent %s could not publish data.', self._proc_name)

    def _async_driver_event_error(self, val, ts):
        """
        """
        try:
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
        except:
            log.error('Instrument agent %s could not publish driver error event.',
                      self._proc_name)

    def _async_driver_event_result(self, val, ts):
        """
        """
        try:
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
        except:
            log.error('Instrument agent %s could not publish driver result event.',
                      self._proc_name)            

    def _async_driver_event_direct_access(self, val, ts):
        """
        """
        try:
            evt = "Unknown"
            if (self._da_server):
                if (val):
                    self._da_server.send(val)
                else:
                    log.error('Instrument agent %s error %s processing driver event %s',
                              self._proc_name, '<no value present in event>', str(evt))
            else:
                log.error('Instrument agent %s error %s processing driver event %s',
                          self._proc_name, '<no DA server>', str(evt))
        except:
            log.error('Instrument agent %s could not publish driver result event.',
                      self._proc_name)            

    def _async_driver_event_agent_event(self, val, ts):
        """
        """
        # File instrument agent FSM event (e.g. to switch agent state).
        try:
            self._fsm.on_event(val)
            
        except:
            log.warning('Instrument agent %s error processing \
                      asynchronous agent event %s', self.id, val)


    ##############################################################
    # FSM setup.
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
        self._fsm.add_handler(ResourceAgentState.INACTIVE, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)

        # IDLE state event handlers.
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.RUN, self._handler_idle_run)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
 
        # STOPPED state event handlers.
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.RESET, self._handler_stopped_reset)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.GO_INACTIVE, self._handler_stopped_go_inactive)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.RESUME, self._handler_stopped_resume)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.CLEAR, self._handler_stopped_clear)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)

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
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        
        # STREAMING state event handlers.
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.RESET, self._handler_streaming_reset)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GO_INACTIVE, self._handler_streaming_go_inactive)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        
        # TEST state event handlers.
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.DONE, self._handler_done)
        
        # CALIBRATE state event handlers.
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.DONE, self._handler_done)
                
        # BUSY state event handlers.
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.DONE, self._handler_done)

        # DIRECT_ACCESS state event handlers.
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GO_COMMAND, self._handler_direct_access_go_command)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        
    ##############################################################
    # Start and stop driver.
    ##############################################################    

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

    ##############################################################
    # Convert instrument exceptions to ION exceptions.
    ##############################################################    

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

    ##############################################################
    # Publishing helpers.
    ##############################################################    

    def _construct_data_publishers(self):
        """
        Construct the stream publishers from the stream_config agent
        config variable.
        @retval None
        """
        # The registrar to create publishers.
        agent_info = self.CFG.get('agent', None)
        if not agent_info:
            log.warning('No agent config found in agent config -- publishers \
                        not constructed.')
        else:
            self.resource_id = agent_info['resource_id']

        stream_info = self.CFG.get('stream_config', None)
        if not stream_info:
            log.warning('No stream config found in agent config -- publishers \
                        not constructed.')
        
        else:
            log.info("stream_info = %s" % stream_info)
 
            for (stream_name, stream_config) in stream_info.iteritems():
                try:
                    stream_id = stream_config['stream_id']
                    exchange_point = stream_config['exchange_point']
                    routing_key = stream_config['routing_key']
                    param_dict_flat = stream_config['parameter_dictionary']
                    self._param_dicts[stream_name] = ParameterDictionary.load(param_dict_flat)
                    self._stream_defs[stream_name] = stream_config['stream_definition_ref']
                    self.route = StreamRoute(exchange_point=exchange_point, routing_key=routing_key)
                    publisher = StreamPublisher(process=self, stream_id=stream_id, stream_route=self.route)

                    self._data_publishers[stream_name] = publisher
                    log.info("Instrument agent '%s' created publisher for stream_name "
                         "%s" % (self._proc_name, stream_name))
                
                except:
                    log.warning('Instrument agent %s failed to create \
                                publisher for stream %s.', self._proc_name,
                                stream_name)


    ###############################################################################
    # Event callback and handling for direct access.
    ###############################################################################
    
    def _da_server_input_processor(self, data):
        # callback passed to DA Server for receiving input from server       
        if isinstance(data, int):
            # not character data, so check for lost connection
            if data == SessionCloseReasons.client_closed:
                self._da_session_close_reason = "due to client closing session"
            elif data == SessionCloseReasons.session_timeout:
                self._da_session_close_reason = "due to session exceeding maximum time"
            elif data == SessionCloseReasons.inactivity_timeout:
                self._da_session_close_reason = "due to inactivity"
            else:
                log.error("InstAgent.telnet_input_processor: got unexpected integer " + str(data))
                return
            log.warning("InstAgent.telnet_input_processor: connection closed %s" %self._da_session_close_reason)
            cmd = AgentCommand(command=ResourceAgentEvent.GO_COMMAND)
            self.execute_agent(command=cmd)
            return
        log.debug("InstAgent.telnetInputProcessor: data = <" + str(data) + "> len=" + str(len(data)))
        # send the data to the driver
        self._dvr_client.cmd_dvr('execute_resource', DriverEvent.EXECUTE_DIRECT, data)
        
        
