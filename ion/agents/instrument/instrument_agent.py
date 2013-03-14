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
from pyon.public import IonObject, log, RT, PRED, LCS, OT, CFG
from pyon.ion.stream import StreamPublisher
from pyon.agent.agent import ResourceAgent
from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentStreamStatus
from pyon.util.containers import get_ion_ts
from pyon.core.governance import ORG_MANAGER_ROLE, GovernanceHeaderValues, has_org_role, get_resource_commitments
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE
from pyon.public import IonObject

# Pyon exceptions.
from pyon.core.exception import IonException, Inconsistent
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
import copy
import uuid

# Packages
import numpy
import gevent

# ION imports.
from ion.agents.instrument.driver_process import DriverProcess
from ion.agents.instrument.common import BaseEnum
from ion.agents.instrument.instrument_fsm import FSMStateError
from ion.agents.instrument.instrument_fsm import FSMCommandUnknownError
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessServer
from ion.agents.instrument.direct_access.direct_access_server import SessionCloseReasons
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool

# Alarms.
from interface.objects import StreamAlarmType
from interface.objects import AlarmDef
from ion.agents.alarms.alarms import construct_alarm_expression
from ion.agents.alarms.alarms import eval_alarm
from ion.agents.alarms.alarms import make_event_data
from interface.objects import StreamWarningAlarmEvent
from interface.objects import StreamAlertAlarmEvent
from interface.objects import StreamAllClearAlarmEvent

# MI imports
from ion.core.includes.mi import DriverAsyncEvent
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
                
        # Dictionary of stream definition objects.
        self._stream_defs = {}
        
        # Data buffers for each stream.
        self._stream_buffers = {}
        
        # Publisher timer greenlets for stream buffering.
        self._stream_greenlets = {}
        
        # Dictionary of data stream publishers. Constructed by
        # stream_config agent config member during process on_init.
        self._data_publishers = {}

        # List of current alarm objects.
        self.aparam_alarms = []
        
        # Dictionary of stream fields.
        self.aparam_streams = {}
        
        # Dictionary of stream publication rates.
        self.aparam_pubfreq = {}

        # Autoreconnect thread.
        self._autoreconnect_greenlet = None

        # State when lost.
        self._state_when_lost = None

        # Connection ID.
        self._connection_ID = None
        
        # Connection index.
        self._connection_index = None

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

    #TODO - When/If the Instrument and Platform agents are dervied from a
    # common device agent class, then relocate to the parent class and share
    def check_resource_operation_policy(self, msg,  headers):
        '''
        This function is used for governance validation for certain agent operations.
        @param msg:
        @param headers:
        @return:
        '''

        try:
            gov_values = GovernanceHeaderValues(headers, resource_id_required=False)
        except Inconsistent, ex:
            return False, ex.message

        if has_org_role(gov_values.actor_roles ,self._get_process_org_name(),
                        ORG_MANAGER_ROLE):
            return True, ''

        if not has_org_role(gov_values.actor_roles ,self._get_process_org_name(),
                            INSTRUMENT_OPERATOR_ROLE):
            return False, ''

        com = get_resource_commitments(gov_values.actor_id,
                                       gov_values.resource_id)
        if com is None:
            return False, '%s(%s) has been denied since the user %s has not acquired the resource %s' \
                % (self.name, gov_values.op, gov_values.actor_id,
                   self.resource_id)

        return True, ''



    ##############################################################
    # Resource interface and common resource event handlers.
    ##############################################################    

    def _handler_get_resource(self, *args, **kwargs):
        try:
            params = args[0]
        # Raise ION BadRequest if required parameters missing.
        except KeyError:
            raise BadRequest('get_resource missing parameters argument.')
        result = self._dvr_client.cmd_dvr('get_resource', params)
        return (None, result)

    def _handler_set_resource(self, *args, **kwargs):
        try:
            params = args[0]
        except KeyError:
            raise BadRequest('set_resource missing parameters argument.')
        result = self._dvr_client.cmd_dvr('set_resource', params)
        return (None, result)

    def _handler_execute_resource(self, *args, **kwargs):
        return self._dvr_client.cmd_dvr('execute_resource', *args, **kwargs)

    def _handler_get_resource_state(self, *args, **kwargs):
        result = self._dvr_client.cmd_dvr('get_resource_state',*args, **kwargs)
        return (None, result)

    def _handler_ping_resource(self, *args, **kwargs):
        result = '%s, time:%s' % (self._dvr_client.cmd_dvr('process_echo', *args, **kwargs),
                                  get_ion_ts())
        return (None, result)

    def _handler_done(self, *args, **kwargs):
        return (ResourceAgentState.COMMAND, None)

    ##############################################################
    # UNINITIALIZED event handlers.
    ##############################################################    

    def _handler_uninitialized_initialize(self, *args, **kwargs):
        # If a config is passed, update member.
        try:
            self._dvr_config = args[0]
        except IndexError:
            pass
        
        # If config not valid, fail.
        if not self._validate_driver_config():
            raise BadRequest('The driver configuration is missing or invalid.')

        # Start the driver and switch to inactive.
        self._start_driver(self._dvr_config)
        return (ResourceAgentState.INACTIVE, None)

    ##############################################################
    # INACTIVE event handlers.
    ##############################################################    

    def _handler_inactive_reset(self, *args, **kwargs):
        result = self._stop_driver()
        return (ResourceAgentState.UNINITIALIZED, result)

    def _handler_inactive_go_active(self, *args, **kwargs):
        # Set the driver config if passed as a parameter.
        try:
            self._dvr_config['comms_config'] = args[0]
        except IndexError:
            pass
        
        # Connect to the device.
        dvr_comms = self._dvr_config.get('comms_config', None)   
        self._dvr_client.cmd_dvr('configure', dvr_comms)
        self._dvr_client.cmd_dvr('connect')
        
        # Reset the connection id and index.
        self._connection_ID = uuid.uuid4()
        self._connection_index = {key : 0 for key in self.aparam_streams.keys()}

        max_tries = kwargs.get('max_tries', 5)
        if not isinstance(max_tries, int) or max_tries < 1:
            max_tries = 5
        no_tries = 0
        while True:
            try:
                next_state = self._dvr_client.cmd_dvr('discover_state')
                break
            except Timeout, ResourceError:
                no_tries += 1
                if no_tries >= max_tries:
                    log.error("Could not discover instrument state")
                    next_state = ResourceAgentState.ACTIVE_UNKNOWN
                    #self._dvr_client.cmd_dvr('disconnect')
                    #raise 
        
        return (next_state, None)

    ##############################################################
    # IDLE event handlers.
    ##############################################################    

    def _handler_idle_reset(self, *args, **kwargs):
        # Disconnect, initialize, stop driver and go to uninitialized.
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        next_state = ResourceAgentState.UNINITIALIZED
  
        return (next_state, result)

    def _handler_idle_go_inactive(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        return (ResourceAgentState.INACTIVE, None)

    def _handler_idle_run(self, *args, **kwargs):
        # TODO: need to determine correct obs state to enter (streaming or
        # command, and follow agent transitions as needed.)
        return (ResourceAgentState.COMMAND, None)

    ##############################################################
    # STOPPED event handlers.
    ##############################################################    

    def _handler_stopped_reset(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        return (ResourceAgentState.UNINITIALIZED, result)

    def _handler_stopped_go_inactive(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        return (ResourceAgentState.INACTIVE, None)

    def _handler_stopped_resume(self, *args, **kwargs):
        return (ResourceAgentState.COMMAND, None)

    def _handler_stopped_clear(self, *args, **kwargs):
        return (ResourceAgentState.IDLE, None)

    ##############################################################
    # COMMAND event handlers.
    ##############################################################    

    def _handler_command_reset(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        return (ResourceAgentState.UNINITIALIZED, result)
    
    def _handler_command_go_inactive(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        return (ResourceAgentState.INACTIVE, None)

    def _handler_command_clear(self, *args, **kwargs):
        return (ResourceAgentState.IDLE, None)

    def _handler_command_pause(self, *args, **kwargs):
        return (ResourceAgentState.STOPPED, None)

    def _handler_command_go_direct_access(self, *args, **kwargs):
        session_timeout = kwargs.get('session_timeout', 10)
        inactivity_timeout = kwargs.get('inactivity_timeout', 5)
        session_type = kwargs.get('session_type', None)

        if not session_type:
            raise BadRequest('Instrument parameter error attempting direct access: session_type not present') 

        log.info("Instrument agent requested to start direct access mode: sessionTO=%d, inactivityTO=%d,  session_type=%s",
                 session_timeout, inactivity_timeout,
                 dir(DirectAccessTypes)[session_type])

        # get 'address' of host
        hostname = socket.gethostname()
        ip_addresses = socket.gethostbyname_ex(hostname)
        log.debug("hostname: %s, ip address: %s", hostname, ip_addresses)
#        ip_address = ip_addresses[2][0]
        ip_address = hostname
        
        # create a DA server instance (TODO: just telnet for now) and pass
        # in callback method
        try:
            self._da_server = DirectAccessServer(session_type, 
                                                self._da_server_input_processor, 
                                                ip_address,
                                                session_timeout,
                                                inactivity_timeout)
        except Exception as ex:
            log.warning("InstrumentAgent: failed to start DA Server <%s>",ex)
            raise ex
        
        # get the connection info from the DA server to return to the user
        port, token = self._da_server.get_connection_info()
        result = {'ip_address':ip_address, 'port':port, 'token':token}
        # tell driver to start direct access mode
        next_state, _ = self._dvr_client.cmd_dvr('start_direct')
        return (next_state, result)

    ##############################################################
    # STREAMING event handlers.
    ##############################################################    
    
    def _handler_streaming_reset(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        return (ResourceAgentState.UNINITIALIZED, result)
    
    def _handler_streaming_go_inactive(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('disconnect')
        self._dvr_client.cmd_dvr('initialize')        
        return (ResourceAgentState.INACTIVE, None)
    
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
        log.info("Instrument agent requested to stop direct access mode - %s",
                 self._da_session_close_reason)
        # tell driver to stop direct access mode
        next_state, _ = self._dvr_client.cmd_dvr('stop_direct')
        # stop DA server
        if (self._da_server):
            self._da_server.stop()
            self._da_server = None
        # re-set the 'reason' to be the default
        self._da_session_close_reason = 'due to ION request'
        return (next_state, None)

    ##############################################################
    # CONNECTION_LOST event handler, available in any active state.
    ##############################################################    

    def _handler_connection_lost_driver_event(self, *args, **kwargs):
        """
        Handle a connection lost event from the driver.
        """
        self._state_when_lost = self._fsm.get_current_state()
        return (ResourceAgentState.LOST_CONNECTION, None)

    ##############################################################
    # CONNECTION_LOST event handlers.
    ##############################################################    

    def _handler_lost_connection_enter(self, *args, **kwargs):
        super(InstrumentAgent, self)._common_state_enter(*args, **kwargs)
        log.error('Instrument agent %s lost connection to the device.',
                  self._proc_name)
        self._event_publisher.publish_event(
            event_type='ResourceAgentConnectionLostErrorEvent',
            origin_type=self.ORIGIN_TYPE,
            origin=self.resource_id)
        
        # Setup reconnect timer.
        self._autoreconnect_greenlet = gevent.spawn(self._autoreconnect)

    def _handler_lost_connection_exit(self, *args, **kwargs):
        super(InstrumentAgent, self)._common_state_exit(*args, **kwargs)
        if self._autoreconnect_greenlet:
            self._autoreconnect_greenlet = None

    def _autoreconnect(self):
        while self._autoreconnect_greenlet:
            gevent.sleep(10)
            try:
                print '## attempting reconnect...'
                self._fsm.on_event(ResourceAgentEvent.AUTORECONNECT)
            except:
                pass
    
    def _handler_lost_connection__reset(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        return (ResourceAgentState.UNINITIALIZED, result)
    
    def _handler_lost_connection__go_inactive(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('initialize')        
        return (ResourceAgentState.INACTIVE, None)

    def _handler_lost_connection__autoreconnect(self, *args, **kwargs):
    
        try:
            self._dvr_client.cmd_dvr('connect')

            # Reset the connection id and index.
            self._connection_ID = uuid.uuid4()
            self._connection_index = {key : 0 for key in self.aparam_streams.keys()}

        except:
            return (None, None)

        max_tries = kwargs.get('max_tries', 5)
        if not isinstance(max_tries, int) or max_tries < 1:
            max_tries = 5
        no_tries = 0
        while True:
            try:
                next_state = self._dvr_client.cmd_dvr('discover_state')
                break
            except Timeout, ResourceError:
                no_tries += 1
                if no_tries >= max_tries:
                    log.error("Could not discover instrument state")
                    next_state = ResourceAgentState.ACTIVE_UNKNOWN
   
        if next_state == ResourceAgentState.IDLE and \
            self._state_when_lost == ResourceAgentState.COMMAND:
                next_state = ResourceAgentState.COMMAND
   
        return (next_state, None)

    ##############################################################
    # ACTIVE_UNKNOWN event handlers.
    ##############################################################    

    def _handler_active_unknown_go_active(self, *args, **kwargs):
        max_tries = kwargs.get('max_tries', 5)
        if not isinstance(max_tries, int) or max_tries < 1:
            max_tries = 5
        no_tries = 0
        while True:
            try:
                next_state = self._dvr_client.cmd_dvr('discover_state')
                break
            except Timeout, ResourceError:
                no_tries += 1
                if no_tries >= max_tries:
                    log.error("Could not discover instrument state")
                    next_state = None
    
    def _handler_active_unknown_go_inactive(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('initialize')        
        return (ResourceAgentState.INACTIVE, None)

    def _handler_active_unknown_reset(self, *args, **kwargs):
        self._dvr_client.cmd_dvr('initialize')        
        result = self._stop_driver()
        return (ResourceAgentState.UNINITIALIZED, result)

    def _handler_active_unknown_go_direct_access(self, *args, **kwargs):
        session_timeout = kwargs.get('session_timeout', 10)
        inactivity_timeout = kwargs.get('inactivity_timeout', 5)
        session_type = kwargs.get('session_type', None)

        if not session_type:
            raise BadRequest('Instrument parameter error attempting direct access: session_type not present') 

        log.info("Instrument agent requested to start direct access mode: sessionTO=%d, inactivityTO=%d,  session_type=%s",
                 session_timeout, inactivity_timeout, dir(DirectAccessTypes)[session_type])

        # get 'address' of host
        hostname = socket.gethostname()
        ip_addresses = socket.gethostbyname_ex(hostname)
        log.debug("hostname: %s, ip address: %s", hostname, ip_addresses)
#        ip_address = ip_addresses[2][0]
        ip_address = hostname
        
        # create a DA server instance (TODO: just telnet for now) and pass in callback method
        try:
            self._da_server = DirectAccessServer(session_type, 
                                                self._da_server_input_processor, 
                                                ip_address,
                                                session_timeout,
                                                inactivity_timeout)
        except Exception as ex:
            log.warning("InstrumentAgent: failed to start DA Server <%s>",ex)
            raise ex
        
        # get the connection info from the DA server to return to the user
        port, token = self._da_server.get_connection_info()
        result = {'ip_address':ip_address, 'port':port, 'token':token}
        # tell driver to start direct access mode
        next_state, _ = self._dvr_client.cmd_dvr('start_direct')
        return (next_state, result)        

    ##############################################################
    # Asynchronous driver event callback and handlers.
    ##############################################################    

    def evt_recv(self, evt):
        log.info('Instrument agent %s got async driver event %s', self.id, evt)
        try:
            type = evt['type']
            val = evt['value']
            ts = evt['time']
        except KeyError, ValueError:
            log.error('Instrument agent %s received driver event %s has missing required fields.',
                      self.id, evt)
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
            log.info('Instrument agent %s driver state change: %s',
                     self._proc_name, val)
            event_data = { 'state' : val }
            self._event_publisher.publish_event(
                event_type='ResourceAgentResourceStateEvent',
                origin_type=self.ORIGIN_TYPE,
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
                origin_type=self.ORIGIN_TYPE,
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
            self._stream_buffers[stream_name].insert(0,val)

        except KeyError:
            log.error('Instrument agent %s received sample with bad \
                stream name %s.', self._proc_name, stream_name)

        else:
            self._process_alarms(val)
            state = self._fsm.get_current_state()
            pubfreq = self.aparam_pubfreq[stream_name]

            if state != ResourceAgentState.STREAMING or pubfreq == 0:
                self._publish_stream_buffer(stream_name)

    def _process_alarms(self, val):
        """
        """
        
        try:
            stream_name = val['stream_name']
            values = val['values']
        
        except KeyError:
            log.error('Tomato missing stream_name or values keys. Could not process alarms.')
            return

        stream_alarms = []
        first_time_alarms = []
        og_alarms = []
 
        for v in values:
            try:
                # Grab the value and id.
                value_id = v['value_id']
                value = v['value']

                # Retrieve the alarms relevant to this stream and id.
                stream_value_alarms = [a for a in self.aparam_alarms if
                    a.stream_name == stream_name and a.value_id == value_id]

                # Evaluate the alarms relevant to this stream and id.
                [eval_alarm(a, value) for a in stream_value_alarms]
                 
                # Accumulate all alarms relevant to this stream.
                stream_alarms.extend(stream_value_alarms)
                
            except KeyError:
                log.error('Tomato value missing value_id or value keys. Could not process alarms for stream %s, value_id %s.',
                          stream_name, value_id)

        # Determine first time alarms.
        first_time_alarms = [a for a in stream_alarms if a.first_time == 1]
                
        # Ongoing alarms.
        og_alarms = [a for a in stream_alarms if a.first_time > 1]
        
        # Determine newly cleared alarms.
        new_pos_alarms = [a for a in og_alarms if a.status and not a.old_status]
                
        # Determine newly bad alarms.
        new_neg_alarms = [a for a in og_alarms if a.old_status and not a.status]
                
        # Determine all cleared alarms.
        # pos_alarms = [a for a in og_alarms if a.status and not a.first_time]
                
        # Publish all statuses first time.
        if first_time_alarms:
            self._publish_alarms(first_time_alarms)
                
        # Publish newly bad alarms.                
        if new_neg_alarms:
            self._publish_alarms(new_neg_alarms)
        
        # Publish newly cleared alarms.                
        if new_pos_alarms:
            self._publish_alarms(new_pos_alarms)
            
        # TODO.
        # Publish stream all clear if something cleared and there
        # are no more bad alarms.
                            
    def _publish_alarms(self, alarms):
        """
        """
        events = [make_event_data(a) for a in alarms]
        events = [x for x in events if x]
        for event_data in events:
            try:
                self._event_publisher.publish_event(
                    origin=self.resource_id,
                    origin_type=self.ORIGIN_TYPE,
                    **event_data)
                log.info('Instrument agent %s published alarm event %s.',
                        self._proc_name, str(event_data))
                
            except Exception as ex:
                log.error('Instrument agent %s could not publish alarm event %s. Exception: %s',
                        self._proc_name, str(event_data), str(ex))
        
    def _publish_stream_buffer(self, stream_name):
        """
        """
        
        """
        ['quality_flag', 'preferred_timestamp', 'port_timestamp', 'lon', 'raw', 'internal_timestamp', 'time', 'lat', 'driver_timestamp']
        ['quality_flag', 'preferred_timestamp', 'temp', 'density', 'port_timestamp', 'lon', 'salinity', 'pressure', 'internal_timestamp', 'time', 'lat', 'driver_timestamp', 'conductivit
        
        {"driver_timestamp": 3564867147.743795, "pkt_format_id": "JSON_Data", "pkt_version": 1, "preferred_timestamp": "driver_timestamp", "quality_flag": "ok", "stream_name": "raw",
        "values": [{"binary": true, "value": "MzIuMzkxOSw5MS4wOTUxMiwgNzg0Ljg1MywgICA2LjE5OTQsIDE1MDUuMTc5LCAxOSBEZWMgMjAxMiwgMDA6NTI6Mjc=", "value_id": "raw"}]}', 'time': 1355878347.744123}
        
        {"driver_timestamp": 3564867147.743795, "pkt_format_id": "JSON_Data", "pkt_version": 1, "preferred_timestamp": "driver_timestamp", "quality_flag": "ok", "stream_name": "parsed",
        "values": [{"value": 32.3919, "value_id": "temp"}, {"value": 91.09512, "value_id": "conductivity"}, {"value": 784.853, "value_id": "pressure"}]}', 'time': 1355878347.744127}
        
        {'quality_flag': [u'ok'], 'preferred_timestamp': [u'driver_timestamp'], 'port_timestamp': [None], 'lon': [None], 'raw': ['-4.9733,16.02390, 539.527,   34.2719, 1506.862, 19 Dec 2012, 01:03:07'],
        'internal_timestamp': [None], 'time': [3564867788.0627117], 'lat': [None], 'driver_timestamp': [3564867788.0627117]}
        
        {'quality_flag': [u'ok'], 'preferred_timestamp': [u'driver_timestamp'], 'temp': [-4.9733], 'density': [None], 'port_timestamp': [None], 'lon': [None], 'salinity': [None], 'pressure': [539.527],
        'internal_timestamp': [None], 'time': [3564867788.0627117], 'lat': [None], 'driver_timestamp': [3564867788.0627117], 'conductivity': [16.0239]}
        """

        try:
            stream_def = self._stream_defs[stream_name]
            rdt = RecordDictionaryTool(stream_definition_id=stream_def)
            publisher = self._data_publishers[stream_name]
    
            buf_len = len(self._stream_buffers[stream_name])
            if buf_len == 0:
                return
            
            vals = []
            for x in range(buf_len):
                vals.append(self._stream_buffers[stream_name].pop())
    
            data_arrays = {}
            for x in rdt.fields:
                data_arrays[x] = [None for y in range(buf_len)]

            for i in range(buf_len):
                tomato = vals[i]
                for (tk, tv) in tomato.iteritems():
                    if tk == 'values':
                        for tval_dict in tv:
                            tval_id = tval_dict['value_id']
                            if tval_id in rdt:
                                tval_val = tval_dict['value']
                                if tval_dict.get('binary', None):
                                    tval_val = base64.b64decode(tval_val)
                                data_arrays[tval_id][i] = tval_val
                                                               
                    elif tk in rdt:
                        data_arrays[tk][i] = tv
                        if tk == 'driver_timestamp':
                            data_arrays['time'][i] = tv    
            
            for (k,v) in data_arrays.iteritems():
                rdt[k] = numpy.array(v)
            
            log.info('Outgoing granule: %s' % ['%s: %s'%(k,v) for k,v in rdt.iteritems()])
            g = rdt.to_granule(data_producer_id=self.resource_id)
            g.connection_id = self._connection_ID.hex
            g.connection_index = self._connection_index[stream_name]
            self._connection_index[stream_name] += 1
            
            publisher.publish(g)
            log.info('Instrument agent %s published data granule on stream %s.',
                self._proc_name, stream_name)
            log.info('Connection id: %s, connection index: %i.',
                     self._connection_ID.hex, self._connection_index[stream_name]-1)
            
        except:
            log.exception('Instrument agent %s could not publish data on stream %s.',
                self._proc_name, stream_name)
                         
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
                origin_type=self.ORIGIN_TYPE,
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
                origin_type=self.ORIGIN_TYPE,
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
            log.warning('Instrument agent %s error processing asynchronous agent event %s', self.id, val)


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
        self._fsm.add_handler(ResourceAgentState.IDLE, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
 
        # STOPPED state event handlers.
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.RESET, self._handler_stopped_reset)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.GO_INACTIVE, self._handler_stopped_go_inactive)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.RESUME, self._handler_stopped_resume)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.CLEAR, self._handler_stopped_clear)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.STOPPED, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
  
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
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
        
        # STREAMING state event handlers.
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.RESET, self._handler_streaming_reset)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GO_INACTIVE, self._handler_streaming_go_inactive)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
        
        # TEST state event handlers.
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.DONE, self._handler_done)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
        
        # CALIBRATE state event handlers.
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.DONE, self._handler_done)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
                
        # BUSY state event handlers.
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.DONE, self._handler_done)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)

        # DIRECT_ACCESS state event handlers.
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GO_COMMAND, self._handler_direct_access_go_command)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)

        # LOST_CONNECTION state event handlers.
        self._fsm.add_handler(ResourceAgentState.LOST_CONNECTION, ResourceAgentEvent.RESET, self._handler_lost_connection__reset)
        self._fsm.add_handler(ResourceAgentState.LOST_CONNECTION, ResourceAgentEvent.AUTORECONNECT, self._handler_lost_connection__autoreconnect)
        self._fsm.add_handler(ResourceAgentState.LOST_CONNECTION, ResourceAgentEvent.GO_INACTIVE, self._handler_lost_connection__go_inactive)
        self._fsm.add_handler(ResourceAgentState.LOST_CONNECTION, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.LOST_CONNECTION, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.LOST_CONNECTION, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)

        # ACTIVE_UNKNOWN state event handlers.
        self._fsm.add_handler(ResourceAgentState.ACTIVE_UNKNOWN, ResourceAgentEvent.GO_ACTIVE, self._handler_active_unknown_go_active)
        self._fsm.add_handler(ResourceAgentState.ACTIVE_UNKNOWN, ResourceAgentEvent.RESET, self._handler_active_unknown_reset)
        self._fsm.add_handler(ResourceAgentState.ACTIVE_UNKNOWN, ResourceAgentEvent.GO_INACTIVE, self._handler_active_unknown_go_inactive)
        self._fsm.add_handler(ResourceAgentState.ACTIVE_UNKNOWN, ResourceAgentEvent.GO_DIRECT_ACCESS, self._handler_active_unknown_go_direct_access)
        self._fsm.add_handler(ResourceAgentState.ACTIVE_UNKNOWN, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.ACTIVE_UNKNOWN, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.ACTIVE_UNKNOWN, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)

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

        self._construct_data_publishers()        
        self._start_publisher_greenlets()        
        log.info('Instrument agent %s started its driver.', self._proc_name)
        
    def _stop_driver(self):
        """
        Stop the driver process and driver client.
        """
        self._stop_publisher_greenlets()        
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
                    stream_def = stream_config['stream_definition_ref']
                    self._stream_defs[stream_name] = stream_def                
                    exchange_point = stream_config['exchange_point']
                    routing_key = stream_config['routing_key']
                    route = StreamRoute(exchange_point=exchange_point, routing_key=routing_key)
                    stream_id = stream_config['stream_id']
                    publisher = StreamPublisher(process=self, stream_id=stream_id, stream_route=route)
                    self._data_publishers[stream_name] = publisher
                    log.info("Instrument agent '%s' created publisher for stream_name "
                         "%s" % (self._proc_name, stream_name))
                    
                    stream_def
                    
                except:
                    log.error('Instrument agent %s failed to create publisher for stream %s.',
                              self._proc_name, stream_name)
                
                else:
                    self._stream_greenlets[stream_name] = None
                    self._stream_buffers[stream_name] = []
                    
                    pubfreq = stream_config.get('pubfreq', 0)
                    if not isinstance(pubfreq, int) or pubfreq <0:
                        log.error('pubfreq config for stream %s not valid',
                                  stream_name)
                    else:
                        self.aparam_pubfreq[stream_name] = pubfreq
                    
                    rdt = RecordDictionaryTool(stream_definition_id=stream_def)
                    self.aparam_streams[stream_name] = rdt.fields
                    
                    alarm_defs = stream_config.get('alarms',None)
                    if isinstance(alarm_defs, (list,tuple)):
                        alarms = []
                        for x in alarm_defs:
                            try:
                                type = x['type']
                                kwargs = x['kwargs']
                                a = IonObject(type,**kwargs)
                                alarms.append(a)
                            except:
                                log.error('Instrument agent %s failed to create alarm from def %s', str(x))
                    
                        if len(alarms) > 0:
                            params = ['set']
                            params.extend(alarms)
                            self.aparam_set_alarms(params)
                    
    def _start_publisher_greenlets(self):
        """
        """
        for stream in self._stream_greenlets.keys():
            self._stream_greenlets[stream] = gevent.spawn(self._pub_loop, stream)
            
    def _stop_publisher_greenlets(self):
        """
        """
        streams = self._stream_greenlets.keys()
        greenlets = self._stream_greenlets.values()
        gevent.killall(greenlets)
        gevent.joinall(greenlets)
        for stream in streams:
            self._stream_greenlets[stream] = None
            self._publish_stream_buffer(stream)
            
    def _pub_loop(self, stream):
        """
        """
        while True:
            pubfreq = self.aparam_pubfreq[stream]
            if pubfreq == 0:
                gevent.sleep(1)
            else:
                gevent.sleep(pubfreq)
                self._publish_stream_buffer(stream)
    
    ##############################################################
    # Agent parameter set functions.
    ##############################################################    
    
    def aparam_set_streams(self, params):
        """
        """
        return -1
    
    def aparam_set_pubfreq(self, params):
        """
        """
        if not isinstance(params, dict):
            return -1
        
        retval = 0
        for (k,v) in params.iteritems():
            if self.aparam_pubfreq.has_key(k) and isinstance(v, int) and v >= 0:
                self.aparam_pubfreq[k] = v
            else:
                retval = -1
                
        return retval

    def aparam_set_alarms(self, params):
        """
        """
        
        if not isinstance(params, (list,tuple)) or len(params)==0:
            return -1
        
        action = params[0]
        params = params[1:]
        
        if action not in ('set','add','remove','clear'):
            return -1
        
        if action in ('set', 'clear'):
            self.aparam_alarms = []
                
        if action in ('set', 'add'):
            for a in params:
                try:
                    a = construct_alarm_expression(a)
                    self.aparam_alarms.append(a)
                except:
                    log.error('Error constructing alarm.')
                    
        elif action == 'remove':
            new_alarms = copy.deepcopy(self.aparam_alarms)
            for a in params:
                if isinstance(a, str):
                    new_alarms = [x for x in new_alarms if x.name != a]
                elif isinstance(a, AlarmDef):
                    new_alarms = [x for x in new_alarms if not
                        (x.stream_name == a.stream_name and
                        x.value_id == a.value_id and
                        x.name == a.name)]
                else:
                    log.error('Attempted to remove an invalid alarm.')
                    
            self.aparam_alarms = new_alarms
        
        for a in self.aparam_alarms:
            log.info('Instrument agent %s has alarm: %s', self._proc_name, str(a))
                
        return len(self.aparam_alarms)
        
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
        self._dvr_client.cmd_dvr('execute_direct', data)

        
