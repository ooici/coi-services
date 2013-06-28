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
from pyon.agent.agent import ResourceAgent
from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentStreamStatus
from pyon.util.containers import get_ion_ts
from pyon.core.governance import ORG_MANAGER_ROLE, GovernanceHeaderValues, has_org_role, get_valid_resource_commitments
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE, OBSERVATORY_OPERATOR_ROLE
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
import copy

# Packages
import gevent

# ION imports.
from ion.agents.instrument.driver_process import DriverProcess
from ion.agents.instrument.common import BaseEnum
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessServer
from ion.agents.instrument.direct_access.direct_access_server import SessionCloseReasons
from ion.agents.agent_stream_publisher import AgentStreamPublisher
from ion.agents.agent_alert_manager import AgentAlertManager

# MI imports
from ion.core.includes.mi import DriverAsyncEvent
from interface.objects import StreamAlertType
from interface.objects import AgentCommand, DeviceStatusType, AggregateStatusType

class InstrumentAgentState(BaseEnum):
    POWERED_DOWN = ResourceAgentState.POWERED_DOWN
    UNINITIALIZED = ResourceAgentState.UNINITIALIZED
    INACTIVE = ResourceAgentState.INACTIVE
    IDLE = ResourceAgentState.IDLE
    STOPPED = ResourceAgentState.STOPPED
    COMMAND = ResourceAgentState.COMMAND
    STREAMING = ResourceAgentState.STREAMING
    TEST = ResourceAgentState.TEST
    CALIBRATE = ResourceAgentState.CALIBRATE
    BUSY = ResourceAgentState.BUSY
    LOST_CONNECTION = ResourceAgentState.LOST_CONNECTION
    ACTIVE_UNKNOWN = ResourceAgentState.ACTIVE_UNKNOWN

class InstrumentAgentEvent(BaseEnum):
    ENTER = ResourceAgentEvent.ENTER
    EXIT = ResourceAgentEvent.EXIT
    POWER_UP = ResourceAgentEvent.POWER_UP
    POWER_DOWN = ResourceAgentEvent.POWER_DOWN
    INITIALIZE = ResourceAgentEvent.INITIALIZE
    GO_ACTIVE = ResourceAgentEvent.GO_ACTIVE
    GO_INACTIVE = ResourceAgentEvent.GO_INACTIVE
    RUN = ResourceAgentEvent.RUN
    CLEAR = ResourceAgentEvent.CLEAR
    PAUSE = ResourceAgentEvent.PAUSE
    RESUME = ResourceAgentEvent.RESUME
    GO_COMMAND = ResourceAgentEvent.GO_COMMAND
    GO_DIRECT_ACCESS = ResourceAgentEvent.GO_DIRECT_ACCESS
    GET_RESOURCE = ResourceAgentEvent.GET_RESOURCE
    SET_RESOURCE = ResourceAgentEvent.SET_RESOURCE
    EXECUTE_RESOURCE = ResourceAgentEvent.EXECUTE_RESOURCE
    GET_RESOURCE_STATE = ResourceAgentEvent.GET_RESOURCE_STATE
    GET_RESOURCE_CAPABILITIES = ResourceAgentEvent.GET_RESOURCE_CAPABILITIES
    DONE = ResourceAgentEvent.DONE
    PING_RESOURCE = ResourceAgentEvent.PING_RESOURCE
    LOST_CONNECTION = ResourceAgentEvent.LOST_CONNECTION
    AUTORECONNECT = ResourceAgentEvent.AUTORECONNECT
    GET_RESOURCE_SCHEMA = ResourceAgentEvent.GET_RESOURCE_SCHEMA
    CHANGE_STATE_ASYNC = ResourceAgentEvent.CHANGE_STATE_ASYNC

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

from ion.agents.instrument.schema import get_schema

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
        super(InstrumentAgent, self).__init__(self, *args, **kwargs)
        
        ###############################################################################
        # Instrument agent internal parameters.
        ###############################################################################

        
        #This is the type of Resource managed by this agent
        self.resource_type = RT.InstrumentDevice
        
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

        # List of current alarm objects.
        self.aparam_alerts = []

        # The get/set helpers are set by the manager class.
        self.aparam_get_alerts = None
        self.aparam_set_alerts = None

        #list of the aggreate status states for this device
        self.aparam_aggstatus = {}
        
        # The set helpers are set by the manager class.
        # Set is read only. Use base class get function.
        self.aparam_set_aggstatus = None

        # Dictionary of stream fields.
        self.aparam_streams = {}
        
        # The set helper are set by the manager class (read only).
        # We use the default base class get function.
        self.aparam_set_streams = None

        # Dictionary of stream publication rates.
        self.aparam_pubrate = {}

        # The set helper is set by the manager class.
        # Use default base class get function.
        self.aparam_set_pubrate = None

        # Autoreconnect thread.
        self._autoreconnect_greenlet = None

        # State when lost.
        self._state_when_lost = None

        # Agent stream publisher.
        self._asp = None

        # Agent alert manager.
        self._aam = None

        # Agent schema.
        # Loaded with driver start/stop.
        self._resource_schema = {}
        
        # Resource schema.
        self._agent_schema = get_schema()

        # Default initial state.
        self._initial_state = ResourceAgentState.UNINITIALIZED

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

        # Set up streams.
        self._asp = AgentStreamPublisher(self)        
        self._agent_schema['streams'] = copy.deepcopy(self.aparam_streams)
        
        # Set up alert manager.
        self._aam = AgentAlertManager(self)

        # Superclass on_init attemtps state restore.
        super(InstrumentAgent, self).on_init()        

    def on_quit(self):
        """
        """
        super(InstrumentAgent, self).on_quit()

        self._aam.stop_all()
        
        params = {}
        for (k,v) in self.aparam_pubrate.iteritems():
            if v > 0:
                params[k] = 0
        
        if len(params)>0:
            self.aparam_set_pubrate(params)
                
        state = self._fsm.get_current_state()
        if state == ResourceAgentState.UNINITIALIZED:
            pass

        elif state == ResourceAgentState.INACTIVE:
            result = self._stop_driver()

        else:
            self._dvr_client.cmd_dvr('disconnect')
            self._dvr_client.cmd_dvr('initialize')        
            result = self._stop_driver()
            
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

    def check_if_direct_access_mode(self, message, headers):
        try:
            #Putting in a hack for testing
            if headers.has_key('test_for_proc_name'):
                test_for_proc_name = headers['test_for_proc_name']
                if test_for_proc_name != self._proc_name:
                    return False, "This is not the correct agent (%s != %s)" % (test_for_proc_name, self._proc_name)

            state = self._fsm.get_current_state()
            if state == ResourceAgentState.DIRECT_ACCESS:
                return False, "This operation is unavailable while the agent is in the Direct Access state"
        except Exception, e:
            log.warning("Could not determine the state of the agent:", e.message)

        return True, ''

    def check_resource_operation_policy(self, process, message, headers):
        '''
        Inst Operators must have a shared commitment to call set_resource(), execute_resource() or ping_resource()
        Org Managers and Observatory Operators do not have to have a commitment to call set_resource(), execute_resource() or ping_resource()
        However, an actor cannot call these if someone else has an exclusive commitment
        Agent policy is fully documented on confluence
        @param msg:
        @param headers:
        @return:
        '''


        try:
            gov_values = GovernanceHeaderValues(headers, resource_id_required=False)
        except Inconsistent, ex:
            return False, ex.message

        log.debug("check_resource_operation_policy: actor info: %s %s %s", gov_values.actor_id, gov_values.actor_roles, gov_values.resource_id)

        resource_name = process.resource_type if process.resource_type is not None else process.name

        coms = get_valid_resource_commitments(gov_values.resource_id)
        if coms is None:
            log.debug('commitments: None')
        else:
            log.debug('commitment count: %d', len(coms))

        if coms is None and has_org_role(gov_values.actor_roles ,self._get_process_org_governance_name(),
            [ORG_MANAGER_ROLE, OBSERVATORY_OPERATOR_ROLE]):
            return True, ''

        if not has_org_role(gov_values.actor_roles ,self._get_process_org_governance_name(),
            [ORG_MANAGER_ROLE, OBSERVATORY_OPERATOR_ROLE, INSTRUMENT_OPERATOR_ROLE]):
            return False, '%s(%s) has been denied since the user %s does not have the %s role for Org %s'\
                          % (resource_name, gov_values.op, gov_values.actor_id, INSTRUMENT_OPERATOR_ROLE,
                             self._get_process_org_governance_name())

        if coms is None:
            return False, '%s(%s) has been denied since the user %s has not acquired the resource %s'\
                          % (resource_name, gov_values.op, gov_values.actor_id,
                             self.resource_id)


        actor_has_shared_commitment = False

        #Iterrate over commitments and look to see if actor or others have an exclusive access
        for com in coms:
            log.debug("checking commitments: actor_id: %s exclusive: %s",com.consumer,  str(com.commitment.exclusive))

            if com.consumer == gov_values.actor_id:
                actor_has_shared_commitment = True

            if com.commitment.exclusive and com.consumer == gov_values.actor_id:
                return True, ''

            if com.commitment.exclusive and com.consumer != gov_values.actor_id:
                return False, '%s(%s) has been denied since another user %s has acquired the resource exclusively' % (resource_name, gov_values.op, com.consumer)



        if not actor_has_shared_commitment and has_org_role(gov_values.actor_roles ,self._get_process_org_governance_name(), INSTRUMENT_OPERATOR_ROLE):
            return False, '%s(%s) has been denied since the user %s does not have the %s role for Org %s'\
                          % (resource_name, gov_values.op, gov_values.actor_id, INSTRUMENT_OPERATOR_ROLE,
                             self._get_process_org_governance_name())

        return True, ''

    def check_agent_operation_policy(self, process, message, headers):
        """
        Inst Operators must have an exclusive commitment to call set_agent(), execute_agent() or ping_agent()
        Org Managers and Observatory Operators do not have to have a commitment to call set_agent(), execute_agent() or ping_agent()
        However, an actor cannot call these if someone else has an exclusive commitment
        Agent policy is fully documented on confluence

        @param process:
        @param message:
        @param headers:
        @return:
        """
        try:
            gov_values = GovernanceHeaderValues(headers=headers, process=process)
        except Inconsistent, ex:
            return False, ex.message

        log.debug("check_agent_operation_policy: actor info: %s %s %s", gov_values.actor_id, gov_values.actor_roles, gov_values.resource_id)

        resource_name = process.resource_type if process.resource_type is not None else process.name

        coms = get_valid_resource_commitments(gov_values.resource_id)
        if coms is None:
            log.debug('commitments: None')
        else:
            log.debug('commitment count: %d', len(coms))

        if coms is None and has_org_role(gov_values.actor_roles ,self._get_process_org_governance_name(),
            [ORG_MANAGER_ROLE, OBSERVATORY_OPERATOR_ROLE]):
            return True, ''

        if coms is None and has_org_role(gov_values.actor_roles ,self._get_process_org_governance_name(), INSTRUMENT_OPERATOR_ROLE):
            return False, '%s(%s) has been denied since the user %s has not acquired the resource exclusively' % (resource_name, gov_values.op, gov_values.actor_id)

        #TODO - this commitment might not be with the right Org - may have to relook at how this is working in R3.
        #Iterrate over commitments and look to see if actor or others have an exclusive access
        for com in coms:

            log.debug("checking commitments: actor_id: %s exclusive: %s",com.consumer,  str(com.commitment.exclusive))
            if com.commitment.exclusive and com.consumer == gov_values.actor_id:
                return True, ''

            if com.commitment.exclusive and com.consumer != gov_values.actor_id:
                return False, '%s(%s) has been denied since another user %s has acquired the resource exclusively' % (resource_name, gov_values.op, com.consumer)

        if has_org_role(gov_values.actor_roles ,self._get_process_org_governance_name(), [ORG_MANAGER_ROLE, OBSERVATORY_OPERATOR_ROLE]):
            return True, ''

        return False, '%s(%s) has been denied since the user %s has not acquired the resource exclusively' % (resource_name, gov_values.op, gov_values.actor_id)

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

    def _handler_state_change_async(self, *args, **kwargs):
        try:
            next_state = args[0]
        except KeyError:
            log.error('_handler_state_change_async did not recieve required state parameter.')
            return (None, None)
        
        if next_state not in [
            InstrumentAgentState.CALIBRATE,
            InstrumentAgentState.BUSY,
            InstrumentAgentState.COMMAND,
            InstrumentAgentState.STREAMING,
            InstrumentAgentState.TEST]:
            log.error('_handler_state_change_async: invalid async state change: %s', next_state)
            next_state = None
        return (next_state, None)

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
        self._asp.reset_connection()

        resource_schema = self._dvr_client.cmd_dvr('get_config_metadata')
        if isinstance(resource_schema, str):
            resource_schema = json.loads(resource_schema)
            if isinstance(resource_schema, dict):
                self._resource_schema = resource_schema
            else:
                self._resource_schema = {}                    
        else:
            self._resource_schema = {}

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
        log.error("Instrument agent requested to stop direct access mode - %s",
                 self._da_session_close_reason)

        # Stop the DA server if it is still running.  This will
        # only be the case if operator explicitly calls
        # GO_COMMAND.
        if (self._da_server):
            self._da_server.stop()
            self._da_server = None

            # re-set the 'reason' to be the default
            self._da_session_close_reason = 'due to ION request'

        # tell driver to stop direct access mode
        next_state, _ = self._dvr_client.cmd_dvr('stop_direct')
        log.error("_handler_direct_access_go_command: next agent state: %s", next_state)

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
            self._asp.reset_connection()

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
        """
        Route async event received from driver.
        """
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
        Publish driver state change event.
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
        Publsih resource config change event and update persisted info.
        """
        self._set_state('rparams', val)
        self._flush_state()
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
        Publish sample on sample data streams.
        """

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
            
        u'quality_flag': u'ok',
        u'preferred_timestamp': u'port_timestamp',
        u'stream_name': u'raw',
        u'port_timestamp': 3575139438.357514,
        u'pkt_format_id': u'JSON_Data',
        u'pkt_version': 1,
        u'values': [
            {u'binary': True, u'value_id': u'raw', u'value': u'aABlAGEAcgB0AGIAZQBhAHQAXwBpAG4AdABlAHIAdgBhAGwAIAAwAA=='},
            {u'value_id': u'length', u'value': 40},
            {u'value_id': u'type', u'value': 1},
            {u'value_id': u'checksum', u'value': None}
            ],
        u'driver_timestamp': 3575139438.206242       
        """
        
        # If the sample event is encoded, load it back to a dict.
        if isinstance(val, str):
            val = json.loads(val)

        self._asp.on_sample(val)
        try:
            stream_name = val['stream_name']
            values = val['values']
            for v in values:
                value = v['value']
                value_id = v['value_id']
                self._aam.process_alerts(stream_name=stream_name,
                                         value=value, value_id=value_id)
        except Exception as ex:
            log.error('Insturment agent %s could not process alerts for driver tomato %s',
                      self._proc_name, str(val))
                         
                         
    def _async_driver_event_error(self, val, ts):
        """
        Publish async driver error.
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
        Publish async driver result.
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
        Send async DA event.
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
        Driver initiated agent FSM event.
        """
        try:
            if isinstance(val,str):
                agt_evt = val
                args = []
                kwargs = {}
            elif isinstance(val, dict):
                agt_evt = val['event']
                args = val.get('args',[])
                kwargs = val.get('kwargs',{})
            else:
                raise Exception('Invalid async agent event type.')
                
            self._fsm.on_event(agt_evt, *args, **kwargs)
            
        except:
            log.warning('Instrument agent %s error processing asynchronous agent event %s', self.id, str(val))


    ##############################################################
    # FSM setup.
    ##############################################################    

    def _construct_fsm(self):
        """
        Setup instrument agent FSM.
        """
        # Here we could define subsets of states and events to specialize fsm.
        
        # Construct default state machine states and handlers.
        super(InstrumentAgent, self)._construct_fsm()
        
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
        self._fsm.add_handler(ResourceAgentState.COMMAND, ResourceAgentEvent.CHANGE_STATE_ASYNC, self._handler_state_change_async)
        
        # STREAMING state event handlers.
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.RESET, self._handler_streaming_reset)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GO_INACTIVE, self._handler_streaming_go_inactive)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
        self._fsm.add_handler(ResourceAgentState.STREAMING, ResourceAgentEvent.CHANGE_STATE_ASYNC, self._handler_state_change_async)
        
        # TEST state event handlers.
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.DONE, self._handler_done)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
        self._fsm.add_handler(ResourceAgentState.TEST, ResourceAgentEvent.CHANGE_STATE_ASYNC, self._handler_state_change_async)
        
        # CALIBRATE state event handlers.
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.DONE, self._handler_done)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
        self._fsm.add_handler(ResourceAgentState.CALIBRATE, ResourceAgentEvent.CHANGE_STATE_ASYNC, self._handler_state_change_async)
                
        # BUSY state event handlers.
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.DONE, self._handler_done)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)
        self._fsm.add_handler(ResourceAgentState.BUSY, ResourceAgentEvent.CHANGE_STATE_ASYNC, self._handler_state_change_async)

        # DIRECT_ACCESS state event handlers.
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GO_COMMAND, self._handler_direct_access_go_command)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.DIRECT_ACCESS, ResourceAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)

        # LOST_CONNECTION state event handlers.
        self._fsm.add_handler(ResourceAgentState.LOST_CONNECTION, ResourceAgentEvent.ENTER, self._handler_lost_connection_enter)
        self._fsm.add_handler(ResourceAgentState.LOST_CONNECTION, ResourceAgentEvent.EXIT, self._handler_lost_connection_exit)
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

        self._dvr_proc = DriverProcess.get_process(dvr_config, self._test_mode)
        self._dvr_proc.launch()

        # Verify the driver has started.
        if not self._dvr_proc.getpid():
            log.error('Instrument agent %s error starting driver process.', self._proc_name)
            self._dvr_proc = None
            raise ResourceError('Error starting driver process.')

        try:
            driver_client = self._dvr_proc.get_client()
            driver_client.start_messaging(self.evt_recv)
            retval = driver_client.cmd_dvr('process_echo', 'Test.')
            startup_config = dvr_config.get('startup_config',None)
            if startup_config:
                retval = driver_client.cmd_dvr('set_init_params', startup_config)
            self._dvr_client = driver_client

        except Exception, e:
            self._dvr_proc.stop()
            self._dvr_proc = None
            self._dvr_client = None
            log.error('Instrument agent %s error starting driver client. %s', self._proc_name, e)
            raise ResourceError('Error starting driver client.')

        log.info('Instrument agent %s started its driver.', self._proc_name)
        
    def _stop_driver(self):
        """
        Stop the driver process and driver client.
        """
            
        if self._dvr_proc:
            self._dvr_proc.stop()
            self._dvr_proc = None
            self._dvr_client = None
            self._resource_schema = {}
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
    # Agent parameter functions.
    ##############################################################    
    
    def _configure_aparams(self, aparams=[]):
        """
        Configure aparams from agent config values.
        """
        # If specified and configed, build the pubrate aparam.
        aparam_pubrate_config = self.CFG.get('aparam_pubrate_config', None)
        if aparam_pubrate_config and 'pubrate' in aparams:
            self.aparam_set_pubrate(aparam_pubrate_config)

        # If specified and configed, build the alerts aparam.                
        aparam_alerts_config = self.CFG.get('aparam_alerts_config', None)
        print str(aparam_alerts_config)
        if aparam_alerts_config and 'alerts' in aparams:
            self.aparam_set_alerts(aparam_alerts_config)
                
    def _restore_resource(self, state, prev_state):
        """
        Restore agent/resource configuration and state.
        """
        if not self._dvr_config:
            log.error('Instrument agent %s error no driver config on launch, cannot restore state.',
                      self.id)
            return
                
        # Get resource parameters and agent state from persistence.
        # Enable this when new eggs have read-only startup parameters ready.
        
        rparams = self._get_state('rparams')
        log.info('restoring rparams: %s', str(rparams))
        if rparams:
            startup_config = self._dvr_config.get('startup_config', None)
            if not startup_config:
                startup_config = {'parameters':rparams, 'scheduler':None}
            startup_config['parameters'] = rparams
            self._dvr_config['startup_config'] = startup_config
        
        
        # Get state to restore. If the last state was lost connection,
        # use the prior connected state.
        if not state:
            return
        
        if state == ResourceAgentState.LOST_CONNECTION:        
            state = prev_state
        
        try:
            cur_state = self._fsm.get_current_state()
            
            # If unitialized, confirm and do nothing.
            if state == ResourceAgentState.UNINITIALIZED:
                if cur_state != state:
                    raise Exception()
                
            # If inactive, initialize and confirm.
            elif state == ResourceAgentState.INACTIVE:
                self._fsm.on_event(ResourceAgentEvent.INITIALIZE)
                cur_state = self._fsm.get_current_state()
                if cur_state != state:
                    raise Exception()

            # If idle, initialize, activate and confirm.
            elif state == ResourceAgentState.IDLE:
                self._fsm.on_event(ResourceAgentEvent.INITIALIZE)
                self._fsm.on_event(ResourceAgentEvent.GO_ACTIVE)
                cur_state = self._fsm.get_current_state()
                if cur_state != state:
                    raise Exception()

            # If streaming, initialize, activate and confirm.
            # Driver discover should put us in streaming mode.
            elif state == ResourceAgentState.STREAMING:
                self._fsm.on_event(ResourceAgentEvent.INITIALIZE)
                self._fsm.on_event(ResourceAgentEvent.GO_ACTIVE)
                cur_state = self._fsm.get_current_state()
                if cur_state != state:
                    raise Exception()

            # If command, initialize, activate, confirm idle,
            # run and confirm command.
            elif state == ResourceAgentState.COMMAND:
                self._fsm.on_event(ResourceAgentEvent.INITIALIZE)
                self._fsm.on_event(ResourceAgentEvent.GO_ACTIVE)
                cur_state = self._fsm.get_current_state()
                if cur_state != ResourceAgentState.IDLE:
                    raise Exception()
                self._fsm.on_event(ResourceAgentEvent.RUN)
                cur_state = self._fsm.get_current_state()
                if cur_state != state:
                    raise Exception()

            # If paused, initialize, activate, confirm idle,
            # run, confirm command, pause and confirm stopped.
            elif state == ResourceAgentState.STOPPED:
                self._fsm.on_event(ResourceAgentEvent.INITIALIZE)
                self._fsm.on_event(ResourceAgentEvent.GO_ACTIVE)
                cur_state = self._fsm.get_current_state()
                if cur_state != ResourceAgentState.IDLE:
                    raise Exception()
                self._fsm.on_event(ResourceAgentEvent.RUN)
                cur_state = self._fsm.get_current_state()
                if cur_state != ResourceAgentState.COMMAND:
                    raise Exception()
                self._fsm.on_event(ResourceAgentEvent.PAUSE)
                cur_state = self._fsm.get_current_state()
                if cur_state != state:
                    raise Exception()

            # If in a command reachable substate, attempt to return to command.
            # Initialize, activate, confirm idle, run confirm command.
            elif state in [ResourceAgentState.TEST,
                    ResourceAgentState.CALIBRATE,
                    ResourceAgentState.DIRECT_ACCESS,
                    ResourceAgentState.BUSY]:
                self._fsm.on_event(ResourceAgentEvent.INITIALIZE)
                self._fsm.on_event(ResourceAgentEvent.GO_ACTIVE)
                cur_state = self._fsm.get_current_state()
                if cur_state != ResourceAgentState.IDLE:
                    raise Exception()
                self._fsm.on_event(ResourceAgentEvent.RUN)
                cur_state = self._fsm.get_current_state()
                if cur_state != ResourceAgentState.COMMAND:
                    raise Exception()

            # If active unknown, return to active unknown or command if
            # possible. Initialize, activate, confirm active unknown, else
            # confirm idle, run, confirm command.
            elif state == ResourceAgentState.ACTIVE_UNKNOWN:
                self._fsm.on_event(ResourceAgentEvent.INITIALIZE)
                self._fsm.on_event(ResourceAgentEvent.GO_ACTIVE)
                cur_state = self._fsm.get_current_state()
                if cur_state == ResourceAgentState.ACTIVE_UNKNOWN:
                    return
                elif cur_state != ResourceAgentState.IDLE:
                    raise Exception()
                self._fsm.on_event(ResourceAgentEvent.RUN)
                cur_state = self._fsm.get_current_state()
                if cur_state != ResourceAgentState.COMMAND:
                    raise Exception()

            else:
                log.error('Instrument agent %s error restoring unhandled state %s, current state %s.',
                        self.id, state, cur_state)
        
        except Exception as ex:
            log.error('Instrument agent %s error restoring state %s, current state %s, exception %s.',
                    self.id, state, cur_state, str(ex))
            
        else:
            log.info('Instrument agent %s restored state %s = %s.',
                     self.id, state, cur_state)
    
    ##############################################################
    # On state enter, on command, on command error handlers.
    ##############################################################    

    def _on_state_enter(self, state):
        self._aam.process_alerts(state=state)

    def _on_command_error(self, cmd, execute_cmd, args, kwargs, ex):
        self._aam.process_alerts(command=execute_cmd, command_success=False)
        super(InstrumentAgent, self)._on_command_error(cmd, execute_cmd, args,
                                                       kwargs, ex)
        
    ###############################################################################
    # Event callback and handling for direct access.
    ###############################################################################
    
    def _da_server_input_processor(self, data):
        """
        Callback passed to DA Server for receiving input from server.
        """
        if self._da_server == None:
            # This might be the case when data is sent to the DA sever right before
            # the client is disconnected.  You will see this in the logs when the
            # disconnect comes before the execute direct command.
            log.warn('No DA session started. Bytes not sent to driver: "%s"', data)

        elif isinstance(data, int):
            log.warning("Stopping DA Server")
            # stop DA server
            self._da_server.stop()
            self._da_server = None

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

        else:
            log.debug("InstAgent.telnetInputProcessor: data = <" + str(data) + "> len=" + str(len(data)))
            # send the data to the driver
            self._dvr_client.cmd_dvr('execute_direct', data)

