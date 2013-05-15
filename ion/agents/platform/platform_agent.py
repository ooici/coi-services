#!/usr/bin/env python

"""
@package ion.agents.platform.platform_agent
@file    ion/agents/platform/platform_agent.py
@author  Carlos Rueda
@brief   Supporting types for platform agents.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log, RT
from pyon.ion.stream import StreamPublisher
from pyon.agent.agent import ResourceAgent
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from interface.objects import AgentCommand
from pyon.agent.agent import ResourceAgentClient

from pyon.event.event import EventSubscriber

from pyon.core.exception import NotFound, Inconsistent

from pyon.core.governance import ORG_MANAGER_ROLE, GovernanceHeaderValues, has_org_role, get_valid_resource_commitments, ION_MANAGER
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE, OBSERVATORY_OPERATOR_ROLE


from ion.agents.platform.exceptions import PlatformException, PlatformConfigurationException
from ion.agents.platform.platform_driver_event import AttributeValueDriverEvent
from ion.agents.platform.platform_driver_event import ExternalEventDriverEvent
from ion.agents.platform.platform_driver_event import StateChangeDriverEvent
from ion.agents.platform.platform_driver_event import AsyncAgentEvent
from ion.agents.platform.exceptions import CannotInstantiateDriverException
from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.agent_alert_manager import AgentAlertManager

from ion.agents.platform.platform_driver import PlatformDriverEvent, PlatformDriverState

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
import numpy

from pyon.util.containers import DotDict

from pyon.agent.common import BaseEnum
from pyon.agent.instrument_fsm import FSMStateError

from ion.agents.platform.launcher import Launcher

from ion.agents.platform.platform_resource_monitor import PlatformResourceMonitor

from coverage_model.parameter import ParameterDictionary
from interface.objects import StreamRoute

from ion.agents.platform.status_manager import StatusManager

import logging
import time
from gevent import Greenlet
from gevent import sleep
from gevent import spawn
from gevent.event import AsyncResult

import pprint


PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


class PlatformAgentState(BaseEnum):
    """
    Platform agent state enum.
    """
    UNINITIALIZED     = ResourceAgentState.UNINITIALIZED
    INACTIVE          = ResourceAgentState.INACTIVE
    IDLE              = ResourceAgentState.IDLE
    STOPPED           = ResourceAgentState.STOPPED
    COMMAND           = ResourceAgentState.COMMAND
    MONITORING        = 'PLATFORM_AGENT_STATE_MONITORING'
    LAUNCHING         = 'PLATFORM_AGENT_STATE_LAUNCHING'
    LOST_CONNECTION   = ResourceAgentState.LOST_CONNECTION


class PlatformAgentEvent(BaseEnum):
    ENTER                     = ResourceAgentEvent.ENTER
    EXIT                      = ResourceAgentEvent.EXIT

    INITIALIZE                = ResourceAgentEvent.INITIALIZE
    RESET                     = ResourceAgentEvent.RESET
    GO_ACTIVE                 = ResourceAgentEvent.GO_ACTIVE
    GO_INACTIVE               = ResourceAgentEvent.GO_INACTIVE
    RUN                       = ResourceAgentEvent.RUN
    SHUTDOWN                  = 'PLATFORM_AGENT_SHUTDOWN'

    CLEAR                     = ResourceAgentEvent.CLEAR
    PAUSE                     = ResourceAgentEvent.PAUSE
    RESUME                    = ResourceAgentEvent.RESUME

    GET_RESOURCE_CAPABILITIES = ResourceAgentEvent.GET_RESOURCE_CAPABILITIES
    PING_RESOURCE             = ResourceAgentEvent.PING_RESOURCE
    GET_RESOURCE              = ResourceAgentEvent.GET_RESOURCE
    SET_RESOURCE              = ResourceAgentEvent.SET_RESOURCE
    EXECUTE_RESOURCE          = ResourceAgentEvent.EXECUTE_RESOURCE
    GET_RESOURCE_STATE        = ResourceAgentEvent.GET_RESOURCE_STATE

    START_MONITORING          = 'PLATFORM_AGENT_START_MONITORING'
    STOP_MONITORING           = 'PLATFORM_AGENT_STOP_MONITORING'
    LAUNCH_COMPLETE           = 'PLATFORM_AGENT_LAUNCH_COMPLETE'

    LOST_CONNECTION           = ResourceAgentEvent.LOST_CONNECTION
    AUTORECONNECT             = ResourceAgentEvent.AUTORECONNECT


class PlatformAgentCapability(BaseEnum):
    INITIALIZE                = PlatformAgentEvent.INITIALIZE
    RESET                     = PlatformAgentEvent.RESET
    GO_ACTIVE                 = PlatformAgentEvent.GO_ACTIVE
    GO_INACTIVE               = PlatformAgentEvent.GO_INACTIVE
    RUN                       = PlatformAgentEvent.RUN
    SHUTDOWN                  = PlatformAgentEvent.SHUTDOWN

    CLEAR                     = PlatformAgentEvent.CLEAR
    PAUSE                     = PlatformAgentEvent.PAUSE
    RESUME                    = PlatformAgentEvent.RESUME

    GET_RESOURCE_CAPABILITIES = PlatformAgentEvent.GET_RESOURCE_CAPABILITIES
    PING_RESOURCE             = PlatformAgentEvent.PING_RESOURCE
    GET_RESOURCE              = PlatformAgentEvent.GET_RESOURCE
    SET_RESOURCE              = PlatformAgentEvent.SET_RESOURCE
    EXECUTE_RESOURCE          = PlatformAgentEvent.EXECUTE_RESOURCE
    GET_RESOURCE_STATE        = PlatformAgentEvent.GET_RESOURCE_STATE

    START_MONITORING          = PlatformAgentEvent.START_MONITORING
    STOP_MONITORING           = PlatformAgentEvent.STOP_MONITORING


class PlatformAgentAlertManager(AgentAlertManager):
    """
    Overwrites _update_aggstatus and do appropriate handling.
    """
    def _update_aggstatus(self, aggregate_type, new_status):
        """
        Called to set a new status value for an aggstatus type.
        Note that an update to an aggstatus in a platform does not trigger
        any event publication by itself. Rather, this update may cascade to
        an update of the corresponding rollup_status, in which case an event
        does get published. StatusManager takes care of that handling.
        """
        log.debug("%r: _update_aggstatus called", self._agent._platform_id)
        self._agent._status_manager.set_aggstatus(aggregate_type, new_status)


class PlatformAgent(ResourceAgent):
    """
    Platform resource agent.
    """

    # Override to publish specific types of events
    COMMAND_EVENT_TYPE = "DeviceCommandEvent" #TODO how this works?

    # Override to set specific origin type
    ORIGIN_TYPE = "PlatformDevice"

    def __init__(self):
        log.info("PlatformAgent constructor called")
        ResourceAgent.__init__(self)

        #This is the type of Resource managed by this agent
        self.resource_type = RT.PlatformDevice
        self.resource_id = None

        #########################################
        # <platform configuration and dependent elements>
        self._plat_config = None
        self._plat_config_processed = False

        # my platform ID
        self._platform_id = None

        # platform ID of my parent, if any. -mainly for diagnostic purposes
        self._parent_platform_id = None

        self._network_definition = None  # NetworkDefinition object
        self._pnode = None  # PlatformNode object corresponding to this platform

        self._children_resource_ids = None

        # </platform configuration>
        #########################################

        self._driver_config = None
        self._plat_driver = None

        # PlatformResourceMonitor
        self._platform_resource_monitor = None

        # Dictionaries used for data publishing. Constructed in _initialize_this_platform
        self._data_streams = {}
        self._param_dicts = {}
        self._stream_defs = {}
        self._data_publishers = {}

        # Set of parameter names received in event notification but not
        # configured. Allows to log corresponding warning only once.
        self._unconfigured_params = set()

        # {subplatform_id: DotDict(ResourceAgentClient, PID), ...}
        self._pa_clients = {}  # Never None

        # see on_init
        self._launcher = None

        # {instrument_id: DotDict(ResourceAgentClient, PID), ...}
        self._ia_clients = {}  # Never None

        # self.CFG.endpoint.receive.timeout -- see on_init
        self._timeout = 160

        # to sync in _initialize
        self._async_children_launched = None

        # Default initial state.
        #self._initial_state = PlatformAgentState.UNINITIALIZED
        self._initial_state = PlatformAgentState.LAUNCHING

        # List of current alarm objects.
        self.aparam_alerts = []

        # The get/set helpers are set by AgentAlertManager.
        self.aparam_get_alerts = None
        self.aparam_set_alerts = None

        # dict of aggregate statuses for the platform itself (depending on its
        # own attributes and ports)
        self.aparam_aggstatus = {}

        # Agent alert manager.
        self._aam = None

        # dict of aggregate statuses for all descendants (immediate children
        # and all their descendants)
        self.aparam_child_agg_status = {}

        # Dict with consolidation of both aggregate statuses above
        self.aparam_rollup_status = {}

        # StatusManager created on_start after the configuration has been
        # verified. This object does all main status management including
        # event subscribers, and helps with related publications.
        self._status_manager = None

        #####################################
        # lost_connection handling:

        # Autoreconnect thread.
        self._autoreconnect_greenlet = None

        # State when lost.
        self._state_when_lost = None

        log.info("PlatformAgent constructor complete.")

        # for debugging purposes
        self._pp = pprint.PrettyPrinter()

    def on_init(self):
        super(PlatformAgent, self).on_init()
        log.trace("on_init")

        self._timeout = self.CFG.get_safe("endpoint.receive.timeout", self._timeout)
        self._plat_config = self.CFG.get("platform_config", None)
        self._plat_config_processed = False

        self._launcher = Launcher(self._timeout)

        if log.isEnabledFor(logging.DEBUG):  # pragma: no cover
            platform_id = self.CFG.get_safe('platform_config.platform_id', '')
            log.debug("%r: self._timeout = %s", platform_id, self._timeout)
            outname = "logs/platform_CFG_received_%s.txt" % platform_id
            try:
                pprint.PrettyPrinter(stream=file(outname, "w")).pprint(self.CFG)
                log.debug("%r: on_init: CFG printed to %s", platform_id, outname)
            except Exception as e:
                log.warn("%r: on_init: error printing CFG to %s: %s", platform_id, outname, e)

    def on_start(self):
        """
        - Validates the given configuration and does related preparations.
        - creates AgentAlertManager and StatusManager
        - Children agents (platforms and instruments) are launched here (in a
          separate greenlet).
        """
        super(PlatformAgent, self).on_start()
        log.info('platform agent is running: on_start called.')

        self._validate_configuration()

        # Set up alert manager.
        self._aam = PlatformAgentAlertManager(self)

        # create StatusManager now that we have all needed elements:
        self._status_manager = StatusManager(self)

        self._async_children_launched = AsyncResult()

        # NOTE: the launch of the children is only started here via a greenlet
        # so we can return from this method quickly.
        log.info("%r: starting _children_launch", self._platform_id)
        Greenlet(self._children_launch).start()
        log.info("%r: started _children_launch", self._platform_id)

    def _validate_configuration(self):
        """
        Does verifications and preparations dependent on self.CFG.

        @raises PlatformException if the verification fails for some reason.
        """

        if not self._plat_config:
            msg = "'platform_config' entry not provided in agent configuration"
            log.error(msg)
            raise PlatformException(msg)

        if self._plat_config_processed:
            # nothing else to do here
            return

        log.debug("verifying/processing _plat_config ...")

        self._driver_config = self.CFG.get('driver_config', None)
        if None is self._driver_config:
            msg = "'driver_config' key not in configuration"
            log.error(msg)
            raise PlatformException(msg)

        log.debug("driver_config: %s", self._driver_config)

        for k in ['platform_id']:
            if not k in self._plat_config:
                msg = "'%s' key not given in configuration" % k
                log.error(msg)
                raise PlatformException(msg)

        self._platform_id = self._plat_config['platform_id']

        for k in ['dvr_mod', 'dvr_cls']:
            if not k in self._driver_config:
                msg = "%r: '%s' key not given in driver_config: %s" % (
                    self._platform_id, k, self._driver_config)
                log.error(msg)
                raise PlatformException(msg)

        # Create network definition from the provided CFG:
        self._network_definition = NetworkUtil.create_network_definition_from_ci_config(self.CFG)
        log.debug("%r: created network_definition from CFG", self._platform_id)

        # verify the given platform_id is contained in the NetworkDefinition:
        if not self._platform_id in self._network_definition.pnodes:
            msg = "%r: this platform_id not found in network definition." % self._platform_id
            log.error(msg)
            raise PlatformException(msg)

        # get PlatformNode corresponding to this agent:
        self._pnode = self._network_definition.pnodes[self._platform_id]
        assert self._pnode.platform_id == self._platform_id

        self._children_resource_ids = self._get_children_resource_ids()

        #
        # set platform attributes:
        #
        if 'attributes' in self._driver_config:
            attrs = self._driver_config['attributes']
            self._platform_attributes = attrs
            log.debug("%r: platform attributes taken from driver_config: %s",
                      self._platform_id, self._platform_attributes)
        else:
            self._platform_attributes = dict((attr.attr_id, attr.defn) for attr
                                             in self._pnode.attrs.itervalues())
            log.warn("%r: platform attributes taken from network definition: %s",
                     self._platform_id, self._platform_attributes)

        #
        # set platform ports:
        # TODO the ports may probably be applicable only in particular
        # drivers (like in RSN), so move these there if that's the case.
        #
        if 'ports' in self._driver_config:
            ports = self._driver_config['ports']
            self._platform_ports = ports
            log.debug("%r: platform ports taken from driver_config: %s",
                      self._platform_id, self._platform_ports)
        else:
            self._platform_ports = {}
            for port_id, port in self._pnode.ports.iteritems():
                self._platform_ports[port_id] = dict(port_id=port_id,
                                                     network=port.network)
            log.warn("%r: platform ports taken from network definition: %s",
                     self._platform_id, self._platform_ports)

        ppid = self._plat_config.get('parent_platform_id', None)
        if ppid:
            self._parent_platform_id = ppid
            log.debug("_parent_platform_id set to: %s", self._parent_platform_id)

        self._plat_config_processed = True

        log.debug("%r: _validate_configuration complete",  self._platform_id)

    def _children_launch(self):
        """
        Launches the sub-platform agents.
        Launches the associated instrument agents;
        """
        log.info('_children_launch ...')

        # launch the sub-platform agents:
        self._subplatforms_launch()

        # launch instruments:
        self._instruments_launch()

        # Ready, children launched and event subscribers in place
        self._async_children_launched.set()
        log.debug("%r: _children_launch completed", self._platform_id)

        #processing complete so unblock and accept cmds
        self._fsm.on_event(PlatformAgentEvent.LAUNCH_COMPLETE)
        cur_state = self._fsm.get_current_state()
        if cur_state != ResourceAgentState.UNINITIALIZED:
            raise Exception()

    def _get_children_resource_ids(self):
        """
        Gets the resource IDs of the immediate children (platforms and
        instruments) of this agent.

        @return list of resouce_ids
        """
        resource_ids = []
        pnode = self._pnode

        # get resource_ids of immediate sub-platforms:
        for subplatform_id in pnode.subplatforms:
            sub_pnode = pnode.subplatforms[subplatform_id]
            sub_agent_config = sub_pnode.CFG
            sub_resource_id = sub_agent_config.get("agent", {}).get("resource_id", None)
            assert sub_resource_id, "agent.resource_id must be present for child %r" % subplatform_id
            resource_ids.append(sub_resource_id)

        # get resource_ids of instruments:
        for instrument_id in pnode.instruments:
            inode = pnode.instruments[instrument_id]
            agent_config = inode.CFG
            resource_id = agent_config.get("agent", {}).get("resource_id", None)
            assert resource_id, "agent.resource_id must be present for child %r" % instrument_id
            resource_ids.append(resource_id)

        return resource_ids

    def on_quit(self):
        try:
            log.debug("%r: PlatformAgent: on_quit called. current_state=%s",
                      self._platform_id, self._fsm.get_current_state())

            self._do_quit()

        finally:
            super(PlatformAgent, self).on_quit()

    def _do_quit(self):
        """
        - terminates status handling
        - brings this agent to the UNINITIALIZED state (with no interation
          with children at all)
        - destroys launcher helper
        """
        log.info("%r: PlatformAgent: executing quit secuence", self._platform_id)

        self._status_manager.destroy()

        self._aam.stop_all()

        self._bring_to_uninitialized_state(recursion=False)

        self._launcher.destroy()

        log.info("%r: PlatformAgent: quit secuence complete.", self._platform_id)

    def _bring_to_uninitialized_state(self, recursion=True):
        """
        Performs steps to transition this agent to the UNINITIALIZED state
        (if not already there).

        @param recursion    Passed as argument to the RESET command to this
                            platform.
        """
        curr_state = self._fsm.get_current_state()

        if PlatformAgentState.UNINITIALIZED == curr_state:
            log.debug("%r: _bring_to_uninitialized_state: already in UNINITIALIZED state.",
                      self._platform_id)
            return

        attempts = 0
        while PlatformAgentState.UNINITIALIZED != curr_state and attempts <= 3:
            attempts += 1
            try:
                # most states accept the RESET event except as handled below.

                if PlatformAgentState.STOPPED == curr_state:
                    self._fsm.on_event(PlatformAgentEvent.CLEAR)
                    continue

                # RESET is accepted in any other state:
                self._fsm.on_event(PlatformAgentEvent.RESET, recursion=recursion)

            except FSMStateError:  # pragma: no cover
                # Should not happen.
                log.warn("For the quit sequence, a RESET event was tried "
                         "in a state (%s) that does not handle it; please "
                         "report this bug. (platform_id=%r)",
                         curr_state, self._platform_id)
                break

            finally:
                curr_state = self._fsm.get_current_state()

        log.debug("%r: _bring_to_uninitialized_state complete. "
                  "attempts=%d;  final state=%s",
                  self._platform_id, attempts, curr_state)

    ##############################################################
    # Operations on "this platform," that is, operations that act on the
    # platform itself (and that are called by operations that also dispatch
    # the associated command on the children)
    ##############################################################

    def _initialize_this_platform(self):
        """
        Does the main initialize sequence for this platform: creation of
        publishers and creation/configuration of the driver.
        """
        self._construct_data_publishers()
        self._create_driver()
        self._configure_driver()
        log.debug("%r: _initialize_this_platform completed.", self._platform_id)

    def _go_active_this_platform(self):
        """
        Connects the driver.
        """
        self._trigger_driver_event(PlatformDriverEvent.CONNECT)

    def _go_inactive_this_platform(self):
        """
        Disconnects the driver.
        """
        self._trigger_driver_event(PlatformDriverEvent.DISCONNECT)

    def _run_this_platform(self):
        """
        Nothing is done here. The method exists for consistency.
        Basically the RUN command is just to move the FSM to the COMMAND state,
        which is done by the handler.
        """
        pass

    def _reset_this_platform(self):
        """
        Resets this platform agent (stops resource monitoring, destroys driver).

        The "platform_config" configuration object provided via self.CFG
        (see on_init) is kept. This allows to issue the INITIALIZE command
        again with the already configured agent.
        """
        log.debug("%r: resetting this platform", self._platform_id)

        if self._platform_resource_monitor:
            self._stop_resource_monitoring()

        if self._plat_driver:
            # disconnect driver if connected:
            driver_state = self._plat_driver._fsm.get_current_state()
            log.debug("_reset_this_platform: driver_state = %s", driver_state)
            if driver_state == PlatformDriverState.CONNECTED:
                self._trigger_driver_event(PlatformDriverEvent.DISCONNECT)
            # destroy driver:
            self._plat_driver.destroy()
            self._plat_driver = None

        self._unconfigured_params.clear()

    def _shutdown_this_platform(self):
        """
        The name of this method is to keep consistency with the naming scheme
        for related operations involving children and the platform itself.
        The platform can not actually "shutdown" itself. What this method does
        is to bring this platform agent to the uninitialized state (with no
        interaction with children at all because _shutdown should have
        taken care of the children as instructed).
        """
        self._bring_to_uninitialized_state(recursion=False)

    def _pause_this_platform(self):
        """
        Pauses this platform agent. Actually, nothing is done here; we only
        need the state transition (which is done by the handler).
        """
        pass

    def _resume_this_platform(self):
        """
        Resumes this platform agent. Actually, nothing is done here; we only
        need the state transition (which is done by the handler).
        """
        pass

    def _clear_this_platform(self):
        """
        "Clears" this platform agent. Actually, nothing is done here; we only
        need the state transition (which is done by the handler).
        """
        pass

    ##############################################################
    # Governance interfaces
    ##############################################################


    #TODO - When/If the Instrument and Platform agents are dervied from a
    # common device agent class, then relocate to the parent class and share

    def check_if_direct_access_mode(self, message, headers):
        try:
            state = self._fsm.get_current_state()
            if state == ResourceAgentState.DIRECT_ACCESS:
                return False, "This operation is unavailable while the agent is in the Direct Access state"
        except Exception, e:
            log.warning("Could not determine the state of the agent:", e.message)

        return True

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

    ##############################################################
    # misc supporting routines
    ##############################################################

    def _create_publisher(self, stream_id=None, stream_route=None):
        publisher = StreamPublisher(process=self, stream_id=stream_id, stream_route=stream_route)
        return publisher

    def _construct_data_publishers(self):
        """
        Construct the stream publishers from the stream_config agent
        config variable.
        @retval None
        """

        agent_info = self.CFG.get('agent', None)
        if not agent_info:
            log.warning("%r: No agent config found in agent config.", self._platform_id)
        else:
            self.resource_id = agent_info['resource_id']
            log.debug("%r: resource_id set to %r", self.resource_id, self.resource_id)

        stream_configs = self.CFG.get('stream_config', None)
        if stream_configs is None:
            raise PlatformConfigurationException(
                "%r: No stream_config given in CFG", self._platform_id)

        for stream_name, stream_config in stream_configs.iteritems():
            self._construct_data_publisher_using_stream_config(stream_name, stream_config)

        log.debug("%r: _construct_data_publishers complete", self._platform_id)

    def _construct_data_publisher_using_stream_config(self, stream_name, stream_config):

        # granule_publish_rate
        # records_per_granule

        if log.isEnabledFor(logging.TRACE):
            log.trace("%r: _construct_data_publisher_using_stream_config: "
                      "stream_name:%r, stream_config=%s",
                      self._platform_id, stream_name, self._pp.pformat(stream_config))

        routing_key           = stream_config['routing_key']
        stream_id             = stream_config['stream_id']
        exchange_point        = stream_config['exchange_point']
        parameter_dictionary  = stream_config['parameter_dictionary']
        stream_definition_ref = stream_config['stream_definition_ref']

        self._data_streams[stream_name] = stream_id
        self._param_dicts[stream_name] = ParameterDictionary.load(parameter_dictionary)
        self._stream_defs[stream_name] = stream_definition_ref
        stream_route = StreamRoute(exchange_point=exchange_point, routing_key=routing_key)
        publisher = self._create_publisher(stream_id=stream_id, stream_route=stream_route)
        self._data_publishers[stream_name] = publisher

        log.debug("%r: created publisher for stream_name=%r", self._platform_id, stream_name)

    def _create_driver(self):
        """
        Creates the platform driver object for this platform agent.

        NOTE: the driver object is created directly (not via a spawned process)
        """
        driver_module = self._driver_config['dvr_mod']
        driver_class  = self._driver_config['dvr_cls']

        assert self._platform_id is not None, "must know platform_id to create driver"

        if log.isEnabledFor(logging.DEBUG):
            log.debug('%r: creating driver: driver_module=%s driver_class=%s' % (
                self._platform_id, driver_module, driver_class))

        try:
            module = __import__(driver_module, fromlist=[driver_class])
            classobj = getattr(module, driver_class)
            driver = classobj(self._pnode, self.evt_recv)

        except Exception as e:
            msg = '%r: could not import/construct driver: module=%r, class=%r' % (
                self._platform_id, driver_module, driver_class)
            log.exception("%s", msg)  #, exc_Info=True)
            raise CannotInstantiateDriverException(msg=msg, reason=e)

        self._plat_driver = driver

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: driver created: %s" % (self._platform_id, str(driver)))

    def _trigger_driver_event(self, event, **kwargs):
        """
        Helper to trigger a driver event.
        """
        result = self._plat_driver._fsm.on_event(event, **kwargs)
        return result

    def _configure_driver(self):
        """
        Configures the platform driver object for this platform agent.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug('%r: configuring driver: %s' % (self._platform_id, self._driver_config))

        self._trigger_driver_event(PlatformDriverEvent.CONFIGURE, driver_config=self._driver_config)

        self._assert_driver_state(PlatformDriverState.DISCONNECTED)

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: driver configured." % self._platform_id)

    def _assert_driver_state(self, state):
        self._assert_driver()
        curr_state = self._plat_driver._fsm.get_current_state()
        assert state == curr_state, \
            "Assertion failed: expected driver state to be %s but got %s" % (
                state, curr_state)

    def _assert_driver(self):
        assert self._plat_driver is not None, "_create_driver must have been called first"

    def _get_attribute_values(self, attrs):
        """
        The callback for resource monitoring.
        This method will return None is case of lost connection.
        """
        self._assert_driver()
        kwargs = dict(attrs=attrs)
        result = self._plat_driver.get_resource(**kwargs)
        return result

    def _set_attribute_values(self, attrs):
        self._assert_driver()
        kwargs = dict(attrs=attrs)
        result = self._plat_driver.set_resource(**kwargs)
        return result

    ##############################################################
    # Asynchronous driver event callback and handlers.
    ##############################################################

    def evt_recv(self, driver_event):
        """
        Callback to receive asynchronous driver events.
        @param driver_event The driver event received.
        """

        if isinstance(driver_event, AttributeValueDriverEvent):
            self._handle_attribute_value_event(driver_event)
            return

        if isinstance(driver_event, ExternalEventDriverEvent):
            self._handle_external_event_driver_event(driver_event)
            return

        if isinstance(driver_event, StateChangeDriverEvent):
            self._async_driver_event_state_change(driver_event.state)
            return

        if isinstance(driver_event, AsyncAgentEvent):
            self._async_driver_event_agent_event(driver_event.event)
            return

        else:
            log.warn('%r: driver_event not handled: %s',
                     self._platform_id, str(type(driver_event)))
            return

    def _async_driver_event_state_change(self, state):
        """
        @param state   the state entered by the driver.
        """
        log.debug('%r: platform agent driver state change: %s', self._platform_id, state)
        try:
            event_data = {'state': state}
            self._event_publisher.publish_event(event_type='ResourceAgentResourceStateEvent',
                                                origin_type=self.ORIGIN_TYPE,
                                                origin=self.resource_id,
                                                **event_data)
        except:
            log.exception('%r: platform agent could not publish driver state change event',
                          self._platform_id)

    def _handle_attribute_value_event(self, driver_event):

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: driver_event = %s", self._platform_id, driver_event.brief())
        elif log.isEnabledFor(logging.TRACE):
            # show driver_event as retrieved (driver_event.vals_dict might be large)
            log.trace("%r: driver_event = %s", self._platform_id, driver_event)

        stream_name = driver_event.stream_name

        publisher = self._data_publishers.get(stream_name, None)
        if not publisher:
            log.warn('%r: no publisher configured for stream_name=%r. '
                     'Configured streams are: %s',
                     self._platform_id, stream_name, self._data_publishers.keys())
            return

        param_dict = self._param_dicts[stream_name]
        stream_def = self._stream_defs[stream_name]

        self._publish_granule_with_multiple_params(publisher, driver_event,
                                                   param_dict, stream_def)

    def _publish_granule_with_multiple_params(self, publisher, driver_event,
                                              param_dict, stream_def):

        stream_name = driver_event.stream_name

        rdt = RecordDictionaryTool(param_dictionary=param_dict.dump(),
                                   stream_definition_id=stream_def)

        pub_params = {}
        selected_timestamps = None

        for param_name, param_value in driver_event.vals_dict.iteritems():

            param_name = param_name.lower()

            if param_name not in rdt:
                if param_name not in self._unconfigured_params:
                    # an unrecognized attribute for this platform:
                    self._unconfigured_params.add(param_name)
                    log.warn('%r: got attribute value event for unconfigured parameter %r in stream %r'
                             ' rdt.keys=%s',
                             self._platform_id, param_name, stream_name. rdt.keys())
                continue

            # Note that notification from the driver has the form
            # of a non-empty list of pairs (val, ts)
            assert isinstance(param_value, list)
            assert isinstance(param_value[0], tuple)

            # separate values and timestamps:
            vals, timestamps = zip(*param_value)

            # Use fill_value in context to replace any None values:
            param_ctx = param_dict.get_context(param_name)
            if param_ctx:
                fill_value = param_ctx.fill_value
                log.debug("%r: param_name=%r fill_value=%s",
                          self._platform_id, param_name, fill_value)
                # do the replacement:
                vals = [fill_value if val is None else val for val in vals]
            else:
                log.warn("%r: unexpected: parameter context not found for %r",
                         self._platform_id, param_name)

            # Set values in rdt:
            rdt[param_name] = numpy.array(vals)

            pub_params[param_name] = vals

            selected_timestamps = timestamps

        if selected_timestamps is None:
            # that is, all param_name's were unrecognized; just return:
            return

        self._publish_granule(stream_name, publisher, param_dict, rdt,
                              pub_params, selected_timestamps)

    def _publish_granule(self, stream_name, publisher, param_dict, rdt,
                         pub_params, timestamps):

        # Set timestamp info in rdt:
        if param_dict.temporal_parameter_name is not None:
            temp_param_name = param_dict.temporal_parameter_name
            rdt[temp_param_name]       = numpy.array(timestamps)
            #@TODO: Ensure that the preferred_timestamp field is correct
            rdt['preferred_timestamp'] = numpy.array(['internal_timestamp'] * len(timestamps))
            log.warn('Preferred timestamp is unresolved, using "internal_timestamp"')
        else:
            log.warn("%r: Not including timestamp info in granule: "
                     "temporal_parameter_name not defined in parameter dictionary",
                     self._platform_id)

        g = rdt.to_granule(data_producer_id=self.resource_id)
        try:
            publisher.publish(g)

            if log.isEnabledFor(logging.DEBUG):  # pragma: no cover
                summary_params = {attr_id: "(%d vals)" % len(vals)
                                  for attr_id, vals in pub_params.iteritems()}
                summary_timestamps = "(%d vals)" % len(timestamps)
                log.debug("%r: Platform agent published data granule on stream %r: "
                          "%s  timestamps: %s",
                          self._platform_id, stream_name,
                          summary_params, summary_timestamps)
            elif log.isEnabledFor(logging.TRACE):  # pragma: no cover
                log.trace("%r: Platform agent published data granule on stream %r: "
                          "%s  timestamps: %s",
                          self._platform_id, stream_name,
                          self._pp.pformat(pub_params), self._pp.pformat(timestamps))

        except:
            log.exception("%r: Platform agent could not publish data on stream %s.",
                          self._platform_id, stream_name)

    def _handle_external_event_driver_event(self, driver_event):

        event_type = driver_event.event_type

        event_instance = driver_event.event_instance
        platform_id = event_instance.get('platform_id', None)
        message = event_instance.get('message', None)
        timestamp = event_instance.get('timestamp', None)
        group = event_instance.get('group', None)

        description  = "message: %s" % message
        description += "; group: %s" % group
        description += "; external_event_type: %s" % event_type
        description += "; external_timestamp: %s" % timestamp

        event_data = {
            'description':  description,
            'sub_type':     'platform_event',
        }

        log.info("%r: publishing external platform event: event_data=%s",
                 self._platform_id, event_data)

        try:
            self._event_publisher.publish_event(
                event_type='DeviceEvent',
                origin_type=self.ORIGIN_TYPE,
                origin=self.resource_id,
                **event_data)

        except:
            log.exception("Error while publishing platform event")

    def _async_driver_event_agent_event(self, event):
        """
        Driver initiated agent FSM event.
        """
        log.debug("%r/%s: received _async_driver_event_agent_event=%s",
                  self._platform_id, self.get_agent_state(), event)

        try:
            self._fsm.on_event(event)

        except:
            log.warn("%r/%s: error processing asynchronous agent event %s",
                     self._platform_id, self.get_agent_state(), event)

    ##############################################################
    # some more utilities
    ##############################################################

    def _create_resource_agent_client(self, sub_id, sub_resource_id):
        """
        Creates and returns a ResourceAgentClient instance.

        @param sub_id          ID of sub-component (platform or instrument)
                               for logging
        @param sub_resource_id Id for ResourceAgentClient
        """
        log.debug("%r: _create_resource_agent_client: sub_id=%r, sub_resource_id=%r",
                  self._platform_id, sub_id, sub_resource_id)

        a_client = ResourceAgentClient(sub_resource_id, process=self)

        log.debug("%r: ResourceAgentClient CREATED: sub_id=%r, sub_resource_id=%r",
                  self._platform_id, sub_id, sub_resource_id)

        return a_client

    def _execute_agent(self, poi, a_client, cmd, sub_id):
        """
        Calls execute_agent with the given cmd on the given client.

        @param poi        "platform" or "instrument" for logging
        @param a_client   Resource agent client (platform or instrument)
        @param cmd        the command to execute
        @param sub_id     for logging
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: _execute_agent: cmd=%r %s=%r ...",
                      self._platform_id, cmd.command, poi, sub_id)

            time_start = time.time()
            retval = a_client.execute_agent(cmd, timeout=self._timeout)
            elapsed_time = time.time() - time_start
            log.debug("%r: _execute_agent: cmd=%r %s=%r elapsed_time=%s",
                      self._platform_id, cmd.command, poi, sub_id, elapsed_time)
        else:
            retval = a_client.execute_agent(cmd, timeout=self._timeout)

        return retval

    def _get_recursion_parameter(self, method_name, *args, **kwargs):
        """
        Utility to extract the 'recursion' parameter.
        """
        recursion = kwargs.get('recursion', None)
        if recursion is None:
            log.info("%r: %s called with no recursion parameter. "
                     "Using recursion=True by default.",
                     self._platform_id, method_name)
            recursion = True
        else:
            log.info("%r: %s called with recursion parameter: %r",
                     self._platform_id, method_name, recursion)
            recursion = bool(recursion)

        return recursion

    def _prepare_await_state(self, origin, state):
        """
        Does preparations to wait until the given origin publishes a
        ResourceAgentStateEvent with the given state.

        @param origin  Origin for the EventSubscriber
        @param state   Expected state

        @return (asyn_res, sub) Arguments for subsequent call self_await_state(async_res, sun)
        """

        asyn_res = AsyncResult()

        def consume_event(evt, *args, **kwargs):
            log.debug("%r: got ResourceAgentStateEvent %s from origin %r",
                      self._platform_id, evt.state, evt.origin)
            if evt.state == state:
                asyn_res.set(evt)

        subscriber = EventSubscriber(event_type="ResourceAgentStateEvent",
                                     origin=origin,
                                     callback=consume_event)

        subscriber.start()
        log.debug("%r: registered event subscriber to wait for state=%r from origin %r",
                  self._platform_id, state, origin)
        subscriber._ready_event.wait(timeout=self._timeout)

        return asyn_res, subscriber

    def _await_state(self, async_res, sub):
        """
        Waits until the state given to _prepare_await_state is published and
        received.

        @param async_res   As returned by call to _prepare_await_state
        @param sub         As returned by call to _prepare_await_state
        """
        try:
            # wait for the event:
            async_res.get(timeout=self._timeout)

        finally:
            try:
                sub.stop()
            except Exception as ex:
                log.warn("%r: error stopping event subscriber to wait for a state: %s",
                         self._platform_id, ex)

    ##############################################################
    # supporting routines dealing with sub-platforms
    ##############################################################

    def _launch_platform_agent(self, subplatform_id):
        """
        Launches a sub-platform agent (f not already running) and waits until
        the sub-platform transitions to UNINITIALIZED state.
        It creates corresponding ResourceAgentClient,
        and publishes device_added event.

        The mechanism to detect whether the sub-platform agent is already
        running is by simply trying to create a ResourceAgentClient to it.

        @param subplatform_id Platform ID
        """

        # get PlatformNode, corresponding CFG, and resource_id:
        sub_pnode = self._pnode.subplatforms[subplatform_id]
        sub_agent_config = sub_pnode.CFG
        sub_resource_id = sub_agent_config.get("agent", {}).get("resource_id", None)

        assert sub_resource_id, "agent.resource_id must be present for child %r" % subplatform_id

        # first, is the agent already running?
        pa_client = None  # assume it's not.
        pid = None
        try:
            # try to connect:
            pa_client = self._create_resource_agent_client(subplatform_id, sub_resource_id)
            # it is actually running.

            # get PID:
            # TODO is there a more public method to get the pid?
            pid = ResourceAgentClient._get_agent_process_id(sub_resource_id)

        except NotFound:
            # not running.
            pass

        if pa_client:
            #
            # agent process already running.
            #
            # TODO if any, what kind of state check should be done? For the
            # moment don't do any state verification.
            state = pa_client.get_agent_state()
            log.debug("%r: [LL] sub-platform agent already running=%r, "
                      "sub_resource_id=%s, state=%s",
                      self._platform_id, subplatform_id, sub_resource_id, state)
        else:
            #
            # agent process NOT running.
            #
            log.debug("%r: [LL] sub-platform agent NOT running=%r, sub_resource_id=%s",
                      self._platform_id, subplatform_id, sub_resource_id)

            # prepare to wait for the UNINITIALIZED state; this is done before the
            # launch below to avoid potential race condition
            asyn_res, subscriber = self._prepare_await_state(sub_resource_id,
                                                             PlatformAgentState.UNINITIALIZED)

            if log.isEnabledFor(logging.TRACE):  # pragma: no cover
                log.trace("%r: launching sub-platform agent %r: CFG=%s",
                          self._platform_id, subplatform_id, self._pp.pformat(sub_agent_config))
            else:
                log.debug("%r: launching sub-platform agent %r", self._platform_id, subplatform_id)

            pid = self._launcher.launch_platform(subplatform_id, sub_agent_config)
            log.debug("%r: DONE launching sub-platform agent %r", self._platform_id, subplatform_id)

            # create resource agent client:
            pa_client = self._create_resource_agent_client(subplatform_id, sub_resource_id)

            # wait until UNINITIALIZED:
            self._await_state(asyn_res, subscriber)

        # here, sub-platform agent process is running.

        self._pa_clients[subplatform_id] = DotDict(pa_client=pa_client,
                                                   pid=pid,
                                                   resource_id=sub_resource_id)

        # publish device_added:
        self._status_manager.publish_device_added_event(sub_resource_id)

    def _execute_platform_agent(self, a_client, cmd, sub_id):
        return self._execute_agent("platform", a_client, cmd, sub_id)

    def _ping_subplatform(self, subplatform_id):
        """
        Pings the given sub-platform, publishing a "device_failed_command"
        event is some error occurs.

        @return None if completed OK, otherwise an error message.
        """
        log.debug("%r: _ping_subplatform -> %r,  _pa_clients=%s",
                  self._platform_id, subplatform_id, self._pa_clients)

        dd = self._pa_clients[subplatform_id]

        err_msg = None
        try:
            retval = dd.pa_client.ping_agent(timeout=self._timeout)
            log.debug("%r: _ping_subplatform %r  retval = %s",
                      self._platform_id, subplatform_id, retval)

            if retval is None:
                err_msg = "%r: unexpected None ping response from sub-platform agent: %r" % (
                          self._platform_id, subplatform_id)

        except Exception as ex:
            err_msg = str(ex)

        if err_msg is not None:
            log.error(err_msg)
            self._status_manager.publish_device_failed_command_event(dd.resource_id,
                                                                     "ping_agent",
                                                                     err_msg)
        return err_msg

    def _initialize_subplatform(self, subplatform_id):
        """
        Issues INITIALIZE command to the given (sub-)platform agent so the
        agent network gets initialized recursively.

        @return None if completed OK, otherwise an error message.
        """
        log.debug("%r: _initialize_subplatform -> %r",
                  self._platform_id, subplatform_id)

        # first, ping sub-platform
        err_msg = self._ping_subplatform(subplatform_id)
        if err_msg is not None:
            # event already published. Do not proceed with initialize.
            return err_msg

        # now, do initialize:
        err_msg = None
        dd = self._pa_clients[subplatform_id]

        sub_state = dd.pa_client.get_agent_state()
        if PlatformAgentState.INACTIVE == sub_state:
            # already initialized.
            log.trace("%r: _initialize_subplatform: already initialized: %r",
                      self._platform_id, subplatform_id)
            return

        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE)

        try:
            retval = self._execute_platform_agent(dd.pa_client, cmd, subplatform_id)
            if log.isEnabledFor(logging.DEBUG):
                log.debug("%r: _initialize_subplatform %r  retval = %s",
                          self._platform_id, subplatform_id, str(retval))
        except Exception as ex:
            err_msg = str(ex)

        if err_msg is not None:
            log.error(err_msg)
            self._status_manager.publish_device_failed_command_event(dd.resource_id,
                                                                     cmd,
                                                                     err_msg)
        return err_msg

    def _get_subplatform_ids(self):
        """
        Gets the IDs of my sub-platforms.
        """
        return self._pnode.subplatforms.keys()

    def _subplatforms_launch(self):
        """
        Launches all my sub-platforms storing the corresponding
        ResourceAgentClient objects in _pa_clients.
        """
        self._pa_clients.clear()
        subplatform_ids = self._get_subplatform_ids()
        if len(subplatform_ids):
            log.debug("%r: launching subplatforms %s", self._platform_id, subplatform_ids)
            for subplatform_id in subplatform_ids:
                self._launch_platform_agent(subplatform_id)

            log.debug("%r: _subplatforms_launch completed. _pa_clients=%s",
                      self._platform_id, self._pa_clients)

    def _subplatforms_initialize(self):
        """
        Initializes all my sub-platforms.

        @return dict with failing children. Empty if all ok.
        """
        log.debug("%r: _subplatforms_initialize. _pa_clients=%s",
                  self._platform_id, self._pa_clients)

        subplatform_ids = self._get_subplatform_ids()
        children_with_errors = {}
        if len(subplatform_ids):
            log.debug("%r: initializing subplatforms %s", self._platform_id, subplatform_ids)
            for subplatform_id in subplatform_ids:
                err_msg = self._initialize_subplatform(subplatform_id)
                if err_msg is not None:
                    children_with_errors[subplatform_id] = err_msg

            log.debug("%r: _subplatforms_initialize completed. children_with_errors=%s",
                      self._platform_id, children_with_errors)

        return children_with_errors

    def _subplatforms_execute_agent(self, command=None, create_command=None,
                                    expected_state=None):
        """
        Supporting routine for various commands sent to sub-platforms.

        @param command        Can be a AgentCommand, and string, or None.

        @param create_command If command is None, create_command is invoked as
                              create_command(subplatform_id) for each
                              sub-platform to create the command to be executed.

        @param expected_state  If given, it is checked that the child gets to
                               this state.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        subplatform_ids = self._get_subplatform_ids()
        assert subplatform_ids == self._pa_clients.keys()

        children_with_errors = {}

        if not len(subplatform_ids):
            return children_with_errors

        def execute_cmd(subplatform_id, cmd):
            pa_client = self._pa_clients[subplatform_id].pa_client

            try:
                self._execute_platform_agent(pa_client, cmd, subplatform_id)

            except FSMStateError as e:
                err_msg = "%r: subplatform %r: FSMStateError for command %r: %s" % (
                          self._platform_id, subplatform_id, command, e)
                log.warn(err_msg)
                return err_msg

            except Exception as e:
                err_msg = "%r: exception executing command %r in subplatform %r: %s" % (
                          self._platform_id, command, subplatform_id, e)
                log.exception(err_msg) #, exc_Info=True)
                return err_msg

            # verify expected_state if given:
            try:
                state = pa_client.get_agent_state()
                if expected_state and expected_state != state:
                    err_msg = "%r: expected subplatform state %r but got %r" % (
                              self._platform_id, expected_state, state)
                    log.error(err_msg)
                    return err_msg
            except Exception as e:
                err_msg = "%r: exception while calling get_agent_state to subplatform %r: %s" % (
                          self._platform_id, subplatform_id, e)
                log.exception(err_msg) #, exc_Info=True)
                return err_msg

            return None  # OK

        if command:
            log.debug("%r: executing command %r on my sub-platforms: %s",
                      self._platform_id, command, str(subplatform_ids))
        else:
            log.debug("%r: executing command on my sub-platforms: %s",
                      self._platform_id, str(subplatform_ids))

        for subplatform_id in self._pa_clients:
            if expected_state:
                pa_client = self._pa_clients[subplatform_id].pa_client
                sub_state = pa_client.get_agent_state()
                if expected_state == sub_state:
                    #
                    # already in the desired state; do nothing for this child:
                    #
                    log.trace("%r: sub-platform %r already in state: %r",
                              self._platform_id, subplatform_id, expected_state)
                    continue

            if isinstance(command, AgentCommand):
                cmd = command
            elif command is not None:
                cmd = AgentCommand(command=command)
            else:
                cmd = create_command(subplatform_id)

            err_msg = execute_cmd(subplatform_id, cmd)

            if err_msg is not None:
                # some error happened; publish event:
                children_with_errors[subplatform_id] = err_msg
                dd = self._pa_clients[subplatform_id]
                self._status_manager.publish_device_failed_command_event(dd.resource_id,
                                                                         cmd,
                                                                         err_msg)
        return children_with_errors

    def _subplatforms_reset(self):
        """
        Executes RESET with recursion=True on all my sub-platforms.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # pass recursion=True to each sub-platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.RESET, kwargs=kwargs)
        children_with_errors = self._subplatforms_execute_agent(
            command=cmd,
            expected_state=PlatformAgentState.UNINITIALIZED)

        return children_with_errors

    def _subplatforms_go_active(self):
        """
        Executes GO_ACTIVE with recursion=True on all my sub-platforms.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # pass recursion=True to each sub-platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE, kwargs=kwargs)
        children_with_errors = self._subplatforms_execute_agent(
            command=cmd,
            expected_state=PlatformAgentState.IDLE)

        return children_with_errors

    def _subplatforms_go_inactive(self):
        """
        Executes GO_INACTIVE with recursion=True on all my sub-platforms.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # pass recursion=True to each sub-platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE, kwargs=kwargs)
        children_with_errors = self._subplatforms_execute_agent(
            command=cmd,
            expected_state=PlatformAgentState.INACTIVE)

        return children_with_errors

    def _subplatforms_run(self):
        """
        Executes RUN with recursion=True on all my sub-platforms.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # pass recursion=True to each sub-platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.RUN, kwargs=kwargs)
        children_with_errors = self._subplatforms_execute_agent(
            command=cmd,
            expected_state=PlatformAgentState.COMMAND)

        return children_with_errors

    def _shutdown_and_terminate_subplatform(self, subplatform_id):
        """
        Executes SHUTDOWN with recursion=True on the given sub-platform and
        then terminates that sub-platform process.

        @return None if the shutdown and termination completed without errors.
                Otherwise a string with an error message.
        """

        dd = self._pa_clients[subplatform_id]
        cmd = AgentCommand(command=PlatformAgentEvent.SHUTDOWN, kwargs=dict(recursion=True))

        def shutdown():
            log.debug("%r: shutting down %r", self._platform_id, subplatform_id)

            # execute command:
            try:
                retval = self._execute_platform_agent(dd.pa_client, cmd, subplatform_id)
            except:
                err_msg = "%r: exception executing command %r in subplatform %r" % (
                          self._platform_id, cmd, subplatform_id)
                log.exception(err_msg)
                return err_msg

            # verify state:
            try:
                expected_state = PlatformAgentState.UNINITIALIZED
                state = dd.pa_client.get_agent_state()
                if expected_state and expected_state != state:
                    err_msg = "%r: expected subplatform state %r but got %r" % (
                              self._platform_id, expected_state, state)
                    log.error(err_msg)
                    return err_msg
            except:
                err_msg = "%r: exception while calling get_agent_state to subplatform %r" % (
                          self._platform_id, subplatform_id)
                log.exception(err_msg)
                return err_msg

            return None  # OK

        def terminate():
            pid = dd.pid

            log.debug("%r: canceling sub-platform process: subplatform_id=%r, pid=%r",
                      self._platform_id, subplatform_id, pid)
            try:
                self._launcher.cancel_process(pid)

                self._status_manager.publish_device_removed_event(dd.resource_id)

                log.debug(
                    "%r: canceled sub-platform process: subplatform_id=%r, pid=%r",
                    self._platform_id, subplatform_id, pid)

                return None

            except:
                if log.isEnabledFor(logging.TRACE):
                    err_msg = "%r: Exception in cancel_process for subplatform_id=%r, pid=%r" % (
                              self._platform_id, subplatform_id, pid)
                    log.exception(err_msg)  # , exc_Info=True)
                    return err_msg
                else:
                    err_msg = "%r: Exception in cancel_process for subplatform_id=%r, pid=%r" \
                              ". Perhaps already dead." % (
                              self._platform_id, subplatform_id, pid)
                    log.warn(err_msg)  # , exc_Info=True)
                    return err_msg

        err_msg = shutdown()
        if err_msg is None:
            # shutdown ok; now terminate:
            err_msg = terminate()

        if err_msg is not None:
            # some error happened; publish event:
            self._status_manager.publish_device_failed_command_event(dd.resource_id,
                                                                     cmd,
                                                                     err_msg)

        return err_msg

    def _subplatforms_shutdown_and_terminate(self):
        """
        Executes SHUTDOWN with recursion=True on all sub-platform and then
        terminates those sub-platform process.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        subplatform_ids = self._get_subplatform_ids()
        assert subplatform_ids == self._pa_clients.keys()

        children_with_errors = {}
        if len(subplatform_ids):
            for subplatform_id in subplatform_ids:
                err_msg = self._shutdown_and_terminate_subplatform(subplatform_id)
                if err_msg is not None:
                    children_with_errors[subplatform_id] = err_msg

        return children_with_errors

    def _subplatforms_pause(self):
        """
        Executes PAUSE with recursion=True on all my sub-platforms.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # pass recursion=True to each sub-platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.PAUSE, kwargs=kwargs)
        children_with_errors = self._subplatforms_execute_agent(
            command=cmd,
            expected_state=PlatformAgentState.STOPPED)

        return children_with_errors

    def _subplatforms_resume(self):
        """
        Executes RESUME with recursion=True on all my sub-platforms.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # pass recursion=True to each sub-platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.RESUME, kwargs=kwargs)
        children_with_errors = self._subplatforms_execute_agent(
            command=cmd,
            expected_state=PlatformAgentState.COMMAND)

        return children_with_errors

    def _subplatforms_clear(self):
        """
        Executes CLEAR with recursion=True on all my sub-platforms.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # pass recursion=True to each sub-platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.CLEAR, kwargs=kwargs)
        children_with_errors = self._subplatforms_execute_agent(
            command=cmd,
            expected_state=PlatformAgentState.IDLE)

        return children_with_errors

    ##############################################################
    # supporting routines dealing with instruments
    ##############################################################

    def _execute_instrument_agent(self, a_client, cmd, sub_id):
        return self._execute_agent("instrument", a_client, cmd, sub_id)

    def _get_instrument_ids(self):
        """
        Gets the IDs of my instruments.
        """
        return self._pnode.instruments.keys()

    def _ping_instrument(self, instrument_id):
        log.debug("%r: _ping_instrument -> %r",
                  self._platform_id, instrument_id)

        dd = self._ia_clients[instrument_id]

        err_msg = None
        try:
            retval = dd.ia_client.ping_agent(timeout=self._timeout)
            log.debug("%r: _ping_instrument %r  retval = %s",
                      self._platform_id, instrument_id, str(retval))

            if retval is None:
                err_msg = "%r: unexpected None ping response from instrument agent: %r" % (
                          self._platform_id, instrument_id)

        except Exception as ex:
            err_msg = str(ex)

        if err_msg is not None:
            log.error(err_msg)
            self._status_manager.publish_device_failed_command_event(dd.resource_id,
                                                                     "ping_agent",
                                                                     err_msg)
        return err_msg

    def _initialize_instrument(self, instrument_id):
        """
        Issues INITIALIZE command to the given instrument agent.

        @return None if completed OK, otherwise an error message.
        """
        log.debug("%r: _initialize_instrument -> %r",
                  self._platform_id, instrument_id)

        # first, ping:
        err_msg = self._ping_instrument(instrument_id)
        if err_msg is not None:
            # event already published. Do not proceed with initialize.
            return err_msg

        # now, do initialize:
        err_msg = None
        dd = self._ia_clients[instrument_id]

        sub_state = dd.ia_client.get_agent_state()
        if PlatformAgentState.INACTIVE == sub_state:
            # already initialized.
            log.trace("%r: _initialize_subplatform: already initialized: %r",
                      self._platform_id, instrument_id)
            return

        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE)
        try:
            retval = self._execute_instrument_agent(dd.ia_client, cmd, instrument_id)
            log.debug("%r: _initialize_instrument %r  retval = %s",
                      self._platform_id, instrument_id, retval)

        except Exception as ex:
            err_msg = str(ex)

        if err_msg is not None:
            log.error(err_msg)
            self._status_manager.publish_device_failed_command_event(dd.resource_id,
                                                                     cmd,
                                                                     err_msg)
        return err_msg

    def _launch_instrument_agent(self, instrument_id):
        """
        Launches an instrument agent (f not already running).
        It creates corresponding ResourceAgentClient,
        and publishes device_added event.

        The mechanism to detect whether the instrument agent is already
        running is by simply trying to create a ResourceAgentClient to it.

        @param instrument_id
        """

        # get InstrumentsNode, corresponding CFG, and resource_id:
        inode = self._pnode.instruments[instrument_id]
        i_CFG = inode.CFG
        i_resource_id = i_CFG.get("agent", {}).get("resource_id", None)

        assert i_resource_id, "agent.resource_id must be present for child %r" % instrument_id

        # first, is the agent already running?
        ia_client = None
        pid = None
        try:
            # try to connect
            ia_client = self._create_resource_agent_client(instrument_id, i_resource_id)
            # it is running.

            # get PID:
            # TODO is there a more public method to get the pid?
            pid = ResourceAgentClient._get_agent_process_id(i_resource_id)

        except NotFound:
            # not running.
            pass

        if ia_client:
            #
            # agent process already running.
            #
            # TODO if any, what kind of state check should be done? For the
            # moment we don't do any state verification.
            state = ia_client.get_agent_state()
            log.debug("%r: [LL] instrument agent already running=%r, "
                      "i_resource_id=%s, state=%s",
                      self._platform_id, instrument_id, i_resource_id, state)
        else:
            #
            # agent process NOT running.
            #
            log.debug("%r: [LL] instrument agent NOT running=%r, i_resource_id=%s",
                      self._platform_id, instrument_id, i_resource_id)

            if log.isEnabledFor(logging.TRACE):  # pragma: no cover
                log.trace("%r: launching instrument agent %r: CFG=%s",
                          self._platform_id, instrument_id, self._pp.pformat(i_CFG))
            else:
                log.debug("%r: launching instrument agent %r", self._platform_id, instrument_id)

            pid = self._launcher.launch_instrument(instrument_id, i_CFG)

            ia_client = self._create_resource_agent_client(instrument_id, i_resource_id)

            state = ia_client.get_agent_state()
            assert ResourceAgentState.UNINITIALIZED == state

        # here, instrument agent process is running.

        self._ia_clients[instrument_id] = DotDict(ia_client=ia_client,
                                                  pid=pid,
                                                  resource_id=i_resource_id)

        self._status_manager.publish_device_added_event(i_resource_id)

    def _instruments_launch(self):
        """
        Launches all my instruments storing the corresponding
        ResourceAgentClient objects in _ia_clients.
        """
        self._ia_clients.clear()
        instrument_ids = self._get_instrument_ids()
        if len(instrument_ids):
            log.debug("%r: launching instruments %s", self._platform_id, instrument_ids)
            for instrument_id in instrument_ids:
                self._launch_instrument_agent(instrument_id)

            log.debug("%r: _instruments_launch completed.", self._platform_id)

    def _instruments_initialize(self):
        """
        Initializes all my instruments.

        @return dict with failing children. Empty if all ok.
        """
        instrument_ids = self._get_instrument_ids()
        children_with_errors = {}
        if len(instrument_ids):
            log.debug("%r: initializing instruments %s", self._platform_id, instrument_ids)
            for instrument_id in instrument_ids:
                err_msg = self._initialize_instrument(instrument_id)
                if err_msg is not None:
                    children_with_errors[instrument_id] = err_msg

            log.debug("%r: _instruments_initialize completed. children_with_errors=%s",
                      self._platform_id, children_with_errors)

        return children_with_errors

    def _instruments_execute_agent(self, command=None, create_command=None,
                                   expected_state=None):
        """
        Supporting routine for various commands sent to instruments.

        @param create_command invoked as create_command(instrument_id) for each
                              instrument to create the command to be executed.

        @param expected_state  If given, it is checked that the child gets to
                               this state.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        instrument_ids = self._get_instrument_ids()
        assert instrument_ids == self._ia_clients.keys()

        children_with_errors = {}

        if not len(instrument_ids):
            return children_with_errors

        def execute_cmd(instrument_id, cmd):
            ia_client = self._ia_clients[instrument_id].ia_client

            try:
                self._execute_instrument_agent(ia_client, cmd,
                                               instrument_id)
            except FSMStateError as e:
                err_msg = "%r: instrument %r: FSMStateError for command %r: %s" % (
                          self._platform_id, instrument_id, command, e)
                log.warn(err_msg)
                return err_msg

            except Exception as e:
                err_msg = "%r: exception executing command %r in instrument %r: %s" % (
                          self._platform_id, command, instrument_id, e)
                log.exception(err_msg) #, exc_Info=True)
                return err_msg

            # verify expected_state if given:
            try:
                state = ia_client.get_agent_state()
                if expected_state and expected_state != state:
                    err_msg = "%r: expected instrument state %r but got %r" % (
                              self._platform_id, expected_state, state)
                    log.error(err_msg)
                    return err_msg

            except Exception as e:
                err_msg = "%r: exception while calling get_agent_state to instrument %r: %s" % (
                          self._platform_id, instrument_id, e)
                log.exception(err_msg) #, exc_Info=True)
                return err_msg

            return None

        if command:
            log.debug("%r: executing command %r on my instruments: %s",
                      self._platform_id, command, str(instrument_ids))
        else:
            log.debug("%r: executing command on my instruments: %s",
                      self._platform_id, str(instrument_ids))

        for instrument_id in self._ia_clients:
            if expected_state:
                ia_client = self._ia_clients[instrument_id].ia_client
                sub_state = ia_client.get_agent_state()
                if expected_state == sub_state:
                    #
                    # already in the desired state; do nothing for this child:
                    #
                    log.trace("%r: instrument %r already in state: %r",
                              self._platform_id, instrument_id, expected_state)
                    continue

            cmd = AgentCommand(command=command) if command else create_command(instrument_id)
            err_msg = execute_cmd(instrument_id, cmd)

            if err_msg is not None:
                # some error happened; publish event:
                children_with_errors[instrument_id] = err_msg
                dd = self._ia_clients[instrument_id]
                self._status_manager.publish_device_failed_command_event(dd.resource_id,
                                                                         cmd,
                                                                         err_msg)
        return children_with_errors

    def _instruments_reset(self):
        """
        Executes RESET on all my instruments.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        children_with_errors = self._instruments_execute_agent(
            command=ResourceAgentEvent.RESET,
            expected_state=ResourceAgentState.UNINITIALIZED)

        return children_with_errors

    def _instruments_go_active(self):
        """
        Executes GO_ACTIVE on all my instruments.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # NOTE: the instrument performs a "state discovery" as part of GO_ACTIVE;
        # so we do not check for any particular expected instrument state here:
        expected_state = None
        children_with_errors = self._instruments_execute_agent(
            command=ResourceAgentEvent.GO_ACTIVE,
            expected_state=expected_state)

        return children_with_errors

    def _instruments_go_inactive(self):
        """
        Executes GO_INACTIVE on all my instruments.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        children_with_errors = self._instruments_execute_agent(
            command=ResourceAgentEvent.GO_INACTIVE,
            expected_state=ResourceAgentState.INACTIVE)

        return children_with_errors

    def _instruments_run(self):
        """
        Executes RUN on all my instruments.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        children_with_errors = self._instruments_execute_agent(
            command=ResourceAgentEvent.RUN,
            expected_state=ResourceAgentState.COMMAND)

        return children_with_errors

    def _shutdown_and_terminate_instrument(self, instrument_id):
        """
        Executes RESET on the instrument and then terminates it.
        ("shutting down" an instrument is just resetting it if not already in
         UNINITIALIZED state)
        """

        dd = self._ia_clients[instrument_id]
        cmd = AgentCommand(ResourceAgentEvent.RESET)

        def reset():
            log.debug("%r: resetting %r", self._platform_id, instrument_id)

            ia_client = self._ia_clients[instrument_id].ia_client

            # do not reset if instrument already in UNINITIALIZED:
            if ResourceAgentState.UNINITIALIZED == ia_client.get_agent_state():
                return

            try:
                retval = self._execute_instrument_agent(ia_client, cmd,
                                                        instrument_id)
            except:
                err_msg = "%r: exception executing command %r in instrument %r" % (
                          self._platform_id, cmd, instrument_id)
                log.exception(err_msg) #, exc_Info=True)
                return err_msg

            # verify state:
            try:
                expected_state = ResourceAgentState.UNINITIALIZED
                state = ia_client.get_agent_state()
                if expected_state != state:
                    err_msg = "%r: expected instrument state %r but got %r" % (
                              self._platform_id, expected_state, state)
                    log.error(err_msg)
                    return err_msg

                return None

            except:
                err_msg = "%r: exception while calling get_agent_state to instrument %r" % (
                          self._platform_id, instrument_id)
                log.exception(err_msg) #, exc_Info=True)
                return err_msg

        def terminate():
            pid = dd.pid
            i_resource_id = dd.resource_id

            log.debug("%r: canceling instrument process: instrument_id=%r, pid=%r",
                      self._platform_id, instrument_id, pid)

            try:
                self._launcher.cancel_process(pid)

                self._status_manager.publish_device_removed_event(i_resource_id)

                log.debug(
                    "%r: canceled instrument process: instrument_id=%r, pid=%r",
                    self._platform_id, instrument_id, pid)

                return None

            except:
                log.exception("%r: exception in cancel_process for instrument_id=%r, pid=%r",
                              self._platform_id, instrument_id, pid)  # , exc_Info=True)

        err_msg = reset()
        if err_msg is None:
            # reset ok; now terminate:
            err_msg = terminate()

        if err_msg is not None:
            # some error happened; publish event:
            self._status_manager.publish_device_failed_command_event(dd.resource_id,
                                                                     cmd,
                                                                     err_msg)

        return err_msg

    def _instruments_shutdown_and_terminate(self):
        """
        Executes RESET and terminates all my instruments.
        ("shutting down" the instruments is just resetting them.)

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        instrument_ids = self._get_instrument_ids()
        assert instrument_ids == self._ia_clients.keys()

        instruments_with_errors = {}
        if len(instrument_ids):
            for instrument_id in instrument_ids:
                err_msg = self._shutdown_and_terminate_instrument(instrument_id)
                if err_msg is not None:
                    instruments_with_errors[instrument_id] = err_msg

        return instruments_with_errors

    def _instruments_pause(self):
        """
        Executes PAUSE on all my instruments.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        children_with_errors = self._instruments_execute_agent(
            command=ResourceAgentEvent.PAUSE,
            expected_state=ResourceAgentState.STOPPED)

        return children_with_errors

    def _instruments_resume(self):
        """
        Executes RESUME on all my instruments.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        children_with_errors = self._instruments_execute_agent(
            command=ResourceAgentEvent.RESUME,
            expected_state=ResourceAgentState.COMMAND)

        return children_with_errors

    def _instruments_clear(self):
        """
        Executes CLEAR on all my instruments.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        children_with_errors = self._instruments_execute_agent(
            command=ResourceAgentEvent.CLEAR,
            expected_state=ResourceAgentState.IDLE)

        return children_with_errors

    ##############################################################
    # major operations dealing with orchestration and state
    # transitions involving child agents
    ##############################################################

    def _initialize(self, recursion):
        """
        Dispatches the INITIALIZE command.

        INITIALIZE is dispatched in a top-down fashion:
        - does proper initialization
        - if recursion is True, initializes instruments and sub-platforms

        @param recursion   True to initialize children.

        """
        assert self._plat_config, "platform_config must have been provided"

        log.info("%r: _initializing with provided platform_config...",
                 self._plat_config['platform_id'])

        # first, my own initialization:
        self._initialize_this_platform()

        # here, no errors occurred during initialization above. So, this agent
        # will transition to INACTIVE (see _handler_uninitialized_initialize)
        # regardless of the outcome of initializing the children below.

        # then, children initialization
        if recursion:
            # note that the launch of the children is started in on_start;
            # wait here until they are actually launched (see _children_launch):
            try:
                self._async_children_launched.get(timeout=self._timeout)
                log.debug("%r: _children_launch: LAUNCHED. Continuing with initialization",
                          self._platform_id)
            except:
                #
                # Do not proceed further with initialization of children.
                #
                log.exception("%r: exception while waiting children "
                              "to be launched. "
                              "Not proceeding with initialization of children.",
                              self._platform_id)
                return

            # All is OK here. Proceed with initializing children:
            try:
                # initialize sub-platforms
                self._subplatforms_initialize()

                # initialize instruments
                self._instruments_initialize()

            except:
                log.exception("%r: unexpected exception while initializing children: "
                              "any errors during this initialization should have "
                              "been handled with event notifications.", self._platform_id)

        result = None
        return result

    def _go_active(self, recursion):
        """
        Dispatches the GO_ACTIVE command.

        GO_ACTIVE is dispatched in a top-down fashion:
        - activates this platform (connects the driver)
        - if recursion is True, sends GO_ACTIVE to instruments and sub-platforms

        @param recursion   True to activate children.
        """

        # first myself
        self._go_active_this_platform()

        # then instruments and sub-platforms
        if recursion:
            try:
                self._instruments_go_active()
                self._subplatforms_go_active()

            except:
                log.exception("%r: unexpected exception while sending go_active to children: "
                              "any errors during this sequence should have "
                              "been handled with event notifications.", self._platform_id)
        result = None
        return result

    def _go_inactive(self, recursion):
        """
        Dispatches the GO_INACTIVE command.

        GO_INACTIVE is dispatched in a bottom-up fashion:
         - if recursion is True, issues GO_INACTIVE to sub-platforms
         - if recursion is True, issues GO_INACTIVE to instruments
         - processes the command at this platform.

        If recursion==True and any sub-platform or instrument fails the command,
        then this agent does NOT perform any actions on itself.

        @param recursion  If True, children are sent the GO_INACTIVE command
                          (with corresponding recursion parameter set to True
                           in the case of platforms).
        """

        if recursion:
            # first sub-platforms:
            subplatforms_with_errors = self._subplatforms_go_inactive()

            # we proceed with instruments even if some sub-platforms failed.

            # then my own instruments:
            instruments_with_errors = self._instruments_go_inactive()

            if len(subplatforms_with_errors) or len(instruments_with_errors):
                #
                # Do not proceed further. Note that events with the errors
                # should have already been published.
                #
                log.debug("%r: some sub-platforms or instruments failed to go_inactive. "
                          "Not proceeding with my own go_inactive. "
                          "subplatforms_with_errors=%s "
                          "instruments_with_errors=%s",
                          self._platform_id, subplatforms_with_errors, instruments_with_errors)
                return

        # then myself:
        result = self._go_inactive_this_platform()
        return result

    def _run(self, recursion):
        """
        Dispatches the RUN command.

        RUN is dispatched in a top-down fashion:
         - processes the command at this platform (which does nothing, actually)
         - if recursion is True, issues RUN to instruments
         - if recursion is True, issues RUN to sub-platforms
        """
        # first myself
        self._run_this_platform()

        if recursion:
            # first sub-platforms:
            self._instruments_run()

            # we proceed with instruments even if some sub-platforms failed.

            # then my own instruments:
            self._subplatforms_run()

        result = None
        return result

    def _reset(self, recursion):
        """
        Dispatches the RESET command.

        RESET is dispatched in a bottom-up fashion:
        - if recursion is True, reset the children
        - then reset this platform itself.

        @param recursion  If True, sub-platform are sent the RESET command
                          (with corresponding recursion parameter set to True),
                          and intruments are also sent RESET (no recursion
                          parameter is applicable to instruments).
        """

        log.debug("%r: _reset: recursion=%s", self._platform_id, recursion)

        if recursion:
            # first sub-platforms:
            subplatforms_with_errors = self._subplatforms_reset()

            # we proceed with instruments even if some sub-platforms failed.

            # then my own instruments:
            instruments_with_errors = self._instruments_reset()

            if len(subplatforms_with_errors) or len(instruments_with_errors):
                #
                # Do not proceed further. Note that events with the errors
                # should have already been published.
                #
                log.debug("%r: some sub-platforms or instruments failed to reset. "
                          "Not proceeding with my own reset. "
                          "subplatforms_with_errors=%s "
                          "instruments_with_errors=%s",
                          self._platform_id, subplatforms_with_errors, instruments_with_errors)
                return

        # then myself
        self._reset_this_platform()

    def _shutdown(self, recursion):
        """
        Dispatches a SHUTDOWN request.

        SHUTDOWN is dispatched in a bottom-up fashion:
        - if recursion is True, shuts down and terminates the sub-platforms
          and the instruments
        - then brings this platform agent to the uninitialized state.
          Note that the platform can not actually "shutdown" itself.

        If recursion==True and any sub-platform or instrument fails to shutdown
        or terminate, then this agent does NOT perform any actions on itself.

        @param recursion   True to "shutdown" children.
        """

        log.debug("%r: _shutdown: recursion=%s", self._platform_id, recursion)

        if recursion:
            # shutdown and terminate sub-platforms:
            subplatforms_with_errors = self._subplatforms_shutdown_and_terminate()

            # we proceed with instruments even if some sub-platforms failed.

            # shutdown and terminate instruments:
            instruments_with_errors = self._instruments_shutdown_and_terminate()

            if len(subplatforms_with_errors) or len(instruments_with_errors):
                #
                # Do not proceed further. Note that events with the errors
                # should have already been published.
                #
                log.debug("%r: some sub-platforms or instruments failed to shutdown "
                          "or terminate. Not proceeding with my own shutdown. "
                          "subplatforms_with_errors=%s "
                          "instruments_with_errors=%s",
                          self._platform_id, subplatforms_with_errors, instruments_with_errors)
                return

        # "shutdown" myself
        self._shutdown_this_platform()

    def _pause(self, recursion):
        """
        Dispatches the PAUSE command.

        PAUSE is dispatched in a bottom-up fashion:
        - if recursion is True, pause the children
        - then pause this platform itself.

        If recursion==True and any sub-platform or instrument fails the command,
        then this agent does NOT perform any actions on itself.

        @param recursion  If True, children are sent the PAUSE command
                          (with corresponding recursion parameter set to True
                           in the case of platforms).
        """
        if recursion:
            # first sub-platforms:
            subplatforms_with_errors = self._subplatforms_pause()

            # we proceed with instruments even if some sub-platforms failed.

            # then my own instruments:
            instruments_with_errors = self._instruments_pause()

            if len(subplatforms_with_errors) or len(instruments_with_errors):
                #
                # Do not proceed further. Note that events with the errors
                # should have already been published.
                #
                log.debug("%r: some sub-platforms or instruments failed to pause. "
                          "Not proceeding with my own pause. "
                          "subplatforms_with_errors=%s "
                          "instruments_with_errors=%s",
                          self._platform_id, subplatforms_with_errors, instruments_with_errors)
                return

        # then myself
        self._pause_this_platform()

    def _resume(self, recursion):
        """
        Dispatches the RESUME command.

        RESUME is dispatched in a bottom-up fashion:
        - if recursion is True, resume the children
        - then resume this platform itself.

        If recursion==True and any sub-platform or instrument fails the command,
        then this agent does NOT perform any actions on itself.

        @param recursion  If True, children are sent the RESUME command
                          (with corresponding recursion parameter set to True
                           in the case of platforms).
        """
        if recursion:
            # first sub-platforms:
            subplatforms_with_errors = self._subplatforms_resume()

            # we proceed with instruments even if some sub-platforms failed.

            # then my own instruments:
            instruments_with_errors = self._instruments_resume()

            if len(subplatforms_with_errors) or len(instruments_with_errors):
                #
                # Do not proceed further. Note that events with the errors
                # should have already been published.
                #
                log.debug("%r: some sub-platforms or instruments failed to resume. "
                          "Not proceeding with my own resume. "
                          "subplatforms_with_errors=%s "
                          "instruments_with_errors=%s",
                          self._platform_id, subplatforms_with_errors, instruments_with_errors)
                return

        # then myself
        self._resume_this_platform()

    def _clear(self, recursion):
        """
        Dispatches the CLEAR command.

        CLEAR is dispatched in a bottom-up fashion:
        - if recursion is True, "clear" the children
        - then "clear" this platform itself.

        If recursion==True and any sub-platform or instrument fails the command,
        then this agent does NOT perform any actions on itself.

        @param recursion  If True, children are sent the CLEAR command
                          (with corresponding recursion parameter set to True
                           in the case of platforms).
        """
        if recursion:
            # first sub-platforms:
            subplatforms_with_errors = self._subplatforms_clear()

            # we proceed with instruments even if some sub-platforms failed.

            # then my own instruments:
            instruments_with_errors = self._instruments_clear()

            if len(subplatforms_with_errors) or len(instruments_with_errors):
                #
                # Do not proceed further. Note that events with the errors
                # should have already been published.
                #
                log.debug("%r: some sub-platforms or instruments failed to clear. "
                          "Not proceeding with my own clear. "
                          "subplatforms_with_errors=%s "
                          "instruments_with_errors=%s",
                          self._platform_id, subplatforms_with_errors, instruments_with_errors)
                return

        # then myself:
        self._clear_this_platform()

    ##############################################################
    # main operations *not* involving commands to child agents
    ##############################################################

    def _start_resource_monitoring(self):
        """
        Starts resource monitoring.
        """
        self._assert_driver()

        self._platform_resource_monitor = PlatformResourceMonitor(
            self._platform_id, self._platform_attributes,
            self._get_attribute_values, self.evt_recv)

        self._platform_resource_monitor.start_resource_monitoring()

    def _stop_resource_monitoring(self):
        """
        Stops resource monitoring.
        """
        assert self._platform_resource_monitor is not None, \
            "_start_resource_monitoring must have been called first"

        self._platform_resource_monitor.destroy()
        self._platform_resource_monitor = None

    ##############################################################
    # LAUNCHING event handlers.
    ##############################################################

    def _handler_launching_launch_complete(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = PlatformAgentState.UNINITIALIZED
        result = None

        return next_state, result

    ##############################################################
    # UNINITIALIZED event handlers.
    ##############################################################

    def _handler_uninitialized_initialize(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_uninitialized_initialize", args, kwargs)

        next_state = PlatformAgentState.INACTIVE
        result = self._initialize(recursion)

        return next_state, result

    ##############################################################
    # INACTIVE event handlers.
    ##############################################################

    def _handler_inactive_reset(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_inactive_reset", args, kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._reset(recursion)

        return next_state, result

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_inactive_reset", args, kwargs)

        next_state = PlatformAgentState.IDLE
        result = self._go_active(recursion)

        return next_state, result

    ##############################################################
    # IDLE event handlers.
    ##############################################################

    def _handler_idle_reset(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_idle_reset", args, kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._reset(recursion)

        return next_state, result

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_idle_go_inactive", args, kwargs)

        next_state = PlatformAgentState.INACTIVE
        result = self._go_inactive(recursion)

        return next_state, result

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_idle_run", args, kwargs)

        next_state = PlatformAgentState.COMMAND
        result = self._run(recursion)

        return next_state, result

    ##############################################################
    # STOPPED event handlers.
    ##############################################################

    def _handler_stopped_resume(self, *args, **kwargs):
        """
        Transitions to COMMAND state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_stopped_resume", args, kwargs)

        next_state = PlatformAgentState.COMMAND
        result = self._resume(recursion)

        return next_state, result

    def _handler_stopped_clear(self, *args, **kwargs):
        """
        Transitions to IDLE state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_stopped_clear", args, kwargs)

        next_state = PlatformAgentState.IDLE
        result = self._clear(recursion)

        return next_state, result

    ##############################################################
    # COMMAND event handlers.
    ##############################################################

    def _handler_command_reset(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_command_reset", args, kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._reset(recursion)

        return next_state, result

    def _handler_command_clear(self, *args, **kwargs):
        """
        Transitions to IDLE state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_command_clear", args, kwargs)

        next_state = PlatformAgentState.IDLE
        result = self._clear(recursion)

        return next_state, result

    def _handler_command_pause(self, *args, **kwargs):
        """
        Transitions to STOPPED state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_command_pause", args, kwargs)

        next_state = PlatformAgentState.STOPPED
        result = self._pause(recursion)

        return next_state, result

    ##############################################################
    # Capabilities interface and event handlers.
    ##############################################################

    def _handler_get_resource_capabilities(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._plat_driver.get_resource_capabilities(*args, **kwargs)
        next_state = None
        return next_state, result

    def _filter_capabilities(self, events):

        events_out = [x for x in events if PlatformAgentCapability.has(x)]
        return events_out

    ##############################################################
    # Resource interface and common resource event handlers.
    ##############################################################

    def _handler_get_resource(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._plat_driver.get_resource(*args, **kwargs)
        return None, result

    def _handler_set_resource(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._plat_driver.set_resource(*args, **kwargs)
        return None, result

    def _handler_execute_resource(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._plat_driver.execute_resource(*args, **kwargs)
        return None, result

    def _handler_get_resource_state(self, *args, **kwargs):
        result = self._plat_driver.get_resource_state(*args, **kwargs)
        return None, result

    def _handler_ping_resource(self, *args, **kwargs):
        """
        Pings the driver.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._trigger_driver_event(PlatformDriverEvent.PING)

        return None, result

    ##############################################################
    # Resource monitoring
    ##############################################################

    def _handler_start_resource_monitoring(self, *args, **kwargs):
        """
        Starts resource monitoring and transitions to MONITORING state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        try:
            result = self._start_resource_monitoring()

            next_state = PlatformAgentState.MONITORING

        except:
            log.exception("error in _start_resource_monitoring") #, exc_Info=True)
            raise

        return (next_state, result)

    def _handler_stop_resource_monitoring(self, *args, **kwargs):
        """
        Stops resource monitoring and transitions to COMMAND state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        try:
            result = self._stop_resource_monitoring()

            next_state = PlatformAgentState.COMMAND

        except:
            log.exception("error in _stop_resource_monitoring") #, exc_Info=True)
            raise

        return (next_state, result)

    ##############################################################
    # LOST_CONNECTION event handlers.
    ##############################################################

    def _handler_lost_connection_enter(self, *args, **kwargs):
        """
        Publishes a ResourceAgentConnectionLostErrorEvent
        and starts reconnection greenlet.
        """
        super(PlatformAgent, self)._common_state_enter(*args, **kwargs)
        log.error("%r: (LC) lost connection to the device. Will attempt to reconnect...",
                  self._platform_id)

        self._event_publisher.publish_event(
            event_type='ResourceAgentConnectionLostErrorEvent',
            origin_type=self.ORIGIN_TYPE,
            origin=self.resource_id)

        # Setup reconnect timer.
        self._autoreconnect_greenlet = spawn(self._autoreconnect)

    def _handler_lost_connection_exit(self, *args, **kwargs):
        """
        Updates flag to stop the reconnection greenlet.
        """
        self._autoreconnect_greenlet = None
        super(PlatformAgent, self)._common_state_exit(*args, **kwargs)

    def _autoreconnect(self):
        """
        Calls self._fsm.on_event(ResourceAgentEvent.AUTORECONNECT) every few
        seconds.
        """
        log.debug("%r: (LC) _autoreconnect starting", self._platform_id)
        while self._autoreconnect_greenlet:
            sleep(5)
            if self._autoreconnect_greenlet:
                try:
                    self._fsm.on_event(ResourceAgentEvent.AUTORECONNECT)
                except:
                    pass

        log.debug("%r: (LC) _autoreconnect ended", self._platform_id)

    def _handler_lost_connection_autoreconnect(self, *args, **kwargs):

        log.debug("%r: (LC) _handler_lost_connection_autoreconnect: trying CONNECT...",
                  self._platform_id)
        try:
            driver_state = self._trigger_driver_event(PlatformDriverEvent.CONNECT)

        except Exception as e:
            log.debug("%r: (LC) Exception while trying CONNECT: %s", self._platform_id, e)
            return None, None

        assert driver_state == PlatformDriverState.CONNECTED

        if self._state_when_lost == PlatformAgentState.MONITORING:
            self._start_resource_monitoring()

        next_state = self._state_when_lost

        log.debug("%r: (LC) _handler_lost_connection_autoreconnect: next_state=%s",
                  self._platform_id, next_state)

        return next_state, None

    def _handler_lost_connection_reset(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_lost_connection_reset", args, kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._reset(recursion)

        return next_state, result

    def _handler_lost_connection_go_inactive(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_lost_connection_go_inactive", args, kwargs)

        next_state = PlatformAgentState.INACTIVE
        result = self._go_inactive(recursion)

        return next_state, result

    ##############################################################
    # some common event handlers.
    ##############################################################

    def _handler_connection_lost_driver_event(self, *args, **kwargs):
        """
        Handle a connection lost event from the driver.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        self._state_when_lost = self.get_agent_state()
        log.debug("%r: (LC) _handler_connection_lost_driver_event: _state_when_lost=%s",
                  self._platform_id, self._state_when_lost)

        if self._platform_resource_monitor:
            self._stop_resource_monitoring()

        return PlatformAgentState.LOST_CONNECTION, None

    def _handler_shutdown(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_shutdown", args, kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._shutdown(recursion)

        return next_state, result

    ##############################################################
    # Base class overrides for state and cmd error alerts.
    ##############################################################

    # TODO incoporate the following as appropriate
    """
    Some version of this code needs to be placed wherever a sample arrives
    for publication.

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
    """    

    def _on_state_enter(self, state):
        if self._aam:
            self._aam.process_alerts(state=state)

    def _on_command_error(self, cmd, execute_cmd, args, kwargs, ex):
        if self._aam:
            self._aam.process_alerts(command=execute_cmd, command_success=False)
        super(PlatformAgent, self)._on_command_error(cmd, execute_cmd, args,
                                                     kwargs, ex)

    ##############################################################
    # FSM setup.
    ##############################################################

    def _construct_fsm(self, states=PlatformAgentState, events=PlatformAgentEvent):
        """
        """
        log.debug("constructing fsm")

        super(PlatformAgent, self)._construct_fsm(states=states,
                                                  events=events)

        # LAUNCHING state event handlers.
        self._fsm.add_handler(PlatformAgentState.LAUNCHING, PlatformAgentEvent.LAUNCH_COMPLETE, self._handler_launching_launch_complete)

        # UNINITIALIZED state event handlers.
        self._fsm.add_handler(PlatformAgentState.UNINITIALIZED, PlatformAgentEvent.INITIALIZE, self._handler_uninitialized_initialize)
        self._fsm.add_handler(PlatformAgentState.UNINITIALIZED, PlatformAgentEvent.SHUTDOWN, self._handler_shutdown)

        # INACTIVE state event handlers.
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.RESET, self._handler_inactive_reset)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.SHUTDOWN, self._handler_shutdown)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GO_ACTIVE, self._handler_inactive_go_active)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # IDLE state event handlers.
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.SHUTDOWN, self._handler_shutdown)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RUN, self._handler_idle_run)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)

        # STOPPED state event handlers.
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.RESUME, self._handler_stopped_resume)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.CLEAR, self._handler_stopped_clear)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)

        # COMMAND and MONITORING common state event handlers.
        for state in [PlatformAgentState.COMMAND, PlatformAgentState.MONITORING]:
            self._fsm.add_handler(state, PlatformAgentEvent.RESET, self._handler_command_reset)
            self._fsm.add_handler(state, PlatformAgentEvent.SHUTDOWN, self._handler_shutdown)
            self._fsm.add_handler(state, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
            self._fsm.add_handler(state, PlatformAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
            self._fsm.add_handler(state, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
            self._fsm.add_handler(state, PlatformAgentEvent.GET_RESOURCE, self._handler_get_resource)
            self._fsm.add_handler(state, PlatformAgentEvent.SET_RESOURCE, self._handler_set_resource)
            self._fsm.add_handler(state, PlatformAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
            self._fsm.add_handler(state, PlatformAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)

        # additional COMMAND state event handlers.
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.PAUSE, self._handler_command_pause)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.CLEAR, self._handler_command_clear)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.START_MONITORING, self._handler_start_resource_monitoring)

        # additional MONITORING state event handlers.
        self._fsm.add_handler(PlatformAgentState.MONITORING, PlatformAgentEvent.STOP_MONITORING, self._handler_stop_resource_monitoring)

        # LOST_CONNECTION state event handlers.
        self._fsm.add_handler(PlatformAgentState.LOST_CONNECTION, PlatformAgentEvent.ENTER, self._handler_lost_connection_enter)
        self._fsm.add_handler(PlatformAgentState.LOST_CONNECTION, PlatformAgentEvent.EXIT, self._handler_lost_connection_exit)
        self._fsm.add_handler(PlatformAgentState.LOST_CONNECTION, PlatformAgentEvent.RESET, self._handler_lost_connection_reset)
        self._fsm.add_handler(PlatformAgentState.LOST_CONNECTION, PlatformAgentEvent.AUTORECONNECT, self._handler_lost_connection_autoreconnect)
        self._fsm.add_handler(PlatformAgentState.LOST_CONNECTION, PlatformAgentEvent.GO_INACTIVE, self._handler_lost_connection_go_inactive)
        self._fsm.add_handler(PlatformAgentState.LOST_CONNECTION, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(PlatformAgentState.LOST_CONNECTION, PlatformAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)

