#!/usr/bin/env python

"""
@package ion.agents.platform.platform_agent
@file    ion/agents/platform/platform_agent.py
@author  Carlos Rueda
@brief   Supporting types for platform agents.
"""

__author__ = 'Carlos Rueda'


from pyon.public import log, RT
from pyon.agent.agent import ResourceAgent
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from interface.objects import AgentCommand
from pyon.agent.agent import ResourceAgentClient

from pyon.event.event import EventSubscriber

from pyon.core.exception import NotFound, Inconsistent
from pyon.core.exception import BadRequest

from pyon.core.governance import ORG_MANAGER_ROLE, GovernanceHeaderValues, has_org_role, get_valid_resource_commitments, ION_MANAGER
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE, OBSERVATORY_OPERATOR_ROLE

from ion.agents.platform.platform_agent_enums import PlatformAgentState
from ion.agents.platform.platform_agent_enums import PlatformAgentEvent
from ion.agents.platform.platform_agent_enums import PlatformAgentCapability
from ion.agents.platform.platform_agent_enums import ResourceInterfaceCapability

from ion.agents.platform.exceptions import PlatformDriverException
from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.exceptions import PlatformConfigurationException
from ion.agents.platform.platform_driver_event import AttributeValueDriverEvent
from ion.agents.platform.platform_driver_event import ExternalEventDriverEvent
from ion.agents.platform.platform_driver_event import StateChangeDriverEvent
from ion.agents.platform.platform_driver_event import AsyncAgentEvent
from ion.agents.platform.exceptions import CannotInstantiateDriverException
from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.platform.util.network_util import NetworkDefinitionException
from ion.agents.agent_alert_manager import AgentAlertManager

from ion.agents.platform.platform_driver import PlatformDriverEvent, PlatformDriverState
from ion.core.includes.mi import DriverEvent

from pyon.util.containers import DotDict

from pyon.agent.instrument_fsm import FSMStateError
from pyon.agent.instrument_fsm import FSMError

from ion.agents.platform.launcher import Launcher

from ion.agents.platform.platform_resource_monitor import PlatformResourceMonitor

from ion.agents.platform.status_manager import StatusManager
from ion.agents.platform.platform_agent_stream_publisher import PlatformAgentStreamPublisher

from ion.agents.platform.mission_manager import MissionManager

from ion.agents.instrument.instrument_agent import InstrumentAgentState
from ion.agents.instrument.instrument_agent import InstrumentAgentEvent

import logging
import time
from gevent import Greenlet
from gevent import sleep
from gevent import spawn
from gevent.event import AsyncResult
from gevent.coros import RLock

import pprint


PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


class PlatformAgentAlertManager(AgentAlertManager):
    """
    Overwrites _update_aggstatus and do appropriate handling.
    """
    def _update_aggstatus(self, aggregate_type, new_status, alerts_list=None):
        """
        Called to set a new status value for an aggstatus type.
        Note that an update to an aggstatus in a platform does not trigger
        any event publication by itself. Rather, this update may cascade to
        an update of the corresponding rollup_status, in which case an event
        does get published. StatusManager takes care of that handling.
        """
        log.debug("%r: _update_aggstatus called", self._agent._platform_id)
        # alerts_list passed per OOIION-1275
        self._agent._status_manager.set_aggstatus(aggregate_type, new_status, alerts_list)


_INVALIDATED_CHILD = "INVALIDATED"


class PlatformAgent(ResourceAgent):
    """
    Platform resource agent.
    """

    COMMAND_EVENT_TYPE = "DeviceCommandEvent"

    ORIGIN_TYPE = "PlatformDevice"

    def __init__(self):
        log.info("PlatformAgent constructor called")
        ResourceAgent.__init__(self)

        #This is the type of Resource managed by this agent
        self.resource_type = RT.PlatformDevice

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

        # {subplatform_id: DotDict(ResourceAgentClient, PID), ...}
        # (or subplatform_id : _INVALIDATED_CHILD)
        self._pa_clients = {}  # Never None

        # see on_init
        self._launcher = None

        # {instrument_id: DotDict(ResourceAgentClient, PID), ...}
        # (or instrument_id : _INVALIDATED_CHILD)
        self._ia_clients = {}  # Never None

        # self.CFG.endpoint.receive.timeout -- see on_init
        self._timeout = None

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
        # mission execution handling:

        # MissionManager created in _initialize_this_platform and destroyed in
        # _reset_this_platform
        self._mission_manager = None

        # state to return to after mission termination
        self._pre_mission_state = None

        # to sync with mission execution
        self._mission_greenlet = None

        #####################################
        # lost_connection handling:

        # Autoreconnect thread.
        self._autoreconnect_greenlet = None

        # State when lost.
        self._state_when_lost = None

        #####################################
        # Agent stream publisher.
        self._asp = None

        ##################################################################
        # children validation upon reception of regular status events

        # resource_id's of children being currently validated
        # (each one is dispatched in a greenlet)
        self._children_being_validated = set()
        self._children_being_validated_lock = RLock()

        #
        # TODO overall synchronization is also needed in other places!
        #
        from ion.agents.platform.schema import get_schema

        self._agent_schema = get_schema()

        self._provider_id = None
        self._actor_id = None

        #####################################
        log.info("PlatformAgent constructor complete.")

        # for debugging purposes
        self._pp = pprint.PrettyPrinter()

    def on_init(self):
        """
        - Validates the given configuration and does related preparations.
        - configures aparams if not already config'ed (see _configure_aparams)
        """
        super(PlatformAgent, self).on_init()
        log.trace("on_init")

        platform_id = self._validate_configuration()

        #######################################################################
        # CFG.endpoint.receive.timeout: if the value is less than the one we
        # have been using successfully for a while, log a warning and use the
        # larger value.
        #
        # TODO actually just use whatever timeout is given from configuration.
        # The adjustment here should be considered temporary while there's an
        # appropriate mechanism to guarantee patched values are seen.
        #
        cfg_timeout = self.CFG.get_safe("endpoint.receive.timeout", 0)
        log.info("%r: === CFG.endpoint.receive.timeout = %s", platform_id, cfg_timeout)
        if cfg_timeout < 180:
            log.warn("%r: !!! CFG.endpoint.receive.timeout=%s < 180. "
                     "You may want to review your pyon{.local}.yml, @patch.dict, "
                     "etc. or adjust the code in this method as appropriate. "
                     "For now, will use 180 based on previous testing.",
                     platform_id, cfg_timeout)
            cfg_timeout = 180
        self._timeout = cfg_timeout
        #######################################################################

        self._plat_config_processed = False

        self._launcher = Launcher(self._timeout)

        if log.isEnabledFor(logging.DEBUG):  # pragma: no cover
            log.debug("%r: self._timeout = %s", platform_id, self._timeout)
            outname = "logs/platform_CFG_received_%s.txt" % platform_id
            try:
                pprint.PrettyPrinter(stream=file(outname, "w")).pprint(self.CFG)
                log.debug("%r: on_init: CFG printed to %s", platform_id, outname)
            except Exception as e:
                log.warn("%r: on_init: error printing CFG to %s: %s", platform_id, outname, e)

    def on_start(self):
        """
        - creates AgentAlertManager and StatusManager
        - Children agents (platforms and instruments) are launched here (in a
          separate greenlet).
        """
        super(PlatformAgent, self).on_start()
        log.info('%r: platform agent is running: on_start called.', self._platform_id)

        # Set up alert manager.
        self._aam = PlatformAgentAlertManager(self)

        # create StatusManager now that we have all needed elements:
        self._status_manager = StatusManager(self)

        if self._configure_aparams_arg:
            # see explanation in _configure_aparams
            self._do_configure_aparams(self._configure_aparams_arg)
            self._configure_aparams_arg = None

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

        if self._plat_config_processed:
            return

        log.debug("verifying/processing platform config configuration ...")

        self._plat_config = self.CFG.get("platform_config", None)
        if self._plat_config is None:
            msg = "'platform_config' entry not provided in agent configuration"
            log.error(msg)
            raise PlatformConfigurationException(msg=msg)

        platform_id = self._plat_config.get('platform_id', None)
        if platform_id is None:
            msg = "'platform_id' entry not found in 'platform_config'"
            log.error(msg)
            raise PlatformConfigurationException(msg=msg)

        self._platform_id = platform_id

        stream_info = self.CFG.get('stream_config', None)
        if stream_info is None:
            msg = "%r: 'stream_config' key not in configuration" % self._platform_id
            log.error(msg)
        for stream_name, stream_config in stream_info.iteritems():
            if 'stream_def_dict' not in stream_config:
                msg = "%r: 'stream_def_dict' key not in configuration for stream %r" % (
                    self._platform_id, stream_name)
                log.error(msg)

        self._driver_config = self.CFG.get('driver_config', None)
        if None is self._driver_config:
            msg = "%r: 'driver_config' key not in configuration" % self._platform_id
            log.error(msg)
            raise PlatformConfigurationException(msg=msg)

        log.debug("%r: driver_config: %s", self._platform_id, self._driver_config)

        for k in ['dvr_mod', 'dvr_cls']:
            if not k in self._driver_config:
                msg = "%r: %r key not given in driver_config: %s" % (
                    self._platform_id, k, self._driver_config)
                log.error(msg)
                raise PlatformConfigurationException(msg=msg)

        self._create_network_definition_from_CFG()

        # verify the given platform_id is contained in the NetworkDefinition:
        if not self._platform_id in self._network_definition.pnodes:
            msg = "platform_id=%r not found in network definition." % self._platform_id
            log.error(msg)
            raise PlatformConfigurationException(msg=msg)

        # get PlatformNode corresponding to this agent:
        self._pnode = self._network_definition.pnodes[self._platform_id]
        log.debug("%r: _validate_configuration: _pnode = %s", self._platform_id, self._pnode)

        self._children_resource_ids = self._get_children_resource_ids()

        if 'ports' in self._driver_config:
            # Remove this device from the ports information, the driver does not use this
            platform_port = self._driver_config['ports'].pop(self.resource_id, None)
            log.debug('%r: _validate_configuration removed platform port info from ports config for driver.  '
                      'dev_id:  %s   platform_port: %s', self._platform_id, self.resource_id, platform_port)

        ppid = self._plat_config.get('parent_platform_id', None)
        if ppid:
            self._parent_platform_id = ppid
            log.debug("%r: _parent_platform_id set to: %s", self._platform_id, self._parent_platform_id)

        self._provider_id = self.CFG.get('provider_id', None)
        if self._provider_id is None:
            log.warn("%r: [xa] 'provider_id' key not in configuration", self._platform_id)
        self._actor_id = self.CFG.get('actor_id', None)
        if self._actor_id is None:
            log.warn("%r: [xa] 'actor_id' key not in configuration", self._platform_id)

        self._plat_config_processed = True

        log.debug("%r: _validate_configuration complete", self._platform_id)
        return self._platform_id

    def _create_network_definition_from_CFG(self):
        """
        @raises PlatformException if any exception
        """
        try:
            self._network_definition = NetworkUtil.create_network_definition_from_ci_config(self.CFG)
            log.debug("%r: created network_definition from CFG", self._platform_id)
        except NetworkDefinitionException as ex:
            # just re-raise as PlatformConfigurationException
            raise PlatformConfigurationException(msg=ex.msg)
        except Exception as ex:
            log.exception("unexpected exception in NetworkUtil.create_network_definition_from_ci_config")
            raise PlatformException(reason=ex)

    def _children_launch(self):
        """
        Method for the greenlet started in on_start.
        Launches the sub-platform agents.
        Launches the associated instrument agents;
        """
        log.info("%r: _children_launch greenlet: launching sub-platform agents", self._platform_id)
        self._subplatforms_launch()
        log.info("%r: _children_launch greenlet: launching sub-platform agents completed", self._platform_id)

        log.info("%r: _children_launch greenlet: launching instrument agents", self._platform_id)
        self._instruments_launch()
        log.info("%r: _children_launch greenlet: launching instrument agents completed", self._platform_id)

        # Ready, children launched and event subscribers in place
        self._async_children_launched.set()

        # launch complete so let FSM transition to UNINITIALIZED
        self._fsm.on_event(PlatformAgentEvent.LAUNCH_COMPLETE)
        log.info("%r: _children_launch greenlet stopping", self._platform_id)

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
            if sub_resource_id:
                resource_ids.append(sub_resource_id)
            else:
                log.error("%r: _get_children_resource_ids: agent.resource_id must be present for sub-platform child %r",
                          self._platform_id, subplatform_id)

        # get resource_ids of instruments:
        for instrument_id in pnode.instruments:
            inode = pnode.instruments[instrument_id]
            agent_config = inode.CFG
            resource_id = agent_config.get("agent", {}).get("resource_id", None)
            if resource_id:
                resource_ids.append(resource_id)
            else:
                log.error("%r: _get_children_resource_ids: agent.resource_id must be present for instrument child %r",
                          self._platform_id, instrument_id)

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
    # *_this_platform operations are operations that act on the
    # platform itself (these are called by operations that also dispatch
    # the associated command on the children)
    ##############################################################

    def _initialize_this_platform(self):
        """
        Does the main initialize sequence for this platform: creation of
        publishers, creation/configuration of the driver, creation of mission
        manager.
        """
        self._asp = PlatformAgentStreamPublisher(self)
        self._create_driver()
        self._configure_driver()
        self._mission_manager = MissionManager(self)
        log.debug("%r: _initialize_this_platform completed.", self._platform_id)

    def _go_active_this_platform(self, recursion=None):
        """
        Connects the driver
        and resets stream publisher connection id and index.
        """
        log.debug("%r: triggering driver event CONNECT", self._platform_id)

        kwargs = dict(recursion=recursion)
        self._trigger_driver_event(PlatformDriverEvent.CONNECT, **kwargs)

        self._asp.reset_connection()

    def _go_inactive_this_platform(self, recursion=None):
        """
        Disconnects the driver.
        """
        log.debug("%r: triggering driver event DISCONNECT", self._platform_id)
        kwargs = dict(recursion=recursion)
        self._trigger_driver_event(PlatformDriverEvent.DISCONNECT, **kwargs)

    def _run_this_platform(self):
        """
        Nothing is done here. The method exists for consistency.
        Basically the RUN command is just to move the FSM to the COMMAND state,
        which is done by the handler.
        """
        pass

    def _reset_this_platform(self):
        """
        Resets this platform agent: stops resource monitoring,
        destroys driver, resets publishers, destroys mission manager.

        The "platform_config" configuration object provided via self.CFG
        (see on_init) is kept. This allows to issue the INITIALIZE command
        again with the already configured agent.
        """
        log.debug("%r: resetting this platform", self._platform_id)

        if self._platform_resource_monitor:
            self._stop_resource_monitoring(recursion=True)

        if self._plat_driver:
            # disconnect driver if connected:
            driver_state = self._plat_driver._fsm.get_current_state()
            log.debug("%r: _reset_this_platform: driver_state = %s", self._platform_id, driver_state)
            if driver_state == PlatformDriverState.CONNECTED:
                self._trigger_driver_event(PlatformDriverEvent.DISCONNECT)
            # destroy driver:
            self._plat_driver.destroy()
            self._plat_driver = None
            self._resource_schema = {}

        if self._asp:
            self._asp.reset()

        if self._mission_manager:
            self._mission_manager.destroy()
            self._mission_manager = None

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

    ##############################################################
    # misc supporting routines
    ##############################################################

    def _create_driver(self):
        """
        Creates the platform driver object for this platform agent.

        NOTE: the driver object is created directly (not via a spawned process)
        """
        driver_module = self._driver_config['dvr_mod']
        driver_class  = self._driver_config['dvr_cls']

        if log.isEnabledFor(logging.DEBUG):
            log.debug('%r: creating driver: driver_module=%s driver_class=%s',
                      self._platform_id, driver_module, driver_class)

        try:
            module = __import__(driver_module, fromlist=[driver_class])
            classobj = getattr(module, driver_class)
            driver = classobj(self._pnode, self.evt_recv,
                              self._create_event_subscriber,
                              self._destroy_event_subscriber)

        except Exception as e:
            msg = '%r: could not import/construct driver: module=%r, class=%r' % (
                self._platform_id, driver_module, driver_class)
            log.exception("%s", msg)  #, exc_Info=True)
            raise CannotInstantiateDriverException(msg=msg, reason=e)

        self._plat_driver = driver

        log.debug("%r: driver created: %s", self._platform_id, driver)

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
        log.debug('%r: configuring driver: %s', self._platform_id, self._driver_config)

        self._trigger_driver_event(PlatformDriverEvent.CONFIGURE, driver_config=self._driver_config)

        curr_state = self._plat_driver._fsm.get_current_state()
        if PlatformDriverState.DISCONNECTED != curr_state:
            msg = "%r: _configure_driver: expected driver state to be %s but got %s" % (
                  self._platform_id, PlatformDriverState.DISCONNECTED, curr_state)
            log.error(msg)
            raise PlatformDriverException(msg)

        self._resource_schema = self._plat_driver.get_config_metadata()

        log.debug("%r: driver configured.", self._platform_id)

    def _get_attribute_values(self, attrs):
        """
        The callback for resource monitoring.
        This method will return None is case of lost connection.
        """
        kwargs = dict(attrs=attrs)
        result = self._plat_driver.get_resource(**kwargs)
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
            self._asp.handle_attribute_value_event(driver_event)
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
            log.warn('%r: driver_event not handled: %s', self._platform_id, str(type(driver_event)))
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
        except Exception:
            log.exception('%r: platform agent could not publish driver state change event',
                          self._platform_id)

    def _dispatch_value_alerts(self, stream_name, param_name, vals):
        """
        Dispatches alerts related with the values that were just generated.
        Note: for convenience at the moment, the AgentAlertManager.process_alerts
        call is done for each value in the vals list. A future version may include
        a more elaborated algorithm to analyze the sequence for alert purposes.
        """
        for value in vals:
            if value is not None:
                log.trace('%r: to call process_alerts: stream_name=%r '
                          'value_id=%r value=%s',
                          self._platform_id, stream_name, param_name, value)
                self._aam.process_alerts(stream_name=stream_name,
                                         value=value, value_id=param_name)

    def _handle_external_event_driver_event(self, driver_event):
        """
        Dispatches any needed handling arising from the external the event.
        """
        # TODO any needed external event dispatch handling.

        evt = driver_event.event_instance

        # log.debug("%r: handling external platform event: %s", self._platform_id, evt)

    def _async_driver_event_agent_event(self, event):
        """
        Driver initiated agent FSM event.
        """
        log.debug("%r/%s: received _async_driver_event_agent_event=%s",
                  self._platform_id, self.get_agent_state(), event)

        try:
            self._fsm.on_event(event)

        except Exception:
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

    def _child_terminated(self, child_resource_id):
        """
        Called by StatusManager upon the reception of a process lifecycle
        TERMINATED event to invalidate the child associated to the given
        resource_id.

        @param child_resource_id
        """
        if child_resource_id in self._ia_clients:
            self._ia_clients[child_resource_id] = _INVALIDATED_CHILD
            log.debug("%r: OOIION-1077 _child_terminated: instrument: %r",
                      self._platform_id, child_resource_id)
            return

        if child_resource_id in self._pa_clients:
            self._pa_clients[child_resource_id] = _INVALIDATED_CHILD
            log.debug("%r: OOIION-1077 _child_terminated: sub-platform: %r",
                      self._platform_id, child_resource_id)
            return

        log.trace("%r: OOIION-1077 _child_terminated: %r is not a direct child",
                  self._platform_id, child_resource_id)

    def _get_invalidated_children(self):
        """
        For diagnostic purposes.
        @return list of reource_ids of children that have been
        notified by the status manager as terminated and that
        have not been revalidated.
        """
        res      = [i_id for i_id in self._ia_clients
                    if self._ia_clients[i_id] == _INVALIDATED_CHILD]
        res.extend([p_id for p_id in self._pa_clients
                    if self._pa_clients[p_id] == _INVALIDATED_CHILD])
        return res

    def _child_running(self, child_resource_id):
        """
        Called by StatusManager upon the reception of any regular status
        event from a child as an indication that the child is running.
        Here we "revalidate" the child in case it was invalidated.
        This is done in a separate greenlet to return quikcly to the event
        handler in the status manager. The greenlet does a number of attempts
        with some wait in between to allow the creation of the new
        ResourceAgentClient (and the retrieval of associated PID) to be successful.
        (In initial tests it was noticed that the child may have generated status
        events but the immediate creation of the ResourceAgentClient here was failing).

        @param child_resource_id
        """

        with self._children_being_validated_lock:
            if child_resource_id in self._children_being_validated:
                return

            if child_resource_id in self._ia_clients and \
               self._ia_clients[child_resource_id] == _INVALIDATED_CHILD:

                self._children_being_validated.add(child_resource_id)
                log.info("%r: OOIION-1077 starting _validate_child_greenlet for "
                         "instrument: %r", self._platform_id, child_resource_id)
                Greenlet.spawn(self._validate_child_greenlet, child_resource_id, True)
                return

            if child_resource_id in self._pa_clients and \
               self._pa_clients[child_resource_id] == _INVALIDATED_CHILD:

                self._children_being_validated.add(child_resource_id)
                log.info("%r: OOIION-1077 starting _validate_child_greenlet for "
                         "platform: %r", self._platform_id, child_resource_id)
                Greenlet.spawn(self._validate_child_greenlet, child_resource_id, False)
                return

        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            if not child_resource_id in self._ia_clients and \
               not child_resource_id in self._pa_clients:
                log.trace("%r: OOIION-1077 _child_running: %r is not a direct child",
                          self._platform_id, child_resource_id)

    def _validate_child_greenlet(self, child_resource_id, is_instrument):
        #
        # TODO synchronize access to self._ia_clients or self._pa_clients in
        # general.
        #
        max_attempts = 12
        attempt_period = 5   # so 12 x 5 = 60 secs max attempt time

        attempt = 0
        last_exc = None
        trace = None

        if is_instrument:
            dic = self._ia_clients
            key = "ia_client"
        else:
            dic = self._pa_clients
            key = "pa_client"

        while attempt <= max_attempts:
            attempt += 1
            log.info("%r: OOIION-1077 _validate_child_greenlet=%r attempt=%d",
                     self._platform_id, child_resource_id, attempt)
            sleep(attempt_period)

            # get new elements:
            try:
                a_client = self._create_resource_agent_client(child_resource_id,
                                                              child_resource_id)
                pid = ResourceAgentClient._get_agent_process_id(child_resource_id)

                dic[child_resource_id] = DotDict({
                    key:           a_client,
                    'pid':         pid,
                    'resource_id': child_resource_id})

                log.info("%r: OOIION-1077 _child_running: revalidated child "
                         "with resource_id=%r, new pid=%r",
                         self._platform_id, child_resource_id, pid)

                last_exc = None
                break  # success

            except Exception as ex:
                import traceback
                last_exc = ex
                trace = traceback.format_exc()

        with self._children_being_validated_lock:
            # remove from list regardless of successful revalidation or not:
            self._children_being_validated.remove(child_resource_id)

        if last_exc:
            log.warn("%r: OOIION-1077 _validate_child_greenlet: could not "
                     "revalidate child=%r after %d attempts: exception=%s\n%s",
                     self._platform_id, child_resource_id, max_attempts, last_exc, trace)

    def _execute_agent(self, poi, a_client, cmd, sub_id):
        """
        Calls execute_agent with the given cmd on the given client.

        @param poi        "platform" or "instrument" for logging
        @param a_client   Resource agent client (platform or instrument)
        @param cmd        the command to execute
        @param sub_id     for logging
        """
        log.debug("%r: _execute_agent: cmd=%r %s=%r ...", self._platform_id, cmd.command, poi, sub_id)

        time_start = time.time()

        if ResourceAgentEvent.has(cmd.command) or PlatformAgentEvent.has(cmd.command):
            retval = a_client.execute_agent(cmd, timeout=self._timeout)
        elif DriverEvent.has(cmd.command) :
            retval = a_client.execute_resource(cmd, timeout=self._timeout)
        else:
            log.error('%r: _execute_agent invalid command: %s is not ResourceAgentEvent or DriverEvent', self._platform_id, cmd)

        if log.isEnabledFor(logging.DEBUG):
            elapsed_time = time.time() - time_start
            log.debug("%r: _execute_agent: cmd=%r %s=%r elapsed_time=%s",
                      self._platform_id, cmd.command, poi, sub_id, elapsed_time)

        return retval

    def _get_recursion_parameter(self, method_name, *args, **kwargs):
        """
        Utility to extract the 'recursion' parameter.

        @param method_name   for logging purposes
        @param *args         as received by FSM handler (not used)
        @param *kwargs       as received by FSM handler
        @return              The boolean value of the 'recursion' parameter in
                             kwargs; True by default.
        """
        recursion = kwargs.get('recursion', None)
        if recursion is None:
            log.info("%r: %s called with no recursion parameter. Using recursion=True by default.",
                     self._platform_id, method_name)
            recursion = True
        else:
            log.info("%r: %s called with recursion parameter: %r", self._platform_id, method_name, recursion)
            recursion = bool(recursion)

        return recursion

    def _create_event_subscriber(self, **kwargs):
        """
        Creates and returns an EventSubscriber, which is registered with the
        Managed Endpoint API.

        @param kwargs passed to the EventSubscriber constructor.

        @see https://lists.oceanobservatories.org/mailman/private/ciswdev/2013-April/001668.html
        @see https://jira.oceanobservatories.org/tasks/browse/OOIION-987
        """
        sub = EventSubscriber(**kwargs)
        self.add_endpoint(sub)
        return sub

    def _destroy_event_subscriber(self, sub):
        """
        Destroys an EventSubscriber created with _create_event_subscriber.

        @param sub   subscriber
        """
        #self.remove_endpoint(sub) -- why this is making tests fail?
        # TODO determine whether self.remove_endpoint is the appropriate call
        # here and if so, how it should be used. For now, calling sub.close()
        # (this only change made the difference between successful tests and
        # failing tests that actually never exited -- I had to kill them).
        # Update 19/Sep/2013: I thought that remove_endpoint was now working
        # but not!  It seems I did testing with the --with-pycc flag, but
        # using it actually causes the same behavior as noted above a few
        # months ago. So, keeping the use of close() again.
        # See https://jira.oceanobservatories.org/tasks/browse/OOIION-987
        sub.close()

        # per discussion with JC also calling self.remove_endpoint(sub)
        self.remove_endpoint(sub)

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

        subscriber = self._create_event_subscriber(event_type="ResourceAgentStateEvent",
                                                   origin=origin,
                                                   callback=consume_event)

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

        # wait for the event:
        log.debug("%r: _await_state: _timeout=%s", self._platform_id, self._timeout)
        async_res.get(timeout=self._timeout)

        self._destroy_event_subscriber(sub)

    ##############################################################
    # supporting routines dealing with sub-platforms
    ##############################################################

    def _launch_platform_agent(self, subplatform_id):
        """
        Launches a sub-platform agent (if not already running) and waits until
        the sub-platform transitions to UNINITIALIZED state.
        It creates corresponding ResourceAgentClient,
        set entry self._pa_clients[subplatform_id],
        and publishes device_added event.

        The mechanism to detect whether the sub-platform agent is already
        running is by simply trying to create a ResourceAgentClient to it.

        @param subplatform_id Platform ID
        """

        # get PlatformNode, corresponding CFG, and resource_id:
        sub_pnode = self._pnode.subplatforms[subplatform_id]
        sub_agent_config = sub_pnode.CFG
        sub_resource_id = sub_agent_config.get("agent", {}).get("resource_id", None)

        if sub_resource_id is None:
            log.error("%r: _launch_platform_agent: agent.resource_id must be present for child %r", self._platform_id, subplatform_id)
            return

        # first, is the agent already running?
        pa_client = None  # assume it's not.
        pid = None
        try:
            # try to connect:
            log.debug("%r: [LL] trying to determine whether my child is already running: %r",
                      self._platform_id, subplatform_id)
            pa_client = self._create_resource_agent_client(subplatform_id, sub_resource_id)
            # it is actually running.

            # get PID:
            pid = pa_client.get_agent_process_id()

            log.debug("%r: [LL] my child is already running: %r. pid=%s",
                      self._platform_id, subplatform_id, pid)

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
            log.debug("%r: [LL] sub-platform agent already running=%r, sub_resource_id=%s, state=%s",
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
                log.trace("%r: [LL] launching sub-platform agent %r: CFG=%s",
                          self._platform_id, subplatform_id, self._pp.pformat(sub_agent_config))
            else:
                log.debug("%r: [LL] launching sub-platform agent %r",
                          self._platform_id, subplatform_id)

            pid = self._launcher.launch_platform(subplatform_id, sub_agent_config)
            log.debug("%r: [LL] DONE launching sub-platform agent %r",
                      self._platform_id, subplatform_id)

            # create resource agent client:
            pa_client = self._create_resource_agent_client(subplatform_id, sub_resource_id)

            # wait until UNINITIALIZED:
            log.debug("%r: [LL] Got pa_client for sub-platform agent %r. Waiting for UNINITIALIZED state...",
                      self._platform_id, subplatform_id)

            self._await_state(asyn_res, subscriber)

            log.debug("%r: [LL] ok, my sub-platform agent %r is now in UNINITIALIZED state",
                      self._platform_id, subplatform_id)

        # here, sub-platform agent process is running.

        self._pa_clients[subplatform_id] = DotDict(pa_client=pa_client,
                                                   pid=pid,
                                                   resource_id=sub_resource_id)

        self._status_manager.subplatform_launched(pa_client, sub_resource_id)

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
            log.debug("%r: _initialize_subplatform %r  retval = %s", self._platform_id, subplatform_id, retval)
        except Exception as ex:
            err_msg = str(ex)

        if err_msg is not None:
            log.error(err_msg)
            self._status_manager.publish_device_failed_command_event(dd.resource_id,
                                                                     cmd,
                                                                     err_msg)
        return err_msg

    def _subplatforms_launch(self):
        """
        Launches all my configured sub-platforms storing the corresponding
        ResourceAgentClient objects in _pa_clients.

        Note that any failure while trying to launch a child agent is just logged out.
        """
        # TODO failure in a child agent launch should probably abort the whole launch?

        self._pa_clients.clear()
        subplatform_ids = self._pnode.subplatforms.keys()
        if not len(subplatform_ids):
            return

        log.debug("%r: launching subplatforms %s", self._platform_id, subplatform_ids)
        for subplatform_id in subplatform_ids:
            try:
                self._launch_platform_agent(subplatform_id)
            except Exception:
                log.exception("%r: _subplatforms_launch: exception while launching sub-platform %r",
                              self._platform_id, subplatform_id)

        log.debug("%r: _subplatforms_launch completed. _pa_clients=%s", self._platform_id, self._pa_clients)

    def _subplatforms_initialize(self):
        """
        Initializes all my sub-platforms that have been launched and not
        currently invalidated.

        Note that invalidated children are ignored.

        @return dict with failing children. Empty if all ok.
        """
        log.debug("%r: _subplatforms_initialize. _pa_clients=%s", self._platform_id, self._pa_clients)

        subplatform_ids = self._pa_clients.keys()
        children_with_errors = {}

        if not len(subplatform_ids):
            return children_with_errors

        # act only on the children that are not invalidated:
        valid_clients = dict((k, v) for k, v in self._pa_clients.iteritems()
                             if v != _INVALIDATED_CHILD)

        if not len(valid_clients):
            log.warn("%r: OOIION-1077 all sub-platforms (%s) are "
                     "invalidated or could not be re-validated. Not executing "
                     "initialize on them.",
                     self._platform_id, subplatform_ids)
            return children_with_errors   # that is, none.

        log.debug("%r: initializing subplatforms %s", self._platform_id, valid_clients)
        for subplatform_id in valid_clients:
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
        Executes a command on sub-platforms that have been launched and not
        currently invalidated

        Note that invalidated children are ignored.

        @param command        Can be a AgentCommand, and string, or None.

        @param create_command If command is None, create_command is invoked as
                              create_command(subplatform_id) for each
                              sub-platform to create the command to be executed.

        @param expected_state  If given, it is checked that the child gets to
                               this state.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        subplatform_ids = self._pa_clients.keys()

        children_with_errors = {}

        if not len(subplatform_ids):
            return children_with_errors

        # act only on the children that are not invalidated:
        valid_clients = dict((k, v) for k, v in self._pa_clients.iteritems()
                             if v != _INVALIDATED_CHILD)

        if not len(valid_clients):
            log.warn("%r: OOIION-1077 all sub-platforms (%s) are "
                     "invalidated or could not be re-validated. Not executing"
                     " command. command=%r, create_command=%r",
                     self._platform_id, subplatform_ids, command, create_command)
            return children_with_errors   # that is, none.

        def execute_cmd(subplatform_id, cmd):
            pa_client = valid_clients[subplatform_id].pa_client

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
                      self._platform_id, command, valid_clients.keys())
        else:
            log.debug("%r: executing command on my sub-platforms: %s",
                      self._platform_id, valid_clients.keys())

        for subplatform_id in valid_clients:
            if expected_state:
                pa_client = valid_clients[subplatform_id].pa_client
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
                dd = valid_clients[subplatform_id]
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

    def _subplatforms_start_streaming(self):
        """
        Executes START_MONITORING with recursion=True on all my sub-platforms.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # pass recursion=True to each sub-platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.START_MONITORING, kwargs=kwargs)
        children_with_errors = self._subplatforms_execute_agent(
            command=cmd,
            expected_state=PlatformAgentState.MONITORING)

        return children_with_errors

    def _subplatforms_stop_streaming(self):
        """
        Executes STOP_MONITORING with recursion=True on all my sub-platforms.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        # pass recursion=True to each sub-platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.STOP_MONITORING, kwargs=kwargs)
        children_with_errors = self._subplatforms_execute_agent(
            command=cmd,
            expected_state=PlatformAgentState.COMMAND)

        return children_with_errors

    def _shutdown_and_terminate_subplatform(self, subplatform_id):
        """
        Executes SHUTDOWN with recursion=True on the given sub-platform and
        then terminates that sub-platform process.

        Note that only a warning is logged out if the child is invalidated.

        @return None if the shutdown and termination completed without errors
                or the child is marked invalidated.
                Otherwise a string with an error message.
        """

        if self._pa_clients[subplatform_id] == _INVALIDATED_CHILD:
            log.warn("%r: OOIION-1077 sub-platform has been invalidated or "
                     "could not be re-validated: %r",
                     self._platform_id, subplatform_id)
            # consider this no error to continue shutdown sequence:
            return None

        dd = self._pa_clients[subplatform_id]
        cmd = AgentCommand(command=PlatformAgentEvent.SHUTDOWN, kwargs=dict(recursion=True))

        def shutdown():
            log.debug("%r: shutting down %r", self._platform_id, subplatform_id)

            # execute command:
            try:
                retval = self._execute_platform_agent(dd.pa_client, cmd, subplatform_id)
            except Exception:
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
            except Exception:
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

            except Exception:
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
        Executes SHUTDOWN with recursion=True on all sub-platforms that have
        been launched and not currently invalidated, and then
        terminates those sub-platform processes.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        subplatform_ids = self._pa_clients.keys()

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
        if InstrumentAgentState.INACTIVE == sub_state:
            # already initialized.
            log.trace("%r: _initialize_instrument: already initialized: %r",
                      self._platform_id, instrument_id)
            return

        cmd = AgentCommand(command=InstrumentAgentEvent.INITIALIZE)
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
        Launches an instrument agent (if not already running).
        It creates corresponding ResourceAgentClient,
        and publishes device_added event.

        The mechanism to detect whether the instrument agent is already
        running is by simply trying to create a ResourceAgentClient to it.

        @param instrument_id
        """

        # get InstrumentsNode, corresponding CFG, and resource_id:
        inode = self._pnode.instruments[instrument_id]
        i_CFG = inode.CFG
        agent_CFG = i_CFG.get("agent", {})
        i_resource_id = agent_CFG.get("resource_id", None)

        if i_resource_id is None:
            log.error("%r: _launch_instrument_agent: agent.resource_id must be present for child %r", self._platform_id, instrument_id)
            return
        if i_resource_id != instrument_id:
            log.error("%r: _launch_instrument_agent: agent.resource_id %r must be equal to %r", self._platform_id, i_resource_id, instrument_id)
            return

        # first, is the agent already running?
        ia_client = None
        pid = None
        try:
            # try to connect
            ia_client = self._create_resource_agent_client(instrument_id, i_resource_id)
            # it is running.

            # get PID:
            pid = ia_client.get_agent_process_id()

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

            log.debug("%r: OOIION-1077 launched instrument: instrument_id=%r pid=%r i_resource_id=%r",
                      self._platform_id, instrument_id, pid, i_resource_id)

            ia_client = self._create_resource_agent_client(instrument_id, i_resource_id)

            state = ia_client.get_agent_state()
            if state != ResourceAgentState.UNINITIALIZED:
                log.error("%r: _launch_instrument_agent: child instrument %r started but state is not %s, it is %s",
                          self._platform_id, instrument_id, ResourceAgentState.UNINITIALIZED, state)

        # here, instrument agent process is running.

        self._ia_clients[instrument_id] = DotDict(ia_client=ia_client,
                                                  pid=pid,
                                                  resource_id=i_resource_id,
                                                  alt_ids=agent_CFG.get("alt_ids", []))

        self._status_manager.instrument_launched(ia_client, i_resource_id)

    def _instruments_launch(self):
        """
        Launches all my configured instruments storing the corresponding
        ResourceAgentClient objects in _ia_clients.
        Note that any failure while trying to launch a child agent is just logged out.
        """
        # TODO failure in a child agent launch should probably abort the whole launch?

        self._ia_clients.clear()
        instrument_ids = self._pnode.instruments.keys()
        if not len(instrument_ids):
            return

        log.debug("%r: launching instruments %s", self._platform_id, instrument_ids)
        for instrument_id in instrument_ids:
            try:
                self._launch_instrument_agent(instrument_id)
            except Exception:
                log.exception("%r: _instruments_launch: exception while launching instrument %r",
                              self._platform_id, instrument_id)

        log.debug("%r: _instruments_launch completed. _ia_clients=%s", self._platform_id, self._ia_clients)

    def _instruments_initialize(self):
        """
        Initializes all my launched not-invalidated instruments.

        Note that invalidated children are ignored.

        @return dict with failing children. Empty if all ok.
        """
        instrument_ids = self._ia_clients.keys()
        children_with_errors = {}

        if not len(instrument_ids):
            return children_with_errors

        # act only on the children that are not invalidated:
        valid_clients = dict((k, v) for k, v in self._ia_clients.iteritems()
                             if v != _INVALIDATED_CHILD)

        if not len(valid_clients):
            log.warn("%r: OOIION-1077 all instrument children (%s) are "
                     "invalidated or could not be re-validated. Not executing "
                     "initialize on them.",
                     self._platform_id, instrument_ids)
            return children_with_errors   # that is, none.

        log.debug("%r: initializing instruments %s", self._platform_id, valid_clients)
        for instrument_id in valid_clients:
            err_msg = self._initialize_instrument(instrument_id)
            if err_msg is not None:
                children_with_errors[instrument_id] = err_msg

        log.debug("%r: _instruments_initialize completed. children_with_errors=%s",
                  self._platform_id, children_with_errors)

        return children_with_errors

    def _instruments_execute_agent(self, command, expected_state):
        """
        Supporting routine for various commands sent to instruments.
        Executes a command on instruments that have been launched and not
        currently invalidated.

        If expected_state is not None, the command will actually NOT be issued
        against a child agent if that child is already in this state,
        or if its current state is already at a "level of activation" in the
        "direction" of the command. The level of activation for this purpose
        is given by the following ordering of relevant states:

         UNINITIALIZED < INACTIVE < IDLE < COMMAND < STREAMING

        Example: If command = CLEAR (which "decreases" level of activation)
        and expected_state = IDLE, then the command is *not* issued if child's
        current state is UNINITIALIZED, INACTIVE, or IDLE.

        Note that invalidated children are ignored.

        @param command
                    Command to be issued
        @param expected_state
                    Desired state to determine if current state is acceptable
                    (in which case the command is not actually issued).

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """

        # act only on the children that are not invalidated:
        valid_clients = dict((k, v) for k, v in self._ia_clients.iteritems()
                             if v != _INVALIDATED_CHILD)

        instrument_ids = valid_clients.keys()

        children_with_errors = {}

        if not len(instrument_ids):
            return children_with_errors

        if not len(valid_clients):
            log.warn("%r: OOIION-1077 all instrument children (%s) are "
                     "invalidated or could not be re-validated. Not executing"
                     " command. command=%r",
                     self._platform_id, instrument_ids, command)
            return children_with_errors   # that is, none.

        def acceptable_state(current_state, expected_state, command):
            """
            Returns true only if the instrument's current state is acceptable
            for the desired expected state.
            """
            def activation_level(state):
                """
                Returns number for ordering purposes; None if state is not
                associated with any activation level.
                """
                order = [
                    ResourceAgentState.UNINITIALIZED,
                    ResourceAgentState.INACTIVE,
                    ResourceAgentState.IDLE,
                    ResourceAgentState.COMMAND,
                    ResourceAgentState.STREAMING,
                ]
                return order.index(state) if state in order else None

            def compare_states(state1, state2):
                """
                Returns numeric result of comparison; None if the states are
                not comparable.
                """
                al1 = activation_level(state1)
                al2 = activation_level(state2)
                if al1 is None or al2 is None:
                    return None
                else:
                    return int(al1) - int(al2)

            def activation_level_direction(command):
                """
                Returns 'incr' or 'decr' if command increases or
                decreases activation level; otherwise None.
                """
                incr_commands = [
                    ResourceAgentEvent.GO_ACTIVE,
                    ResourceAgentEvent.RUN,
                    DriverEvent.START_AUTOSAMPLE,
                ]
                decr_commands = [
                    ResourceAgentEvent.RESET,
                    ResourceAgentEvent.GO_INACTIVE,
                    DriverEvent.STOP_AUTOSAMPLE,
                    ResourceAgentEvent.CLEAR,
                ]
                if command in incr_commands:
                    return 'incr'
                elif command in decr_commands:
                    return 'decr'
                else:
                    return None

            acceptable = False

            if current_state == expected_state:
                log.trace("%r: instrument %r already in state: %r",
                          self._platform_id, instrument_id, expected_state)
                acceptable = True
            else:
                comp = compare_states(current_state, expected_state)
                actl = activation_level_direction(command)
                if comp is not None:
                    acceptable = (comp <= 0 and actl == 'decr') or \
                                 (comp > 0  and actl == 'incr')

                log.debug("%r: instrument %r: current_state=%r expected_state=%r"
                          " command=%r, comp=%s, actl=%s, acceptable_state=%s",
                          self._platform_id, instrument_id,
                          current_state, expected_state,
                          command, comp, actl, acceptable)
            return acceptable

        def execute_cmd(instrument_id, cmd):
            ia_client = valid_clients[instrument_id].ia_client

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

        log.debug("%r: executing command %r on my instruments: %s",
                  self._platform_id, command, valid_clients.keys())

        for instrument_id in valid_clients:
            if expected_state:
                ia_client = valid_clients[instrument_id].ia_client
                current_state = ia_client.get_agent_state()
                if acceptable_state(current_state, expected_state, command):
                    continue

            cmd = AgentCommand(command=command)
            err_msg = execute_cmd(instrument_id, cmd)

            if err_msg is not None:
                # some error happened; publish event:
                children_with_errors[instrument_id] = err_msg
                dd = valid_clients[instrument_id]
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

    def _instruments_start_streaming(self):
        """
        Executes START_AUTOSAMPLE on all my instruments.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """

        children_with_errors = self._instruments_execute_agent(
            command=DriverEvent.START_AUTOSAMPLE,
            expected_state=ResourceAgentState.STREAMING)

        return children_with_errors

    def _instruments_stop_streaming(self):
        """
        Executes STOP_AUTOSAMPLE on all my instruments.

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        children_with_errors = self._instruments_execute_agent(
            command=DriverEvent.STOP_AUTOSAMPLE,
            expected_state=ResourceAgentState.COMMAND)

        return children_with_errors

    def _shutdown_and_terminate_instrument(self, instrument_id):
        """
        Executes RESET on the instrument and then terminates it.
        ("shutting down" an instrument is just resetting it if not already in
        UNINITIALIZED state).

        Note that only a warning is logged out if the child is invalidated.

        @return None if the shutdown and termination completed without errors
                or the child is marked invalidated.
                Otherwise a string with an error message.
        """

        if self._ia_clients[instrument_id] == _INVALIDATED_CHILD:
            log.warn("%r: OOIION-1077 instrument has been invalidated or "
                     "could not be re-validated: %r",
                     self._platform_id, instrument_id)
            # consider this no error to continue shutdown sequence:
            return None

        dd = self._ia_clients[instrument_id]
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)

        def reset():
            log.debug("%r: resetting %r", self._platform_id, instrument_id)

            ia_client = self._ia_clients[instrument_id].ia_client

            # do not reset if instrument already in UNINITIALIZED:
            if ResourceAgentState.UNINITIALIZED == ia_client.get_agent_state():
                return

            try:
                retval = self._execute_instrument_agent(ia_client, cmd,
                                                        instrument_id)
            except Exception:
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

            except Exception:
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

            except Exception:
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
        Executes RESET and terminates all my currently valid instruments.
        ("shutting down" the instruments is just resetting them.)

        @return dict with children having caused some error. Empty if all
                children were processed OK.
        """
        instruments_with_errors = {}

        # Act only on the children that are not invalidated:
        valid_clients = dict((k, v) for k, v in self._ia_clients.iteritems()
                             if v != _INVALIDATED_CHILD)

        for instrument_id in valid_clients:
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
            except Exception:
                #
                # Do not proceed further with initialization of children.
                #
                msg = ("%r: exception while waiting for children to be launched. "
                       "Not proceeding with initialization of children." %self._platform_id)
                log.exception(msg)
                raise PlatformException(msg)

            # All is OK here. Proceed with initializing children:
            try:
                # initialize instruments
                self._instruments_initialize()

                # initialize sub-platforms
                self._subplatforms_initialize()

            except Exception:
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
        self._go_active_this_platform(recursion=recursion)

        # then instruments and sub-platforms
        if recursion:
            try:
                self._instruments_go_active()
                self._subplatforms_go_active()

            except Exception:
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

        @return None if all went ok; otherwise a dict
                {"subplatforms_with_errors": dict, "instruments_with_errors": dict},
                where each entry is a dict indicating any errors generated by
                the corresp children.
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

                return dict(subplatforms_with_errors=subplatforms_with_errors,
                            instruments_with_errors=instruments_with_errors)

        # then myself:
        self._go_inactive_this_platform(recursion=recursion)
        return None

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
            try:
                self._instruments_run()
                self._subplatforms_run()

            except Exception:
                log.exception("%r: unexpected exception while sending RUN to children: "
                              "any errors during this sequence should have "
                              "been handled with event notifications.", self._platform_id)

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

        @return None if all went ok; otherwise a dict
                {"subplatforms_with_errors": dict, "instruments_with_errors": dict},
                where each entry is a dict indicating any errors generated by
                the corresp children.
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

                return dict(subplatforms_with_errors=subplatforms_with_errors,
                            instruments_with_errors=instruments_with_errors)

        # then myself
        self._reset_this_platform()
        return None

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

        @return None if all went ok; otherwise a dict
                {"subplatforms_with_errors": dict, "instruments_with_errors": dict},
                where each entry is a dict indicating any errors generated by
                the corresp children.
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

                return dict(subplatforms_with_errors=subplatforms_with_errors,
                            instruments_with_errors=instruments_with_errors)

        # "shutdown" myself
        self._shutdown_this_platform()
        return None

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

        @return None if all went ok; otherwise a dict
                {"subplatforms_with_errors": dict, "instruments_with_errors": dict},
                where each entry is a dict indicating any errors generated by
                the corresp children.
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

                return dict(subplatforms_with_errors=subplatforms_with_errors,
                            instruments_with_errors=instruments_with_errors)

        # then myself
        self._pause_this_platform()
        return None

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

        @return None if all went ok; otherwise a dict
                {"subplatforms_with_errors": dict, "instruments_with_errors": dict},
                where each entry is a dict indicating any errors generated by
                the corresp children.
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

                return dict(subplatforms_with_errors=subplatforms_with_errors,
                            instruments_with_errors=instruments_with_errors)

        # then myself
        self._resume_this_platform()
        return None

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

        @return None if all went ok; otherwise a dict
                {"subplatforms_with_errors": dict, "instruments_with_errors": dict},
                where each entry is a dict indicating any errors generated by
                the corresp children.
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

                return dict(subplatforms_with_errors=subplatforms_with_errors,
                            instruments_with_errors=instruments_with_errors)

        # then myself:
        self._clear_this_platform()
        return None

    def _start_resource_monitoring(self, recursion):
        """
        Starts resource monitoring.

        STREAMING is dispatched in a bottom-up fashion:
        - if recursion is True, "stream" the children
        - then "stream" this platform itself.


        @param recursion  If True, children are sent the START_STREAMING command
                          (with corresponding recursion parameter set to True
                           in the case of platforms).

        @return None if all went ok; otherwise a dict
                {"subplatforms_with_errors": dict, "instruments_with_errors": dict},
                where each entry is a dict indicating any errors generated by
                the corresp children.
        """

        if self._plat_driver is None:
            msg = "%r: _start_resource_monitoring: _create_driver must be called first" % self._platform_id
            log.error(msg)
            raise PlatformDriverException(msg)

        if recursion:
            # first sub-platforms:
            subplatforms_with_errors = self._subplatforms_start_streaming()

            # we proceed with instruments even if some sub-platforms failed.

            # then my own instruments:
            instruments_with_errors = self._instruments_start_streaming()

            if len(subplatforms_with_errors) or len(instruments_with_errors):
                log.warning("%r: some sub-platforms or instruments failed to start streaming. "
                          "subplatforms_with_errors=%s "
                          "instruments_with_errors=%s",
                          self._platform_id, subplatforms_with_errors, instruments_with_errors)

        platform_attributes = self._plat_driver.get_attributes()
        self._platform_resource_monitor = PlatformResourceMonitor(
            self._platform_id, platform_attributes,
            self._get_attribute_values, self.evt_recv)

        self._platform_resource_monitor.start_resource_monitoring()

    def _stop_resource_monitoring(self, recursion):
        """
        Stops resource monitoring.
        """

        if recursion:
            # first sub-platforms:
            subplatforms_with_errors = self._subplatforms_stop_streaming()

            # we proceed with instruments even if some sub-platforms failed.

            # then my own instruments:
            instruments_with_errors = self._instruments_stop_streaming()

            if len(subplatforms_with_errors) or len(instruments_with_errors):
                log.warn("%r: some sub-platforms or instruments failed to stop streaming. "
                         "subplatforms_with_errors=%s "
                         "instruments_with_errors=%s",
                         self._platform_id, subplatforms_with_errors, instruments_with_errors)

        if self._platform_resource_monitor:
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

        recursion = self._get_recursion_parameter("_handler_uninitialized_initialize", *args, **kwargs)

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

        recursion = self._get_recursion_parameter("_handler_inactive_reset", *args, **kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._reset(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_inactive_go_active", *args, **kwargs)

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

        recursion = self._get_recursion_parameter("_handler_idle_reset", *args, **kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._reset(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_idle_go_inactive", *args, **kwargs)

        next_state = PlatformAgentState.INACTIVE
        result = self._go_inactive(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_idle_run", *args, **kwargs)

        next_state = PlatformAgentState.COMMAND
        result = self._run(recursion)

        return next_state, result

    ##############################################################
    # STOPPED event handlers.
    ##############################################################

    def _handler_stopped_reset(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_stopped_reset", *args, **kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._reset(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    def _handler_stopped_go_inactive(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_stopped_go_inactive", *args, **kwargs)

        next_state = PlatformAgentState.INACTIVE
        result = self._go_inactive(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    def _handler_stopped_resume(self, *args, **kwargs):
        """
        Transitions to COMMAND state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_stopped_resume", *args, **kwargs)

        next_state = PlatformAgentState.COMMAND
        result = self._resume(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    def _handler_stopped_clear(self, *args, **kwargs):
        """
        Transitions to IDLE state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_stopped_clear", *args, **kwargs)

        next_state = PlatformAgentState.IDLE
        result = self._clear(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

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

        recursion = self._get_recursion_parameter("_handler_command_reset", *args, **kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._reset(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    def _handler_command_clear(self, *args, **kwargs):
        """
        Transitions to IDLE state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_command_clear", *args, **kwargs)

        next_state = PlatformAgentState.IDLE
        result = self._clear(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    def _handler_command_pause(self, *args, **kwargs):
        """
        Transitions to STOPPED state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_command_pause", *args, **kwargs)

        next_state = PlatformAgentState.STOPPED
        result = self._pause(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

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

    def _get_resource_interface(self, current_state=True):
        """
        """
        agent_cmds = self._fsm.get_events(current_state)
        res_iface_cmds = [x for x in agent_cmds if ResourceInterfaceCapability.has(x)]

        # convert agent mediated resource commands into interface names.
        result = []
        for x in res_iface_cmds:
            if x == PlatformAgentEvent.GET_RESOURCE:
                result.append('get_resource')
            elif x == PlatformAgentEvent.SET_RESOURCE:
                result.append('set_resource')
            elif x == PlatformAgentEvent.PING_RESOURCE:
                result.append('ping_resource')
            elif x == PlatformAgentEvent.GET_RESOURCE_STATE:
                result.append('get_resource_state')
            elif x == PlatformAgentEvent.EXECUTE_RESOURCE:
                result.append('execute_resource')

        log.debug("%r/%s: _get_resource_interface(current_state=%r) => %s)",
                  self._platform_id, self.get_agent_state(),
                  current_state, result)
        return result

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

        recursion = self._get_recursion_parameter("_handler_start_resource_monitoring", *args, **kwargs)

        try:
            result = self._start_resource_monitoring(recursion)

            next_state = PlatformAgentState.MONITORING

        except Exception:
            log.exception("%r: error in _start_resource_monitoring", self._platform_id) #, exc_Info=True)
            raise

        return (next_state, result)

    def _handler_stop_resource_monitoring(self, *args, **kwargs):
        """
        Stops resource monitoring and transitions to COMMAND state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_stop_resource_monitoring", *args, **kwargs)

        try:
            result = self._stop_resource_monitoring(recursion)

            next_state = PlatformAgentState.COMMAND

        except Exception:
            log.exception("%r: error in _stop_resource_monitoring", self._platform_id) #, exc_Info=True)
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
        log.error("%r: (LC) lost connection to the device. Will attempt to reconnect...", self._platform_id)

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
                except Exception:
                    pass

        log.debug("%r: (LC) _autoreconnect ended", self._platform_id)

    def _handler_lost_connection_autoreconnect(self, *args, **kwargs):

        log.debug("%r: (LC) _handler_lost_connection_autoreconnect: trying CONNECT...", self._platform_id)
        try:
            driver_state = self._trigger_driver_event(PlatformDriverEvent.CONNECT)

            # Reset the connection id and index.
            self._asp.reset_connection()

        except Exception as e:
            log.error("%r: _handler_lost_connection_autoreconnect: Exception while trying CONNECT: %s", self._platform_id, e)
            return None, None

        if driver_state != PlatformDriverState.CONNECTED:
            log.error("%r: _handler_lost_connection_autoreconnect: driver state not %s, it is %s",
                      self._platform_id, PlatformDriverState.CONNECTED, driver_state)
            return None, None

        if self._state_when_lost == PlatformAgentState.MONITORING:
            recursion = self._get_recursion_parameter("_handler_lost_connection_autoreconnect", *args, **kwargs)
            self._start_resource_monitoring(recursion)

        next_state = self._state_when_lost

        log.debug("%r: (LC) _handler_lost_connection_autoreconnect: next_state=%s", self._platform_id, next_state)

        return next_state, None

    def _handler_lost_connection_reset(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_lost_connection_reset", *args, **kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._reset(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    def _handler_lost_connection_go_inactive(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_lost_connection_go_inactive", *args, **kwargs)

        next_state = PlatformAgentState.INACTIVE
        result = self._go_inactive(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

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
            self._stop_resource_monitoring(recursion=False)

        return PlatformAgentState.LOST_CONNECTION, None

    def _handler_shutdown(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        recursion = self._get_recursion_parameter("_handler_shutdown", *args, **kwargs)

        next_state = PlatformAgentState.UNINITIALIZED
        result = self._shutdown(recursion)
        if result is not None:
            # problems with children; do not make transition:
            next_state = None

        return next_state, result

    ##############################################################
    # mission related handlers
    ##############################################################

    def _run_mission(self, mission_id, mission_loader, mission_scheduler, instrument_objs):
        """
        Method launched in a separate greenlet to run a mission that has been
        already loaded sucessfully. At the end of the execution, if no remaining
        missions are executing, EXIT_MISSION is triggered to return to saved state.
        """
        time_start = time.time()
        log.debug('%r: [mm] _run_mission: running mission_id=%r ...', self._platform_id, mission_id)
        self._mission_manager.run_mission(mission_id, mission_loader, mission_scheduler, instrument_objs)

        elapsed_time = time.time() - time_start
        log.debug('%r: [mm] _run_mission: completed mission_id=%s. elapsed_time=%s',
                  self._platform_id, mission_id, elapsed_time)

        remaining = self._mission_manager.get_number_of_running_missions()
        if remaining == 0:
            # check state before attempting the EXIT_MISSION.
            # (but we could still have a "check-then-act" race condition here)
            curr_state = self.get_agent_state()
            if curr_state in [PlatformAgentState.MISSION_COMMAND,
                              PlatformAgentState.MISSION_STREAMING]:
                log.debug('%r: [mm] _run_mission: triggering EXIT_MISSION', self._platform_id)
                self._fsm.on_event(PlatformAgentEvent.EXIT_MISSION)
            else:
                log.warn('%r: [mm] _run_mission: not triggering EXIT_MISSION: FSM not in '
                         'mission substate (%s)', self._platform_id, curr_state)

    def _handler_mission_run(self, *args, **kwargs):
        """
        Loads and executes mission indicated with the kwargs 'mission_id' and 'mission_yml'.
        The load is done in current thread with an immediate failure (BadRequest) if
        there's any problem with the mission plan.
        The execution is started in a separate thread.
        If not already in a mission state, it saves current state to return to
        upon all mission executions are completed, and transitions to the
        appropriate mission substate.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        mission_id = kwargs.get('mission_id', None)
        if mission_id is None:
            raise FSMError('mission_run: missing mission_id argument')
        mission_yml = kwargs.get('mission_yml', None)
        if mission_yml is None:
            raise FSMError('mission_run: missing mission_yml argument')

        # immediately fail if any problem with the mission plan itself:
        try:
            exec_args = self._mission_manager.load_mission(mission_id, mission_yml)
        except BadRequest as ex:
            raise
        except Exception as ex:
            raise BadRequest('load_mission: %s', ex)

        curr_state = self.get_agent_state()
        if curr_state in [PlatformAgentState.COMMAND, PlatformAgentState.MONITORING]:
            # save state to return to when all mission executions are completed
            self._pre_mission_state = curr_state

            next_state = PlatformAgentState.MISSION_COMMAND if curr_state == \
                PlatformAgentState.COMMAND else PlatformAgentState.MISSION_STREAMING
        else:
            # just stay in the current state:
            next_state = None

        self._mission_greenlet = spawn(self._run_mission, *exec_args)
        log.info("%r: started mission execution, mission_id=%r", self._platform_id, mission_id)

        result = None
        return next_state, result

    def _handler_mission_exit(self, *args, **kwargs):
        """
        Simply transitions to saved state prior to mission execution.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = self._pre_mission_state
        self._pre_mission_state = None
        result = None

        return next_state, result

    def _handler_mission_abort(self, *args, **kwargs):
        """
        Aborts the execution of the mission indicated with the kwarg 'mission_id'.
        Transitions to saved state if no remaining missions are left running.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                      self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        mission_id = kwargs.get('mission_id', None)
        if mission_id is None:
            raise FSMError('mission_run: missing mission_id argument')

        result = self._mission_manager.abort_mission(mission_id)

        remaining = self._mission_manager.get_number_of_running_missions()
        if remaining == 0:
            next_state = self._pre_mission_state
            self._pre_mission_state = None
        else:
            next_state = None

        return next_state, result

    ##############################################################
    # Agent parameter functions.
    ##############################################################

    def _configure_aparams(self, aparams=None):
        """
        Configure aparams from agent config values.

        **NOTE**:
        the base class calls this as part of on_init, but the PlatforAgent
        currently does much of its preparations in on_start, including the
        set-up of the alert manager and the status manager. So, as a mechanism
        to handle this, here we basically save the argument `aparams` for
        actual processing during on_start.
        """

        log.debug("%r: _configure_aparams: aparams=%s", self._platform_id, aparams)

        self._configure_aparams_arg = None

        if self._aam:
            # if we already have an alert manager, do the thing right away (but
            # this is not the case at time of writing).
            self._do_configure_aparams(aparams)
        else:
            # save the argument for subsequent processing in on_start:
            self._configure_aparams_arg = aparams

    def _do_configure_aparams(self, aparams):

        aparams = aparams or []

        # TODO: pubrate
        # # If specified and configed, build the pubrate aparam.
        # aparam_pubrate_config = self.CFG.get('aparam_pubrate_config', None)
        # if aparam_pubrate_config and 'pubrate' in aparams:
        #     self.aparam_set_pubrate(aparam_pubrate_config)

        # If specified and configed, build the alerts aparam.
        aparam_alerts_config = self.CFG.get('aparam_alerts_config', None)
        log.debug("%r: _configure_aparams: aparam_alerts_config=%s",
                  self._platform_id, aparam_alerts_config)
        if aparam_alerts_config and 'alerts' in aparams:
            self.aparam_set_alerts(aparam_alerts_config)

    def _restore_resource(self, state, prev_state):
        """
        Restore agent/resource configuration and state.
        """
        log.warn("%r: PlatformAgent._restore_resource not implemented yet", self._platform_id)

    ##############################################################
    # Base class overrides for state and cmd error alerts.
    ##############################################################

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

        # IDLE state event handlers.
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.SHUTDOWN, self._handler_shutdown)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RUN, self._handler_idle_run)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.LOST_CONNECTION, self._handler_connection_lost_driver_event)

        # STOPPED state event handlers.
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.RESET, self._handler_stopped_reset)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.SHUTDOWN, self._handler_shutdown)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.GO_INACTIVE, self._handler_stopped_go_inactive)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.RESUME, self._handler_stopped_resume)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.CLEAR, self._handler_stopped_clear)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
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

            self._fsm.add_handler(state, PlatformAgentEvent.RUN_MISSION, self._handler_mission_run)

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
        self._fsm.add_handler(PlatformAgentState.LOST_CONNECTION, PlatformAgentEvent.GET_RESOURCE_STATE, self._handler_get_resource_state)

        # MISSION_COMMAND state event handlers.
        self._fsm.add_handler(PlatformAgentState.MISSION_COMMAND, PlatformAgentEvent.RUN_MISSION, self._handler_mission_run)
        self._fsm.add_handler(PlatformAgentState.MISSION_COMMAND, PlatformAgentEvent.EXIT_MISSION, self._handler_mission_exit)
        self._fsm.add_handler(PlatformAgentState.MISSION_COMMAND, PlatformAgentEvent.ABORT_MISSION, self._handler_mission_abort)
        self._fsm.add_handler(PlatformAgentState.MISSION_COMMAND, PlatformAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)

        # MISSION_STREAMING state event handlers.
        self._fsm.add_handler(PlatformAgentState.MISSION_STREAMING, PlatformAgentEvent.RUN_MISSION, self._handler_mission_run)
        self._fsm.add_handler(PlatformAgentState.MISSION_STREAMING, PlatformAgentEvent.EXIT_MISSION, self._handler_mission_exit)
        self._fsm.add_handler(PlatformAgentState.MISSION_STREAMING, PlatformAgentEvent.ABORT_MISSION, self._handler_mission_abort)
        self._fsm.add_handler(PlatformAgentState.MISSION_STREAMING, PlatformAgentEvent.EXECUTE_RESOURCE, self._handler_execute_resource)
