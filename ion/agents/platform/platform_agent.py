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

# Pyon exceptions.
from pyon.core.exception import BadRequest, Inconsistent

from pyon.core.governance import ORG_MANAGER_ROLE, GovernanceHeaderValues, has_org_role, get_resource_commitments
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE


from ion.agents.instrument.common import BaseEnum

from ion.agents.platform.exceptions import PlatformException, PlatformConfigurationException
from ion.agents.platform.platform_driver_event import AttributeValueDriverEvent
from ion.agents.platform.platform_driver_event import ExternalEventDriverEvent
from ion.agents.platform.exceptions import CannotInstantiateDriverException
from ion.agents.platform.util.network_util import NetworkUtil

from ion.agents.platform.platform_driver import PlatformDriverEvent, PlatformDriverState

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
import numpy
from ion.agents.platform.test.adhoc import adhoc_get_parameter_dictionary

from ion.agents.instrument.instrument_fsm import InstrumentFSM, FSMStateError

from ion.agents.platform.platform_agent_launcher import LauncherFactory

from ion.agents.platform.platform_resource_monitor import PlatformResourceMonitor

from coverage_model.parameter import ParameterDictionary
from interface.objects import StreamRoute

import logging
import time

import pprint


# NOTE: the bigger the platform network size starting from the platform
# associated with a PlatformAgent instance, the more the time that should be
# given for commands to sub-platforms to complete. The following TIMEOUT value
# intends to be big enough for all typical cases.
TIMEOUT = 90


PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


# TODO clean up log-and-throw anti-idiom in several places, which is used
# because the exception alone does not show up in the logs!


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


class PlatformAgentEvent(BaseEnum):
    ENTER                     = ResourceAgentEvent.ENTER
    EXIT                      = ResourceAgentEvent.EXIT

    INITIALIZE                = ResourceAgentEvent.INITIALIZE
    RESET                     = ResourceAgentEvent.RESET
    GO_ACTIVE                 = ResourceAgentEvent.GO_ACTIVE
    GO_INACTIVE               = ResourceAgentEvent.GO_INACTIVE
    RUN                       = ResourceAgentEvent.RUN
    CLEAR                     = ResourceAgentEvent.CLEAR
    PAUSE                     = ResourceAgentEvent.PAUSE
    RESUME                    = ResourceAgentEvent.RESUME
    GET_RESOURCE_CAPABILITIES = ResourceAgentEvent.GET_RESOURCE_CAPABILITIES
    PING_RESOURCE             = ResourceAgentEvent.PING_RESOURCE
    GET_RESOURCE              = ResourceAgentEvent.GET_RESOURCE
    SET_RESOURCE              = ResourceAgentEvent.SET_RESOURCE

    GET_METADATA              = 'PLATFORM_AGENT_GET_METADATA'
    GET_PORTS                 = 'PLATFORM_AGENT_GET_PORTS'
    CONNECT_INSTRUMENT        = 'PLATFORM_AGENT_CONNECT_INSTRUMENT'
    DISCONNECT_INSTRUMENT     = 'PLATFORM_AGENT_DISCONNECT_INSTRUMENT'
    GET_CONNECTED_INSTRUMENTS = 'PLATFORM_AGENT_GET_CONNECTED_INSTRUMENTS'
    TURN_ON_PORT              = 'PLATFORM_AGENT_TURN_ON_PORT'
    TURN_OFF_PORT             = 'PLATFORM_AGENT_TURN_OFF_PORT'
    GET_SUBPLATFORM_IDS       = 'PLATFORM_AGENT_GET_SUBPLATFORM_IDS'
    START_MONITORING          = 'PLATFORM_AGENT_START_MONITORING'
    STOP_MONITORING           = 'PLATFORM_AGENT_STOP_MONITORING'

    CHECK_SYNC                = 'PLATFORM_AGENT_CHECK_SYNC'


class PlatformAgentCapability(BaseEnum):
    INITIALIZE                = PlatformAgentEvent.INITIALIZE
    RESET                     = PlatformAgentEvent.RESET
    GO_ACTIVE                 = PlatformAgentEvent.GO_ACTIVE
    GO_INACTIVE               = PlatformAgentEvent.GO_INACTIVE
    RUN                       = PlatformAgentEvent.RUN
    CLEAR                     = PlatformAgentEvent.CLEAR
    PAUSE                     = PlatformAgentEvent.PAUSE
    RESUME                    = PlatformAgentEvent.RESUME
    GET_RESOURCE_CAPABILITIES = PlatformAgentEvent.GET_RESOURCE_CAPABILITIES
    PING_RESOURCE             = PlatformAgentEvent.PING_RESOURCE
    GET_RESOURCE              = PlatformAgentEvent.GET_RESOURCE
    SET_RESOURCE              = PlatformAgentEvent.SET_RESOURCE

    GET_METADATA              = PlatformAgentEvent.GET_METADATA
    GET_PORTS                 = PlatformAgentEvent.GET_PORTS

    CONNECT_INSTRUMENT        = PlatformAgentEvent.CONNECT_INSTRUMENT
    DISCONNECT_INSTRUMENT     = PlatformAgentEvent.DISCONNECT_INSTRUMENT
    GET_CONNECTED_INSTRUMENTS = PlatformAgentEvent.GET_CONNECTED_INSTRUMENTS

    TURN_ON_PORT              = PlatformAgentEvent.TURN_ON_PORT
    TURN_OFF_PORT             = PlatformAgentEvent.TURN_OFF_PORT
    GET_SUBPLATFORM_IDS       = PlatformAgentEvent.GET_SUBPLATFORM_IDS

    START_MONITORING          = PlatformAgentEvent.START_MONITORING
    STOP_MONITORING           = PlatformAgentEvent.STOP_MONITORING

    CHECK_SYNC                = PlatformAgentEvent.CHECK_SYNC


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

        self._network_definition_ser = None  # string
        self._network_definition = None  # NetworkDefinition object
        self._pnode = None  # PlatformNode object corresponding to this platform

        self._agent_streamconfig_map = None

        # </platform configuration>
        #########################################

        self._plat_driver = None

        # PlatformResourceMonitor
        self._platform_resource_monitor = None

        # Dictionaries used for data publishing. Constructed in _do_initialize
        self._data_streams = {}
        self._param_dicts = {}
        self._stream_defs = {}
        self._data_publishers = {}

        # Set of parameter names received in event notification but not
        # configured. Allows to log corresponding warning only once.
        self._unconfigured_params = set()

        # {subplatform_id: (ResourceAgentClient, PID), ...}
        self._pa_clients = {}  # Never None

        self._launcher = LauncherFactory.createLauncher()
        log.debug("launcher created: %s", str(type(self._launcher)))

        log.info("PlatformAgent constructor complete.")

        # for debugging purposes
        self._pp = pprint.PrettyPrinter(indent=4, depth=12)

    def on_init(self):
        super(PlatformAgent, self).on_init()
        log.trace("on_init")

        self._plat_config = self.CFG.get("platform_config", None)
        self._plat_config_processed = False

        if log.isEnabledFor(logging.DEBUG): # pragma: no cover
            log.debug("on_init: CFG = %s", self._pp.pformat(self.CFG))

    def on_start(self):
        super(PlatformAgent, self).on_start()
        log.info('platform agent is running: on_start called.')

    def on_quit(self):
        try:
            log.debug("%r: PlatformAgent: on_quit called. current_state=%s",
                      self._platform_id, self._fsm.get_current_state())

            self._do_quit()

        finally:
            super(PlatformAgent, self).on_quit()

    def _do_quit(self):
        """
        Performs steps to transition this agent to the UNINITIALIZED state
        (if not already there), so it attempts a "graceful" termination.
        """
        curr_state = self._fsm.get_current_state()

        if PlatformAgentState.UNINITIALIZED == curr_state:
            log.debug("%r: PlatformAgent: quit: already in UNINITIALIZED "
                      "state; nothing to do.", self._platform_id)
            return

        # attempt a "graceful" termination.

        log.info("%r: PlatformAgent: executing quit secuence", self._platform_id)

        attempts = 0
        while PlatformAgentState.UNINITIALIZED != curr_state and attempts <= 3:
            attempts += 1
            try:
                # all main states accept the RESET event so the following
                # should work in general:
                self._fsm.on_event(PlatformAgentEvent.RESET)

            except FSMStateError as e:
                #
                # if this happens, need to consider unhandled case!
                # TODO adjust logic to do appropriate action depending on
                # the current state.
                log.warn("TODO: for the quit sequence, a RESET event was tried "
                         "in a state (%s) that does not handle it; please "
                         "report this bug. (platform_id=%r)",
                         curr_state, self._platform_id)
                break

            finally:
                curr_state = self._fsm.get_current_state()

        log.debug("%r: PlatformAgent: quit secuence complete. "
                  "attempts=%d;  final state=%s",
                  self._platform_id, attempts, curr_state)

    def _reset(self):
        """
        Resets this platform agent (terminates sub-platforms processes,
        clears self._pa_clients, stops resource monitoring, destroys driver).

        Basic configuration is retained: The "platform_config" configuration
        object provided via self.CFG (see on_init) is kept. This allows
        to issue the INITIALIZE command again with the already configured agent.

        The overall "reset" operation happens bottom-up, so this method is
        called after sending the RESET command to the sub-platforms (if any).
        """
        log.debug("%r: resetting", self._platform_id)

        # terminate sub-platform agent processes:
        if len(self._pa_clients):
            log.debug("%r: terminating sub-platform agent processes (%d)",
                self._platform_id, len(self._pa_clients))
            for subplatform_id in self._pa_clients:
                _, pid = self._pa_clients[subplatform_id]
                try:
                    self._launcher.cancel_process(pid)
                except Exception as e:
                    log.warn("%r: exception in cancel_process for subplatform_id=%r, pid=%r: %s",
                             self._platform_id, subplatform_id, pid, str(e)) #, exc_Info=True)

        self._pa_clients.clear()

        if self._platform_resource_monitor:
            self._stop_resource_monitoring()

        if self._plat_driver:
            # disconnect driver if connected:
            driver_state = self._plat_driver._fsm.get_current_state()
            log.debug("_reset: driver_state = %s", driver_state)
            if driver_state == PlatformDriverState.CONNECTED:
                self._trigger_driver_event(PlatformDriverEvent.DISCONNECT)
            # destroy driver:
            self._plat_driver.destroy()
            self._plat_driver = None

        self._unconfigured_params.clear()

    def _pre_initialize(self):
        """
        Does verifications and preparations dependent on self._plat_config.

        @raises PlatformException if the verification fails for some reason.
        """
        if log.isEnabledFor(logging.DEBUG):  # pragma: no cover
            log.debug("_pre_initialize: plat_config=%s", self._pp.pformat(self._plat_config))

        if not self._plat_config:
            msg = "'platform_config' entry not provided in agent configuration"
            log.error(msg)
            raise PlatformException(msg)

        if self._plat_config_processed:
            # nothing else to do here
            return

        log.debug("verifying/processing _plat_config ...")
        for k in ['platform_id', 'driver_config', 'network_definition']:
            if not k in self._plat_config:
                msg = "'%s' key not given in plat_config=%s" % (k, self._plat_config)
                log.error(msg)
                raise PlatformException(msg)

        self._platform_id = self._plat_config['platform_id']
        driver_config = self._plat_config['driver_config']
        self._network_definition_ser = self._plat_config['network_definition']

        for k in ['dvr_mod', 'dvr_cls']:
            if not k in driver_config:
                msg = "%r: '%s' key not given in driver_config=%s" % (
                    self._platform_id, k, driver_config)
                log.error(msg)
                raise PlatformException(msg)

        self._network_definition = NetworkUtil.deserialize_network_definition(
            self._network_definition_ser)

        # verify the given platform_id is contained in the NetworkDefinition:
        if not self._platform_id in self._network_definition.pnodes:
            msg = "%r: this platform_id not found in network definition." % self._platform_id
            log.error(msg)
            raise PlatformException(msg)

        self._pnode = self._network_definition.pnodes[self._platform_id]
        assert self._pnode.platform_id == self._platform_id

        self._platform_attributes = dict((attr.attr_id, attr.defn) for attr
                                         in self._pnode.attrs.itervalues())

        ppid = self._plat_config.get('parent_platform_id', None)
        if ppid:
            self._parent_platform_id = ppid
            log.debug("_parent_platform_id set to: %s", self._parent_platform_id)

        if 'agent_streamconfig_map' in self._plat_config:
            self._agent_streamconfig_map = self._plat_config['agent_streamconfig_map']

        self._plat_config_processed = True

        if log.isEnabledFor(logging.DEBUG):  # pragma: no cover
            log.debug("%r: _plat_config_processed complete" % self._platform_id)

    ##############################################################
    # Governance interfaces
    ##############################################################

    #TODO - When/If the Instrument and Platform agents are dervied from a common device agent class, then relocate to the parent class and share
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

        if has_org_role(gov_values.actor_roles ,self._get_process_org_name(), ORG_MANAGER_ROLE):
            return True, ''

        if not has_org_role(gov_values.actor_roles ,self._get_process_org_name(), INSTRUMENT_OPERATOR_ROLE):
            return False, ''

        com = get_resource_commitments(gov_values.actor_id, gov_values.resource_id)
        if com is None:
            return False, '%s(%s) has been denied since the user %s has not acquired the resource %s' % (self.name, gov_values.op, gov_values.actor_id, self.resource_id)

        return True, ''

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

        #
        # TODO simplify method as only CFG.stream_config will be used.
        #

        if self._agent_streamconfig_map:
            # TODO this part to be removed.
            log.debug("%r: _agent_streamconfig_map = %s", self._platform_id, self._agent_streamconfig_map)

            stream_config = self._agent_streamconfig_map[self._platform_id]
            self._construct_data_publisher_using_stream_config(stream_config)

            # TODO remove the following
            # self._construct_data_publishers_using_agent_streamconfig_map()

        else:
            stream_configs = self.CFG.get('stream_config', None)
            if stream_configs is None:
                raise PlatformConfigurationException(
                    "%r: No stream_config given in CFG", self._platform_id)

            for stream_name, stream_config in stream_configs.iteritems():
                self._construct_data_publisher_using_stream_config(stream_name, stream_config)

            # TODO remove the following
            # self._construct_data_publishers_using_CFG_stream_config()

        log.debug("%r: _construct_data_publishers complete", self._platform_id)

    def _construct_data_publisher_using_stream_config(self, stream_name, stream_config):

        # granule_publish_rate
        # records_per_granule

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: _construct_data_publisher_using_stream_config: "
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

    # TODO remove this obsolete method
    def _construct_data_publishers_using_agent_streamconfig_map(self):
        log.debug("%r: _agent_streamconfig_map = %s",
            self._platform_id, self._agent_streamconfig_map)

        stream_config = self._agent_streamconfig_map[self._platform_id]

        routing_key = stream_config['routing_key']
        stream_id = stream_config['stream_id']
        exchange_point = stream_config['exchange_point']

        #
        # TODO Note: using a single stream for the platform
        #

        stream_name = self._get_platform_name(self._platform_id)

        log.debug("%r: stream_name=%r, routing_key=%r",
            self._platform_id, stream_name, routing_key)

        self._data_streams[stream_name] = stream_id
        self._param_dicts[stream_name] = ParameterDictionary.load(stream_config['parameter_dictionary'])
        self._stream_defs[stream_name] = stream_config['stream_definition_ref']
        stream_route = StreamRoute(exchange_point=exchange_point, routing_key=routing_key)
        publisher = self._create_publisher(stream_id=stream_id, stream_route=stream_route)
        self._data_publishers[stream_name] = publisher
        log.debug("%r: created publisher for stream_name=%r",
              self._platform_id, stream_name)

    # TODO remove this obsolete method
    def _construct_data_publishers_using_CFG_stream_config(self):
        """
        Construct the stream publishers from the stream_config agent
        config variable.
        """

        stream_info = self.CFG.get('stream_config', None)
        if stream_info is None:
            log.warn("%r: No stream_config given in CFG", self._platform_id)
            return

        log.debug("%r: stream_info = %s", self._platform_id, stream_info)

        for (stream_name, stream_config) in stream_info.iteritems():

            stream_route = stream_config['stream_route']

            log.debug("%r: stream_name=%r, stream_route=%r",
                self._platform_id, stream_name, stream_route)

            stream_id = stream_config['stream_id']
            self._data_streams[stream_name] = stream_id
            self._param_dicts[stream_name] = adhoc_get_parameter_dictionary(stream_name)
            publisher = self._create_publisher(stream_id=stream_id, stream_route=stream_route)
            self._data_publishers[stream_name] = publisher
            log.debug("%r: created publisher for stream_name=%r",
                  self._platform_id, stream_name)

    def _get_platform_name(self, platform_id):
        """
        Interim helper to get the platform name associated with a platform_id.
        """

        # simply returning the same platform_id, because those are the IDs
        # currently passed from configuration -- see test_oms_launch
        return platform_id

    def _create_driver(self):
        """
        Creates the platform driver object for this platform agent.

        NOTE: the driver object is created directly (not via a spawned process)
        """
        driver_config = self._plat_config['driver_config']
        driver_module = driver_config['dvr_mod']
        driver_class = driver_config['dvr_cls']

        assert self._platform_id is not None, "must know platform_id to create driver"

        if log.isEnabledFor(logging.DEBUG):
            log.debug('%r: creating driver: driver_module=%s driver_class=%s' % (
                self._platform_id, driver_module, driver_class))

        try:
            module = __import__(driver_module, fromlist=[driver_class])
            classobj = getattr(module, driver_class)
            driver = classobj(self._pnode, self.evt_recv)

        except Exception as e:
            msg = '%r: could not import/construct driver: module=%s, class=%s' % (
                self._platform_id, driver_module, driver_class)
            log.error("%s; reason=%s", msg, str(e))  #, exc_Info=True)
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
        driver_config = self._plat_config['driver_config']
        if log.isEnabledFor(logging.DEBUG):
            log.debug('%r: configuring driver: %s' % (self._platform_id, driver_config))

        self._trigger_driver_event(PlatformDriverEvent.CONFIGURE, driver_config=driver_config)

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

    def _do_initialize(self):
        """
        Does the main initialize sequence, which includes creation of publishers
        and creation/configuration of the driver, but excludes the launch
        of the sub-platforms.
        """
        self._pre_initialize()
        self._construct_data_publishers()
        self._create_driver()
        self._configure_driver()

    def _do_go_active(self):
        """
        Does activation actions at this platform (excluding sub-platforms).
        This base class connects the driver.
        """
        self._trigger_driver_event(PlatformDriverEvent.CONNECT)

    def _do_go_inactive(self):
        """
        Does desactivation actions at this platform (excluding sub-platforms).
        This base class disconnects the driver.
        """
        self._trigger_driver_event(PlatformDriverEvent.DISCONNECT)

    def _go_inactive(self):
        """
        Issues GO_INACTIVE to sub-platforms and then processes the command at
        this platform.
        """
        self._subplatforms_go_inactive()
        self._do_go_inactive()
        result = None
        return result

    def _do_run(self):
        """
        Any actions at this platform (excluding sub-platforms) for the
        COMMAND state.
        Nothing done at the moment.
        """
        pass

    def _run(self):
        """
        Prepares this particular platform agent for the COMMAND state.
        """
        # first myself, then sub-platforms
        self._do_run()
        self._subplatforms_run()
        result = None
        return result

    def _start_resource_monitoring(self):
        """
        Starts resource monitoring.
        """
        self._assert_driver()

        self._platform_resource_monitor = PlatformResourceMonitor(
            self._platform_id, self._platform_attributes,
            self._get_attribute_values, self.evt_recv)

        self._platform_resource_monitor.start_resource_monitoring()

    def _get_attribute_values(self, attr_names, from_time):
        self._assert_driver()
        kwargs = dict(attr_names=attr_names, from_time=from_time)
        result = self._trigger_driver_event(PlatformDriverEvent.GET_ATTRIBUTE_VALUES, **kwargs)
        return result

    def _stop_resource_monitoring(self):
        """
        Stops resource monitoring.
        """
        assert self._platform_resource_monitor is not None, \
        "_start_resource_monitoring must have been called first"

        self._platform_resource_monitor.destroy()
        self._platform_resource_monitor = None

    def evt_recv(self, driver_event):
        """
        Callback to receive asynchronous driver events.
        @param driver_event The driver event received.
        """
        log.debug('%r: in state=%s: received driver_event=%s',
            self._platform_id, self.get_agent_state(), str(driver_event))

        if isinstance(driver_event, AttributeValueDriverEvent):
            self._handle_attribute_value_event(driver_event)
            return

        if isinstance(driver_event, ExternalEventDriverEvent):
            self._handle_external_event_driver_event(driver_event)
            return

        #
        # TODO handle other possible events.
        #

        else:
            log.warn('%r: driver_event not handled: %s',
                self._platform_id, str(type(driver_event)))
            return

    def _handle_attribute_value_event(self, driver_event):

        log.debug("%r: driver_event = %s", self._platform_id, driver_event)

        stream_name = driver_event.stream_name

        publisher = self._data_publishers.get(stream_name, None)
        if not publisher:
            log.warn('%r: no publisher configured for stream_name=%r. '
                     'Configured streams are: %s',
                     self._platform_id, stream_name, self._data_publishers.keys())
            return

        param_dict = self._param_dicts[stream_name]
        stream_def = self._stream_defs[stream_name]
        rdt = RecordDictionaryTool(param_dictionary=param_dict.dump(), stream_definition_id=stream_def)

        selected_timestamps = None

        for param_name, param_value in driver_event.vals_dict.iteritems():

            if param_name not in rdt:
                if param_name not in self._unconfigured_params:
                    # an unrecognized attribute for this platform:
                    self._unconfigured_params.add(param_name)
                    log.warn('%r: got attribute value event for unconfigured parameter %r in stream %r',
                             self._platform_id, param_name, stream_name)
                continue

            # Note that notification from the driver has the form
            # of a non-empty list of pairs (val, ts)
            assert isinstance(param_value, list)
            assert isinstance(param_value[0], tuple)

            log.info("%r: PUBLISHING VALUE ARRAY: %s (%d samples) = %s",
                     self._platform_id, param_name, len(param_value), str(param_value))

            # separate values and timestamps:
            vals, timestamps = zip(*param_value)

            # Set values in rdt:
            rdt[param_name] = numpy.array(vals)

            # TODO: do proper handling of the timestamps for the assignment
            # to the rdt. For the moment, just arbitrarily picking the
            # array of timestamps from the last iteration here.
            selected_timestamps = timestamps

        if selected_timestamps is None:
            # that is, all param_name's were unrecognized; just return:
            return

        # Set timestamp info in rdt:
        if param_dict.temporal_parameter_name is not None:
            temp_param_name = param_dict.temporal_parameter_name
            rdt[temp_param_name]       = numpy.array(selected_timestamps)
            #@TODO: Ensure that the preferred_timestamp field is correct
            rdt['preferred_timestamp'] = numpy.array(['internal_timestamp'] * len(selected_timestamps))
            log.warn('Preferred timestamp is unresolved, using "internal_timestamp"')
        else:
            log.warn("%r: Not including timestamp info in granule: "
                     "temporal_parameter_name not defined in parameter dictionary",
                     self._platform_id)

        g = rdt.to_granule(data_producer_id=self.resource_id)
        try:
            publisher.publish(g)
            log.debug("%r: Platform agent published data granule on stream %r.",
                      self._platform_id, stream_name)

        except:
            log.exception("%r: Platform agent could not publish data on stream %s.",
                          self._platform_id, stream_name)

    def _handle_external_event_driver_event(self, driver_event):

        event_type = driver_event._event_type

        event_instance = driver_event._event_instance
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

        except Exception as e:
            log.error("Error while publishing platform event: %s", str(e))

    ##############################################################
    # supporting routines dealing with sub-platforms
    ##############################################################

    def _launch_platform_agent(self, subplatform_id):
        """
        Launches a sub-platform agent, creates ResourceAgentClient, and pings
        and initializes the sub-platform agent.

        @param subplatform_id Platform ID
        """

        # platform configuration:
        # TODO general config under revision
        platform_config = {
            'platform_id': subplatform_id,
            'network_definition' : self._network_definition_ser,
            'parent_platform_id' : self._platform_id,

            'agent_streamconfig_map': self._agent_streamconfig_map,
            'driver_config': self._plat_config['driver_config'],
        }

        agent_config = {
            'agent':            {'resource_id': subplatform_id},
            'stream_config':    self.CFG.get('stream_config', None),

            'platform_config':  platform_config,
        }

        log.debug("%r: launching sub-platform agent %r",
            self._platform_id, subplatform_id)
        pid = self._launcher.launch(subplatform_id, agent_config)

        pa_client = self._create_resource_agent_client(subplatform_id)

        self._pa_clients[subplatform_id] = (pa_client, pid)

        self._ping_subplatform(subplatform_id)
        self._initialize_subplatform(subplatform_id)

    def _create_resource_agent_client(self, subplatform_id):
        """
        Creates and returns a ResourceAgentClient instance.

        @param subplatform_id Platform ID
        """
        log.debug("%r: _create_resource_agent_client: subplatform_id=%s",
            self._platform_id, subplatform_id)

        pa_client = ResourceAgentClient(subplatform_id, process=self)

        log.debug("%r: got platform agent client %s",
            self._platform_id, str(pa_client))

        state = pa_client.get_agent_state()
        assert PlatformAgentState.UNINITIALIZED == state

        log.debug("%r: ResourceAgentClient CREATED: subplatform_id=%s",
            self._platform_id, subplatform_id)

        return pa_client

    def _execute_agent(self, pa_client, cmd, subplatform_id, timeout=TIMEOUT):
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: _execute_agent: cmd=%r subplatform_id=%r ...",
                      self._platform_id, cmd.command, subplatform_id)

            time_start = time.time()
            retval = pa_client.execute_agent(cmd, timeout=timeout)
            elapsed_time = time.time() - time_start
            log.debug("%r: _execute_agent: cmd=%r subplatform_id=%r elapsed_time=%s",
                      self._platform_id, cmd.command, subplatform_id, elapsed_time)
        else:
            retval = pa_client.execute_agent(cmd, timeout=timeout)

        return retval

    def _ping_subplatform(self, subplatform_id):
        log.debug("%r: _ping_subplatform -> %r",
            self._platform_id, subplatform_id)

        pa_client, _ = self._pa_clients[subplatform_id]

        retval = pa_client.ping_agent(timeout=TIMEOUT)
        log.debug("%r: _ping_subplatform %r  retval = %s",
            self._platform_id, subplatform_id, str(retval))

        if retval is None:
            msg = "%r: unexpected None ping response from sub-platform agent: %r" % (
                    self._platform_id, subplatform_id)
            log.error(msg)
            raise PlatformException(msg)

    def _initialize_subplatform(self, subplatform_id):
        """
        Issues INITIALIZE command to the given (sub-)platform agent so the
        agent network gets built and initialized recursively.
        """
        log.debug("%r: _initialize_subplatform -> %r",
            self._platform_id, subplatform_id)

        pa_client, _ = self._pa_clients[subplatform_id]

        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE)
        retval = self._execute_agent(pa_client, cmd, subplatform_id)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: _initialize_subplatform %r  retval = %s",
                self._platform_id, subplatform_id, str(retval))

    def _get_subplatform_ids(self):
        """
        Gets the IDs of my sub-platforms.
        """
        return self._pnode.subplatforms.keys()

    def _get_ports(self):
        ports = {}
        for port_id, port in self._pnode.ports.iteritems():
            ports[port_id] = {'network': port.network}
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: _get_ports: %s", self._platform_id, ports)
        return ports

    def _subplatforms_launch(self):
        """
        Launches all my sub-platforms storing the corresponding
        ResourceAgentClient objects in _pa_clients.
        """
        self._pa_clients.clear()
        subplatform_ids = self._get_subplatform_ids()
        if len(subplatform_ids):
            if log.isEnabledFor(logging.DEBUG):
                log.debug("%r: launching subplatforms %s",
                    self._platform_id, str(subplatform_ids))
            for subplatform_id in subplatform_ids:
                self._launch_platform_agent(subplatform_id)

    def _subplatforms_execute_agent(self, command=None, create_command=None,
                                    expected_state=None):
        """
        Supporting routine for the ones below.

        @param create_command invoked as create_command(subplatform_id) for
               each sub-platform to create the command to be executed.
        @param expected_state
        """
        subplatform_ids = self._get_subplatform_ids()
        assert subplatform_ids == self._pa_clients.keys()

        if not len(subplatform_ids):
            # I'm a leaf.
            return

        if command:
            log.debug("%r: executing command %r on my sub-platforms: %s",
                        self._platform_id, command, str(subplatform_ids))
        else:
            log.debug("%r: executing command on my sub-platforms: %s",
                        self._platform_id, str(subplatform_ids))

        #
        # TODO what to do if a sub-platform fails in some way?
        #
        for subplatform_id in self._pa_clients:
            pa_client, _ = self._pa_clients[subplatform_id]
            cmd = AgentCommand(command=command) if command else create_command(subplatform_id)

            # execute command:
            try:
                retval = self._execute_agent(pa_client, cmd, subplatform_id)
            except Exception as e:
                exc = "%s: %s" % (e.__class__.__name__, str(e))
                log.error("%r: exception executing command %r in subplatform %r: %s",
                            self._platform_id, command, subplatform_id, exc) #, exc_Info=True)
                continue

            # verify state:
            try:
                state = pa_client.get_agent_state()
                if expected_state and expected_state != state:
                    log.error("%r: expected subplatform state %r but got %r",
                                self._platform_id, expected_state, state)
            except Exception as e:
                exc = "%s: %s" % (e.__class__.__name__, str(e))
                log.error("%r: exception while calling get_agent_state to subplatform %r: %s",
                            self._platform_id, subplatform_id, exc) #, exc_Info=True)

    def _subplatforms_reset(self):
        self._subplatforms_execute_agent(command=PlatformAgentEvent.RESET,
                                         expected_state=PlatformAgentState.UNINITIALIZED)

    def _subplatforms_go_active(self):
        self._subplatforms_execute_agent(command=PlatformAgentEvent.GO_ACTIVE,
                                         expected_state=PlatformAgentState.IDLE)

    def _subplatforms_go_inactive(self):
        self._subplatforms_execute_agent(command=PlatformAgentEvent.GO_INACTIVE,
                                         expected_state=PlatformAgentState.INACTIVE)

    def _subplatforms_run(self):
        self._subplatforms_execute_agent(command=PlatformAgentEvent.RUN,
                                         expected_state=PlatformAgentState.COMMAND)

    ##############################################################
    # major operations
    ##############################################################

    def _initialize(self):
        """
        Processes the overall INITIALIZE command: does proper initialization
        and launches and initializes sub-platforms, so, the whole network
        rooted here gets launched and initialized recursively.
        """
        assert self._plat_config, "platform_config must have been provided"

        log.info("%r: _initializing with provided platform_config...",
                 self._plat_config['platform_id'])

        self._do_initialize()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: _do_initialize completed.", self._platform_id)

        # done with the initialization for this particular agent; and now
        # we have information to launch the sub-platform agents:
        self._subplatforms_launch()

        result = None
        return result

    def _go_active(self):
        # first myself, then sub-platforms
        self._do_go_active()
        self._subplatforms_go_active()
        result = None
        return result

    def _ping_resource(self):
        result = self._trigger_driver_event(PlatformDriverEvent.PING)
        return result

    ##############################################################
    # UNINITIALIZED event handlers.
    ##############################################################

    def _handler_uninitialized_initialize(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._initialize()
        next_state = PlatformAgentState.INACTIVE

        return (next_state, result)

    ##############################################################
    # INACTIVE event handlers.
    ##############################################################

    def _handler_inactive_reset(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = None
        next_state = PlatformAgentState.UNINITIALIZED

        # first sub-platforms, then myself
        self._subplatforms_reset()
        self._reset()

        return (next_state, result)

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = PlatformAgentState.IDLE

        result = self._go_active()

        return (next_state, result)

    ##############################################################
    # IDLE event handlers.
    ##############################################################

    def _handler_idle_reset(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = None
        next_state = PlatformAgentState.UNINITIALIZED

        # first sub-platforms, then myself
        self._subplatforms_reset()
        self._reset()

        return (next_state, result)

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = PlatformAgentState.INACTIVE

        result = self._go_inactive()

        return (next_state, result)

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = PlatformAgentState.COMMAND

        result = self._run()

        return (next_state, result)

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

        next_state = PlatformAgentState.COMMAND
        result = None

        return (next_state, result)

    def _handler_stopped_clear(self, *args, **kwargs):
        """
        Transitions to IDLE state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = PlatformAgentState.IDLE
        result = None

        return (next_state, result)

    ##############################################################
    # COMMAND event handlers.
    ##############################################################

    def _handler_command_reset(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = None
        next_state = PlatformAgentState.UNINITIALIZED

        # first sub-platforms, then myself
        self._subplatforms_reset()
        self._reset()

        return (next_state, result)


    def _handler_command_clear(self, *args, **kwargs):
        """
        Transitions to IDLE state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = PlatformAgentState.IDLE
        result = None

        return (next_state, result)

    def _handler_command_pause(self, *args, **kwargs):
        """
        Transitions to STOPPED state.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = PlatformAgentState.STOPPED
        result = None

        return (next_state, result)

    ##############################################################
    # Capabilities interface and event handlers.
    ##############################################################

    def _handler_get_resource_capabilities(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        # TODO

        result = None
        next_state = None

#        result = self._dvr_client.cmd_dvr('get_resource_capabilities', *args, **kwargs)
        res_cmds = []
        res_params = []
        result = [res_cmds, res_params]
        return (next_state, result)

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

        attr_names = kwargs.get('attr_names', None)
        if attr_names is None:
            raise BadRequest('get_resource missing attr_names argument.')

        from_time = kwargs.get('from_time', None)
        if from_time is None:
            raise BadRequest('get_resource missing from_time argument.')

        try:
            result = self._get_attribute_values(attr_names, from_time)

            next_state = self.get_agent_state()

        except Exception as ex:
            log.error("error in _get_attribute_values %s", str(ex)) #, exc_Info=True)
            raise

        return (next_state, result)

    def _handler_set_resource(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        attrs = kwargs.get('attrs', None)
        if attrs is None:
            raise BadRequest('set_resource missing attrs argument.')

        try:
            result = self._trigger_driver_event(PlatformDriverEvent.SET_ATTRIBUTE_VALUES,
                                        **kwargs)
            next_state = self.get_agent_state()

        except Exception as ex:
            log.error("error in set_attribute_values %s", str(ex)) #, exc_Info=True)
            raise

        return (next_state, result)

    def _handler_ping_resource(self, *args, **kwargs):
        """
        Pings the driver.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._ping_resource()

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_get_metadata(self, *args, **kwargs):
        """
        Gets platform's metadata
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._trigger_driver_event(PlatformDriverEvent.GET_METADATA)

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_get_ports(self, *args, **kwargs):
        """
        Gets info about platform's ports
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._get_ports()

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_connect_instrument(self, *args, **kwargs):
        """
        Connects an instrument to a given port in this platform.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        port_id = kwargs.get('port_id', None)
        if port_id is None:
            raise BadRequest('connect_instrument: missing port_id argument.')

        instrument_id = kwargs.get('instrument_id', None)
        if instrument_id is None:
            raise BadRequest('connect_instrument: missing instrument_id argument.')

        attributes = kwargs.get('attributes', None)
        if attributes is None:
            raise BadRequest('connect_instrument: missing attributes argument.')

        result = self._trigger_driver_event(PlatformDriverEvent.CONNECT_INSTRUMENT,
                                            **kwargs)

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_disconnect_instrument(self, *args, **kwargs):
        """
        Disconnects an instrument from a given port in this platform.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        port_id = kwargs.get('port_id', None)
        if port_id is None:
            raise BadRequest('disconnect_instrument: missing port_id argument.')

        instrument_id = kwargs.get('instrument_id', None)
        if instrument_id is None:
            raise BadRequest('disconnect_instrument: missing instrument_id argument.')

        result = self._trigger_driver_event(PlatformDriverEvent.DISCONNECT_INSTRUMENT,
                                            **kwargs)

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_get_connected_instruments(self, *args, **kwargs):
        """
        Gets the connected instruments to a given port in this platform.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        port_id = kwargs.get('port_id', None)
        if port_id is None:
            raise BadRequest('get_connected_instruments: missing port_id argument.')

        result = self._trigger_driver_event(PlatformDriverEvent.GET_CONNECTED_INSTRUMENTS,
                                            **kwargs)

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_turn_on_port(self, *args, **kwargs):
        """
        Turns on a given port in this platform.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        port_id = kwargs.get('port_id', None)
        if port_id is None:
            raise BadRequest('turn_on_port missing port_id argument.')

        result = self._trigger_driver_event(PlatformDriverEvent.TURN_ON_PORT, **kwargs)

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_turn_off_port(self, *args, **kwargs):
        """
        Turns off a given port in this platform.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        port_id = kwargs.get('port_id', None)
        if port_id is None:
            raise BadRequest('turn_off_port missing port_id argument.')

        result = self._trigger_driver_event(PlatformDriverEvent.TURN_OFF_PORT, **kwargs)

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_get_subplatform_ids(self, *args, **kwargs):
        """
        Gets the IDs of my direct subplatforms.
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._get_subplatform_ids()

        next_state = self.get_agent_state()

        return (next_state, result)

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

        except Exception as ex:
            log.error("error in _start_resource_monitoring %s", str(ex)) #, exc_Info=True)
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

        except Exception as ex:
            log.error("error in _stop_resource_monitoring %s", str(ex)) #, exc_Info=True)
            raise

        return (next_state, result)

    ##############################################################
    # sync/checksum
    ##############################################################

    def _check_sync(self):
        """
        This will be the main operation related with checking that the
        information on this platform agent (and sub-platforms) is consistent
        with the information in the external network rooted at the
        corresponding platform, then publishing relevant notification events.

        For the moment, it only tries to do the following:
        - gets the checksum reported by the external platform
        - compares it with the local checksum
        - if equal ...
        - if different ...

        @todo complete implementation

        @return TODO
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: _check_sync: getting external checksum..." % self._platform_id)

        external_checksum = self._trigger_driver_event(PlatformDriverEvent.GET_CHECKSUM)
        local_checksum = self._pnode.compute_checksum()

        if external_checksum == local_checksum:
            result = "OK: checksum for platform_id=%r: %s" % (
                self._platform_id, local_checksum)
        else:
            result = "ERROR: different external and local checksums for " \
                     "platform_id=%r: %s != %s" % (self._platform_id,
                     external_checksum, local_checksum)

            # TODO
            # - determine what sub-components are in disagreement
            # - publish relevant event(s)

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: _check_sync: result: %s" % (self._platform_id, result))

        return result

    def _handler_check_sync(self, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r/%s args=%s kwargs=%s",
                self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        next_state = None

        result = self._check_sync()

        return next_state, result

    ##############################################################
    # FSM setup.
    ##############################################################

    def _construct_fsm(self):
        """
        """
        log.debug("constructing fsm")

        # Instrument agent state machine.
        self._fsm = InstrumentFSM(PlatformAgentState, PlatformAgentEvent,
                                  PlatformAgentEvent.ENTER, PlatformAgentEvent.EXIT)

        for state in PlatformAgentState.list():
            self._fsm.add_handler(state, PlatformAgentEvent.ENTER, self._common_state_enter)
            self._fsm.add_handler(state, PlatformAgentEvent.EXIT, self._common_state_exit)

        # UNINITIALIZED state event handlers.
        self._fsm.add_handler(PlatformAgentState.UNINITIALIZED, PlatformAgentEvent.INITIALIZE, self._handler_uninitialized_initialize)
        self._fsm.add_handler(PlatformAgentState.UNINITIALIZED, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # INACTIVE state event handlers.
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.RESET, self._handler_inactive_reset)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_METADATA, self._handler_get_metadata)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_PORTS, self._handler_get_ports)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_get_subplatform_ids)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GO_ACTIVE, self._handler_inactive_go_active)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # IDLE state event handlers.
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RUN, self._handler_idle_run)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # STOPPED state event handlers.
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.RESUME, self._handler_stopped_resume)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.CLEAR, self._handler_stopped_clear)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(PlatformAgentState.STOPPED, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # COMMAND/MONITORING common state event handlers.
        # TODO revisit this when introducing the BUSY state
        for state in [PlatformAgentState.COMMAND, PlatformAgentState.MONITORING]:
            self._fsm.add_handler(state, PlatformAgentEvent.RESET, self._handler_command_reset)
            self._fsm.add_handler(state, PlatformAgentEvent.GET_METADATA, self._handler_get_metadata)
            self._fsm.add_handler(state, PlatformAgentEvent.GET_PORTS, self._handler_get_ports)
            self._fsm.add_handler(state, PlatformAgentEvent.CONNECT_INSTRUMENT, self._handler_connect_instrument)
            self._fsm.add_handler(state, PlatformAgentEvent.DISCONNECT_INSTRUMENT, self._handler_disconnect_instrument)
            self._fsm.add_handler(state, PlatformAgentEvent.GET_CONNECTED_INSTRUMENTS, self._handler_get_connected_instruments)
            self._fsm.add_handler(state, PlatformAgentEvent.TURN_ON_PORT, self._handler_turn_on_port)
            self._fsm.add_handler(state, PlatformAgentEvent.TURN_OFF_PORT, self._handler_turn_off_port)
            self._fsm.add_handler(state, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_get_subplatform_ids)
            self._fsm.add_handler(state, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
            self._fsm.add_handler(state, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
            self._fsm.add_handler(state, PlatformAgentEvent.GET_RESOURCE, self._handler_get_resource)
            self._fsm.add_handler(state, PlatformAgentEvent.SET_RESOURCE, self._handler_set_resource)
            self._fsm.add_handler(state, PlatformAgentEvent.CHECK_SYNC, self._handler_check_sync)

        # COMMAND state event handlers.
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.PAUSE, self._handler_command_pause)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.CLEAR, self._handler_command_clear)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.START_MONITORING, self._handler_start_resource_monitoring)

        # MONITORING state event handlers.
        self._fsm.add_handler(PlatformAgentState.MONITORING, PlatformAgentEvent.STOP_MONITORING, self._handler_stop_resource_monitoring)
