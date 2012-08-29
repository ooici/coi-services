#!/usr/bin/env python

"""
@package ion.agents.platform.platform_agent
@file    ion/agents/platform/platform_agent.py
@author  Carlos Rueda
@brief   Supporting types for platform agents.
         PRELIMINARY
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from pyon.agent.agent import ResourceAgent
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

from ion.agents.instrument.common import BaseEnum

from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.platform_driver import AttributeValueDriverEvent
from ion.agents.platform.exceptions import CannotInstantiateDriverException

from pyon.ion.stream import StreamPublisherRegistrar
from ion.agents.platform.test.adhoc import adhoc_get_stream_names
from ion.agents.platform.test.adhoc import adhoc_get_packet_factories


from ion.agents.instrument.instrument_fsm import InstrumentFSM

class PlatformAgentState(ResourceAgentState):
    """
    Platform agent state enum.
    """
    pass

class PlatformAgentEvent(ResourceAgentEvent):
    GET_SUBPLATFORM_IDS = 'get_subplatform_ids'
    ADD_SUBPLATFORM = 'add_subplatform_agent_client'


class PlatformAgentCapability(BaseEnum):
#    INITIALIZE = ResourceAgentEvent.INITIALIZE
#    RESET = ResourceAgentEvent.RESET
#    GO_ACTIVE = ResourceAgentEvent.GO_ACTIVE
#    GO_INACTIVE = ResourceAgentEvent.GO_INACTIVE
#    RUN = ResourceAgentEvent.RUN
#    CLEAR = ResourceAgentEvent.CLEAR
#    PAUSE = ResourceAgentEvent.PAUSE
#    RESUME = ResourceAgentEvent.RESUME
#    GO_COMMAND = ResourceAgentEvent.GO_COMMAND
#    GO_DIRECT_ACCESS = ResourceAgentEvent.GO_DIRECT_ACCESS
    pass


class PlatformAgent(ResourceAgent):
    """
    Platform resource agent.
    """
    #
    # PRELIMINARY
    #

    # Override to publish specific types of events
    COMMAND_EVENT_TYPE = "DeviceCommandEvent"

    # Override to set specific origin type
    ORIGIN_TYPE = "PlatformDevice"

    def __init__(self):
        ResourceAgent.__init__(self)
        self._plat_driver = None
        self._reset()

        # Dictionary of data stream IDs for data publishing. Constructed
        # by stream_config agent config member during process on_init.
        self._data_streams = {}

        # Dictionary of data stream publishers. Constructed by
        # stream_config agent config member during process on_init.
        self._data_publishers = {}

        # Factories for stream packets. Constructed by driver
        # configuration information on transition to inactive.
        self._packet_factories = {}

        # Stream registrar to create publishers. Used to create
        # stream publishers, set during process on_init.
        self._stream_registrar = None

        #
        # TODO the following defined here as in InstrumentAgent,
        # but these will be part of the platform metadata
        self._lat = 0
        self._lon = 0
        self._height = 0


    def _reset(self):
        self._plat_config = None
        self._platform_id = None
        self._subplatform_ids = []  # platform IDs of my direct subplatforms

        if self._plat_driver:
            self._plat_driver.destroy()
            self._plat_driver = None

        self._current_state = PlatformAgentState.UNINITIALIZED
        log.info("_current_state: %s" % str(self._current_state))

    def _pre_initialize(self):
        """
        adapted from InstrumentAgent.on_init
        """

        # The registrar to create publishers.
        self._stream_registrar = StreamPublisherRegistrar(process=self,
                                                    container=self.container)

        # Construct stream publishers.
        self._construct_data_publishers()

    def _construct_data_publishers(self):
        """
        Construct the stream publishers from the stream_config agent
        config variable.
        @retval None
        """
        stream_info = self.CFG.stream_config
        log.info("stream_info = %s" % stream_info)

        for (name, stream_config) in stream_info.iteritems():
            stream_id = stream_config['id']
            self._data_streams[name] = stream_id
            publisher = self._stream_registrar.create_publisher(stream_id=stream_id)
            self._data_publishers[name] = publisher
            log.info("Agent '%s' created publisher for stream_name "
                     "%s (stream_id=%s)" % (self._proc_name, name, stream_id))

    def _construct_packet_factories(self):
        """
        """
        stream_names = adhoc_get_stream_names()
        self._packet_factories = adhoc_get_packet_factories(stream_names, self.CFG.stream_config)

    def _clear_packet_factories(self):
        """
        Delete packet factories.
        @retval None
        """
        self._packet_factories.clear()
        log.info("Agent '%s' deleted packet factories.", self._proc_name)

    def _initialize(self):
        """
        Does basic verification of self._plat_config and instantiates the
        platform driver.

        @raises PlatformException if the verification fails for some reason.
        """
        assert self._plat_config is not None, "plat_config required"

        log.info("plat_config=%s " % str(self._plat_config))

        for k in ['platform_id', 'driver_config']:
            if not k in self._plat_config:
                raise PlatformException("'%s' key not given in plat_config=%s"
                                        % (k, self._plat_config))

        self._platform_id = self._plat_config['platform_id']
        driver_config = self._plat_config['driver_config']
        for k in ['dvr_mod', 'dvr_cls']:
            if not k in driver_config:
                raise PlatformException("'%s' key not given in driver_config=%s"
                                        % (k, driver_config))

        self._create_driver()
        self._plat_driver.set_event_listener(self.evt_recv)

    def _create_driver(self):
        """
        Creates the platform driver object for this platform agent.

        NOTE: the driver object is created directly (not via a spawned process)
        """
        driver_config = self._plat_config['driver_config']
        driver_module = driver_config['dvr_mod']
        driver_class = driver_config['dvr_cls']

        assert self._platform_id is not None, "must know platform_id to create driver"

        log.info('Creating driver: %s', driver_config)

        try:
            module = __import__(driver_module, fromlist=[driver_class])
            classobj = getattr(module, driver_class)
            driver = classobj(self._platform_id, driver_config)

        except Exception as e:
            # TODO log-and-throw is considered an anti-pattern, but the
            # exception alone does not show up in the logs!
            msg = 'Could not import/construct driver: module=%s, class=%s' % (
                driver_module, driver_class)
            log.error("%s; reason=%s" % (msg, str(e)))
            raise CannotInstantiateDriverException(msg=msg, reason=e)

#        driver.set_oms_client(self._oms)
        log.info("Driver created: %s" % str(driver))
        self._plat_driver = driver

    def _go_active(self):
        """
        Subclass does basic check of self._plat_config.

        @raises PlatformException if some error happens
        """
        assert self._plat_driver is not None, "_create_driver must have been called first"
        self._plat_driver.go_active()

    def _start_resource_monitoring(self):
        """
        Starts underlying mechanisms to monitor the resources associated with
        this platform.
        """
        assert self._plat_driver is not None, "_create_driver must have been called"

        self._plat_driver.start_resource_monitoring()

#    def _get_resource_id(self, platform_id):
#        """
#        To be implemented by subclass.
#        Gets the resource ID corresponding to the given platform ID.
#        This resource ID can be used to create a ResourceAgentClient instance
#        to talk with the corresponding agent (if already launched).
#
#        @param platform_id Platform ID
#
#        @retval resource ID corresponding to the given platform ID.
#        """
#        raise NotImplemented()

    def evt_recv(self, driver_event):
        """
        Callback to receive asynchronous driver events.
        @param driver_event The driver event received.
        """
        log.info('Platform agent=%r/%s received driver_event=%s' % (
            self._platform_id, self.get_agent_state(), str(driver_event)))

        value = {}
        if isinstance(driver_event, AttributeValueDriverEvent):
            stream_name = driver_event._attr_id

            #
            # create and publish packet
            #
            if stream_name in self._data_streams:
                value['value'] = [driver_event._value]
                value['lat'] = [self._lat]
                value['lon'] = [self._lon]
                value['height'] = [self._height]
                value['stream_id'] = self._data_streams[stream_name]

                packet = self._packet_factories[stream_name](**value)
                self._data_publishers[stream_name].publish(packet)

                log.info('Platform agent=%r: published data packet. '
                         'stream_name=%s (stream_id=%s)', self._platform_id,
                         stream_name, value['stream_id'])
            else:
                log.warn('Platform agent=%r: unrecognized stream_name=%r' % (
                         self._platform_id, stream_name))


        else:
            log.warn('Platform agent=%r: driver_event not handled yet=%s' % (
                self._platform_id, str(type(driver_event))))


    ##########################################################################
    # ???
    ##########################################################################

    def add_instrument(self, instrument_agent):
        # TODO
        pass


    ##############################################################
    # UNINITIALIZED event handlers.
    ##############################################################

    def _handler_uninitialized_initialize(self, *args, **kwargs):
        """
        """
        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = None

        plat_config = kwargs.get('plat_config', None)
        if not plat_config:
            raise PlatformException("plat_config not provided")

        next_state = PlatformAgentState.INACTIVE

        self._plat_config = plat_config
        self._pre_initialize()
        self._initialize()

        return (next_state, result)

    ##############################################################
    # INACTIVE event handlers.
    ##############################################################

    def _handler_inactive_reset(self, *args, **kwargs):
        """
        """
        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = None

#        result = self._stop_driver()
        self._clear_packet_factories()
        next_state = PlatformAgentState.UNINITIALIZED

        return (next_state, result)

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        """
        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

        next_state = None
        result = None

        self._go_active()

        next_state = PlatformAgentState.IDLE

        return (next_state, result)

    ##############################################################
    # IDLE event handlers.
    ##############################################################

    def _handler_idle_reset(self, *args, **kwargs):
        """
        """
        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = None

        # Disconnect, initialize, stop driver and go to uninitialized.
#        self._dvr_client.cmd_dvr('disconnect')
#        self._dvr_client.cmd_dvr('initialize')
#        result = self._stop_driver()
        self._clear_packet_factories()
        next_state = PlatformAgentState.UNINITIALIZED

        return (next_state, result)

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        """
        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

        next_state = None
        result = None
#        self._dvr_client.cmd_dvr('disconnect')
#        self._dvr_client.cmd_dvr('initialize')
        next_state = PlatformAgentState.INACTIVE

        return (next_state, result)

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

        next_state = None
        result = None

        self._construct_packet_factories()

        self._start_resource_monitoring()

        next_state = PlatformAgentState.COMMAND

        return (next_state, result)


    ##############################################################
    # COMMAND event handlers.
    ##############################################################

    def _handler_command_reset(self, *args, **kwargs):
        """
        """
        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

        next_state = None
        result = None
#        self._dvr_client.cmd_dvr('disconnect')
#        self._dvr_client.cmd_dvr('initialize')
#        result = self._stop_driver()
        self._clear_packet_factories()
        next_state = PlatformAgentState.UNINITIALIZED

        return (next_state, result)

    def _handler_command_add_subplatform_agent_client(self, *args, **kwargs):
        """
        Adds a subplatform to this platform. From the point of view of this
        PlatformAgent instance, only the IDs of the subplatforms are
        maintained here (it is up to other code to launch/terminate the
        corresponding sub-platform instances as appropriate.
        The subplatform is given by the 'subplatform_id' entry in kwargs.
        """
        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

        subplatform_id = kwargs.get('subplatform_id', None)
        if not subplatform_id:
            raise PlatformException("subplatform_id not provided")

        if subplatform_id in self._subplatform_ids:
            raise PlatformException("Subplatform by ID %r already added" %
                                    subplatform_id)

        self._subplatform_ids.append(subplatform_id)

        log.info("subplatform '%s' added to platform '%s'" %(
            subplatform_id, self._platform_id))

        result = None
        next_state = self.get_agent_state()

        return (next_state, result)


    def _handler_command_get_subplatform_ids(self, *args, **kwargs):
        """
        Gets the IDs of the direct subplatforms.

        @retval A list of pairs [(platform_id, resource_id), ...] with each
                pair indicating platform_id and corresponding
                resource_id (which can be used to create a ResourceAgentClient).
        """

        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

        def get_resource_id(platform_id):
            return 'oms_platform_agent_%s' % platform_id

        result = [(platform_id, get_resource_id(platform_id)) \
                  for platform_id in self._subplatform_ids]

        next_state = self.get_agent_state()

        return (next_state, result)


    ##############################################################
    # Capabilities interface and event handlers.
    ##############################################################

    def _handler_get_resource_capabilities(self, *args, **kwargs):
        """
        """
        log.info("state=%s args=%s kwargs=%s" % (self.get_agent_state(), str(args), str(kwargs)))

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
    # FSM setup.
    ##############################################################

    def _construct_fsm(self):
        """
        """
        log.info("constructing fsm")
        # Instrument agent state machine.
        self._fsm = InstrumentFSM(PlatformAgentState, PlatformAgentEvent,
                                  PlatformAgentEvent.ENTER, PlatformAgentEvent.EXIT)

        for state in PlatformAgentState.list():
            self._fsm.add_handler(state, PlatformAgentEvent.ENTER, self._common_state_enter)
            self._fsm.add_handler(state, PlatformAgentEvent.EXIT, self._common_state_exit)

        # UNINITIALIZED state event handlers.
        self._fsm.add_handler(PlatformAgentState.UNINITIALIZED, PlatformAgentEvent.INITIALIZE, self._handler_uninitialized_initialize)
        self._fsm.add_handler(ResourceAgentState.UNINITIALIZED, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # INACTIVE state event handlers.
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.RESET, self._handler_inactive_reset)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.ADD_SUBPLATFORM, self._handler_command_add_subplatform_agent_client)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_command_get_subplatform_ids)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GO_ACTIVE, self._handler_inactive_go_active)

        # IDLE state event handlers.
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RUN, self._handler_idle_run)

        # COMMAND state event handlers.
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.RESET, self._handler_command_reset)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.ADD_SUBPLATFORM, self._handler_command_add_subplatform_agent_client)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_command_get_subplatform_ids)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
#        ...

