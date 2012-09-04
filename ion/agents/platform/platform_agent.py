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
from interface.objects import AgentCommand
from pyon.agent.agent import ResourceAgentClient
from pyon.util.context import LocalContextMixin

from ion.agents.instrument.common import BaseEnum

from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.platform_driver import AttributeValueDriverEvent
from ion.agents.platform.exceptions import CannotInstantiateDriverException

from pyon.ion.stream import StreamPublisherRegistrar
from ion.agents.platform.test.adhoc import adhoc_get_stream_names
from ion.agents.platform.test.adhoc import adhoc_get_packet_factories

from ion.agents.instrument.instrument_fsm import InstrumentFSM

#
# hack: we need to know the container_client so we can spawn subplatform agents.
# TODO how to properly handle this?
#
_container_client = None
def set_container_client(container_client):
    global _container_client
    _container_client = container_client


class PlatformAgentState(ResourceAgentState):
    """
    Platform agent state enum.
    """
    pass


class PlatformAgentEvent(ResourceAgentEvent):
    GET_SUBPLATFORM_IDS = 'get_subplatform_ids'


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


# provisional
def get_resource_id(platform_id):
    return 'platform_agent_rid_%s' % platform_id


# TODO Use appropriate process in ResourceAgentClient instance constructio below.
# for now, just replicating typical mechanism in test cases.
class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


class PlatformAgent(ResourceAgent):
    """
    Platform resource agent.
    """

    # Override to publish specific types of events
    COMMAND_EVENT_TYPE = "DeviceCommandEvent" #TODO how this works?

    # Override to set specific origin type
    ORIGIN_TYPE = "PlatformDevice"  #TODO how this works?

    def __init__(self):
        ResourceAgent.__init__(self)

        self._plat_config = None
        self._platform_id = None
        self._plat_driver = None

        # Dictionary of data stream IDs for data publishing. Constructed
        # by stream_config agent config member during process on_init.
        self._data_streams = {}

        # Dictionary of data stream publishers. Constructed by
        # stream_config agent config member during process on_init.
        self._data_publishers = {}

        # Factories for stream packets. Constructed by driver
        # configuration information on transition to inactive.
        self._packet_factories = {}

        # Stream registrar to create publishers
        self._stream_registrar = None

        # The ResourceAgentClient objects to send commands to my sub-platforms
        self._pa_clients = {}

        #
        # TODO the following defined here as in InstrumentAgent,
        # but these will likely be part of the platform (or associated
        # instruments) metadata
        self._lat = 0
        self._lon = 0
        self._height = 0

    def _reset(self):
        self._plat_config = None
        self._platform_id = None
        if self._plat_driver:
            self._plat_driver.destroy()
            self._plat_driver = None

        self._clear_packet_factories()
        self._clear_pa_clients()

    def _pre_initialize(self):
        """
        Does verification of self._plat_config.

        @raises PlatformException if the verification fails for some reason.
        """
        log.info("%r: plat_config=%s " % (
            self._platform_id, str(self._plat_config)))

        if not self._plat_config:
            raise PlatformException("plat_config not provided")

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

    def _construct_data_publishers(self):
        """
        Construct the stream publishers from the stream_config agent
        config variable.
        @retval None
        """

        # The registrar to create publishers.
        self._stream_registrar = StreamPublisherRegistrar(process=self,
                                                    container=self.container)

        stream_info = self.CFG.stream_config
        log.info("%r: stream_info = %s" % (
            self._platform_id, stream_info))

        for (name, stream_config) in stream_info.iteritems():
            stream_id = stream_config['id']
            self._data_streams[name] = stream_id
            publisher = self._stream_registrar.create_publisher(stream_id=stream_id)
            self._data_publishers[name] = publisher
            log.info("%r: created publisher for stream_name=%r (stream_id=%r)" % (
                self._platform_id, name, stream_id))

    def _construct_packet_factories(self):
        """
        Constructs the packet factories for the streams associated to this
        platform.
        """
        stream_names = adhoc_get_stream_names()
        self._packet_factories = adhoc_get_packet_factories(stream_names, self.CFG.stream_config)

    def _clear_packet_factories(self):
        """
        Deletes packet factories.
        """
        self._packet_factories.clear()

    def _clear_pa_clients(self):
        """
        Deletes all my ResourceAgentClient objects
        """
        self._pa_clients.clear()

    def _create_driver(self):
        """
        Creates the platform driver object for this platform agent.

        NOTE: the driver object is created directly (not via a spawned process)
        """
        driver_config = self._plat_config['driver_config']
        driver_module = driver_config['dvr_mod']
        driver_class = driver_config['dvr_cls']

        assert self._platform_id is not None, "must know platform_id to create driver"

        log.info('%r: creating driver: %s' % (
            self._platform_id,  driver_config))

        try:
            module = __import__(driver_module, fromlist=[driver_class])
            classobj = getattr(module, driver_class)
            driver = classobj(self._platform_id, driver_config)

        except Exception as e:
            # TODO log-and-throw is considered an anti-pattern, but the
            # exception alone does not show up in the logs!
            msg = '%r: could not import/construct driver: module=%s, class=%s' % (
                self._platform_id, driver_module, driver_class)
            log.error("%s; reason=%s" % (msg, str(e)))
            raise CannotInstantiateDriverException(msg=msg, reason=e)

        self._plat_driver = driver
        self._plat_driver.set_event_listener(self.evt_recv)
        log.info("%r: driver created: %s" % (
            self._platform_id, str(driver)))

    def _assert_driver(self):
        assert self._plat_driver is not None, "_create_driver must have been called first"

    def _initialize(self):
        """
        Does the main initialize sequence, which includes activation of the
        driver and launch of the sub-platforms
        """
        self._pre_initialize()
        self._construct_data_publishers()
        self._create_driver()
        self._plat_driver.go_active()

    def _go_active(self):
        """
        Does nothing at the moment.
        """
        pass

    def _go_inactive(self):
        """
        Does nothing at the moment.
        """
        pass

    def _run(self):
        """
        """
        self._construct_packet_factories()
        self._start_resource_monitoring()

    def _start_resource_monitoring(self):
        """
        Calls self._plat_driver.start_resource_monitoring()
        """
        self._assert_driver()
        self._plat_driver.start_resource_monitoring()

    def _stop_resource_monitoring(self):
        """
        Calls self._plat_driver.stop_resource_monitoring()
        """
        self._assert_driver()
        self._plat_driver.stop_resource_monitoring()

    def evt_recv(self, driver_event):
        """
        Callback to receive asynchronous driver events.
        @param driver_event The driver event received.
        """
        log.info('%r: in state=%s: received driver_event=%s' % (
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

                log.info('%r: published data packet. stream_name=%s (stream_id=%s)' % (
                    self._platform_id, stream_name, value['stream_id']))
            else:
                log.warn('%r: unrecognized stream_name=%r' % (
                         self._platform_id, stream_name))


        else:
            log.warn('%r: driver_event not handled yet=%s' % (
                self._platform_id, str(type(driver_event))))

    ##########################################################################
    # TBD
    ##########################################################################

    def add_instrument(self, instrument_config):
        # TODO addition of instruments TBD in general
        pass

    def add_instruments(self):
        # TODO this is just a sketch; not all operations will necessarily happen
        # in this same call.
        # query resource registry to find all instruments
#        for instr in my_instruments:
#            launch_instrument_agent(...)
#            launch_port_agent(...)
#            activate_instrument(...)
        pass


    ##############################################################
    # supporting routines dealing with sub-platforms
    ##############################################################

    def _launch_platform_agent(self, container_client, subplatform_id):
        """
        Launches a platform agent including the INITIALIZE command,
        and returns corresponding ResourceAgentClient instance.

        @param container_client used to spawn the agent process
        @param subplatform_id Platform ID
        """
        log.info("%r: launching subplatform %r" % (
            self._platform_id, subplatform_id))

        pa_resource_id = get_resource_id(subplatform_id)
        pa_name = 'PlatformAgent_%s' % subplatform_id
        pa_module = 'ion.agents.platform.platform_agent'
        pa_class = 'PlatformAgent'

        agent_config = {
            'agent': {'resource_id': pa_resource_id},
            'stream_config' : self.CFG.stream_config,
            'test_mode' : True
        }

        # Start agent
        log.info("%r: starting agent: agent_config=%s" % (
            self._platform_id, str(agent_config)))

        pa_pid = container_client.spawn_process(name=pa_name,
                                                module=pa_module,
                                                cls=pa_class,
                                                config=agent_config)

        log.info("%r: spawned platform agent process '%s': pa_pid=%s" % (
            self._platform_id, subplatform_id, str(pa_pid)))

        # Start a resource agent client so we can initialize the agent
        pa_client = ResourceAgentClient(pa_resource_id, process=FakeProcess())

        log.info("%r: got platform agent client %s" % (
            self._platform_id, str(pa_client)))

        state = pa_client.get_agent_state()
        assert PlatformAgentState.UNINITIALIZED == state

        # now, initialize the sub-platform agent so the agent network gets
        # built and initialized recursively:
        platform_config = {
            'platform_id': subplatform_id,
            'driver_config': self._plat_config['driver_config']
        }

        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=dict(plat_config=platform_config))
        retval = pa_client.execute_agent(cmd)
        log.info("%r: subplatform %r initialize's retval = %s" % (
            self._platform_id, subplatform_id, str(retval)))

        return pa_client

    def _subplatforms_launch(self):
        """
        Launches all my sub-platforms storing the corresponding
        ResourceAgentClient objects in _pa_clients.
        """
        subplatform_ids = self._plat_driver.get_subplatform_ids()
        log.info("%r: launching subplatforms %s" % (
            self._platform_id, str(subplatform_ids)))
        container_client = _container_client
        self._pa_clients.clear()
        for subplatform_id in subplatform_ids:
            pa_client = self._launch_platform_agent(container_client, subplatform_id)
            self._pa_clients[subplatform_id] = pa_client

    def _subplatforms_execute_agent(self, command=None, create_command=None,
                                    expected_state=None):
        """
        Supporting routine for the ones below.

        @param create_command invoked as create_command(subplatform_id) for
               each sub-platform to create the command to be executed.
        @param expected_state
        """
        subplatform_ids = self._plat_driver.get_subplatform_ids()

        if command:
            log.info("%r: executing command %r on my sub-platforms: %s" % (
                        self._platform_id, command, str(subplatform_ids)))
        else:
            log.info("%r: executing command on my sub-platforms: %s" % (
                        self._platform_id, str(subplatform_ids)))

        assert subplatform_ids == self._pa_clients.keys()

        #
        # TODO what to do if a sub-platform fails in some way?
        #
        for subplatform_id, pa_client in self._pa_clients.iteritems():
            cmd = AgentCommand(command=command) if command else create_command(subplatform_id)
            try:
                retval = pa_client.execute_agent(cmd)
                state = pa_client.get_agent_state()
                if expected_state and expected_state != state:
                    log.error("%r: expected subplatform state %r but got %r" % (
                                self._platform_id, expected_state, state))
            except Exception, ex:
                log.error("%r: exception executing command %r in subplatform %r" % (
                            self._platform_id, cmd, subplatform_id))

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
    # UNINITIALIZED event handlers.
    ##############################################################

    def _handler_uninitialized_initialize(self, *args, **kwargs):
        """
        """
        log.info("%r/%s args=%s kwargs=%s" % (
            self._platform_id, self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = PlatformAgentState.INACTIVE

        self._plat_config = kwargs.get('plat_config', None)
        self._initialize()

        # done with the initialization for this particular agent; and now
        # we have information to launch the sub-platform agents:
        self._subplatforms_launch()

        return (next_state, result)

    ##############################################################
    # INACTIVE event handlers.
    ##############################################################

    def _handler_inactive_reset(self, *args, **kwargs):
        """
        """
        log.info("%r/%s args=%s kwargs=%s" % (
            self._platform_id, self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = PlatformAgentState.UNINITIALIZED

        # first sub-platforms, then myself
        self._subplatforms_reset()
        self._reset()

        return (next_state, result)

    def _handler_inactive_go_active(self, *args, **kwargs):
        """
        """
        log.info("%r/%s args=%s kwargs=%s" % (
            self._platform_id, self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = PlatformAgentState.IDLE

        # first myself, then sub-platforms
        self._go_active()
        self._subplatforms_go_active()

        return (next_state, result)

    ##############################################################
    # IDLE event handlers.
    ##############################################################

    def _handler_idle_reset(self, *args, **kwargs):
        """
        """
        log.info("%r/%s args=%s kwargs=%s" % (
            self._platform_id, self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = PlatformAgentState.UNINITIALIZED

        # first sub-platforms, then myself
        self._subplatforms_reset()
        self._reset()

        return (next_state, result)

    def _handler_idle_go_inactive(self, *args, **kwargs):
        """
        """
        log.info("%r/%s args=%s kwargs=%s" % (
            self._platform_id, self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = PlatformAgentState.INACTIVE

        # first sub-platforms, then myself
        self._subplatforms_go_inactive()
        self._go_inactive()

        return (next_state, result)

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        log.info("%r/%s args=%s kwargs=%s" % (
            self._platform_id, self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = PlatformAgentState.COMMAND

        # first myself, then sub-platforms
        self._run()
        self._subplatforms_run()

        return (next_state, result)


    ##############################################################
    # COMMAND event handlers.
    ##############################################################

    def _handler_command_reset(self, *args, **kwargs):
        """
        """
        log.info("%r/%s args=%s kwargs=%s" % (
            self._platform_id, self.get_agent_state(), str(args), str(kwargs)))

        result = None
        next_state = PlatformAgentState.UNINITIALIZED

        # first sub-platforms, then myself
        self._subplatforms_reset()
        self._reset()

        return (next_state, result)

    def _handler_command_get_subplatform_ids(self, *args, **kwargs):
        """
        Gets the IDs of my direct subplatforms.
        """
        log.info("%r/%s args=%s kwargs=%s" % (
            self._platform_id, self.get_agent_state(), str(args), str(kwargs)))

        result = self._plat_driver.get_subplatform_ids()

        next_state = self.get_agent_state()

        return (next_state, result)


    ##############################################################
    # Capabilities interface and event handlers.
    ##############################################################

    def _handler_get_resource_capabilities(self, *args, **kwargs):
        """
        """
        log.info("%r/%s args=%s kwargs=%s" % (
            self._platform_id, self.get_agent_state(), str(args), str(kwargs)))

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
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_command_get_subplatform_ids)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GO_ACTIVE, self._handler_inactive_go_active)

        # IDLE state event handlers.
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RUN, self._handler_idle_run)

        # COMMAND state event handlers.
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.RESET, self._handler_command_reset)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_command_get_subplatform_ids)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
#        ...

