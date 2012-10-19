#!/usr/bin/env python

"""
@package ion.agents.platform.platform_agent
@file    ion/agents/platform/platform_agent.py
@author  Carlos Rueda
@brief   Supporting types for platform agents.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from pyon.ion.stream import StreamPublisher
from pyon.ion.stream import StandaloneStreamPublisher
from pyon.agent.agent import ResourceAgent
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from interface.objects import AgentCommand
from pyon.agent.agent import ResourceAgentClient

# Pyon exceptions.
from pyon.core.exception import BadRequest

from ion.agents.instrument.common import BaseEnum

from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.platform_driver import AttributeValueDriverEvent
from ion.agents.platform.platform_driver import AlarmDriverEvent
from ion.agents.platform.exceptions import CannotInstantiateDriverException

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
import numpy
from ion.agents.platform.test.adhoc import adhoc_get_parameter_dictionary

from ion.agents.instrument.instrument_fsm import InstrumentFSM

from ion.agents.platform.platform_agent_launcher import LauncherFactory

from coverage_model.parameter import ParameterDictionary
from interface.objects import StreamRoute

import logging
import time

# NOTE: the bigger the platform network size starting from the platform
# associated with a PlatformAgent instance, the more the time that should be
# given for commands to sub-platforms to complete. The following TIMEOUT value
# intends to be big enough for all typical cases.
TIMEOUT = 90


PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


# TODO clean up log-and-throw anti-idiom in several places, which is used
# because the exception alone does not show up in the logs!


class PlatformAgentState(ResourceAgentState):
    """
    Platform agent state enum.
    """
    pass


class PlatformAgentEvent(ResourceAgentEvent):
    GET_SUBPLATFORM_IDS       = 'PLATFORM_AGENT_GET_SUBPLATFORM_IDS'
    START_ALARM_DISPATCH      = 'PLATFORM_AGENT_START_ALARM_DISPATCH'
    STOP_ALARM_DISPATCH       = 'PLATFORM_AGENT_STOP_ALARM_DISPATCH'


class PlatformAgentCapability(BaseEnum):
    INITIALIZE                = PlatformAgentEvent.INITIALIZE
    RESET                     = PlatformAgentEvent.RESET
    GO_ACTIVE                 = PlatformAgentEvent.GO_ACTIVE
    GO_INACTIVE               = PlatformAgentEvent.GO_INACTIVE
    RUN                       = PlatformAgentEvent.RUN
    GET_RESOURCE_CAPABILITIES = PlatformAgentEvent.GET_RESOURCE_CAPABILITIES
    PING_RESOURCE             = PlatformAgentEvent.PING_RESOURCE
    GET_RESOURCE              = PlatformAgentEvent.GET_RESOURCE
    SET_RESOURCE              = PlatformAgentEvent.SET_RESOURCE

    GET_SUBPLATFORM_IDS       = PlatformAgentEvent.GET_SUBPLATFORM_IDS

    START_ALARM_DISPATCH      = PlatformAgentEvent.START_ALARM_DISPATCH
    STOP_ALARM_DISPATCH       = PlatformAgentEvent.STOP_ALARM_DISPATCH



class PlatformAgent(ResourceAgent):
    """
    Platform resource agent.
    """

    # Override to publish specific types of events
    COMMAND_EVENT_TYPE = "DeviceCommandEvent" #TODO how this works?

    # Override to set specific origin type
    ORIGIN_TYPE = "PlatformDevice"  #TODO how this works?

    def __init__(self, standalone=None):
        log.info("PlatformAgent constructor called")
        ResourceAgent.__init__(self)
        self._standalone = standalone
        self._plat_config = None
        self._platform_id = None
        self._topology = None
        self._agent_device_map = None
        self._agent_streamconfig_map = None
        self._plat_driver = None

        # Platform ID of my parent, if any. This is mainly used for diagnostic
        # purposes
        self._parent_platform_id = None

        # Dictionaries used for data publishing. Constructed in _do_initialize
        self._data_streams = {}
        self._param_dicts = {}
        self._stream_defs = {}
        self._data_publishers = {}

        # {subplatform_id: (ResourceAgentClient, PID), ...}
        self._pa_clients = {}  # Never None

        self._launcher = LauncherFactory.createLauncher(standalone=standalone)
        log.debug("launcher created: %s", str(type(self._launcher)))

        # standalone stuff
        self.container = None
        if self._standalone:
            self.resource_id = self._standalone['platform_id']
            self.container = self._standalone.get('container', None)
            self._on_init()

        log.info("PlatformAgent constructor complete.")

    def _reset(self):
        """
        Resets this platform agent (terminates sub-platforms processes,
        clears self._pa_clients, destroys driver).

        NOTE that this method is to be called *after* sending the RESET command
        to my sub-platforms (if any).
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

        self._plat_config = None
        self._platform_id = None
        if self._plat_driver:
            self._plat_driver.destroy()
            self._plat_driver = None

    def _pre_initialize(self):
        """
        Does verification of self._plat_config.

        @raises PlatformException if the verification fails for some reason.
        """
        log.debug("%r: plat_config=%s ",
            self._platform_id, str(self._plat_config))

        if not self._plat_config:
            msg = "plat_config not provided"
            log.error(msg)
            raise PlatformException(msg)

        for k in ['platform_id', 'driver_config', 'container_name']:
            if not k in self._plat_config:
                msg = "'%s' key not given in plat_config=%s" % (k, self._plat_config)
                log.error(msg)
                raise PlatformException(msg)

        self._platform_id = self._plat_config['platform_id']
        driver_config = self._plat_config['driver_config']
        for k in ['dvr_mod', 'dvr_cls']:
            if not k in driver_config:
                msg = "%r: '%s' key not given in driver_config=%s" % (
                    self._platform_id, k, driver_config)
                log.error(msg)
                raise PlatformException(msg)

        self._container_name = self._plat_config['container_name']

        if 'platform_topology' in self._plat_config:
            self._topology = self._plat_config['platform_topology']

        if 'agent_device_map' in self._plat_config:
            self._agent_device_map = self._plat_config['agent_device_map']

        if 'agent_streamconfig_map' in self._plat_config:
            self._agent_streamconfig_map = self._plat_config['agent_streamconfig_map']

        ppid = self._plat_config.get('parent_platform_id', None)
        if ppid:
            self._parent_platform_id = ppid
            log.debug("_parent_platform_id set to: %s", self._parent_platform_id)


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

        return True, ''

    def check_ping_resource(self, msg,  headers):
        '''
        This function is used for governance validation for the ping_resource operation.
        '''
        com = self._get_resource_commitments(headers['ion-actor-id'])
        if com is None:
            return False, '(ping_resource) has been denied since the user %s has not acquired the resource %s' % (headers['ion-actor-id'], self.resource_id)

        return True, ''


    def _create_publisher(self, stream_id=None, stream_route=None):
        if self._standalone:
            publisher = StandaloneStreamPublisher(stream_id, stream_route)
        else:
            publisher = StreamPublisher(process=self, stream_id=stream_id, stream_route=stream_route)

        return publisher

    def _construct_data_publishers(self):
        if self._agent_streamconfig_map:
            self._construct_data_publishers_using_agent_streamconfig_map()
        else:
            self._construct_data_publishers_using_CFG_stream_config()

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

    def _construct_data_publishers_using_CFG_stream_config(self):
        """
        Construct the stream publishers from the stream_config agent
        config variable.
        """

        stream_info = self.CFG.stream_config
        log.debug("%r: stream_info = %s",
            self._platform_id, stream_info)

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

#        if self._agent_device_map:
#            platform_name = self._agent_device_map[platform_id].name
#        else:
#            platform_name = platform_id
#
#        return platform_name

    def _create_driver(self):
        """
        Creates the platform driver object for this platform agent.

        NOTE: the driver object is created directly (not via a spawned process)
        """
        driver_config = self._plat_config['driver_config']
        driver_module = driver_config['dvr_mod']
        driver_class = driver_config['dvr_cls']

        assert self._platform_id is not None, "must know platform_id to create driver"

        log.debug('%r: creating driver: %s',
            self._platform_id,  driver_config)

        try:
            module = __import__(driver_module, fromlist=[driver_class])
            classobj = getattr(module, driver_class)
            driver = classobj(self._platform_id, driver_config, self._parent_platform_id)

        except Exception as e:
            msg = '%r: could not import/construct driver: module=%s, class=%s' % (
                self._platform_id, driver_module, driver_class)
            log.error("%s; reason=%s", msg, str(e))  #, exc_Info=True)
            raise CannotInstantiateDriverException(msg=msg, reason=e)

        self._plat_driver = driver
        self._plat_driver.set_event_listener(self.evt_recv)

        if self._topology or self._agent_device_map or self._agent_streamconfig_map:
            self._plat_driver.set_topology(self._topology,
                                           self._agent_device_map,
                                           self._agent_streamconfig_map)

        log.debug("%r: driver created: %s",
            self._platform_id, str(driver))

    def _assert_driver(self):
        assert self._plat_driver is not None, "_create_driver must have been called first"

    def _do_initialize(self):
        """
        Does the main initialize sequence, which includes activation of the
        driver and launch of the sub-platforms
        """
        self._pre_initialize()
        self._construct_data_publishers()
        self._create_driver()
        self._plat_driver.go_active()

    def _do_go_active(self):
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
        log.debug('%r: in state=%s: received driver_event=%s',
            self._platform_id, self.get_agent_state(), str(driver_event))

        if isinstance(driver_event, AttributeValueDriverEvent):
            self._handle_attribute_value_event(driver_event)
            return

        if isinstance(driver_event, AlarmDriverEvent):
            self._handle_alarm_driver_event(driver_event)
            return

        #
        # TODO handle other possible events.
        #

        else:
            log.warn('%r: driver_event not handled: %s',
                self._platform_id, str(type(driver_event)))
            return

    def _handle_attribute_value_event(self, driver_event):

        if self._agent_streamconfig_map:
            self._handle_attribute_value_event_using_agent_streamconfig_map(driver_event)
        else:
            self._handle_attribute_value_event_using_CFG_stream_config(driver_event)

    def _handle_attribute_value_event_using_agent_streamconfig_map(self, driver_event):

        # NOTE: we are using platform_id as the stream_name, see comment
        # elsewhere in this file.
        stream_name = self._get_platform_name(self._platform_id)

        publisher = self._data_publishers.get(stream_name, None)
        if not publisher:
            log.warn('%r: no publisher configured for stream_name=%r',
                     self._platform_id, stream_name)
            return

        param_dict = self._param_dicts[stream_name]
        stream_def = self._stream_defs[stream_name]
        rdt = RecordDictionaryTool(param_dictionary=param_dict.dump(), stream_definition_id=stream_def)

        # because currently using param-dict for 'simple_data_particle_raw_param_dict',
        # the following are invalid:
#        rdt['value'] =  numpy.array([driver_event._value])

        # ... so, simply fill in 'raw':
        rdt['raw'] =  numpy.array([driver_event._value])

        g = rdt.to_granule(data_producer_id=self.resource_id)
        try:
            publisher.publish(g)
        except AssertionError as e:
            #
            # Occurs but not always, at least locally. But it shows up
            # repeatedly in the coi_coverage buildbot with test_oms_launch:
            #
            # Traceback (most recent call last):
            #   File "/home/buildbot-runner/bbot/slaves/centoslca6_py27/coi_coverage/build/ion/agents/platform/platform_agent.py", line 505, in _handle_attribute_value_event_using_agent_streamconfig_map
            #     publisher.publish(g)
            #   File "/home/buildbot-runner/bbot/slaves/centoslca6_py27/coi_coverage/build/extern/pyon/pyon/ion/stream.py", line 80, in publish
            #     super(StreamPublisher,self).publish(msg, to_name=xp.create_route(stream_route.routing_key), headers={'exchange_point':stream_route.exchange_point, 'stream':stream_id or self.stream_id})
            #   File "/home/buildbot-runner/bbot/slaves/centoslca6_py27/coi_coverage/build/extern/pyon/pyon/net/endpoint.py", line 647, in publish
            #     self._pub_ep.send(msg, headers)
            #   File "/home/buildbot-runner/bbot/slaves/centoslca6_py27/coi_coverage/build/extern/pyon/pyon/net/endpoint.py", line 133, in send
            #     return self._send(_msg, _header, **kwargs)
            #   File "/home/buildbot-runner/bbot/slaves/centoslca6_py27/coi_coverage/build/extern/pyon/pyon/net/endpoint.py", line 153, in _send
            #     self.channel.send(new_msg, new_headers)
            #   File "/home/buildbot-runner/bbot/slaves/centoslca6_py27/coi_coverage/build/extern/pyon/pyon/net/channel.py", line 691, in send
            #     self._declare_exchange(self._send_name.exchange)
            #   File "/home/buildbot-runner/bbot/slaves/centoslca6_py27/coi_coverage/build/extern/pyon/pyon/net/channel.py", line 156, in _declare_exchange
            #     assert self._transport
            # AssertionError
            #
            # Not sure what the reason is, perhaps the route is no longer
            # valid, or the publisher gets closed somehow (?)
            # TODO determine what's going on here
            #
            exc_msg = "%s: %s" % (e.__class__.__name__, str(e))
            msg = "%r: AssertionError while calling publisher.publish(g) on stream %r, exception=%s" % (
                            self._platform_id, stream_name, exc_msg)

            # do not inundate the output with stacktraces, just log an error
            # line for the time being.
#            print msg
#            import traceback
#            traceback.print_exc()
            log.error(msg)
            return

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: published data granule on stream %r, rdt=%s, granule=%s",
                self._platform_id, stream_name, str(rdt), str(g))

    def _handle_attribute_value_event_using_CFG_stream_config(self, driver_event):
        """
        Old mechanism, before using _agent_streamconfig_map
        """
        stream_name = driver_event._attr_id
        if not stream_name in self._data_streams:
            log.warn('%r: got attribute value event for unconfigured stream %r',
                     self._platform_id, stream_name)
            return

        publisher = self._data_publishers.get(stream_name, None)
        if not publisher:
            log.warn('%r: no publisher configured for stream %r',
                     self._platform_id, stream_name)
            return

        param_dict = self._param_dicts.get(stream_name, None)
        if not param_dict:
            log.warn('%r: No ParameterDictionary given for stream %r',
                     self._platform_id, stream_name)
            return

        rdt = RecordDictionaryTool(param_dictionary=param_dict)

        rdt['value'] =  numpy.array([driver_event._value])

        g = rdt.to_granule(data_producer_id=self.resource_id)

        stream_id = self._data_streams[stream_name]
        publisher.publish(g, stream_id=stream_id)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: published data granule on stream %r, rdt=%s, granule=%s",
                self._platform_id, stream_name, str(rdt), str(g))

    def _handle_alarm_driver_event(self, driver_event):
        #
        # TODO How are alarm events to be notified? Publish to some stream?
        #
        log.debug("Got alarm event but nothing done with it yet: %s", str(driver_event))

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

    def _launch_platform_agent(self, subplatform_id):
        """
        Launches a sub-platform agent, creates ResourceAgentClient, and pings
        and initializes the sub-platform agent.

        @param subplatform_id Platform ID
        """
        agent_config = {
            'agent':            {'resource_id': subplatform_id},
            'stream_config':    self.CFG.stream_config,
            'test_mode':        True
        }

        log.debug("%r: launching sub-platform agent %r",
            self._platform_id, subplatform_id)
        pid = self._launcher.launch(subplatform_id, agent_config)

        if self._standalone:
            pa_client = pid
        else:
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
        log.debug("%r: _initialize_subplatform -> %r",
            self._platform_id, subplatform_id)

        pa_client, _ = self._pa_clients[subplatform_id]

        # now, initialize the sub-platform agent so the agent network gets
        # built and initialized recursively:
        platform_config = {
            'platform_id': subplatform_id,
            'platform_topology' : self._topology,
            'agent_device_map' : self._agent_device_map,
            'agent_streamconfig_map': self._agent_streamconfig_map,
            'parent_platform_id' : self._platform_id,
            'driver_config': self._plat_config['driver_config'],
            'container_name': self._container_name,
        }

        kwargs = dict(plat_config=platform_config)
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=kwargs)
        retval = self._execute_agent(pa_client, cmd, subplatform_id)
        log.debug("%r: _initialize_subplatform %r  retval = %s",
            self._platform_id, subplatform_id, str(retval))

    def _subplatforms_launch(self):
        """
        Launches all my sub-platforms storing the corresponding
        ResourceAgentClient objects in _pa_clients.
        """
        self._pa_clients.clear()
        subplatform_ids = self._plat_driver.get_subplatform_ids()
        if len(subplatform_ids):
            if self._parent_platform_id is None:
                log.debug("%r: I'm the root platform", self._platform_id)
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
        subplatform_ids = self._plat_driver.get_subplatform_ids()
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

    def _initialize(self, *args, **kwargs):
        self._plat_config = kwargs.get('plat_config', None)
        self._do_initialize()

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

    def _ping_resource(self, *args, **kwargs):
        result = self._plat_driver.ping()
        return result

    ##############################################################
    # UNINITIALIZED event handlers.
    ##############################################################

    def _handler_uninitialized_initialize(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._initialize(*args, **kwargs)
        next_state = PlatformAgentState.INACTIVE

        return (next_state, result)

    ##############################################################
    # INACTIVE event handlers.
    ##############################################################

    def _handler_inactive_reset(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
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
        log.debug("%r/%s args=%s kwargs=%s",
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
        log.debug("%r/%s args=%s kwargs=%s",
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
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = None
        next_state = PlatformAgentState.INACTIVE

        # first sub-platforms, then myself
        self._subplatforms_go_inactive()
        self._go_inactive()

        return (next_state, result)

    def _handler_idle_run(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

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
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

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
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._plat_driver.get_subplatform_ids()

        next_state = self.get_agent_state()

        return (next_state, result)


    ##############################################################
    # Capabilities interface and event handlers.
    ##############################################################

    def _handler_get_resource_capabilities(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
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
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        attr_names = kwargs.get('attr_names', None)
        if attr_names is None:
            raise BadRequest('get_resource missing attr_names argument.')

        from_time = kwargs.get('from_time', None)
        if from_time is None:
            raise BadRequest('get_resource missing from_time argument.')

        try:
            result = self._plat_driver.get_attribute_values(attr_names, from_time)

            next_state = self.get_agent_state()

        except Exception as ex:
            log.error("error in get_attribute_values %s", str(ex)) #, exc_Info=True)
            raise

        return (next_state, result)

    def _handler_set_resource(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        attrs = kwargs.get('attrs', None)
        if attrs is None:
            raise BadRequest('set_resource missing attrs argument.')

        try:
            result = self._plat_driver.set_attribute_values(attrs)

            next_state = self.get_agent_state()

        except Exception as ex:
            log.error("error in set_attribute_values %s", str(ex)) #, exc_Info=True)
            raise

        return (next_state, result)

    def _handler_ping_resource(self, *args, **kwargs):
        """
        Pings the driver.
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        result = self._ping_resource(*args, **kwargs)

        next_state = self.get_agent_state()

        return (next_state, result)

    def _handler_start_alarm_dispatch(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        params = kwargs.get('params', None)
        if params is None:
            raise BadRequest('start_alarm_dispatch missing params argument.')

        try:
            result = self._plat_driver.start_alarm_dispatch(params)

            next_state = self.get_agent_state()

        except Exception as ex:
            log.error("error in start_alarm_dispatch %s", str(ex)) #, exc_Info=True)
            raise

        return (next_state, result)

    def _handler_stop_alarm_dispatch(self, *args, **kwargs):
        """
        """
        log.debug("%r/%s args=%s kwargs=%s",
            self._platform_id, self.get_agent_state(), str(args), str(kwargs))

        try:
            result = self._plat_driver.stop_alarm_dispatch()

            next_state = self.get_agent_state()

        except Exception as ex:
            log.error("error in stop_alarm_dispatch %s", str(ex)) #, exc_Info=True)
            raise

        return (next_state, result)

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
        self._fsm.add_handler(ResourceAgentState.UNINITIALIZED, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # INACTIVE state event handlers.
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.RESET, self._handler_inactive_reset)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_command_get_subplatform_ids)
        self._fsm.add_handler(PlatformAgentState.INACTIVE, PlatformAgentEvent.GO_ACTIVE, self._handler_inactive_go_active)
        self._fsm.add_handler(ResourceAgentState.INACTIVE, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.INACTIVE, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # IDLE state event handlers.
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RESET, self._handler_idle_reset)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.IDLE, PlatformAgentEvent.RUN, self._handler_idle_run)
        self._fsm.add_handler(ResourceAgentState.IDLE, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.IDLE, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)

        # COMMAND state event handlers.
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.GO_INACTIVE, self._handler_idle_go_inactive)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.RESET, self._handler_command_reset)
        self._fsm.add_handler(PlatformAgentState.COMMAND, PlatformAgentEvent.GET_SUBPLATFORM_IDS, self._handler_command_get_subplatform_ids)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.GET_RESOURCE_CAPABILITIES, self._handler_get_resource_capabilities)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.PING_RESOURCE, self._handler_ping_resource)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.GET_RESOURCE, self._handler_get_resource)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.SET_RESOURCE, self._handler_set_resource)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.START_ALARM_DISPATCH, self._handler_start_alarm_dispatch)
        self._fsm.add_handler(ResourceAgentState.COMMAND, PlatformAgentEvent.STOP_ALARM_DISPATCH, self._handler_stop_alarm_dispatch)
