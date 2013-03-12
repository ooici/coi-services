#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_platform_agent_with_oms
@file    ion/agents/platform/test/test_platform_agent_with_oms.py
@author  Carlos Rueda
@brief   Test cases for R2 platform agent interacting with OMS
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

# Locally, the following can be prefixed with PLAT_NETWORK=small to exercise
# a small network (a root and some children) to the testing takes less time
# but still representative. See HelperTestMixin.
#
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_oms.py:TestPlatformAgent.test_capabilities
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_oms.py:TestPlatformAgent.test_some_state_transitions
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_oms.py:TestPlatformAgent.test_get_set_resources
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_oms.py:TestPlatformAgent.test_some_commands
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_oms.py:TestPlatformAgent.test_resource_monitoring
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_oms.py:TestPlatformAgent.test_external_event_dispatch
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_oms.py:TestPlatformAgent.test_connect_disconnect_instrument
# bin/nosetests -sv ion/agents/platform/test/test_platform_agent_with_oms.py:TestPlatformAgent.test_check_sync
#


from pyon.public import log
import logging

from pyon.event.event import EventSubscriber

from pyon.util.containers import get_ion_ts
from pyon.core.exception import ServerError
from pyon.util.context import LocalContextMixin
from pyon.public import CFG

from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from interface.objects import CapabilityType
from interface.objects import AgentCapability

from pyon.util.int_test import IonIntegrationTestCase
from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent
from ion.agents.platform.platform_agent_launcher import LauncherFactory

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.oms_util import RsnOmsUtil
from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.platform.responses import NormalResponse

from ion.agents.platform.test.helper import HelperTestMixin

from pyon.ion.stream import StandaloneStreamSubscriber
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from ion.agents.platform.test.adhoc import adhoc_get_parameter_dictionary
from ion.agents.platform.test.adhoc import adhoc_get_stream_names

from gevent.event import AsyncResult
from mock import patch

import os
import time
from nose.plugins.attrib import attr
from unittest import skip


# By default, test against "embedded" simulator. The OMS environment variable
# can be used to indicate a different RSN OMS server endpoint. Some aliases for
# the "oms_uri" parameter include "localsimulator" and "simulator".
# See CIOMSClientFactory.
DVR_CONFIG = {
    'dvr_mod': 'ion.agents.platform.rsn.rsn_platform_driver',
    'dvr_cls': 'RSNPlatformDriver',
    'oms_uri': os.getenv('OMS', 'embsimulator'),
}

# Agent parameters.
PA_RESOURCE_ID = 'platform_agent_001'
PA_NAME = 'PlatformAgent001'
PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'

# DATA_TIMEOUT: timeout for reception of data sample
DATA_TIMEOUT = 25

# EVENT_TIMEOUT: timeout for reception of event
EVENT_TIMEOUT = 25

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
@skip("skipped while aligning with new configuration structure")
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 180}}})
class TestPlatformAgent(IonIntegrationTestCase, HelperTestMixin):

    @classmethod
    def setUpClass(cls):
        HelperTestMixin.setUpClass()

        # Use the network definition provided by RSN OMS directly.
        rsn_oms = CIOMSClientFactory.create_instance(DVR_CONFIG['oms_uri'])
        network_definition = RsnOmsUtil.build_network_definition(rsn_oms)
        network_definition_ser = NetworkUtil.serialize_network_definition(network_definition)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("NetworkDefinition serialization:\n%s", network_definition_ser)

        cls.PLATFORM_CONFIG = {
            'platform_id': cls.PLATFORM_ID,
            'driver_config': DVR_CONFIG,

            'network_definition' : network_definition_ser
        }

        NetworkUtil._gen_open_diagram(network_definition.pnodes[cls.PLATFORM_ID])

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self._pubsub_client = PubsubManagementServiceClient(node=self.container.node)

        # Start data subscribers, add stop to cleanup.
        # Define stream_config.
        self._async_data_result = AsyncResult()
        self._data_greenlets = []
        self._stream_config = {}
        self._samples_received = []
        self._data_subscribers = []
        self._start_data_subscribers()
        self.addCleanup(self._stop_data_subscribers)

        # start event subscriber:
        self._async_event_result = AsyncResult()
        self._event_subscribers = []
        self._events_received = []
        self.addCleanup(self._stop_event_subscribers)
        self._start_event_subscriber()


        self._agent_config = {
            'agent'         : {'resource_id': PA_RESOURCE_ID},
            'stream_config' : self._stream_config,

            # pass platform config here
            'platform_config': self.PLATFORM_CONFIG
        }

        if log.isEnabledFor(logging.TRACE):
            log.trace("launching with agent_config=%s" % str(self._agent_config))

        self._launcher = LauncherFactory.createLauncher()
        self._pid = self._launcher.launch(self.PLATFORM_ID, self._agent_config)

        log.debug("LAUNCHED PLATFORM_ID=%r", self.PLATFORM_ID)

        # Start a resource agent client to talk with the agent.
        self._pa_client = ResourceAgentClient(PA_RESOURCE_ID, process=FakeProcess())
        log.info('Got pa client %s.' % str(self._pa_client))

    def tearDown(self):
        try:
            self._launcher.cancel_process(self._pid)
        finally:
            super(TestPlatformAgent, self).tearDown()

    def _start_data_subscribers(self):
        """
        """

        # Create streams and subscriptions for each stream named in driver.
        self._stream_config = {}
        self._data_subscribers = []

        #
        # TODO retrieve appropriate stream definitions; for the moment, using
        # adhoc_get_stream_names
        #

        # A callback for processing subscribed-to data.
        def consume_data(message, stream_route, stream_id):
            log.info('Subscriber received data message: %s.' % str(message))
            self._samples_received.append(message)
            self._async_data_result.set()

        for stream_name in adhoc_get_stream_names():
            log.info('creating stream %r ...', stream_name)

            # TODO use appropriate exchange_point
            stream_id, stream_route = self._pubsub_client.create_stream(
                name=stream_name, exchange_point='science_data')

            log.info('create_stream(%r): stream_id=%r, stream_route=%s',
                     stream_name, stream_id, str(stream_route))

            pdict = adhoc_get_parameter_dictionary(stream_name)
            stream_config = dict(stream_route=stream_route.routing_key,
                                 stream_id=stream_id,
                                 parameter_dictionary=pdict.dump())

            self._stream_config[stream_name] = stream_config
            log.info('_stream_config[%r]= %r', stream_name, stream_config)

            # Create subscriptions for each stream.
            exchange_name = '%s_queue' % stream_name
            self._purge_queue(exchange_name)
            sub = StandaloneStreamSubscriber(exchange_name, consume_data)
            sub.start()
            self._data_subscribers.append(sub)
            sub_id = self._pubsub_client.create_subscription(name=exchange_name, stream_ids=[stream_id])
            self._pubsub_client.activate_subscription(sub_id)
            sub.subscription_id = sub_id

    def _purge_queue(self, queue):
        xn = self.container.ex_manager.create_xn_queue(queue)
        xn.purge()

    def _stop_data_subscribers(self):
        """
        Stop the data subscribers on cleanup.
        """
        for sub in self._data_subscribers:
            if hasattr(sub, 'subscription_id'):
                try:
                    self._pubsub_client.deactivate_subscription(sub.subscription_id)
                except:
                    pass
                self._pubsub_client.delete_subscription(sub.subscription_id)
            sub.stop()

    def _start_event_subscriber(self, event_type="DeviceEvent", sub_type="platform_event"):
        """
        Starts event subscriber for events of given event_type ("DeviceEvent"
        by default) and given sub_type ("platform_event" by default).
        """

        def consume_event(evt, *args, **kwargs):
            # A callback for consuming events.
            log.info('Event subscriber received evt: %s.', str(evt))
            self._events_received.append(evt)
            self._async_event_result.set(evt)

        sub = EventSubscriber(event_type=event_type,
            sub_type=sub_type,
            callback=consume_event)

        sub.start()
        log.info("registered event subscriber for event_type=%r, sub_type=%r",
            event_type, sub_type)

        self._event_subscribers.append(sub)
        sub._ready_event.wait(timeout=EVENT_TIMEOUT)

    def _stop_event_subscribers(self):
        """
        Stops the event subscribers on cleanup.
        """
        try:
            for sub in self._event_subscribers:
                if hasattr(sub, 'subscription_id'):
                    try:
                        self.pubsubcli.deactivate_subscription(sub.subscription_id)
                    except:
                        pass
                    self.pubsubcli.delete_subscription(sub.subscription_id)
                sub.stop()
        finally:
            self._event_subscribers = []

    def _get_state(self):
        state = self._pa_client.get_agent_state()
        return state

    def _assert_state(self, state):
        self.assertEquals(self._get_state(), state)

    #def _execute_agent(self, cmd, timeout=TIMEOUT):
    def _execute_agent(self, cmd):
        log.info("_execute_agent: cmd=%r kwargs=%r ...", cmd.command, cmd.kwargs)
        time_start = time.time()
        #retval = self._pa_client.execute_agent(cmd, timeout=timeout)
        retval = self._pa_client.execute_agent(cmd)
        elapsed_time = time.time() - time_start
        log.info("_execute_agent: cmd=%r elapsed_time=%s, retval = %s",
                 cmd.command, elapsed_time, str(retval))
        return retval

    def _reset(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RESET)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.UNINITIALIZED)

    def _ping_agent(self):
        retval = self._pa_client.ping_agent()
        self.assertIsInstance(retval, str)

    def _ping_resource(self):
        cmd = AgentCommand(command=PlatformAgentEvent.PING_RESOURCE)
        if self._get_state() == PlatformAgentState.UNINITIALIZED:
            # should get ServerError: "Command not handled in current state"
            with self.assertRaises(ServerError):
                #self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
                self._pa_client.execute_agent(cmd)
        else:
            # In all other states the command should be accepted:
            retval = self._execute_agent(cmd)
            self.assertEquals("PONG", retval.result)

    def _get_metadata(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GET_METADATA)
        retval = self._execute_agent(cmd)
        md = retval.result
        self.assertIsInstance(md, dict)
        # TODO verify possible subset of required entries in the dict.
        log.info("GET_METADATA = %s", md)

    def _get_ports(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GET_PORTS)
        retval = self._execute_agent(cmd)
        md = retval.result
        self.assertIsInstance(md, dict)
        # TODO verify possible subset of required entries in the dict.
        log.info("GET_PORTS = %s", md)

    def _connect_instrument(self):
        #
        # TODO more realistic settings for the connection
        #
        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID
        instrument_attributes = self.INSTRUMENT_ATTRIBUTES_AND_VALUES

        kwargs = dict(
            port_id = port_id,
            instrument_id = instrument_id,
            attributes = instrument_attributes
        )
        cmd = AgentCommand(command=PlatformAgentEvent.CONNECT_INSTRUMENT, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        result = retval.result
        log.info("CONNECT_INSTRUMENT = %s", result)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertIsInstance(result[port_id], dict)
        returned_attrs = self._verify_valid_instrument_id(instrument_id, result[port_id])
        if isinstance(returned_attrs, dict):
            for attrName in instrument_attributes:
                self.assertTrue(attrName in returned_attrs)

    def _get_connected_instruments(self):
        port_id = self.PORT_ID

        kwargs = dict(
            port_id = port_id,
        )
        cmd = AgentCommand(command=PlatformAgentEvent.GET_CONNECTED_INSTRUMENTS, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        result = retval.result
        log.info("GET_CONNECTED_INSTRUMENTS = %s", result)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertIsInstance(result[port_id], dict)
        instrument_id = self.INSTRUMENT_ID
        self.assertTrue(instrument_id in result[port_id])

    def _disconnect_instrument(self):
        # TODO real settings and corresp verification

        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID

        kwargs = dict(
            port_id = port_id,
            instrument_id = instrument_id
        )
        cmd = AgentCommand(command=PlatformAgentEvent.DISCONNECT_INSTRUMENT, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        result = retval.result
        log.info("DISCONNECT_INSTRUMENT = %s", result)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertIsInstance(result[port_id], dict)
        self.assertTrue(instrument_id in result[port_id])
        self._verify_instrument_disconnected(instrument_id, result[port_id][instrument_id])

    def _turn_on_port(self):
        # TODO real settings and corresp verification

        port_id = self.PORT_ID

        kwargs = dict(
            port_id = port_id
        )
        cmd = AgentCommand(command=PlatformAgentEvent.TURN_ON_PORT, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        result = retval.result
        log.info("TURN_ON_PORT = %s", result)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertEquals(result[port_id], NormalResponse.PORT_TURNED_ON)

    def _turn_off_port(self):
        # TODO real settings and corresp verification

        port_id = self.PORT_ID

        kwargs = dict(
            port_id = port_id
        )
        cmd = AgentCommand(command=PlatformAgentEvent.TURN_OFF_PORT, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        result = retval.result
        log.info("TURN_OFF_PORT = %s", result)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertEquals(result[port_id], NormalResponse.PORT_TURNED_OFF)

    def _get_resource(self):
        attrNames = self.ATTR_NAMES
        #
        # OOIION-631: use get_ion_ts() as a basis for using system time, which is
        # a string.
        #
        cur_time = get_ion_ts()
        from_time = str(int(cur_time) - 50000)  # a 50-sec time window
        kwargs = dict(attr_names=attrNames, from_time=from_time)
        cmd = AgentCommand(command=PlatformAgentEvent.GET_RESOURCE, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        attr_values = retval.result
        self.assertIsInstance(attr_values, dict)
        for attr_name in attrNames:
            self._verify_valid_attribute_id(attr_name, attr_values)

    def _set_resource(self):
        attrNames = self.ATTR_NAMES
        writ_attrNames = self.WRITABLE_ATTR_NAMES

        # do valid settings:

        # TODO more realistic value depending on attribute's type
        attrs = [(attrName, self.VALID_ATTR_VALUE) for attrName in attrNames]
        log.info("%r: setting attributes=%s", self.PLATFORM_ID, attrs)
        kwargs = dict(attrs=attrs)
        cmd = AgentCommand(command=PlatformAgentEvent.SET_RESOURCE, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        attr_values = retval.result
        self.assertIsInstance(attr_values, dict)
        for attrName in attrNames:
            if attrName in writ_attrNames:
                self._verify_valid_attribute_id(attrName, attr_values)
            else:
                self._verify_not_writable_attribute_id(attrName, attr_values)

        # try invalid settings:

        # set invalid values to writable attributes:
        attrs = [(attrName, self.INVALID_ATTR_VALUE) for attrName in writ_attrNames]
        log.info("%r: setting attributes=%s", self.PLATFORM_ID, attrs)
        kwargs = dict(attrs=attrs)
        cmd = AgentCommand(command=PlatformAgentEvent.SET_RESOURCE, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        attr_values = retval.result
        self.assertIsInstance(attr_values, dict)
        for attrName in writ_attrNames:
            self._verify_attribute_value_out_of_range(attrName, attr_values)

    def _initialize(self):
        self._assert_state(PlatformAgentState.UNINITIALIZED)
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.INACTIVE)

    def _go_active(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.IDLE)

    def _run(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.COMMAND)

    def _pause(self):
        cmd = AgentCommand(command=PlatformAgentEvent.PAUSE)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.STOPPED)

    def _resume(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RESUME)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.COMMAND)

    def _start_resource_monitoring(self):
        cmd = AgentCommand(command=PlatformAgentEvent.START_MONITORING)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.MONITORING)

    def _wait_for_a_data_sample(self):
        log.info("waiting for reception of a data sample...")
        # just wait for at least one -- see consume_data
        self._async_data_result.get(timeout=DATA_TIMEOUT)
        self.assertTrue(len(self._samples_received) >= 1)
        log.info("Received samples: %s", len(self._samples_received))

    def _stop_resource_monitoring(self):
        cmd = AgentCommand(command=PlatformAgentEvent.STOP_MONITORING)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.COMMAND)

    def _go_inactive(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE)
        retval = self._execute_agent(cmd)
        self._assert_state(PlatformAgentState.INACTIVE)

    def _get_subplatform_ids(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GET_SUBPLATFORM_IDS)
        retval = self._execute_agent(cmd)
        self.assertIsInstance(retval.result, list)
        self.assertTrue(x in retval.result for x in self.SUBPLATFORM_IDS)
        return retval.result

    def _wait_for_external_event(self):
        log.info("waiting for reception of an external event...")
        # just wait for at least one -- see consume_event
        self._async_event_result.get(timeout=EVENT_TIMEOUT)
        self.assertTrue(len(self._events_received) >= 1)
        log.info("Received events: %s", len(self._events_received))

    def _check_sync(self):
        cmd = AgentCommand(command=PlatformAgentEvent.CHECK_SYNC)
        retval = self._execute_agent(cmd)
        log.info("CHECK_SYNC result: %s", retval.result)
        self.assertTrue(retval.result is not None)
        self.assertEquals(retval.result[0:3], "OK:")
        return retval.result

    def test_capabilities(self):

        agt_cmds_all = [
            PlatformAgentEvent.INITIALIZE,
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.GO_ACTIVE,
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RUN,
            PlatformAgentEvent.CLEAR,
            PlatformAgentEvent.PAUSE,
            PlatformAgentEvent.RESUME,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE,
            PlatformAgentEvent.SET_RESOURCE,

            PlatformAgentEvent.GET_METADATA,
            PlatformAgentEvent.GET_PORTS,

            PlatformAgentEvent.CONNECT_INSTRUMENT,
            PlatformAgentEvent.DISCONNECT_INSTRUMENT,
            PlatformAgentEvent.GET_CONNECTED_INSTRUMENTS,

            PlatformAgentEvent.TURN_ON_PORT,
            PlatformAgentEvent.TURN_OFF_PORT,
            PlatformAgentEvent.GET_SUBPLATFORM_IDS,

            PlatformAgentEvent.START_MONITORING,
            PlatformAgentEvent.STOP_MONITORING,

            PlatformAgentEvent.CHECK_SYNC,
        ]


        def sort_caps(caps):
            agt_cmds = []
            agt_pars = []
            res_cmds = []
            res_pars = []

            if len(caps)>0 and isinstance(caps[0], AgentCapability):
                agt_cmds = [x.name for x in caps if x.cap_type==CapabilityType.AGT_CMD]
                agt_pars = [x.name for x in caps if x.cap_type==CapabilityType.AGT_PAR]
                res_cmds = [x.name for x in caps if x.cap_type==CapabilityType.RES_CMD]
                res_pars = [x.name for x in caps if x.cap_type==CapabilityType.RES_PAR]

            elif len(caps)>0 and isinstance(caps[0], dict):
                agt_cmds = [x['name'] for x in caps if x['cap_type']==CapabilityType.AGT_CMD]
                agt_pars = [x['name'] for x in caps if x['cap_type']==CapabilityType.AGT_PAR]
                res_cmds = [x['name'] for x in caps if x['cap_type']==CapabilityType.RES_CMD]
                res_pars = [x['name'] for x in caps if x['cap_type']==CapabilityType.RES_PAR]

            return agt_cmds, agt_pars, res_cmds, res_pars


        agt_pars_all = ['example']  # 'cause ResourceAgent defines aparam_example
        res_pars_all = []
        res_cmds_all = []

        ##################################################################
        # UNINITIALIZED
        ##################################################################

        self._assert_state(PlatformAgentState.UNINITIALIZED)

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

        # Validate capabilities for state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_uninitialized = [
            PlatformAgentEvent.INITIALIZE,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
        ]
        self.assertItemsEqual(agt_cmds, agt_cmds_uninitialized)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states.
        retval = self._pa_client.get_capabilities(current_state=False)

        # Validate all capabilities as read from state UNINITIALIZED.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])


        ##################################################################
        # INACTIVE
        ##################################################################
        self._initialize()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

        # Validate capabilities for state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_inactive = [
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.GET_METADATA,
            PlatformAgentEvent.GET_PORTS,
            PlatformAgentEvent.GET_SUBPLATFORM_IDS,
            PlatformAgentEvent.GO_ACTIVE,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_inactive)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states.
        retval = self._pa_client.get_capabilities(False)

         # Validate all capabilities as read from state INACTIVE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])


        ##################################################################
        # IDLE
        ##################################################################
        self._go_active()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities for state IDLE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_idle = [
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RUN,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_idle)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])

        # Get exposed capabilities in all states as read from IDLE.
        retval = self._pa_client.get_capabilities(False)

         # Validate all capabilities as read from state IDLE.
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, [])
        self.assertItemsEqual(res_pars, [])


        ##################################################################
        # COMMAND
        ##################################################################
        self._run()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_command = [
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.PAUSE,
            PlatformAgentEvent.CLEAR,
            PlatformAgentEvent.GET_METADATA,
            PlatformAgentEvent.GET_PORTS,

            PlatformAgentEvent.CONNECT_INSTRUMENT,
            PlatformAgentEvent.DISCONNECT_INSTRUMENT,
            PlatformAgentEvent.GET_CONNECTED_INSTRUMENTS,

            PlatformAgentEvent.TURN_ON_PORT,
            PlatformAgentEvent.TURN_OFF_PORT,
            PlatformAgentEvent.GET_SUBPLATFORM_IDS,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE,
            PlatformAgentEvent.SET_RESOURCE,

            PlatformAgentEvent.START_MONITORING,

            PlatformAgentEvent.CHECK_SYNC,
        ]

        res_cmds_command = [
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_command)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_command)
        self.assertItemsEqual(res_pars, res_pars_all)


        ##################################################################
        # STOPPED
        ##################################################################
        self._pause()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state STOPPED
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_stopped = [
            PlatformAgentEvent.RESUME,
            PlatformAgentEvent.CLEAR,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
        ]

        res_cmds_command = [
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_stopped)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_command)
        self.assertItemsEqual(res_pars, res_pars_all)


        # back to COMMAND:
        self._resume()

        ##################################################################
        # MONITORING
        ##################################################################
        self._start_resource_monitoring()

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state MONITORING
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_monitoring = [
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.GET_METADATA,
            PlatformAgentEvent.GET_PORTS,

            PlatformAgentEvent.CONNECT_INSTRUMENT,
            PlatformAgentEvent.DISCONNECT_INSTRUMENT,
            PlatformAgentEvent.GET_CONNECTED_INSTRUMENTS,

            PlatformAgentEvent.TURN_ON_PORT,
            PlatformAgentEvent.TURN_OFF_PORT,
            PlatformAgentEvent.GET_SUBPLATFORM_IDS,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE,
            PlatformAgentEvent.SET_RESOURCE,

            PlatformAgentEvent.STOP_MONITORING,

            PlatformAgentEvent.CHECK_SYNC,
        ]

        res_cmds_command = [
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_monitoring)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_command)
        self.assertItemsEqual(res_pars, res_pars_all)

        # return to COMMAND state:
        self._stop_resource_monitoring()


        ###################
        # ALL CAPABILITIES
        ###################

        # Get exposed capabilities in all states as read from state COMMAND.
        retval = self._pa_client.get_capabilities(False)

         # Validate all capabilities as read from state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        self.assertItemsEqual(agt_cmds, agt_cmds_all)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_all)
        self.assertItemsEqual(res_pars, res_pars_all)

        self._go_inactive()
        self._reset()

    def test_some_state_transitions(self):

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._initialize()   # -> INACTIVE
        self._reset()        # -> UNINITIALIZED
        self._initialize()   # -> INACTIVE
        self._go_active()    # -> IDLE
        self._reset()        # -> UNINITIALIZED
        self._initialize()   # -> INACTIVE
        self._go_active()    # -> IDLE
        self._run()          # -> COMMAND
        self._pause()        # -> STOPPED
        self._resume()       # -> COMMAND

        self._reset()        # -> UNINITIALIZED

    def test_get_set_resources(self):

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._get_resource()
        self._set_resource()

        self._go_inactive()
        self._reset()

    def test_some_commands(self):

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._ping_agent()
        self._ping_resource()

        self._get_metadata()
        self._get_ports()
        self._get_subplatform_ids()

        self._go_inactive()
        self._reset()

    def test_resource_monitoring(self):

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._start_resource_monitoring()
        self._wait_for_a_data_sample()
        self._stop_resource_monitoring()

        self._go_inactive()
        self._reset()

    def test_external_event_dispatch(self):

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._wait_for_external_event()

        self._go_inactive()
        self._reset()

    def test_connect_disconnect_instrument(self):

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._connect_instrument()
        self._turn_on_port()

        self._get_connected_instruments()

        self._turn_off_port()
        self._disconnect_instrument()

        self._go_inactive()
        self._reset()

    def test_check_sync(self):

        self._assert_state(PlatformAgentState.UNINITIALIZED)
        self._ping_agent()

        self._initialize()
        self._go_active()
        self._run()

        self._check_sync()

        self._connect_instrument()
        self._check_sync()

        self._disconnect_instrument()
        self._check_sync()

        self._go_inactive()
        self._reset()

