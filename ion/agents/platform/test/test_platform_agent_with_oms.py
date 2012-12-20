#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_platform_agent_with_oms
@file    ion/agents/platform/test/test_platform_agent_with_oms.py
@author  Carlos Rueda
@brief   Test cases for R2 platform agent interacting with OMS
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

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

from ion.agents.platform.test.helper import HelperTestMixin

from pyon.ion.stream import StandaloneStreamSubscriber
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from ion.agents.platform.test.adhoc import adhoc_get_parameter_dictionary
from ion.agents.platform.test.adhoc import adhoc_get_stream_names

from gevent.event import AsyncResult
from gevent import sleep
from mock import patch

import time
import ntplib
import unittest
import os
from nose.plugins.attrib import attr


# TIMEOUT: timeout for each execute_agent call.
# TIMEOUT = 180
# Carlos: we remove this and use the default patched from CFG

DVR_CONFIG = {
    'dvr_mod': 'ion.agents.platform.oms.oms_platform_driver',
    'dvr_cls': 'OmsPlatformDriver',
    'oms_uri': 'embsimulator',
}

# Agent parameters.
PA_RESOURCE_ID = 'platform_agent_001'
PA_NAME = 'PlatformAgent001'
PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 180}}})
class TestPlatformAgent(IonIntegrationTestCase, HelperTestMixin):

    @classmethod
    def setUpClass(cls):
        HelperTestMixin.setUpClass()

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self._pubsub_client = PubsubManagementServiceClient(node=self.container.node)

        self.PLATFORM_CONFIG = {
            'platform_id': self.PLATFORM_ID,
            'driver_config': DVR_CONFIG,
        }

        # Start data suscribers, add stop to cleanup.
        # Define stream_config.
        self._async_data_result = AsyncResult()
        self._data_greenlets = []
        self._stream_config = {}
        self._samples_received = []
        self._data_subscribers = []
        self._start_data_subscribers()
        self.addCleanup(self._stop_data_subscribers)

        self._agent_config = {
            'agent'         : {'resource_id': PA_RESOURCE_ID},
            'stream_config' : self._stream_config,

            # pass platform config here
            'platform_config': self.PLATFORM_CONFIG
        }

        log.debug("launching with agent_config=%s",  str(self._agent_config))


        if os.getenv("STANDALONE") is not None:
            standalone = {
                'platform_id': self.PLATFORM_ID,
                'container': self.container,
                'pubsub_client': self._pubsub_client
            }
            self._launcher = LauncherFactory.createLauncher(standalone=standalone)
            self._pid = self._launcher.launch(self.PLATFORM_ID, self._agent_config)
            self._pa_client = self._pid

            log.debug("STANDALONE: LAUNCHED PLATFORM_ID=%r", self.PLATFORM_ID)

        else:
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

    def _get_state(self):
        state = self._pa_client.get_agent_state()
        return state

    def _assert_state(self, state):
        self.assertEquals(self._get_state(), state)

    #def _execute_agent(self, cmd, timeout=TIMEOUT):
    def _execute_agent(self, cmd):
        log.info("_execute_agent: cmd=%r ...", cmd.command)
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

    def _set_up_port(self):
        # TODO real settings and corresp verification

        port_id = self.PORT_ID
        port_attrName = self.PORT_ATTR_NAME

        kwargs = dict(
            port_id = port_id,
            attributes = { port_attrName: self.VALID_PORT_ATTR_VALUE }
        )
        cmd = AgentCommand(command=PlatformAgentEvent.SET_UP_PORT, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        result = retval.result
        log.info("SET_UP_PORT = %s", result)
        self.assertIsInstance(result, dict)
        self.assertTrue(port_id in result)
        self.assertTrue(port_attrName in result[port_id])

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
        self.assertIsInstance(result[port_id], bool)

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
        self.assertIsInstance(result[port_id], bool)

    def _get_resource(self):
        attrNames = self.ATTR_NAMES
        cur_time = ntplib.system_to_ntp_time(time.time())
        from_time = cur_time - 50  # a 50-sec time window
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

        # TODO more realistic value depending on attribute's type
        attrs = [(attrName, self.VALID_ATTR_VALUE) for attrName in attrNames]
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

        # now test setting invalid values to writable attributes:
        attrs = [(attrName, self.INVALID_ATTR_VALUE) for attrName in
                 writ_attrNames]
        log.info("%r: setting attributes=%s", self.PLATFORM_ID, attrs)
        kwargs = dict(attrs=attrs)
        cmd = AgentCommand(command=PlatformAgentEvent.SET_RESOURCE, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        attr_values = retval.result
        self.assertIsInstance(attr_values, dict)
        for attrName in writ_attrNames:
            self._verify_invalid_attribute_id(attrName, attr_values)

    def _initialize(self):
        self._assert_state(PlatformAgentState.UNINITIALIZED)
#        kwargs = dict(plat_config=self.PLATFORM_CONFIG)
#        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=kwargs)
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

    def _start_event_dispatch(self):
        kwargs = dict(params="TODO set params")
        cmd = AgentCommand(command=PlatformAgentEvent.START_EVENT_DISPATCH, kwargs=kwargs)
        retval = self._execute_agent(cmd)
        self.assertTrue(retval.result is not None)
        return retval.result

    def _wait_for_a_data_sample(self):
        log.info("waiting for reception of a data sample...")
        self._async_data_result.get(timeout=15)
        # just wait for at least one -- see consume_data above
        self.assertTrue(len(self._samples_received) >= 1)

    def _stop_event_dispatch(self):
        cmd = AgentCommand(command=PlatformAgentEvent.STOP_EVENT_DISPATCH)
        retval = self._execute_agent(cmd)
        self.assertTrue(retval.result is not None)
        return retval.result

    def test_capabilities(self):

        #log.info("test_capabilities starting.  Default timeout=%s", TIMEOUT)
        log.info("test_capabilities starting.  Default timeout=%i", CFG.endpoint.receive.timeout)

        agt_cmds_all = [
            PlatformAgentEvent.INITIALIZE,
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.GO_ACTIVE,
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RUN,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE,
            PlatformAgentEvent.SET_RESOURCE,

            PlatformAgentEvent.GET_METADATA,
            PlatformAgentEvent.GET_PORTS,
            PlatformAgentEvent.SET_UP_PORT,
            PlatformAgentEvent.TURN_ON_PORT,
            PlatformAgentEvent.TURN_OFF_PORT,
            PlatformAgentEvent.GET_SUBPLATFORM_IDS,

            PlatformAgentEvent.START_EVENT_DISPATCH,
            PlatformAgentEvent.STOP_EVENT_DISPATCH,
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

        self._initialize()

        ##################################################################
        # INACTIVE
        ##################################################################

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

        self._go_active()

        ##################################################################
        # IDLE
        ##################################################################

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

        self._run()

        ##################################################################
        # COMMAND
        ##################################################################

        # Get exposed capabilities in current state.
        retval = self._pa_client.get_capabilities()

         # Validate capabilities of state COMMAND
        agt_cmds, agt_pars, res_cmds, res_pars = sort_caps(retval)

        agt_cmds_command = [
            PlatformAgentEvent.GO_INACTIVE,
            PlatformAgentEvent.RESET,
            PlatformAgentEvent.GET_METADATA,
            PlatformAgentEvent.GET_PORTS,
            PlatformAgentEvent.SET_UP_PORT,
            PlatformAgentEvent.TURN_ON_PORT,
            PlatformAgentEvent.TURN_OFF_PORT,
            PlatformAgentEvent.GET_SUBPLATFORM_IDS,
            PlatformAgentEvent.GET_RESOURCE_CAPABILITIES,
            PlatformAgentEvent.PING_RESOURCE,
            PlatformAgentEvent.GET_RESOURCE,
            PlatformAgentEvent.SET_RESOURCE,

            PlatformAgentEvent.START_EVENT_DISPATCH,
            PlatformAgentEvent.STOP_EVENT_DISPATCH,
        ]

        res_cmds_command = [
        ]

        self.assertItemsEqual(agt_cmds, agt_cmds_command)
        self.assertItemsEqual(agt_pars, agt_pars_all)
        self.assertItemsEqual(res_cmds, res_cmds_command)
        self.assertItemsEqual(res_pars, res_pars_all)

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

    def test_go_active_and_run(self):

        #log.info("test_go_active_and_run starting.  Default timeout=%s", TIMEOUT)
        log.info("test_capabilities starting.  Default timeout=%i", CFG.endpoint.receive.timeout)
        

        self._ping_agent()

#        self._ping_resource() skipping this here while the timeout issue
#                              on r2_light and other builds is investigated.

        self._initialize()
        self._go_active()
        self._run()

        self._ping_agent()
        self._ping_resource()

        self._get_metadata()
        self._get_ports()
        self._get_subplatform_ids()

        self._get_resource()
        self._set_resource()

        self._set_up_port()
        self._turn_on_port()

        self._start_event_dispatch()

        self._wait_for_a_data_sample()

        self._stop_event_dispatch()

        self._turn_off_port()

        self._go_inactive()
        self._reset()
