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

from interface.services.icontainer_agent import ContainerAgentClient
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand

from pyon.util.int_test import IonIntegrationTestCase
from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent

from interface.objects import StreamQuery
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import StreamSubscriberRegistrar
from ion.agents.platform.test.adhoc import adhoc_stream_definition
from ion.agents.platform.test.adhoc import adhoc_get_taxonomy
from ion.agents.platform.test.adhoc import adhoc_get_stream_names
from gevent import spawn
from gevent.event import AsyncResult
from gevent import sleep

import time
import unittest
import os
from nose.plugins.attrib import attr


# The ID of the root platform for this test and the IDs of its sub-platforms.
# These Ids and names should correspond to corresponding entries in network.yml,
# which is used by the OMS simulator.
PLATFORM_ID = 'platA1'
SUBPLATFORM_IDS = ['platA1a', 'platA1b']
ATTR_NAMES = ['fooA1', 'bazA1']

DVR_CONFIG = {
    'dvr_mod': 'ion.agents.platform.oms.oms_platform_driver',
    'dvr_cls': 'OmsPlatformDriver',
    'oms_uri': os.getenv('OMS', 'embsimulator'),
}

PLATFORM_CONFIG = {
    'platform_id': PLATFORM_ID,
    'driver_config': DVR_CONFIG,
    'container_name': None,  # determined in setUp
}

# Agent parameters.
PA_RESOURCE_ID = 'oms_platform_agent_001'
PA_NAME = 'OmsPlatformAgent001'
PA_MOD = 'ion.agents.platform.platform_agent'
PA_CLS = 'PlatformAgent'


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@unittest.skipIf(os.getenv('OMS') is None, "Define OMS to include this test")
@attr('INT', group='sa')
class TestPlatformAgent(IonIntegrationTestCase):

    def setUp(self):

        self._start_container()

        # Bring up services in a deploy file (no need to message)
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        PLATFORM_CONFIG['container_name'] = self.container.name

        # Start data suscribers, add stop to cleanup.
        # Define stream_config.
        self._no_samples = None
        self._async_data_result = AsyncResult()
        self._data_greenlets = []
        self._stream_config = {}
        self._samples_received = []
        self._data_subscribers = []
        self._start_data_subscribers()
        self.addCleanup(self._stop_data_subscribers)

        agent_config = {
            'agent'         : {'resource_id': PA_RESOURCE_ID},
            'stream_config' : self._stream_config,
            'test_mode' : True
        }

        # Start agent
        log.info("TestPlatformAgent.setup(): starting agent")
        container_client = ContainerAgentClient(node=self.container.node,
                                                name=self.container.name)
        self._pa_pid = container_client.spawn_process(name=PA_NAME,
                                                      module=PA_MOD,
                                                      cls=PA_CLS,
                                                      config=agent_config)
        log.info('Agent pid=%s.', str(self._pa_pid))

        # Start a resource agent client to talk with the agent.
        self._pa_client = ResourceAgentClient(PA_RESOURCE_ID, process=FakeProcess())
        log.info('Got pa client %s.', str(self._pa_client))

    def _start_data_subscribers(self):
        """
        """
        # Create a pubsub client to create streams.
        pubsub_client = PubsubManagementServiceClient(node=self.container.node)

        # A callback for processing subscribed-to data.
        def consume_data(message, headers):
            log.info('Subscriber received data message: %s.', str(message))
            self._samples_received.append(message)
            if self._no_samples and self._no_samples == len(self._samples_received):
                self._async_data_result.set()

        # Create a stream subscriber registrar to create subscribers.
        subscriber_registrar = StreamSubscriberRegistrar(process=self.container,
                                                container=self.container)

        # Create streams and subscriptions for each stream named in driver.
        self._stream_config = {}
        self._data_subscribers = []
        for stream_name in adhoc_get_stream_names():
            stream_def = adhoc_stream_definition()
            stream_def_id = pubsub_client.create_stream_definition(
                                                    container=stream_def)
            stream_id = pubsub_client.create_stream(
                        name=stream_name,
                        stream_definition_id=stream_def_id,
                        original=True,
                        encoding='ION R2')

            taxy = adhoc_get_taxonomy(stream_name)
            stream_config = dict(
                id=stream_id,
                taxonomy=taxy.dump()
            )
            self._stream_config[stream_name] = stream_config

            # Create subscriptions for each stream.
            exchange_name = '%s_queue' % stream_name
            sub = subscriber_registrar.create_subscriber(exchange_name=exchange_name,
                                                         callback=consume_data)
            self._listen(sub)
            self._data_subscribers.append(sub)
            query = StreamQuery(stream_ids=[stream_id])
            sub_id = pubsub_client.create_subscription(
                                query=query, exchange_name=exchange_name, exchange_point='science_data')
            pubsub_client.activate_subscription(sub_id)

    def _listen(self, sub):
        """
        Pass in a subscriber here, this will make it listen in a background greenlet.
        """
        gl = spawn(sub.listen)
        self._data_greenlets.append(gl)
        sub._ready_event.wait(timeout=5)
        return gl

    def _stop_data_subscribers(self):
        """
        Stop the data subscribers on cleanup.
        """
        for sub in self._data_subscribers:
            sub.stop()
        for gl in self._data_greenlets:
            gl.kill()

    def _get_state(self):
        state = self._pa_client.get_agent_state()
        return state

    def _assert_state(self, state):
        self.assertEquals(self._get_state(), state)

    def _reset(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RESET)
        retval = self._pa_client.execute_agent(cmd)
        self._assert_state(PlatformAgentState.UNINITIALIZED)

    def _ping_agent(self):
        cmd = AgentCommand(command=PlatformAgentEvent.PING_AGENT)
        retval = self._pa_client.execute_agent(cmd)
        self.assertEquals("PONG", retval.result)

    def _ping_resource(self):
        cmd = AgentCommand(command=PlatformAgentEvent.PING_RESOURCE)
        if self._get_state() == PlatformAgentState.UNINITIALIZED:
            # should get ServerError: "Command not handled in current state"
            with self.assertRaises(ServerError):
                self._pa_client.execute_agent(cmd)
        else:
            # In all other states the command should be accepted:
            retval = self._pa_client.execute_agent(cmd)
            self.assertEquals("PONG", retval.result)

    def _get_resource(self):
        kwargs = dict(attr_names=ATTR_NAMES, from_time=time.time())
        cmd = AgentCommand(command=PlatformAgentEvent.GET_RESOURCE, kwargs=kwargs)
        retval = self._pa_client.execute_agent(cmd)
        attr_values = retval.result
        log.info("get_resource result: %s" % str(attr_values))
        self.assertIsInstance(attr_values, dict)
        for attr_name in ATTR_NAMES:
            self.assertTrue(attr_name in attr_values)

    def _initialize(self):
        kwargs = dict(plat_config=PLATFORM_CONFIG)
        self._assert_state(PlatformAgentState.UNINITIALIZED)
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=kwargs)
        retval = self._pa_client.execute_agent(cmd)
        self._assert_state(PlatformAgentState.INACTIVE)

    def _go_active(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval = self._pa_client.execute_agent(cmd)
        self._assert_state(PlatformAgentState.IDLE)

    def _run(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval = self._pa_client.execute_agent(cmd)
        self._assert_state(PlatformAgentState.COMMAND)

    def _go_inactive(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE)
        retval = self._pa_client.execute_agent(cmd)
        self._assert_state(PlatformAgentState.INACTIVE)

    def _add_subplatform_id(self, subplatform_id):
        kwargs = dict(subplatform_id=subplatform_id)
        cmd = AgentCommand(command=PlatformAgentEvent.ADD_SUBPLATFORM, kwargs=kwargs)
        retval = self._pa_client.execute_agent(cmd)
        log.info("add_subplatform_agent_client's retval = %s" % str(retval))

    def _get_subplatform_ids(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GET_SUBPLATFORM_IDS)
        retval = self._pa_client.execute_agent(cmd)
        self.assertIsInstance(retval.result, list)
        return retval.result

    def test_go_active_and_run(self):

        self._ping_agent()
        self._ping_resource()

        self._initialize()
        self._go_active()
        self._run()

        self._ping_agent()
        self._ping_resource()

        self._get_resource()

        log.info("sleeping...")
        sleep(15)

        # retrieve sub-platform IDs and verify against expected
        out_subplat_ids = self._get_subplatform_ids()
        log.info("get_subplatform_ids's retval = %s" % str(out_subplat_ids))
        self.assertEquals(SUBPLATFORM_IDS, out_subplat_ids)

        self._go_inactive()
        self._reset()
