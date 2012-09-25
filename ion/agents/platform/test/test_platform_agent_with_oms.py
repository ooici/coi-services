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

from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand

from pyon.util.int_test import IonIntegrationTestCase
from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent
from ion.agents.platform.platform_agent_launcher import LauncherFactory

from pyon.ion.stream import StandaloneStreamSubscriber
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from ion.agents.platform.test.adhoc import adhoc_get_parameter_dictionary
from ion.agents.platform.test.adhoc import adhoc_get_stream_names

from gevent.event import AsyncResult
from gevent import sleep

import time
import unittest
import os
from nose.plugins.attrib import attr


# The ID of the root platform for this test and the IDs of its sub-platforms.
# These Ids and names should correspond to corresponding entries in network.yml,
# which is used by the OMS simulator.
PLATFORM_ID = 'Node1A'
SUBPLATFORM_IDS = ['MJ01A', 'Node1B']
ATTR_NAMES = ['Node1A_attr_1', 'Node1A_attr_2']


# TIMEOUT: timeout for each execute_agent call.
# NOTE: the bigger the platform network size starting from the chosen
# PLATFORM_ID above, the more the time that should be given for commands to
# complete, in particular, for those with a cascading effect on all the
# descendents, eg, INITIALIZE. In the current network.yml there are 19
# descendent platforms from 'Node1A' taking around 25secs for the INITIALIZE
# command to complete on my local env (this elapsed time reduces to ~14secs
# when using --with-pycc).
# The following TIMEOUT value intends to be big enough for all typical cases.
TIMEOUT = 90


DVR_CONFIG = {
    'dvr_mod': 'ion.agents.platform.oms.oms_platform_driver',
    'dvr_cls': 'OmsPlatformDriver',
    'oms_uri': 'embsimulator',
}

PLATFORM_CONFIG = {
    'platform_id': PLATFORM_ID,
    'driver_config': DVR_CONFIG,
    'container_name': None,  # determined in setUp
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
class TestPlatformAgent(IonIntegrationTestCase):

    def setUp(self):

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self._pubsub_client = PubsubManagementServiceClient(node=self.container.node)

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

        self._agent_config = {
            'agent'         : {'resource_id': PA_RESOURCE_ID},
            'stream_config' : self._stream_config,
            'test_mode' : True
        }

        log.debug("launching with agent_config=%s",  str(self._agent_config))


        if os.getenv("STANDALONE") is not None:
            standalone = {
                'platform_id': PLATFORM_ID,
                'container': self.container,
                'pubsub_client': self._pubsub_client
            }
            self._launcher = LauncherFactory.createLauncher(standalone=standalone)
            self._pid = self._launcher.launch(PLATFORM_ID, self._agent_config)
            self._pa_client = self._pid

            log.debug("STANDALONE: LAUNCHED PLATFORM_ID=%r", PLATFORM_ID)

        else:
            self._launcher = LauncherFactory.createLauncher()
            self._pid = self._launcher.launch(PLATFORM_ID, self._agent_config)

            log.debug("LAUNCHED PLATFORM_ID=%r", PLATFORM_ID)

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
        # TODO actually build the stream config and data subscribers. At the
        # moment, what follows is an initial sketch of the needed changes but
        # it's not working yet. I'm pushing this now because of the recent
        # RDT refactoring that invalidated the old taxonomy-based mechanism.
        #

#        # A callback for processing subscribed-to data.
#        def consume_data(message, stream_route, stream_id):
#            log.info('Subscriber received data message: %s.' % str(message))
#            self._samples_received.append(message)
#            if self._no_samples and self._no_samples == len(self._samples_received):
#                self._async_data_result.set()
#
#        for stream_name in adhoc_get_stream_names():
#            log.info('creating stream %r ...', stream_name)
#            stream_id, stream_route = self._pubsub_client.create_stream(name=stream_name,exchange_point='science_data')
#
#            log.info('create_stream(%r): stream_id=%r, stream_route=%s',
#                     stream_name, stream_id, str(stream_route))
#
#            pdict = adhoc_get_parameter_dictionary(stream_name)
#            stream_config = dict(stream_route=stream_route,
#                                 stream_id=stream_id,
#                                 parameter_dictionary=pdict.dump())
#
#            self._stream_config[stream_name] = stream_config
#            log.info('_stream_config[%r]= %r', stream_name, stream_config)
#
#            # Create subscriptions for each stream.
#            exchange_name = '%s_queue' % stream_name
#            self._purge_queue(exchange_name)
#            sub = StandaloneStreamSubscriber(exchange_name, consume_data)
#            sub.start()
#            self._data_subscribers.append(sub)
#            sub_id = self._pubsub_client.create_subscription(name=exchange_name, stream_ids=[stream_id])
#            self._pubsub_client.activate_subscription(sub_id)
#            sub.subscription_id = sub_id

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

    def _execute_agent(self, cmd):
        time_start = time.time()
        retval = self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        elapsed_time = time.time() - time_start
        return retval, elapsed_time

    def _reset(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RESET)
        retval, elapsed_time = self._execute_agent(cmd)
        log.info("PlatformAgentEvent.RESET elapsed_time=%s, retval = %s",
                 elapsed_time, str(retval))
        self._assert_state(PlatformAgentState.UNINITIALIZED)

    def _ping_agent(self):
        cmd = AgentCommand(command=PlatformAgentEvent.PING_AGENT)
        retval, elapsed_time = self._execute_agent(cmd)
        log.info("PlatformAgentEvent.PING_AGENT elapsed_time=%s, retval = %s",
                 elapsed_time, str(retval))
        self.assertEquals("PONG", retval.result)

    def _ping_resource(self):
        cmd = AgentCommand(command=PlatformAgentEvent.PING_RESOURCE)
        if self._get_state() == PlatformAgentState.UNINITIALIZED:
            # should get ServerError: "Command not handled in current state"
            with self.assertRaises(ServerError):
                self._pa_client.execute_agent(cmd, timeout=TIMEOUT)
        else:
            # In all other states the command should be accepted:
            retval, elapsed_time = self._execute_agent(cmd)
            log.info("PlatformAgentEvent.PING_RESOURCE elapsed_time=%s, retval = %s",
                     elapsed_time, str(retval))
            self.assertEquals("PONG", retval.result)

    def _get_resource(self):
        kwargs = dict(attr_names=ATTR_NAMES, from_time=time.time())
        cmd = AgentCommand(command=PlatformAgentEvent.GET_RESOURCE, kwargs=kwargs)
        retval, elapsed_time = self._execute_agent(cmd)
        log.info("PlatformAgentEvent.GET_RESOURCE elapsed_time=%s, retval = %s",
                 elapsed_time, str(retval))
        attr_values = retval.result
        self.assertIsInstance(attr_values, dict)
        for attr_name in ATTR_NAMES:
            self.assertTrue(attr_name in attr_values)

    def _initialize(self):
        kwargs = dict(plat_config=PLATFORM_CONFIG)
        self._assert_state(PlatformAgentState.UNINITIALIZED)
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=kwargs)
        retval, elapsed_time = self._execute_agent(cmd)
        log.info("PlatformAgentEvent.INITIALIZE elapsed_time=%s, retval = %s",
                 elapsed_time, str(retval))
        self._assert_state(PlatformAgentState.INACTIVE)

    def _go_active(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval, elapsed_time = self._execute_agent(cmd)
        log.info("PlatformAgentEvent.GO_ACTIVE elapsed_time=%s, retval = %s",
                elapsed_time, str(retval))
        self._assert_state(PlatformAgentState.IDLE)

    def _run(self):
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval, elapsed_time = self._execute_agent(cmd)
        log.info("PlatformAgentEvent.RUN elapsed_time=%s, retval = %s",
                elapsed_time, str(retval))
        self._assert_state(PlatformAgentState.COMMAND)

    def _go_inactive(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE)
        retval, elapsed_time = self._execute_agent(cmd)
        log.info("PlatformAgentEvent.GO_INACTIVE elapsed_time=%s, retval = %s",
                elapsed_time, str(retval))
        self._assert_state(PlatformAgentState.INACTIVE)

    def _get_subplatform_ids(self):
        cmd = AgentCommand(command=PlatformAgentEvent.GET_SUBPLATFORM_IDS)
        retval, elapsed_time = self._execute_agent(cmd)
        log.info("PlatformAgentEvent.GET_SUBPLATFORM_IDS elapsed_time=%s, retval = %s",
                elapsed_time, str(retval))
        self.assertIsInstance(retval.result, list)
        self.assertTrue(x in retval.result for x in SUBPLATFORM_IDS)
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

        self._get_subplatform_ids()

        self._go_inactive()
        self._reset()
