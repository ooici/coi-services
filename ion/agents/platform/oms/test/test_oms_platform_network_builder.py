#!/usr/bin/env python

"""
@package ion.agents.platform.oms.test.test_oms_platform_network_builder
@file    ion/agents/platform/oms/test/test_oms_platform_network_builder.py
@author  Carlos Rueda
@brief   OmsPlatformAgentNetworkBuilder tests
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from pyon.util.int_test import IonIntegrationTestCase
from interface.services.icontainer_agent import ContainerAgentClient
from interface.objects import AgentCommand

from ion.agents.platform.util.network import NNode
from ion.agents.platform.oms.oms_platform_network_builder import OmsPlatformAgentNetworkBuilder
from ion.agents.platform.platform_agent import PlatformAgentState
from ion.agents.platform.platform_agent import PlatformAgentEvent

from gevent import sleep
from nose.plugins.attrib import attr
import unittest
import os


# in general, test intended to be against simulator but allow to say specific
# OmsClient via OMS environment variable for developer's convenience.
PLATFORM_NETWORK_CONFIG = {
    'oms_uri': os.getenv('OMS', 'embsimulator'),
}


@unittest.skip("Skip while preparing new test_platform_agent_with_oms")
@unittest.skipIf(os.getenv('OMS') is None, "Define OMS to include this test")
@attr('INT', group='sa')
class Test(IonIntegrationTestCase):

    def setUp(self):

        self._start_container()

        # Bring up services in a deploy file (no need to message)
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.panb = OmsPlatformAgentNetworkBuilder()
        self.panb.set_config(PLATFORM_NETWORK_CONFIG)
        self.panb.go_active()
        self.panb.create_topology_definition()
        self.panb.check_topology_definition()

    def _create_container_client(self):
        """
        Creates ContainerAgentClient needed to create the platform network.
        """
        self.container_client = ContainerAgentClient(node=self.container.node,
                                                     name=self.container.name)
        log.info('container_client: %s', str(self.container_client))

    def _build_platform_network(self):
        """
        Creates the platform network.
        """
        log.info("creating platform network...")
        self.dummy_pa_client_root = self.panb.build_platform_agent_client_network(self.container_client)

    def _build_topology_from_platform_network(self):
        """
        Creates NNode topology using information provided by the platform agents
        """
        def get_topology(parent_pa_client, parent_platform_id):
            """
            Creates the NNode corresponding to the given platform agent client
            and recursively creates the sub-nodes.
            """
            parent_node = NNode(parent_platform_id)
            cmd = AgentCommand(command=PlatformAgentEvent.GET_SUBPLATFORM_IDS)
            retval = parent_pa_client.execute_agent(cmd)
            log.info("get_subplatform_ids's retval = %s" % str(retval))
            self.assertIsInstance(retval.result, list)
            for pair in retval.result:
                self.assertIsInstance(pair, (tuple, list))
                subplatform_id, pa_resource_id = pair

                pa_client = self.panb.get_resource_agent_client(subplatform_id)

                subplat_node = get_topology(pa_client, subplatform_id)
                parent_node.add_subplatform(subplat_node)
            return parent_node

        self.dummy_root_node = get_topology(self.dummy_pa_client_root, '')
        log.info("get_topology result:\n%s" % self.dummy_root_node.dump(only_topology=True))

    def _verify_topology(self):
        """
        Verifies the topology if the network was built against the simulator.
        """
        if PLATFORM_NETWORK_CONFIG['oms_uri'] == 'embsimulator':
            #
            # do structure comparison with simulated network
            #
            sim_dummy_root_node = self.panb._oms._dummy_root
            diff = self.dummy_root_node.diff_topology(sim_dummy_root_node)
            log.info("diff_topology = %r" % diff)
            self.assertIsNone(diff)

    def _run_platform_network(self):
        """
        Runs the platform network
        """
        log.info("running platform network...")
        self.panb.run_platform_agent_client_network()

    def _verify_all_platform_agents_in_state(self, expected_state):
        """
        Verifies all platform agents are in the given state.
        """
        def traverse(plat_node):
            """
            gets and verifies the state, then recursively does the same for
            the subplatforms.
            """
            plat_id = plat_node.platform_id
            parent_pa_client = self.panb.get_resource_agent_client(plat_id)

            state = parent_pa_client.get_agent_state()
            log.info("platform state: %r -> %s" % (plat_id, state))
            self.assertEquals(expected_state, state)

            for subplat_node in plat_node.subplatforms.itervalues():
                traverse(subplat_node)

        for plat_node in self.dummy_root_node.subplatforms.itervalues():
            traverse(plat_node)

    def _stop_platform_network(self):
        """
        Stops the platform network
        """
        log.info("stopping platform network...")
        self.panb.stop_platform_agent_client_network()

    def test_build_platform_agent_client_network(self):
        self._create_container_client()
        self._build_platform_network()
        self._build_topology_from_platform_network()
        self._verify_topology()
        self._verify_all_platform_agents_in_state(PlatformAgentState.INACTIVE)
        self._run_platform_network()
        self._verify_all_platform_agents_in_state(PlatformAgentState.COMMAND)

        sleep(10)  # to retrieve some platform attributes, etc.

        self._stop_platform_network()
        self._verify_all_platform_agents_in_state(PlatformAgentState.UNINITIALIZED)
