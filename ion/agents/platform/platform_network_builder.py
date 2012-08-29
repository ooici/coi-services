#!/usr/bin/env python

"""
@package ion.agents.platform.platform_network_builder
@file    ion/agents/platform/platform_network_builder.py
@author  Carlos Rueda
@brief   Entity responsible for building a network of platform agents.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from interface.objects import AgentCommand

from ion.agents.platform.platform_agent import PlatformAgentEvent



class PlatformAgentNetworkBuilder(object):
    """
    Builds a network of platform agents, and provides support for some
    associated operations on the network based on operations to be
    implemented by subclasses.
    """

    def __init__(self):
        #
        # platform network configuration
        self._platform_network_config = None

        #
        # self._topology_def, built by create_topology_definition, is the base node
        # (with platform_id == '') having as children the actual root platforms
        # in the network.
        self._topology_def = None

        #
        # self._dummy_pa_client_root, built by
        self._dummy_pa_client_root = None

    def set_config(self, platform_network_config):
        """
        Sets the platform network configuration
        """
        self._platform_network_config = platform_network_config
        log.info("platform_network_config=%s" % platform_network_config)

    def go_active(self):
        """
        To be implemented by subclass.
        Establishes communication with external platform network.
        """
        raise NotImplemented()

    def create_topology_definition(self):
        """
        To be implemented by subclass.
        Creates self._topology_def, the basic topology definition of
        the network.
        """
        raise NotImplemented()

    def check_topology_definition(self):
        """
        To be implemented by subclass.
        Verifies self._topology_def and does any needed sync.
        """
        raise NotImplemented()

    def get_resource_id(self, platform_id):
        """
        To be implemented by subclass.
        For the given platform ID returns the resource ID that can be used to
        create ResourceAgentClient to interact with the launched agent.

        @param platform_id Platform ID

        @retval resource ID
        """
        raise NotImplemented()

    def get_resource_agent_client(self, platform_id):
        """
        To be implemented by subclass.
        Gets a ResourceAgentClient object to interact with a launched PlatformAgent

        @param platform_id Platform ID

        @retval ResourceAgentClient object
        """
        raise NotImplemented()

    def _launch_platform_agent(self, container_client, platform_id):
        """
        To be implemented by subclass.
        Launches a platform agent.

        @param container_client ContainerAgentClient object to use for
                  spawning the agent process
        @param platform_id Platform ID

        @retval a ResourceAgentClient instance that can be used to interact
                with the launched agent.
        """
        raise NotImplemented()

    def build_platform_agent_client_network(self, container_client):
        """
        Based on the self._topology_def created by create_topology_definition,
        creates the whole hierarchy returning the "dummy" platform agent root

        The PlatformAgent instances are launched via calls to
        _launch_platform_agent, and the ResourceAgentClient instance
        corresponding to the dummy root is returned.

        @param container_client ContainerAgentClient object to use for
                  spawning the agent processes

        @retval the ResourceAgentClient instance corresponding to the dummy root.
        """
        assert self._topology_def
        assert self._topology_def.platform_id == ''

        def _build(parent_node, parent_pa_client):
            """
            Recursive routine to launch all the platform agents.
            """
            for subplat_node in parent_node.subplatforms.itervalues():
                subplat_id = subplat_node.platform_id
                log.info("launching subplatform agent for platform_id='%s'" % subplat_id)
                subplat_client = self._launch_platform_agent(container_client, subplat_id)
                _build(subplat_node, subplat_client)

                # add the subplatform to the parent
                kwargs = dict(subplatform_id=subplat_id)
                cmd = AgentCommand(command=PlatformAgentEvent.ADD_SUBPLATFORM, kwargs=kwargs)
                retval = parent_pa_client.execute_agent(cmd)
                log.info("add_subplatform_agent_client's retval = %s" % str(retval))


        self._dummy_pa_client_root = self._launch_platform_agent( container_client, '')
        _build(self._topology_def, self._dummy_pa_client_root)
        return self._dummy_pa_client_root

    def _run_platform_agent(self, platform_id):
        """
        To be implemented by subclass.
        Runs the platform agent by the given platform_id. Assumes the platform
        agent has already been launched.

        @param platform_id Platform ID
        """
        raise NotImplemented()

    def _stop_platform_agent(self, platform_id):
        """
        To be implemented by subclass.
        Stops the platform agent by the given platform_id. Assumes the platform
        agent has already been launched.

        @param platform_id Platform ID
        """
        raise NotImplemented()

    def run_platform_agent_client_network(self):
        """
        Runs all the platform agents in the network created by
        build_platform_agent_client_network

        """
        assert self._topology_def
        assert self._topology_def.platform_id == ''

        def _run(plat_node):
            """
            Recursive routine to run all the platform agents.
            """
            plat_id = plat_node.platform_id
            log.info("running platform agent by platform_id='%s'" % plat_id)
            self._run_platform_agent(plat_id)

            for subplat_node in plat_node.subplatforms.itervalues():
                _run(subplat_node)

        for plat_node in self._topology_def.subplatforms.itervalues():
            _run(plat_node)

    def stop_platform_agent_client_network(self):
        """
        Stops all the platform agents in the network created by
        build_platform_agent_client_network

        """
        assert self._topology_def
        assert self._topology_def.platform_id == ''

        def _stop(plat_node):
            """
            Recursive routine to stop all the platform agents.
            """
            plat_id = plat_node.platform_id
            log.info("stopping platform agent by platform_id='%s'" % plat_id)
            self._stop_platform_agent(plat_id)

            for subplat_node in plat_node.subplatforms.itervalues():
                _stop(subplat_node)

        for plat_node in self._topology_def.subplatforms.itervalues():
            _stop(plat_node)
