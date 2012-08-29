#!/usr/bin/env python

"""
@package ion.agents.platform.oms.oms_platform_network_builder
@file    ion/agents/platform/oms/oms_platform_network_builder.py
@author  Carlos Rueda
@brief   Platform network builder for OMS
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand

from ion.agents.platform.platform_network_builder import PlatformAgentNetworkBuilder
from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.exceptions import PlatformConnectionException
from ion.agents.platform.oms.oms_client_factory import OmsClientFactory
from ion.agents.platform.oms.oms_platform_agent import get_resource_id
from ion.agents.platform.util.network import NNode

from ion.agents.platform.platform_agent import PlatformAgentEvent


DRV_MOD = 'ion.agents.platform.oms.oms_platform_driver'
DRV_CLS = 'OmsPlatformDriver'

DVR_CONFIG = {
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS
}


# TODO Use appropriate process in ResourceAgentClient instance construction below
# for now, just replicating typical mechanism in test cases.
class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


class OmsPlatformAgentNetworkBuilder(PlatformAgentNetworkBuilder):
    """
    Platform network builder for OMS
    """
    #
    # PRELIMINARY
    #

    def __init__(self):
        PlatformAgentNetworkBuilder.__init__(self)
        self._oms = None

    def go_active(self):
        """
        Establish communication with external platform network.
        """
        assert 'oms_uri' in self._platform_network_config

        oms_uri = self._platform_network_config['oms_uri']
        oms = OmsClientFactory.create_instance(oms_uri)

        try:
            retval = oms.hello.ping()
        except Exception, e:
            raise PlatformConnectionException("Cannot ping %s" % str(e))

        if retval is None or retval.lower() != "pong":
            raise PlatformConnectionException(
                "Unexpected ping response: %r" % retval)

        self._oms = oms

    def create_topology_definition(self):
        """
        Assigns self._topology_def according to OMS' getPlatformMap response
        """
        map = self._oms.config.getPlatformMap()
        nodes = NNode.create_network(map)
        if not '' in nodes:
            raise PlatformException("platform map invalid: it does not "
                        "include '' to indicate root platforms. ")

        self._topology_def = nodes['']
        log.info("_topology_def:\n %s" % self._topology_def.dump())

    def check_topology_definition(self):
        """
        Verifies self._topology_def and does any needed sync.
        """
        pass

    def get_resource_id(self, platform_id):
        """
        For the given platform ID returns the resource ID that can be used to
        create ResourceAgentClient to interact with the launched agent.

        @param platform_id Platform ID

        @retval resource ID
        """
        pa_resource_id = get_resource_id(platform_id)
        return pa_resource_id

    def get_resource_agent_client(self, platform_id):
        """
        Gets a ResourceAgentClient object to interact with a launched PlatformAgent.

        NOTE: a new ResourceAgentClient instance is always created in the
        current implementation. Some internal cache (and appropriate
        maintenance) could be implemented later on.

        @param platform_id Platform ID

        @retval ResourceAgentClient object
        """
        #
        # TODO maintain a cache of created ResourceAgentClient's
        #
        pa_resource_id = get_resource_id(platform_id)
        pa_client = ResourceAgentClient(pa_resource_id, process=FakeProcess())
        log.info("Got platform agent client %s" % str(pa_client))
        return pa_client

    def _launch_platform_agent(self, container_client, platform_id):
        """
        Launches a platform agent and returns corresponding
        ResourceAgentClient instance. The launch only goes until the
        initialization of the agent.

        @param container_client used to spawn the agent process
        @param platform_id Platform ID

        @retval A ResourceAgentClient instance
        """

        pa_resource_id = get_resource_id(platform_id)
        pa_name = 'OmsPlatformAgent_%s' % platform_id
        pa_module = 'ion.agents.platform.oms.oms_platform_agent'
        pa_class = 'OmsPlatformAgent'

        agent_config = {
            'agent': {'resource_id': pa_resource_id},
            'stream_config' : {}  # TODO stream_config here!
        }

        # Start agent
        log.info("Starting agent: agent_config=%s" % str(agent_config))
        pa_pid = container_client.spawn_process(name=pa_name,
                                                module=pa_module,
                                                cls=pa_class,
                                                config=agent_config)

        log.info("Spawned platform agent process '%s': pa_pid=%s" % (
            platform_id, str(pa_pid)))

        # Start a resource agent client so we can initialize the agent
        pa_client = self.get_resource_agent_client(platform_id)
        log.info("Got platform agent client %s" % str(pa_client))

        # now, initialize the agent
        oms_uri = self._platform_network_config['oms_uri']
        plat_config = {
            'oms_uri': oms_uri,
            'platform_id': platform_id,
            'driver_config': DVR_CONFIG
        }

        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=dict(plat_config=plat_config))
        retval = pa_client.execute_agent(cmd)
        log.info("initialize's retval = %s" % str(retval))

        return pa_client

    def _run_platform_agent(self, platform_id):
        """
        Runs the platform agent by the given platform_id. Assumes the platform
        agent has already been launched.

        @param platform_id Platform ID
        """

        pa_client = self.get_resource_agent_client(platform_id)

        #
        # go_active agent
        #
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE)
        retval = pa_client.execute_agent(cmd)
        log.info("go_active's retval = %s" % str(retval))

        #
        # run agent
        #
        cmd = AgentCommand(command=PlatformAgentEvent.RUN)
        retval = pa_client.execute_agent(cmd)
        log.info("run's retval = %s" % str(retval))

    def _stop_platform_agent(self, platform_id):
        """
        Stops the platform agent by the given platform_id. Assumes the platform
        agent has already been launched.

        @param platform_id Platform ID
        """

        pa_client = self.get_resource_agent_client(platform_id)

        #
        # reset agent
        #
        cmd = AgentCommand(command=PlatformAgentEvent.RESET)
        retval = pa_client.execute_agent(cmd)
        log.info("reset's retval = %s" % str(retval))
