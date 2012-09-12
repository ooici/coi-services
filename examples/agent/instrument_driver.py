
from pyon.public import log
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iorg_management_service import OrgManagementServiceClient
from nose.plugins.attrib import attr

from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest
from pyon.core.object import IonObjectSerializer

from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
#from ion.agents.instruments.drivers.sbe37.sbe37_driver import SBE37Channel
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter
from mi.instrument.seabird.sbe37smb.ooicore.driver import PACKET_CONFIG
from pyon.public import CFG
from pyon.ion.stream import StandaloneStreamSubscriber
from mock import patch

import time
import unittest
import simplejson, urllib


def instrument_test_driver(container):

    sa_user_header = container.governance_controller.get_system_actor_header()

    # Names of agent data streams to be configured.
    parsed_stream_name = 'ctd_parsed'
    raw_stream_name = 'ctd_raw'

    # Driver configuration.
    #Simulator

    driver_config = {
        'svr_addr': 'localhost',
        'cmd_port': 5556,
        'evt_port': 5557,
        'dvr_mod': 'mi.instrument.seabird.sbe37smb.ooicore.driver',
        'dvr_cls': 'SBE37Driver',
        'comms_config': {
            SBE37Channel.CTD: {
                'method':'ethernet',
                'device_addr': CFG.device.sbe37.host,
                'device_port': CFG.device.sbe37.port,
                'server_addr': 'localhost',
                'server_port': 8888
            }
        }
    }

    #Hardware

    _container_client = ContainerAgentClient(node=container.node,
        name=container.name)

# Create a pubsub client to create streams.
    _pubsub_client = PubsubManagementServiceClient(node=container.node)

    # A callback for processing subscribed-to data.
    def consume(message, *args, **kwargs):
        log.info('Subscriber received message: %s', str(message))


    subs = []

    # Create streams for each stream named in driver.
    stream_config = {}
    for (stream_name, val) in PACKET_CONFIG.iteritems():
        #@TODO: Figure out what goes inside this stream def, do we have a pdict?
        stream_def_id = _pubsub_client.create_stream_definition(
                name='instrument stream def')
        stream_id, route = _pubsub_client.create_stream(
                name=stream_name,
                stream_definition_id=stream_def_id, 
                exchange_point='science_data')
        stream_config[stream_name] = stream_id

        # Create subscriptions for each stream.
        exchange_name = '%s_queue' % stream_name
        sub = StandaloneStreamSubscriber(exchange_name=exchange_name, callback=consume)
        sub.start()
        sub_id = _pubsub_client.create_subscription(\
                name=exchange_name,
                stream_ids=[stream_id])

        _pubsub_client.activate_subscription(sub_id)
        subs.append(sub)


    # Create agent config.

    agent_resource_id = '123xyz'

    agent_config = {
        'driver_config' : driver_config,
        'stream_config' : stream_config,
        'agent'         : {'resource_id': agent_resource_id}
    }

    # Launch an instrument agent process.
    _ia_name = 'agent007'
    _ia_mod = 'ion.agents.instrument.instrument_agent'
    _ia_class = 'InstrumentAgent'
    _ia_pid = _container_client.spawn_process(name=_ia_name,
        module=_ia_mod, cls=_ia_class,
        config=agent_config)


    log.info('got pid=%s for resource_id=%s' % (str(_ia_pid), str(agent_resource_id)))


   # self._ia_client = None
    # Start a resource agent client to talk with the instrument agent.
   # self._ia_client = ResourceAgentClient(self.agent_resource_id, process=FakeProcess())
  #  log.info('got ia client %s', str(self._ia_client))
