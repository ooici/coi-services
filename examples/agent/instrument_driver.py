
from pyon.public import log
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iorg_management_service import OrgManagementServiceClient
from nose.plugins.attrib import attr

from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest
from pyon.core.object import IonObjectSerializer

from interface.objects import StreamQuery
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
from ion.services.mi.drivers.sbe37_driver import SBE37Channel
from ion.services.mi.drivers.sbe37_driver import SBE37Parameter
from ion.services.mi.drivers.sbe37_driver import PACKET_CONFIG
from pyon.public import CFG
from mock import patch

import time
import unittest
import simplejson, urllib
from ion.services.coi.service_gateway_service import GATEWAY_RESPONSE, GATEWAY_ERROR, GATEWAY_ERROR_MESSAGE, GATEWAY_ERROR_EXCEPTION, get_role_message_headers


def instrument_test_driver(container):

    org_client = OrgManagementServiceClient(node=container.node)
    id_client = IdentityManagementServiceClient(node=container.node)

    system_actor = id_client.find_user_identity_by_name(name=CFG.system.system_actor)
    log.info('system actor:' + system_actor._id)

    sa_header_roles = get_role_message_headers(org_client.find_all_roles_by_user(system_actor._id))


    # Names of agent data streams to be configured.
    parsed_stream_name = 'ctd_parsed'
    raw_stream_name = 'ctd_raw'

    # Driver configuration.
    #Simulator

    driver_config = {
        'svr_addr': 'localhost',
        'cmd_port': 5556,
        'evt_port': 5557,
        'dvr_mod': 'ion.services.mi.drivers.sbe37_driver',
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
    def consume(message, headers):
        log.info('Subscriber received message: %s', str(message))

    # Create a stream subscriber registrar to create subscribers.
    subscriber_registrar = StreamSubscriberRegistrar(process=container,
        node=container.node)

    subs = []

    # Create streams for each stream named in driver.
    stream_config = {}
    for (stream_name, val) in PACKET_CONFIG.iteritems():
        stream_def = ctd_stream_definition(stream_id=None)
        stream_def_id = _pubsub_client.create_stream_definition(
            container=stream_def)
        stream_id = _pubsub_client.create_stream(
            name=stream_name,
            stream_definition_id=stream_def_id,
            original=True,
            encoding='ION R2', headers={'ion-actor-id': system_actor._id, 'ion-actor-roles': sa_header_roles })
        stream_config[stream_name] = stream_id

        # Create subscriptions for each stream.
        exchange_name = '%s_queue' % stream_name
        sub = subscriber_registrar.create_subscriber(exchange_name=exchange_name, callback=consume)
        sub.start()
        query = StreamQuery(stream_ids=[stream_id])
        sub_id = _pubsub_client.create_subscription(\
            query=query, exchange_name=exchange_name )
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
    _ia_mod = 'ion.services.mi.instrument_agent'
    _ia_class = 'InstrumentAgent'
    _ia_pid = _container_client.spawn_process(name=_ia_name,
        module=_ia_mod, cls=_ia_class,
        config=agent_config)


    log.info('got pid=%s', str(_ia_pid))


   # self._ia_client = None
    # Start a resource agent client to talk with the instrument agent.
   # self._ia_client = ResourceAgentClient(self.agent_resource_id, process=FakeProcess())
  #  log.info('got ia client %s', str(self._ia_client))
