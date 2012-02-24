#!/usr/bin/env python

__author__ = 'Roger Unwin'
__license__ = 'Apache 2.0'

import unittest
import simplejson, urllib

from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from interface.services.icontainer_agent import ContainerAgentClient
from pyon.core.bootstrap import CFG


@attr('SYSTEM', group='coi')
class TestServiceGateway(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        container_client.start_rel_from_url('res/deploy/r2deploy.yml')

    def tearDown(self):
        self._stop_container()

    def test_list_resource_types(self):
        port = 5000
        hostname = 'localhost'
        if 'web_server' in CFG:
            web_server_cfg = CFG['web_server']
            if web_server_cfg is not None:
                if 'hostname' in web_server_cfg:
                    hostname = web_server_cfg['hostname']
                if 'port' in web_server_cfg:
                    port = web_server_cfg['port']
        SEARCH_BASE = 'http://' + hostname + ':' + str(port) + '/ion-service/list_resource_types'
        args = {}
        args.update({
           #'format': "unix",
           #'output': 'json'
        })
        url = SEARCH_BASE + '?' + urllib.urlencode(args)
        result = simplejson.load(urllib.urlopen(url))

        self.assertIn('AgentDefinition', result['data'])
        self.assertIn('AgentInstance', result['data'])
        self.assertIn('Attachment', result['data'])
        self.assertIn('BankAccount', result['data'])
        self.assertIn('BankCustomer', result['data'])
        self.assertIn('Catalog', result['data'])
        self.assertIn('Collection', result['data'])
        self.assertIn('Conversation', result['data'])
        self.assertIn('ConversationRole', result['data'])
        self.assertIn('ConversationType', result['data'])
        self.assertIn('DataProcess', result['data'])
        self.assertIn('DataProcessDefinition', result['data'])
        self.assertIn('DataProducer', result['data'])
        self.assertIn('DataProduct', result['data'])
        self.assertIn('DataSet', result['data'])
        self.assertIn('DataSource', result['data'])
        self.assertIn('DataStore', result['data'])
        self.assertIn('DeployableType', result['data'])
        self.assertIn('EPU', result['data'])
        self.assertIn('EPUDefinition', result['data'])
        self.assertIn('EventType', result['data'])
        self.assertIn('ExchangeBroker', result['data'])
        self.assertIn('ExchangeName', result['data'])
        self.assertIn('ExchangePoint', result['data'])
        self.assertIn('ExchangeSpace', result['data'])
        self.assertIn('ExecutionEngine', result['data'])
        self.assertIn('ExecutionEngineDefinition', result['data'])
        self.assertIn('ExternalDataAgent', result['data'])
        self.assertIn('ExternalDataAgentInstance', result['data'])
        self.assertIn('ExternalDataProvider', result['data'])
        self.assertIn('ExternalDataSourceModel', result['data'])
        self.assertIn('ExternalDataset', result['data'])
        self.assertIn('Index', result['data'])
        self.assertIn('InformationResource', result['data'])
        self.assertIn('IngestionConfiguration', result['data'])
        self.assertIn('InstrumentAgent', result['data'])
        self.assertIn('InstrumentAgentInstance', result['data'])
        self.assertIn('InstrumentDevice', result['data'])
        self.assertIn('InstrumentModel', result['data'])
        self.assertIn('LogicalInstrument', result['data'])
        self.assertIn('LogicalPlatform', result['data'])
        self.assertIn('MarineFacility', result['data'])
        self.assertIn('NotificationRequest', result['data'])
        self.assertIn('ObjectType', result['data'])
        self.assertIn('Org', result['data'])
        self.assertIn('PersistentArchive', result['data'])
        self.assertIn('PlatformAgent', result['data'])
        self.assertIn('PlatformAgentInstance', result['data'])
        self.assertIn('PlatformDevice', result['data'])
        self.assertIn('PlatformModel', result['data'])
        self.assertIn('Policy', result['data'])
        self.assertIn('ProcessDefinition', result['data'])
        #self.assertIn('ProcessState', result['data'])
        self.assertIn('Resource', result['data'])
        self.assertIn('ResourceAgentType', result['data'])
        self.assertIn('ResourceIdentity', result['data'])
        self.assertIn('ResourceLifeCycle', result['data'])
        self.assertIn('ResourceType', result['data'])
        self.assertIn('SampleResource', result['data'])
        self.assertIn('SensorDevice', result['data'])
        self.assertIn('SensorModel', result['data'])
        self.assertIn('Service', result['data'])
        self.assertIn('ServiceDefinition', result['data'])
        self.assertIn('Site', result['data'])
        self.assertIn('Stream', result['data'])
        self.assertIn('StreamIngestionPolicy', result['data'])
        self.assertIn('StreamTopology', result['data'])
        self.assertIn('Subscription', result['data'])
        self.assertIn('TaskableResource', result['data'])
        self.assertIn('Transform', result['data'])
        self.assertIn('UserCredentials', result['data'])
        self.assertIn('UserIdentity', result['data'])
        self.assertIn('UserInfo', result['data'])
        self.assertIn('UserRole', result['data'])
        self.assertIn('View', result['data'])
        self.assertIn('VisualizationWorkflow', result['data'])


