#!/usr/bin/env python

__author__ = 'Roger Unwin'
__license__ = 'Apache 2.0'

import unittest
import simplejson, urllib

from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from interface.services.icontainer_agent import ContainerAgentClient
from pyon.core.bootstrap import CFG


@attr('INT', group='coi')
class test_service_gateway(IonIntegrationTestCase):

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
        #FILE = open("ROGER","w")
        #FILE.writelines(str(result['data']))
        #FILE.close()

        self.assertTrue('AgentDefinition' in result['data'])
        self.assertTrue('AgentInstance' in result['data'])
        self.assertTrue('Attachment' in result['data'])
        self.assertTrue('BankAccount' in result['data'])
        self.assertTrue('BankCustomer' in result['data'])
        self.assertTrue('Catalog' in result['data'])
        self.assertTrue('Collection' in result['data'])
        self.assertTrue('Conversation' in result['data'])
        self.assertTrue('ConversationRole' in result['data'])
        self.assertTrue('ConversationType' in result['data'])
        self.assertTrue('DataProcess' in result['data'])
        self.assertTrue('DataProcessDefinition' in result['data'])
        self.assertTrue('DataProducer' in result['data'])
        self.assertTrue('DataProduct' in result['data'])
        self.assertTrue('DataSet' in result['data'])
        self.assertTrue('DataSource' in result['data'])
        self.assertTrue('DataStore' in result['data'])
        self.assertTrue('DeployableType' in result['data'])
        self.assertTrue('EPU' in result['data'])
        self.assertTrue('EPUDefinition' in result['data'])
        self.assertTrue('EventType' in result['data'])
        self.assertTrue('ExchangeBroker' in result['data'])
        self.assertTrue('ExchangeName' in result['data'])
        self.assertTrue('ExchangePoint' in result['data'])
        self.assertTrue('ExchangeSpace' in result['data'])
        self.assertTrue('ExecutionEngine' in result['data'])
        self.assertTrue('ExecutionEngineDefinition' in result['data'])
        self.assertTrue('ExternalDataAgent' in result['data'])
        self.assertTrue('ExternalDataAgentInstance' in result['data'])
        self.assertTrue('ExternalDataProvider' in result['data'])
        self.assertTrue('ExternalDataSourceModel' in result['data'])
        self.assertTrue('ExternalDataset' in result['data'])
        self.assertTrue('Index' in result['data'])
        self.assertTrue('InformationResource' in result['data'])
        self.assertTrue('IngestionConfiguration' in result['data'])
        self.assertTrue('InstrumentAgent' in result['data'])
        self.assertTrue('InstrumentAgentInstance' in result['data'])
        self.assertTrue('InstrumentDevice' in result['data'])
        self.assertTrue('InstrumentModel' in result['data'])
        self.assertTrue('LogicalInstrument' in result['data'])
        self.assertTrue('LogicalPlatform' in result['data'])
        self.assertTrue('MarineFacility' in result['data'])
        self.assertTrue('NotificationRequest' in result['data'])
        self.assertTrue('ObjectType' in result['data'])
        self.assertTrue('Org' in result['data'])
        self.assertTrue('PersistentArchive' in result['data'])
        self.assertTrue('PlatformAgent' in result['data'])
        self.assertTrue('PlatformAgentInstance' in result['data'])
        self.assertTrue('PlatformDevice' in result['data'])
        self.assertTrue('PlatformModel' in result['data'])
        self.assertTrue('Policy' in result['data'])
        self.assertTrue('ProcessDefinition' in result['data'])
        self.assertTrue('ProcessState' in result['data'])
        self.assertTrue('Resource' in result['data'])
        self.assertTrue('ResourceAgentType' in result['data'])
        self.assertTrue('ResourceIdentity' in result['data'])
        self.assertTrue('ResourceLifeCycle' in result['data'])
        self.assertTrue('ResourceType' in result['data'])
        self.assertTrue('SampleResource' in result['data'])
        self.assertTrue('SensorDevice' in result['data'])
        self.assertTrue('SensorModel' in result['data'])
        self.assertTrue('Service' in result['data'])
        self.assertTrue('ServiceDefinition' in result['data'])
        self.assertTrue('Site' in result['data'])
        self.assertTrue('Stream' in result['data'])
        self.assertTrue('StreamIngestionPolicy' in result['data'])
        self.assertTrue('StreamTopology' in result['data'])
        self.assertTrue('Subscription' in result['data'])
        self.assertTrue('TaskableResource' in result['data'])
        self.assertTrue('Transform' in result['data'])
        self.assertTrue('UserCredentials' in result['data'])
        self.assertTrue('UserIdentity' in result['data'])
        self.assertTrue('UserInfo' in result['data'])
        self.assertTrue('UserRole' in result['data'])
        self.assertTrue('View' in result['data'])
        self.assertTrue('VisualizationWorkflow' in result['data'])


