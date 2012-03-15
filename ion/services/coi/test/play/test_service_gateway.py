#!/usr/bin/env python

__author__ = 'Roger Unwin'
__license__ = 'Apache 2.0'

import unittest
import simplejson, urllib

from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.core.bootstrap import CFG


@attr('SYSTEM', group='coi')
class TestServiceGateway(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

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
        gateway_resp = result['data']['GatewayResponse']

        self.assertIn('AgentDefinition', gateway_resp)
        self.assertIn('AgentInstance', gateway_resp)
        self.assertIn('Attachment', gateway_resp)
        self.assertIn('BankAccount', gateway_resp)
        self.assertIn('BankCustomer', gateway_resp)
        self.assertIn('Catalog', gateway_resp)
        self.assertIn('Collection', gateway_resp)
        self.assertIn('Conversation', gateway_resp)
        self.assertIn('ConversationRole', gateway_resp)
        self.assertIn('ConversationType', gateway_resp)
        self.assertIn('DataProcess', gateway_resp)
        self.assertIn('DataProcessDefinition', gateway_resp)
        self.assertIn('DataProducer', gateway_resp)
        self.assertIn('DataProduct', gateway_resp)
        self.assertIn('DataSet', gateway_resp)
        self.assertIn('DataSource', gateway_resp)
        self.assertIn('DataStore', gateway_resp)
        self.assertIn('DeployableType', gateway_resp)
        self.assertIn('EPU', gateway_resp)
        self.assertIn('EPUDefinition', gateway_resp)
        self.assertIn('EventType', gateway_resp)
        self.assertIn('ExchangeBroker', gateway_resp)
        self.assertIn('ExchangeName', gateway_resp)
        self.assertIn('ExchangePoint', gateway_resp)
        self.assertIn('ExchangeSpace', gateway_resp)
        self.assertIn('ExecutionEngine', gateway_resp)
        self.assertIn('ExecutionEngineDefinition', gateway_resp)
        self.assertIn('ExternalDataAgent', gateway_resp)
        self.assertIn('ExternalDataAgentInstance', gateway_resp)
        self.assertIn('ExternalDataProvider', gateway_resp)
        self.assertIn('ExternalDataSourceModel', gateway_resp)
        self.assertIn('ExternalDataset', gateway_resp)
        self.assertIn('Index', gateway_resp)
        self.assertIn('InformationResource', gateway_resp)
        self.assertIn('IngestionConfiguration', gateway_resp)
        self.assertIn('InstrumentAgent', gateway_resp)
        self.assertIn('InstrumentAgentInstance', gateway_resp)
        self.assertIn('InstrumentDevice', gateway_resp)
        self.assertIn('InstrumentModel', gateway_resp)
        self.assertIn('LogicalInstrument', gateway_resp)
        self.assertIn('LogicalPlatform', gateway_resp)
        self.assertIn('MarineFacility', gateway_resp)
        self.assertIn('NotificationRequest', gateway_resp)
        self.assertIn('ObjectType', gateway_resp)
        self.assertIn('Org', gateway_resp)
        self.assertIn('PersistentArchive', gateway_resp)
        self.assertIn('PlatformAgent', gateway_resp)
        self.assertIn('PlatformAgentInstance', gateway_resp)
        self.assertIn('PlatformDevice', gateway_resp)
        self.assertIn('PlatformModel', gateway_resp)
        self.assertIn('Policy', gateway_resp)
        self.assertIn('ProcessDefinition', gateway_resp)
        #self.assertIn('ProcessState', result['data'])
        self.assertIn('Resource', gateway_resp)
        self.assertIn('ResourceAgentType', gateway_resp)
        self.assertIn('ResourceIdentity', gateway_resp)
        self.assertIn('ResourceLifeCycle', gateway_resp)
        self.assertIn('ResourceType', gateway_resp)
        self.assertIn('SampleResource', gateway_resp)
        self.assertIn('SensorDevice', gateway_resp)
        self.assertIn('SensorModel', gateway_resp)
        self.assertIn('Service', gateway_resp)
        self.assertIn('ServiceDefinition', gateway_resp)
        self.assertIn('Site', gateway_resp)
        self.assertIn('Stream', gateway_resp)
        self.assertIn('StreamTopology', gateway_resp)
        self.assertIn('Subscription', gateway_resp)
        self.assertIn('TaskableResource', gateway_resp)
        self.assertIn('Transform', gateway_resp)
        self.assertIn('UserCredentials', gateway_resp)
        self.assertIn('UserIdentity', gateway_resp)
        self.assertIn('UserInfo', gateway_resp)
        self.assertIn('UserRole', gateway_resp)
        self.assertIn('View', gateway_resp)
        self.assertIn('VisualizationWorkflow', gateway_resp)


