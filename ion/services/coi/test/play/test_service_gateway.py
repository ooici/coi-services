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


