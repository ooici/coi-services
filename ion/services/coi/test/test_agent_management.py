#!/usr/bin/env python

__author__ = 'Thomas Lennan'

from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest
from pyon.public import IonObject, log
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iagent_management_service import AgentManagementServiceClient

@attr('INT', group='ams')
class TestAgentManagementService(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        # Now create client to service
        self.rr = self.container.resource_registry
        self.ams = AgentManagementServiceClient(node=self.container.node)

    def test_agent_interface(self):
        rid1,_ = self.rr.create(IonObject('Resource', name='res1'))

        cap_list = self.ams.get_capabilities(rid1)
        log.warn("Capabilities: %s", cap_list)
        self.assertTrue(type(cap_list) is list)

        get_res = self.ams.get_resource(rid1, params=['object_size'])
        log.warn("Get result: %s", get_res)

        self.ams.set_resource(rid1, params={'description': 'NEWDESC'})
        res_obj = self.rr.read(rid1)
        self.assertEquals(res_obj.description, 'NEWDESC')

        self.rr.delete(rid1)

        # Test CRUD
        rid2 = self.ams.create_resource(IonObject('Resource', name='res2'))
        res_obj = self.rr.read(rid2)
        self.assertEquals(res_obj.name, 'res2')

        res_obj.description = 'DESC2'
        self.ams.update_resource(res_obj)
        res_obj = self.rr.read(rid2)
        self.assertEquals(res_obj.description, 'DESC2')

        res_obj2 = self.ams.read_resource(rid2)
        self.assertEquals(res_obj.description, res_obj2.description)

        self.ams.delete_resource(rid2)
