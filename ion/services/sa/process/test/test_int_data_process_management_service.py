#!/usr/bin/env python

'''
@file ion/services/sa/process/test/test_int_data_process_management_service.py
@author Alon Yaari
@test ion.services.sa.process.DataProcessManagementService integration test
'''

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
import unittest
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from pyon.public import Container, log, IonObject
from pyon.public import RT, AT, LCS
from pyon.core.exception import BadRequest, NotFound, Conflict

@attr('INT', group='sa')
class TestIntDataProcessManagementService(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        #print 'started container'

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')
        
        print 'started services'

        # Now create client to DataProcessManagementService
        self.client = DataProcessManagementServiceClient(node=self.container.node)

    def test_createDataProcess(self):
        # test creating a new data process 
        print 'Creating new data process definition'
        dpd_obj = IonObject(RT.DataProcessDefinition, 
                           name='DPD1', 
                           description='some new dpd')
        try:
            dpd_id = self.client.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)
        print 'new dpd_id = ', dpd_id

