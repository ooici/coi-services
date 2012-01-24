#!/usr/bin/env python

'''
@file ion/services/sa/process/test/test_int_data_process_management_service.py
@author Alon Yaari
@test ion.services.sa.process.DataProcessManagementService integration test
'''

from nose.plugins.attrib import attr
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.public import Container, log, IonObject
from pyon.public import RT, AT, LCS
from pyon.core.exception import BadRequest, NotFound, Conflict

from pyon.util.int_test import IonIntegrationTestCase
import unittest

class FakeProcess(LocalContextMixin):
    name = ''

@attr('INT', group='sa')
@unittest.skip('coi/dm/sa services not working yet for integration tests to pass')
class TestIntDataProcessManagementService(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')
        
        print 'started services'

        # Now create client to DataProcessManagementService
        self.DPMSclient = DataProcessManagementServiceClient(node=self.container.node)
        self.RRclient = ResourceRegistryServiceClient(node=self.container.node)


    def test_createDataProcess(self):


        #-------------------------------
        # Process Definition
        #-------------------------------
        print 'Creating new data process definition'
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='transform_process_definition',
                            description='some new dpd',
                            process_source='some_source_reference')
        try:
            dprocd_id = self.DPMSclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)
        print 'new dpd_id = ', dprocd_id

#        process_definition = IonObject(RT.ProcessDefinition, name='transform_process_definition')
#        process_definition.executable = {
#           'module': 'ion.services.dm.transformation.example.transform_example',
#           'class':'TransformExample'
#        }
#        process_definition_id, _ = RRclient.create(process_definition)


        #-------------------------------
        # Input Data Product
        #-------------------------------
        dp_obj = IonObject(RT.DataProduct,
                           name='input',
                           description='transform input')
        input_dp_id = self.data_product_management_service.create_data_product(dp_obj)
        #-------------------------------
        # Input Data Product
        #-------------------------------
        dp_obj = IonObject(RT.DataProduct,
                           name='output',
                           description='transform output')
        output_dp_id = self.data_product_management_service.create_data_product(dp_obj)


        try:
            dprocd_id = self.DPMSclient.create_data_process(dprocd_id, input_dp_id, output_dp_id)
        except BadRequest as ex: 
            self.fail("failed to create new data process definition: %s" %ex)

        
#        print 'Creating new subscription'
#        s_obj = IonObject(RT.Subscription,
#                            name='S1',
#                            description='some new subscription')
#        try:
#            s_id, version = self.RRclient.create(s_obj)
#        except BadRequest as ex:
#            self.fail("failed to create new subscription: %s" %ex)
#        print 'new s_id = ', s_id
#
#        print 'Creating new data product'
#        dprod_obj = IonObject(RT.Subscription,
#                            name='DProd1',
#                            description='some new data product')
#        try:
#            dprod_id, version = self.RRclient.create(dprod_obj)
#        except BadRequest as ex:
#            self.fail("failed to create new data product: %s" %ex)
#        print 'new dprod_id = ', dprod_id
#
#        print 'Creating new data process'
#        try:
#            dproc_id = self.DPMSclient.create_data_process(dprocd_id, s_id, dprod_id)
#        except (BadRequest, NotFound, Conflict) as ex:
#            self.fail("failed to create new data process definition: %s" %ex)
