#!/usr/bin/env python

'''
@file ion/services/sa/instrument/test/test_int_data_acquisition_management_service.py
@author Maurice Manning
@test ion.services.sa.acquisition.DataAcquisitionManagementService integration test
'''

from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.public import AT, RT
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin

from ion.services.sa.acquisition.data_acquisition_management_service import DataAcquisitionManagementService
from interface.services.sa.idata_acquisition_management_service import IDataAcquisitionManagementService, DataAcquisitionManagementServiceClient

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
import unittest

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
class TestIntDataAcquisitionManagementService(IonIntegrationTestCase):

    def setUp(self):
        pass

    #@unittest.skip('Not done yet.')
    def test_data_source_ops(self):
        # Start container
        #print 'instantiating container'
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')

        print 'started services'

        # Now create client to DataAcquisitionManagementService
        client = DataAcquisitionManagementServiceClient(node=self.container.node)

        # test creating a new data source
        print 'Creating new data source'
        datasource_obj = IonObject(RT.DataSource,
                           name='DataSource1',
                           description='instrument based new source' ,
                            type='sbe37')
        try:
            ds_id = client.create_data_source(datasource_obj)
        except BadRequest as ex:
            self.fail("failed to create new data source: %s" %ex)
        print 'new data source id = ', ds_id


        # test reading a non-existent data source
        print 'reading non-existent data source'
        try:
            dp_obj = client.read_data_source('some_fake_id')
        except NotFound as ex:
            pass
        else:
            self.fail("non-existing data source was found during read: %s" %dp_obj)

        # update a data source (tests read also)
        print 'Updating data source'
        # first get the existing data source object
        try:
            datasource_obj = client.read_data_source(ds_id)
        except NotFound as ex:
            self.fail("existing data source was not found during read")
        else:
            pass

        # now tweak the object
        datasource_obj.description = 'the very first data source'
        # now write the dp back to the registry
        try:
            update_result = client.update_data_source(datasource_obj)
        except NotFound as ex:
            self.fail("existing data source was not found during update")
        except Conflict as ex:
            self.fail("revision conflict exception during data source update")
        #else:
        #    self.assertTrue(update_result == True)
        # now get the data source back to see if it was updated
        try:
            datasource_obj = client.read_data_source(ds_id)
        except NotFound as ex:
            self.fail("existing data source was not found during read")
        else:
            pass
        self.assertTrue(datasource_obj.description == 'the very first data source')            


        # now 'delete' the data source
        print "deleting data source"
        try:
            delete_result = client.delete_data_source(ds_id)
        except NotFound as ex:
            self.fail("existing data source was not found during delete")
        #self.assertTrue(delete_result == True)
        # now try to get the deleted dp object
        try:
            dp_obj = client.read_data_source(ds_id)
        except NotFound as ex:
            pass
        else:
            self.fail("deleted data source was found during read")

        # now try to delete the already deleted data source object
        print "deleting non-existing data source"
        try:
            delete_result = client.delete_data_source(ds_id)
        except NotFound as ex:
            pass
        else:
            self.fail("non-existing data source was found during delete")


    #@unittest.skip('Not done yet.')
    def test_create_producer(self):
        # Create a data producer in coordination with DM PubSub: create stream, register and create producer object

        # Start container
        #print 'instantiating container'
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')

        print 'started services'

        # Now create client to DataAcquisitionManagementService
        client = DataAcquisitionManagementServiceClient(node=self.container.node)

        # test creating a new data source
        print 'Creating new data producer'
        dataproducer_obj = IonObject(RT.DataProducer,
                           name='DataProducer1',
                           description='instrument producer')
        try:
            ds_id = client.create_data_producer(dataproducer_obj)
        except BadRequest as ex:
            self.fail("failed to create new data producer: %s" %ex)
        print 'new data producer id = ', ds_id




    @unittest.skip('Not done yet.')
    def test_register_process_and_send(self):
        # Register a transform in coordination with DM PubSub: create stream, register and create producer object
        pass
