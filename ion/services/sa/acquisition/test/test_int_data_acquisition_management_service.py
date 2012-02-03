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
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
import unittest

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
#@unittest.skip('not working')
class TestIntDataAcquisitionManagementService(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')

        # Now create client to DataAcquisitionManagementService
        self.client = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)

    def tearDown(self):
        pass


    #@unittest.skip('Not done yet.')
    def test_data_source_ops(self):
        # test creating a new data source
        print 'Creating new data source'
        datasource_obj = IonObject(RT.DataSource,
                           name='DataSource1',
                           description='instrument based new source' ,
                            type='sbe37')
        try:
            ds_id = self.client.create_data_source(datasource_obj)
        except BadRequest as ex:
            self.fail("failed to create new data source: %s" %ex)
        print 'new data source id = ', ds_id


        # test reading a non-existent data source
        print 'reading non-existent data source'
        try:
            dp_obj = self.client.read_data_source('some_fake_id')
        except NotFound as ex:
            pass
        else:
            self.fail("non-existing data source was found during read: %s" %dp_obj)

        # update a data source (tests read also)
        print 'Updating data source'
        # first get the existing data source object
        try:
            datasource_obj = self.client.read_data_source(ds_id)
        except NotFound as ex:
            self.fail("existing data source was not found during read")
        else:
            pass

        # now tweak the object
        datasource_obj.description = 'the very first data source'
        # now write the dp back to the registry
        try:
            update_result = self.client.update_data_source(datasource_obj)
        except NotFound as ex:
            self.fail("existing data source was not found during update")
        except Conflict as ex:
            self.fail("revision conflict exception during data source update")
        #else:
        #    self.assertTrue(update_result == True)
        # now get the data source back to see if it was updated
        try:
            datasource_obj = self.client.read_data_source(ds_id)
        except NotFound as ex:
            self.fail("existing data source was not found during read")
        else:
            pass
        self.assertTrue(datasource_obj.description == 'the very first data source')


        # now 'delete' the data source
        print "deleting data source"
        try:
            delete_result = self.client.delete_data_source(ds_id)
        except NotFound as ex:
            self.fail("existing data source was not found during delete")
        #self.assertTrue(delete_result == True)
        # now try to get the deleted dp object
        try:
            dp_obj = self.client.read_data_source(ds_id)
        except NotFound as ex:
            pass
        else:
            self.fail("deleted data source was found during read")

        # now try to delete the already deleted data source object
        print "deleting non-existing data source"
        try:
            delete_result = self.client.delete_data_source(ds_id)
        except NotFound as ex:
            pass
        else:
            self.fail("non-existing data source was found during delete")


    #@unittest.skip('Not done yet.')
    def test_create_producer(self):
        # Create a data producer in coordination with DM PubSub: create stream, register and create producer object

        # test creating a new data source
        print 'Creating new data producer'
        dataproducer_obj = IonObject(RT.DataProducer,
                           name='DataProducer1',
                           description='instrument producer')
        try:
            ds_id = self.client.create_data_producer(dataproducer_obj)
        except BadRequest as ex:
            self.fail("failed to create new data producer: %s" %ex)
        print 'new data producer id = ', ds_id

    #@unittest.skip('Not done yet.')
    def test_register_instrument(self):
        # Register an instrument as a data producer in coordination with DM PubSub: create stream, register and create producer object


        # set up initial instrument to register
        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
        instrument_id, rev = self.rrclient.create(instrument_obj)

        # test registering a new data producer
        try:
            ds_id = self.client.register_instrument(instrument_id)
        except BadRequest as ex:
            self.fail("failed to create new data producer: %s" %ex)
        print 'new data producer id = ', ds_id

        # test UNregistering a new data producer
        try:
            ds_id = self.client.unregister_instrument(instrument_id)
        except BadRequest as ex:
            self.fail("failed to unregister instrument producer: %s" %ex)

    @unittest.skip('Illegal subject type ExternalDataset for predicate hasAgentInstance')
    def test_eoi_resources(self):

            #
            # test creating a new data provider
            #
            print 'Creating new external_data_provider'
            dataprovider_obj = IonObject(RT.ExternalDataProvider,
                               name='ExtDataProvider1',
                               description='external data provider ')
            try:
                dataprovider_id = self.client.create_external_data_provider(dataprovider_obj)
            except BadRequest as ex:
                self.fail("failed to create new data provider: %s" %ex)
            print 'new data provider id = ', dataprovider_id

            #
            # test creating a new data source
            #
            print 'Creating new data source'
            datasource_obj = IonObject(RT.DataSource,
                               name='DataSource1',
                               description='data source ',
                               type='DAP')
            try:
                datasource_id = self.client.create_data_source(datasource_obj)
            except BadRequest as ex:
                self.fail("failed to create new data source: %s" %ex)
            print 'new data source id = ', datasource_id

            #
            # test creating a new data source model
            #
            print 'Creating new data source model'
            datamodel_obj = IonObject(RT.ExternalDataSourceModel,
                               name='DataSourceModel1',
                               description='data source model',
                               model='model1')
            try:
                datamodel_id = self.client.create_data_source(datamodel_obj)
            except BadRequest as ex:
                self.fail("failed to create new data source model: %s" %ex)
            print 'new data source model id = ', datamodel_id


            #
            # test creating a new external data set
            #
            print 'Creating new external data set'
            dataset_obj = IonObject(RT.ExternalDataset,
                               name='ExternalDataSet1',
                               description='external data set ')
            try:
                extdataset_id = self.client.create_external_dataset(dataset_obj)
            except BadRequest as ex:
                self.fail("failed to create new external data set: %s" %ex)
            print 'new external data set id = ', extdataset_id


            #
            # test creating a new data agent instance
            #
            print 'Creating new external data agent '
            dataagent_obj = IonObject(RT.ExternalDataAgent,
                               name='ExternalAgent1',
                               description='external data agent ')
            try:
                dataagent_id = self.client.create_external_data_agent_instance(dataagent_obj)
            except BadRequest as ex:
                self.fail("failed to create new external data agent: %s" %ex)
            print 'new external data agent  id = ', dataagent_id


            #
            # test creating a new data agent instance
            #
            print 'Creating new external data agent instance'
            dataagentinstance_obj = IonObject(RT.ExternalDataAgentInstance,
                               name='ExternalAgentInstance1',
                               description='external data agent instance ')
            try:
                dataagentinstance_id = self.client.create_external_data_agent_instance(dataagentinstance_obj)
            except BadRequest as ex:
                self.fail("failed to create new external data agent instance: %s" %ex)
            print 'new external data agent instance id = ', dataagentinstance_id



            #
            # test assign / unassign
            #

            self.client.assign_eoi_resources(dataprovider_id, datasource_id, datamodel_id, extdataset_id, dataagent_id, dataagentinstance_id)

            self.client.unassign_data_source_from_external_data_provider(datasource_id, dataprovider_id)

            self.client.unassign_data_source_from_data_model(datasource_id, datamodel_id)

            self.client.unassign_external_dataset_from_data_source(extdataset_id, datasource_id)

            self.client.unassign_external_dataset_from_agent_instance(extdataset_id, dataagentinstance_id)

            #
            # test read
            #

            try:
                dp_obj = self.client.read_external_data_provider(dataprovider_id)
            except NotFound as ex:
                self.fail("existing data provicer was not found during read")
            else:
                pass

            try:
                dp_obj = self.client.read_data_source(datasource_id)
            except NotFound as ex:
                self.fail("existing data source was not found during read")
            else:
                pass

            try:
                dp_obj = self.client.read_external_data_source_model(datamodel_id)
            except NotFound as ex:
                self.fail("existing data model was not found during read")
            else:
                pass
            #
            # test delete
            #
            try:
                self.client.delete_external_data_provider(dataprovider_id)
                self.client.delete_data_source(datasource_id)
                self.client.delete_external_source_model_instance(datamodel_id)
                self.client.delete_external_dataset(extdataset_id)
                self.client.delete_external_data_agent_instance(dataagentinstance_id)
            except NotFound as ex:
                self.fail("existing data product was not found during delete")


            # test reading a non-existent data product
            print 'reading non-existent data product'
            try:
                bad_obj = self.client.read_external_data_provider('some_fake_id')
            except NotFound as ex:
                pass
            else:
                self.fail("non-existing data product was found during read: %s" %bad_obj)
