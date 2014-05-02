#!/usr/bin/env python

'''
@file ion/services/sa/instrument/test/test_int_data_acquisition_management_service.py
@author Maurice Manning
@test ion.services.sa.acquisition.DataAcquisitionManagementService integration test
'''

#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.public import RT, PRED
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin

from ion.services.sa.acquisition.data_acquisition_management_service import DataAcquisitionManagementService
from interface.services.sa.idata_acquisition_management_service import IDataAcquisitionManagementService, DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
import unittest
from ion.services.sa.process.test.test_int_data_process_management_service import global_range_test_document
from interface.objects import Attachment, AttachmentType, ReferenceAttachmentContext, DataProduct, InstrumentDevice, Parser
import numpy as np
from ion.util.stored_values import StoredValueManager

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
#@unittest.skip('not working')
class TestIntDataAcquisitionManagementService(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataAcquisitionManagementService
        self.client = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)

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
            delete_result = self.client.force_delete_data_source(ds_id)
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


    def test_register_instrument(self):
        # set up initial instrument to register
        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
        instrument_id, rev = self.rrclient.create(instrument_obj)

        self.base_register_instrument(instrument_id)

    def test_register_platform(self):
        # set up initial instrument to register
        platform_obj = IonObject(RT.PlatformDevice, name='Plat1',description='a platform that is creating the data product')
        platform_id, rev = self.rrclient.create(platform_obj)

    #@unittest.skip('Not done yet.')
    def base_register_instrument(self, instrument_id):
        # Register an instrument as a data producer in coordination with DM PubSub: create stream, register and create producer object

        dataproduct_obj = IonObject(RT.DataProduct, name='DataProduct1',description='sample data product')
        dataproduct_id, rev = self.rrclient.create(dataproduct_obj)

        # test registering a new data producer
        try:
            ds_id = self.client.register_instrument(instrument_id)
        except BadRequest as ex:
            self.fail("failed to create new data producer: %s" %ex)
        print 'new data producer id = ', ds_id


        # test assigning a data product to an instrument, creating the stream for the product
        try:
            self.client.assign_data_product(instrument_id, dataproduct_id)
            self.client.assign_data_product_source(dataproduct_id, instrument_id)
        except BadRequest as ex:
            self.fail("failed to assign data product to data producer: %s" %ex)
        except NotFound as ex:
            self.fail("failed to assign data product to data producer: %s" %ex)

        assocs = self.rrclient.find_associations(dataproduct_id, PRED.hasSource, instrument_id)
        if not assocs or len(assocs) == 0:
            self.fail("failed to assign data product to data producer")

        # test UNassigning a data product from instrument, deleting the stream for the product
        try:
            self.client.unassign_data_product(instrument_id, dataproduct_id)
            self.client.unassign_data_product_source(dataproduct_id, instrument_id)
        except BadRequest as ex:
            self.fail("failed to failed to UNassign data product to data producer data producer: %s" %ex)
        except NotFound as ex:
            self.fail("failed to failed to UNassign data product to data producer data producer: %s" %ex)

        assocs = self.rrclient.find_associations(dataproduct_id, PRED.hasSource, instrument_id)
        if  assocs:
            self.fail("failed to unassign data product to data producer")

        # test UNregistering a new data producer
        try:
            ds_id = self.client.unregister_instrument(instrument_id)
        except NotFound as ex:
            self.fail("failed to unregister instrument producer: %s" %ex)


    def test_register_external_data_set(self):
        # Register an external data set as a data producer in coordination with DM PubSub: create stream, register and create producer object


        # set up initial instrument to register
        ext_dataset_obj = IonObject(RT.ExternalDataset, name='DataSet1',description='an external data feed')
        ext_dataset_id, rev = self.rrclient.create(ext_dataset_obj)

        dataproduct_obj = IonObject(RT.DataProduct, name='DataProduct1',description='sample data product')
        dataproduct_id, rev = self.rrclient.create(dataproduct_obj)


        # test registering a new external data set
        try:
            ds_id = self.client.register_external_data_set(ext_dataset_id)
        except BadRequest as ex:
            self.fail("failed to create new data producer: %s" %ex)
        print 'new data producer id = ', ds_id

        # test assigning a data product to an ext_dataset_id, creating the stream for the product
        try:
            self.client.assign_data_product(ext_dataset_id, dataproduct_id)
        except BadRequest as ex:
            self.fail("failed to assign data product to data producer: %s" %ex)
        except NotFound as ex:
            self.fail("failed to assign data product to data producer: %s" %ex)

        # test UNassigning a data product from ext_dataset_id, deleting the stream for the product
        try:
            self.client.unassign_data_product(ext_dataset_id, dataproduct_id)
        except BadRequest as ex:
            self.fail("failed to failed to UNassign data product to data producer data producer: %s" %ex)
        except NotFound as ex:
            self.fail("failed to failed to UNassign data product to data producer data producer: %s" %ex)

        # test UNregistering a external data set
        try:
            ds_id = self.client.unregister_external_data_set(ext_dataset_id)
        except NotFound as ex:
            self.fail("failed to unregister instrument producer: %s" %ex)





    #@unittest.skip('not ready')
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
            datamodel_obj = IonObject(RT.DataSourceModel,
                               name='DataSourceModel1',
                               description='data source model')
            try:
                datamodel_id = self.client.create_data_source_model(datamodel_obj)
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
            # test creating a new dataset agent instance
            #
            print 'Creating new external data agent '
            datasetagent_obj = IonObject(RT.ExternalDatasetAgent,
                               name='ExternalDatasetAgent1',
                               description='external data agent ')
            try:
                datasetagent_id = self.client.create_external_dataset_agent(datasetagent_obj)
            except BadRequest as ex:
                self.fail("failed to create new external dataset agent: %s" %ex)
            print 'new external data agent  id = ', datasetagent_id


            #
            # test creating a new datasource agent instance
            #
            print 'Creating new  data source agent '
            datasourceagent_obj = IonObject(RT.DataSourceAgent,
                               name='DataSourceAgent1',
                               description=' DataSource agent ')
            try:
                datasource_agent_id = self.client.create_data_source_agent(datasourceagent_obj)
            except BadRequest as ex:
                self.fail("failed to create new external datasource agent: %s" %ex)
            print 'new external data agent  id = ', datasource_agent_id





            #
            # test creating a new dataset agent instance
            #
            print 'Creating new external dataset agent instance'
            datasetagentinstance_obj = IonObject(RT.ExternalDatasetAgentInstance,
                               name='ExternalDatasetAgentInstance1',
                               description='external dataset agent instance ')
            try:
                datasetagentinstance_id = self.client.create_external_dataset_agent_instance(datasetagentinstance_obj, datasetagent_id)
            except BadRequest as ex:
                self.fail("failed to create new external dataset agent instance: %s" %ex)
            print 'new external data agent instance id = ', datasetagentinstance_id

            #
            # test creating a new datasource agent instance
            #
            print 'Creating new  data source agent '
            datasourceagentinstance_obj = IonObject(RT.DataSourceAgentInstance,
                               name='ExternalDataSourceAgentInstance1',
                               description='external DataSource agent instance ')
            try:
                datasource_agent_instance_id = self.client.create_data_source_agent_instance(datasourceagentinstance_obj)
            except BadRequest as ex:
                self.fail("failed to create new external datasource agent instance: %s" %ex)
            print 'new external data agent  id = ', datasource_agent_instance_id

            #
            # test assign / unassign
            #

            self.client.unassign_data_source_from_external_data_provider(datasource_id, dataprovider_id)

            self.client.unassign_data_source_from_data_model(datasource_id, datamodel_id)

            self.client.unassign_external_dataset_from_data_source(extdataset_id, datasource_id)

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

            #
            # test delete
            #
            try:
                self.client.delete_external_data_provider(dataprovider_id)
                self.client.delete_data_source(datasource_id)
                self.client.delete_external_dataset(extdataset_id)
                self.client.delete_data_source_model(datamodel_id)
                self.client.delete_external_dataset_agent(datasetagent_id)
                self.client.delete_data_source_agent_instance(datasource_agent_instance_id)

                self.client.force_delete_external_data_provider(dataprovider_id)
                self.client.force_delete_data_source(datasource_id)
                self.client.force_delete_external_dataset(extdataset_id)
                self.client.force_delete_data_source_model(datamodel_id)
                self.client.force_delete_external_dataset_agent(datasetagent_id)
                self.client.force_delete_data_source_agent_instance(datasource_agent_instance_id)
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


    def make_grt_parser(self):
        return self.client.create_parser(Parser(name='grt', description='', module='ion.util.parsers.global_range_test', method='grt_parser', config=None))


    @unittest.skip("Deprecated")
    def test_qc_attachment(self):
        instrument_device = InstrumentDevice(name='whatever')
        instrument_device_id,_ = self.rrclient.create(instrument_device)
        self.addCleanup(self.rrclient.delete, instrument_device_id)
        self.client.register_instrument(instrument_device_id)
        self.addCleanup(self.client.unregister_instrument, instrument_device_id)
        dp = DataProduct(name='instrument output')

        dp_id,_ = self.rrclient.create(dp)
        self.addCleanup(self.rrclient.delete, dp_id)

        parser_id = self.make_grt_parser()
        attachment = Attachment(name='qc ref', attachment_type=AttachmentType.REFERENCE,content=global_range_test_document, context=ReferenceAttachmentContext(parser_id=parser_id))
        att_id = self.rrclient.create_attachment(dp_id, attachment)
        self.addCleanup(self.rrclient.delete_attachment, att_id)

        attachment2 = Attachment(name='qc ref2', attachment_type=AttachmentType.REFERENCE, content=global_range_test_document2, context=ReferenceAttachmentContext(parser_id=parser_id))
        att2_id = self.rrclient.create_attachment(dp_id, attachment2)
        self.addCleanup(self.rrclient.delete_attachment, att2_id)

        self.client.assign_data_product(instrument_device_id, dp_id)
        self.addCleanup(self.client.unassign_data_product, instrument_device_id, dp_id)
        svm = StoredValueManager(self.container)
        doc = svm.read_value('grt_CE01ISSM-MF005-01-CTDBPC999_TEMPWAT')
        np.testing.assert_array_almost_equal(doc['grt_min_value'], -2.)



global_range_test_document2 = '''Array,Instrument Class,Reference Designator,Data Products,Units,Data Product Flagged,Minimum Range (lim(1)),Maximum Range (lim(2))
CE,CTDBP,CE01ISSM-MF005-01-CTDBPC999,TEMPWAT,degrees C,,-2,40'''


