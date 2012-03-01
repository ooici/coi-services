from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.sa.product.data_product_management_service import DataProductManagementService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import IDataProductManagementService, DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from prototype.sci_data.ctd_stream import ctd_stream_definition
from interface.objects import HdfStorage, CouchStorage

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS, PRED
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
import time

from ion.services.sa.resource_impl.data_product_impl import DataProductImpl
from ion.services.sa.resource_impl.resource_impl_metatest import ResourceImplMetatest



class FakeProcess(LocalContextMixin):
    name = ''


@attr('UNIT', group='sa')
#@unittest.skip('not working')
class TestDataProductManagementServiceUnit(PyonTestCase):

    def setUp(self):
        self.clients = self._create_service_mock('data_product_management')

        self.data_product_management_service = DataProductManagementService()
        self.data_product_management_service.clients = self.clients

        # must call this manually
        self.data_product_management_service.on_init()

        self.data_source = Mock()
        self.data_source.name = 'data_source_name'
        self.data_source.description = 'data source desc'


    #@unittest.skip('not working')
    def test_createDataProduct_and_DataProducer_success(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.return_value = ('SOME_RR_ID1', 'Version_1')
        self.data_acquisition_management.assign_data_product.return_value = None

        # Data Product
        dpt_obj = IonObject(RT.DataProduct,
                            name='DPT_Y',
                            description='some new data product')

        # test call
        dp_id = self.data_product_management_service.create_data_product(dpt_obj, 'stream_def_id')

        # check results
        self.assertEqual(dp_id, 'SOME_RR_ID1')
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dpt_obj.name, True)
        self.pubsub_management.create_stream.assert_called_once_with('', True, 'stream_def_id', 'DPT_Y', 'some new data product', '')
        self.resource_registry.create.assert_called_once_with(dpt_obj)


    @unittest.skip('not working')
    def test_createDataProduct_and_DataProducer_with_id_NotFound(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.return_value = ('SOME_RR_ID1', 'Version_1')
        self.pubsub_management.create_stream.return_value = 'stream1'

        # Data Product
        dpt_obj = IonObject(RT.DataProduct, name='DPT_X', description='some new data product')

        # test call
        with self.assertRaises(NotFound) as cm:
            dp_id = self.data_product_management_service.create_data_product(dpt_obj, 'stream_def_id')

        # check results
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dpt_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dpt_obj)
        #todo: what are errors to check in create stream?


    def test_findDataProduct_success(self):
        # setup
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           name='DP_X',
                           description='some existing dp')
        self.resource_registry.find_resources.return_value = ([dp_obj], [])

        # test call
        result = self.data_product_management_service.find_data_products()

        # check results
        self.assertEqual(result, [dp_obj])
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, None, False)


@attr('INT', group='sa')
#@unittest.skip('not working')
class TestDataProductManagementServiceIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2sa.yml')

        print 'started services'

        # Now create client to DataProductManagementService
        self.client = DataProductManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)

    def test_createDataProduct(self):
        client = self.client
        rrclient = self.rrclient


        # ingestion configuration parameters
        self.exchange_point_id = 'science_data'
        self.number_of_workers = 2
        self.hdf_storage = HdfStorage(relative_path='ingest')
        self.couch_storage = CouchStorage(datastore_name='test_datastore')
        self.XP = 'science_data'
        self.exchange_name = 'ingestion_queue'

        # Create ingestion configuration and activate it
        ingestion_configuration_id =  self.ingestclient.create_ingestion_configuration(
            exchange_point_id=self.exchange_point_id,
            couch_storage=self.couch_storage,
            hdf_storage=self.hdf_storage,
            number_of_workers=self.number_of_workers
        )
        print 'test_createDataProduct: ingestion_configuration_id', ingestion_configuration_id

        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = ctd_stream_definition()
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(container=ctd_stream_def, name='Simulated CTD data')

        # test creating a new data product w/o a stream definition
        print 'Creating new data product w/o a stream definition'
        dp_obj = IonObject(RT.DataProduct,
                           name='DP1',
                           description='some new dp')
        try:
            dp_id = client.create_data_product(dp_obj, '')
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', dp_id


        # test creating a new data product with  a stream definition
        print 'Creating new data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,
                           name='DP2',
                           description='some new dp')
        try:
            dp_id2 = client.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', dp_id2

        # test activate and suspend data product persistence
        try:
            client.activate_data_product_persistence(dp_id2, persist_data=True, persist_metadata=True)
            time.sleep(3)
            client.suspend_data_product_persistence(dp_id2)
        except BadRequest as ex:
            self.fail("failed to activate / deactivate data product persistence : %s" %ex)


        # test creating a duplicate data product
        print 'Creating the same data product a second time (duplicate)'
        dp_obj.description = 'the first dp'
        try:
            dp_id = client.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            print ex
        else:
            self.fail("duplicate data product was created with the same name")


        # test reading a non-existent data product
        print 'reading non-existent data product'
        try:
            dp_obj = client.read_data_product('some_fake_id')
        except NotFound as ex:
            pass
        else:
            self.fail("non-existing data product was found during read: %s" %dp_obj)

        # update a data product (tests read also)
        print 'Updating data product'
        # first get the existing dp object
        try:
            dp_obj = client.read_data_product(dp_id)
        except NotFound as ex:
            self.fail("existing data product was not found during read")
        else:
            pass
            #print 'dp_obj = ', dp_obj
        # now tweak the object
        dp_obj.description = 'the very first dp'
        # now write the dp back to the registry
        try:
            update_result = client.update_data_product(dp_obj)
        except NotFound as ex:
            self.fail("existing data product was not found during update")
        except Conflict as ex:
            self.fail("revision conflict exception during data product update")

        # now get the dp back to see if it was updated
        try:
            dp_obj = client.read_data_product(dp_id)
        except NotFound as ex:
            self.fail("existing data product was not found during read")
        else:
            pass
            #print 'dp_obj = ', dp_obj
        self.assertTrue(dp_obj.description == 'the very first dp')

        # now 'delete' the data product
        print "deleting data product: ", dp_id
        try:
            client.delete_data_product(dp_id)
        except NotFound as ex:
            self.fail("existing data product was not found during delete")

        # now try to get the deleted dp object
        try:
            dp_obj = client.read_data_product(dp_id)
        except NotFound as ex:
            pass
        else:
            self.fail("deleted data product was found during read")

        # now try to delete the already deleted dp object
        print "deleting non-existing data product"
        try:
            client.delete_data_product(dp_id)
        except NotFound as ex:
            pass
        else:
            self.fail("non-existing data product was found during delete")

        # Shut down container
        #container.stop()

 
#dynamically add tests to the test classes. THIS MUST HAPPEN OUTSIDE THE CLASS

#unit
rim = ResourceImplMetatest(TestDataProductManagementServiceUnit, DataProductManagementService, log)
rim.add_resource_impl_unittests(DataProductImpl)

