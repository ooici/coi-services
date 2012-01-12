from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.sa.product.data_product_management_service import DataProductManagementService
from interface.services.sa.idata_product_management_service import IDataProductManagementService, DataProductManagementServiceClient
from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, AT, LCS
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest

class FakeProcess(LocalContextMixin):
    name = ''


@attr('UNIT', group='sa')
class Test_DataProductManagementService_Unit(PyonTestCase):
    
    def setUp(self):
        self.clients = self._create_service_mock('data_product_management')

        self.data_product_management_service = DataProductManagementService()
        self.data_product_management_service.clients = self.clients

    def test_createDataProduct_success(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.return_value = ('SOME_RR_ID', 'Version_1')
        # Data Product
        dp_obj = IonObject(RT.DataProduct, 
                           name='DP_X', 
                           description='some new dp')
        
        # test call
        dp_id = self.data_product_management_service.create_data_product(dp_obj)
        
        # check results
        self.assertEqual(dp_id, 'SOME_RR_ID')
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dp_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dp_obj)

    def test_createDataProduct_and_DataProducer_success(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.return_value = ('SOME_RR_ID1', 'Version_1')
        self.data_acquisition_management.create_data_producer.return_value = ('SOME_RR_ID2')
        self.resource_registry.create_association.return_value = ('SOME_RR_ID3', 'Version_1')
        # Data Product
        dpt_obj = IonObject(RT.DataProduct, 
                            name='DPT_X', 
                            description='some new data product')
        # Data Producer
        dpr_obj = IonObject(RT.DataProducer, 
                            name='DP_X', 
                            description='some new data producer')
        
        # test call
        dp_id = self.data_product_management_service.create_data_product(dpt_obj, dpr_obj)
        
        # check results
        self.assertEqual(dp_id, 'SOME_RR_ID1')
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dpt_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dpt_obj)
        self.data_acquisition_management.create_data_producer.assert_called_once_with(dpr_obj)
        self.resource_registry.create_association.assert_called_once_with('SOME_RR_ID1',
                                                                          AT.hasDataProducer,
                                                                          'SOME_RR_ID2',
                                                                          None)

    def test_createDataProduct_and_DataProducer_stream_NotFound(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.return_value = ('SOME_RR_ID1', 'Version_1')
        self.data_acquisition_management.create_data_producer.side_effect = NotFound("Stream SOME_RR_ID2 does not exist")
        # Data Product
        dpt_obj = IonObject(RT.DataProduct, 
                            name='DPT_X', 
                            description='some new data product')
        # Data Producer
        dpr_obj = IonObject(RT.DataProducer, 
                            name='DP_X', 
                            description='some new data producer')
        
        # test call
        with self.assertRaises(NotFound) as cm:
            dp_id = self.data_product_management_service.create_data_product(dpt_obj, dpr_obj)
        
        # check results
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dpt_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dpt_obj)
        self.data_acquisition_management.create_data_producer.assert_called_once_with(dpr_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Stream SOME_RR_ID2 does not exist")

    def test_createDataProduct_and_DataProducer_with_id_BadRequest(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.return_value = ('SOME_RR_ID1', 'Version_1')
        self.data_acquisition_management.create_data_producer.side_effect = BadRequest("Create cannot create document with ID: ")
        # Data Product
        dpt_obj = IonObject(RT.DataProduct, 
                            name='DPT_X', 
                            description='some new data product')
        # Data Producer
        dpr_obj = IonObject(RT.DataProducer, 
                            name='DP_X', 
                            description='some new data producer')
        dpr_obj._id = "SOME_OTHER_RR_ID"
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            dp_id = self.data_product_management_service.create_data_product(dpt_obj, dpr_obj)
        
        # check results
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dpt_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dpt_obj)
        self.data_acquisition_management.create_data_producer.assert_called_once_with(dpr_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Create cannot create document with ID: ")

    def test_createDataProduct_and_DataProducer_with_rev_BadRequest(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.return_value = ('SOME_RR_ID1', 'Version_1')
        self.data_acquisition_management.create_data_producer.side_effect = BadRequest("Create cannot create document with Rev: ")
        # Data Product
        dpt_obj = IonObject(RT.DataProduct, 
                            name='DPT_X', 
                            description='some new data product')
        # Data Producer
        dpr_obj = IonObject(RT.DataProducer, 
                            name='DP_X', 
                            description='some new data producer')
        dpr_obj._rev = "SOME_REV"
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            dp_id = self.data_product_management_service.create_data_product(dpt_obj, dpr_obj)
        
        # check results
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dpt_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dpt_obj)
        self.data_acquisition_management.create_data_producer.assert_called_once_with(dpr_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Create cannot create document with Rev: ")

    def test_createDataProduct_already_exists_BadRequest(self):
        # setup
        self.resource_registry.find_resources.return_value = (['SOME_RR_ID'], 'Version_X')
        # Data Product
        dp_obj = IonObject(RT.DataProduct, 
                           name='DP_X', 
                           description='some new dp')
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            dp_id = self.data_product_management_service.create_data_product(dp_obj)
        
        # check results
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dp_obj.name, True)
        ex = cm.exception
        self.assertEqual(ex.message, "A data product named 'DP_X' already exists")
    
    def test_createDataProduct_with_id_BadRequest(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.side_effect = BadRequest("Create cannot create document with ID: ")
        # Data Product
        dp_obj = IonObject(RT.DataProduct, 
                           name='DP_X', 
                           description='some other new dp')
        dp_obj._id = "SOME_OTHER_RR_ID"
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            dp_id = self.data_product_management_service.create_data_product(dp_obj)
        
        # check results
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dp_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dp_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Create cannot create document with ID: ")
    
    def test_createDataProduct_with_rev_BadRequest(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.side_effect = BadRequest("Create cannot create document with Rev: ")
        # Data Product
        dp_obj = IonObject(RT.DataProduct, 
                           name='DP_X', 
                           description='some other new dp')
        dp_obj._rev = "SOME_REV"
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            dp_id = self.data_product_management_service.create_data_product(dp_obj)
        
        # check results
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dp_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dp_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Create cannot create document with Rev: ")
    
    def test_readDataProduct_success(self):
        # setup
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           _id='SOME_RR_ID1',
                           name='DP_X', 
                           description='some new dp')
        self.resource_registry.read.return_value = (dp_obj)
        
        # test call
        returned_dp_obj = self.data_product_management_service.read_data_product('SOME_RR_ID1')
        
        # check results
        self.assertEqual(returned_dp_obj, dp_obj)
        self.resource_registry.read.assert_called_once_with('SOME_RR_ID1', '')

    def test_readDataProduct_NotFound(self):
        # setup
        self.resource_registry.read.side_effect = NotFound("Object with id SOME_RR_ID1 does not exist.")
        
        # test call
        with self.assertRaises(NotFound) as cm:
            returned_dp_obj = self.data_product_management_service.read_data_product('SOME_RR_ID1')
        
        # check results
        self.resource_registry.read.assert_called_once_with('SOME_RR_ID1', '')
        ex = cm.exception
        self.assertEqual(ex.message, "Object with id SOME_RR_ID1 does not exist.")

    def test_updateDataProduct_success(self):
        # setup
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           name='DP_X', 
                           description='some existing dp')
        self.resource_registry.update.return_value = ('SOME_RR_ID1', 'Version_1')
        
        # test call
        result = self.data_product_management_service.update_data_product(dp_obj)
        
        # check results
        self.assertEqual(result, True)
        self.resource_registry.update.assert_called_once_with(dp_obj)

    def test_updateDataProduct_without_id_BadRequest(self):
        # setup
        self.resource_registry.update.side_effect = BadRequest("Update failed: Document has no ID: ")
        # Data Product
        dp_obj = IonObject(RT.DataProduct, 
                           name='DP_X', 
                           description='some existing dp')
        result = None
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            result = self.data_product_management_service.update_data_product(dp_obj)
        
        # check results
        self.resource_registry.update.assert_called_once_with(dp_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Update failed: Document has no ID: ")
        self.assertEqual(result, None)
    
    def test_updateDataProduct_without_rev_BadRequest(self):
        # setup
        self.resource_registry.update.side_effect = BadRequest("Update failed: Document has no Rev: ")
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           _id="SOME_RR_ID1",
                           name='DP_X', 
                           description='some existing dp')
        result = None
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            result = self.data_product_management_service.update_data_product(dp_obj)
        
        # check results
        self.resource_registry.update.assert_called_once_with(dp_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Update failed: Document has no Rev: ")
        self.assertEqual(result, None)
    
    def test_updateDataProduct_not_current_version_BadRequest(self):
        # setup
        self.resource_registry.update.side_effect = BadRequest("Object not based on most current version")
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           _id="SOME_RR_ID1",
                           _rev = "SOME_REV",
                           name='DP_X', 
                           description='some existing dp')
        result = None
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            result = self.data_product_management_service.update_data_product(dp_obj)
        
        # check results
        self.resource_registry.update.assert_called_once_with(dp_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Object not based on most current version")
        self.assertEqual(result, None)
    
    def test_updateDataProduct_setting_lcs_BadRequest(self):
        # setup
        self.resource_registry.update.side_effect = BadRequest("Cannot modify life cycle state in update!")
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           _id="SOME_RR_ID1",
                           _rev = "SOME_REV",
                           name='DP_X',
                           description='some existing dp')
        result = None
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            result = self.data_product_management_service.update_data_product(dp_obj)
        
        # check results
        self.resource_registry.update.assert_called_once_with(dp_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Cannot modify life cycle state in update!")
        self.assertEqual(result, None)
    
    def test_deleteDataProduct_success(self):
        # setup
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           _id="SOME_RR_ID1",
                           _rev = "SOME_REV",
                           name='DP_X', 
                           description='some existing dp')
        self.resource_registry.delete.return_value = (dp_obj)
        
        # test call
        result = self.data_product_management_service.delete_data_product(dp_obj)
        
        # check results
        self.assertEqual(result, True)
        self.resource_registry.delete.assert_called_once_with(dp_obj)

    def test_deleteDataProduct_NotFound(self):
        # setup
        self.resource_registry.delete.side_effect = BadRequest("Object with id SOME_RR_ID1 does not exist.")
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           _id="SOME_RR_ID1",
                           _rev = "SOME_REV",
                           name='DP_X',
                           description='some existing dp')
        result = None
        
        # test call
        with self.assertRaises(BadRequest) as cm:
            result = self.data_product_management_service.delete_data_product(dp_obj)
        
        # check results
        self.resource_registry.delete.assert_called_once_with(dp_obj)
        ex = cm.exception
        self.assertEqual(ex.message, "Object with id SOME_RR_ID1 does not exist.")
        self.assertEqual(result, None)
    
    def test_findDataProduct_success(self):
        # setup
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           _id="SOME_RR_ID1",
                           _rev = "SOME_REV",
                           name='DP_X',
                           description='some existing dp')
        self.resource_registry.find_resources.return_value = ([dp_obj], [])
        
        # test call
        result = self.data_product_management_service.find_data_products()
        
        # check results
        self.assertEqual(result, [dp_obj])
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, None, False)


@attr('INT', group='sa')
#@unittest.skip('coi/dm/sa services not working yet for integration tests to pass')
class Test_DataProductManagementService_Integration(IonIntegrationTestCase):

    def test_createDataProduct(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')
        
        print 'started services'

        # Now create client to DataProductManagementService
        client = DataProductManagementServiceClient(node=self.container.node)

        # test creating a new data product w/o a data producer
        print 'Creating new data product w/o a data producer'
        dp_obj = IonObject(RT.DataProduct, 
                           name='DP1', 
                           description='some new dp')
        try:
            dp_id = client.create_data_product(dp_obj)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', dp_id
        
        # test creating a duplicate data product
        print 'Creating the same data product a second time (duplicate)'
        dp_obj.description = 'the first dp'
        try:
            dp_id = client.create_data_product(dp_obj)
        except BadRequest as ex:
            print ex
        else:
            self.fail("duplicate data product was created with the same name")
        
        """
        # This is broken until the interceptor handles lists properly (w/o converting them to constants)
        # and DAMS works with pubsub_management.register_producer() correctly
        # test creating a new data product with a data producer
        print 'Creating new data product with a data producer'
        dp_obj = IonObject(RT.DataProduct, 
                           name='DP2', 
                           description='another new dp')
        data_producer_obj = IonObject(RT.DataProducer, 
                                      name='DataProducer1', 
                                      description='a new data producer')
        try:
            dp_id = client.create_data_product(dp_obj, data_producer_obj)
        except BadRequest as ex:
            self.fail("failed to create new data product")
        print 'new dp_id = ', dp_id
        """
        
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
        else:
            self.assertTrue(update_result == True)           
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
        print "deleting data product"
        try:
            delete_result = client.delete_data_product(dp_id)
        except NotFound as ex:
            self.fail("existing data product was not found during delete")
        self.assertTrue(delete_result == True)
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
            delete_result = client.delete_data_product(dp_id)
        except NotFound as ex:
            pass
        else:
            self.fail("non-existing data product was found during delete")

        # Shut down container
        #container.stop()
