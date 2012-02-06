from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.sa.product.data_product_management_service import DataProductManagementService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.idata_product_management_service import IDataProductManagementService, DataProductManagementServiceClient


from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS, PRED
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest

from ion.services.sa.resource_impl.data_product_impl import DataProductImpl
from ion.services.sa.resource_impl.resource_impl_metatest import ResourceImplMetatest
from ion.services.sa.resource_impl.resource_impl_metatest_integration import ResourceImplMetatestIntegration



class FakeProcess(LocalContextMixin):
    name = ''


@attr('UNIT', group='sa')
#@unittest.skip('not working')
class Test_DataProductManagementService_Unit(PyonTestCase):

    def setUp(self):
        self.clients = self._create_service_mock('data_product_management')

        self.data_product_management_service = DataProductManagementService()
        self.data_product_management_service.clients = self.clients

        # must call this manually
        self.data_product_management_service.on_init()

        self.data_source = Mock()
        self.data_source.name = 'data_source_name'
        self.data_source.description = 'data source desc'


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
        dp_id = self.data_product_management_service.create_data_product(dpt_obj, 'source_resource_id')

        # check results
        self.assertEqual(dp_id, 'SOME_RR_ID1')
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dpt_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dpt_obj)
        self.data_acquisition_management.assign_data_product.assert_called_once_with('source_resource_id', 'SOME_RR_ID1')

    def test_createDataProduct_and_DataProducer_with_id_NotFound(self):
        # setup
        self.resource_registry.find_resources.return_value = ([], 'do not care')
        self.resource_registry.create.return_value = ('SOME_RR_ID1', 'Version_1')
        self.data_acquisition_management.assign_data_product.return_value = None
        self.data_acquisition_management.assign_data_product.side_effect = NotFound("Object with id SOME_RR_ID1 does not exist.")

        # Data Product
        dpt_obj = IonObject(RT.DataProduct,
                            name='DPT_X',
                            description='some new data product')

        # test call
        with self.assertRaises(NotFound) as cm:
            dp_id = self.data_product_management_service.create_data_product(dpt_obj, 'source_resource_id')

        # check results
        self.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dpt_obj.name, True)
        self.resource_registry.create.assert_called_once_with(dpt_obj)
        self.data_acquisition_management.assign_data_product.assert_called_once_with('source_resource_id', 'SOME_RR_ID1')
        ex = cm.exception
        self.assertEqual(ex.message, "Object with id SOME_RR_ID1 does not exist.")


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
class Test_DataProductManagementService_Integration(IonIntegrationTestCase):

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

    def test_createDataProduct(self):
        client = self.client
        rrclient = self.rrclient


        # set up initial data source and its associated data producer
        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
        instrument_id, rev = rrclient.create(instrument_obj)
        dataproducer_obj = IonObject(RT.DataProducer, name='InstDataProducer',description='an example data producer')
        dataproducer_id, rev = rrclient.create(dataproducer_obj)
        rrclient.create_association(instrument_id, PRED.hasDataProducer, dataproducer_id)

        # test creating a new data product w/o a data producer
        print 'Creating new data product w/o a data producer'
        dp_obj = IonObject(RT.DataProduct,
                           name='DP1',
                           description='some new dp')
        try:
            dp_id = client.create_data_product(dp_obj, instrument_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', dp_id

        # test creating a duplicate data product
        print 'Creating the same data product a second time (duplicate)'
        dp_obj.description = 'the first dp'
        try:
            dp_id = client.create_data_product(dp_obj, 'source_resource_id')
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

 
#dynamically add tests to the test classes. THIS MUST HAPPEN OUTSIDE THE CLASS

#unit
rim = ResourceImplMetatest(Test_DataProductManagementService_Unit, DataProductManagementService, log)
rim.add_resource_impl_unittests(DataProductImpl)


#integration
rimi = ResourceImplMetatestIntegration(Test_DataProductManagementService_Integration, DataProductManagementService, log)
rimi.add_resource_impl_inttests(DataProductImpl)
