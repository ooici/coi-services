#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import  log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.sa.product.data_product_management_service import DataProductManagementService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from prototype.sci_data.stream_defs import ctd_stream_definition, SBE37_CDM_stream_definition
from interface.objects import HdfStorage, CouchStorage, DataProduct, LastUpdate

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED
from mock import Mock
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
import time

from ion.services.sa.product.data_product_impl import DataProductImpl
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


 
#dynamically add tests to the test classes. THIS MUST HAPPEN OUTSIDE THE CLASS

#unit
rim = ResourceImplMetatest(TestDataProductManagementServiceUnit, DataProductManagementService, log)
rim.add_resource_impl_unittests(DataProductImpl)

