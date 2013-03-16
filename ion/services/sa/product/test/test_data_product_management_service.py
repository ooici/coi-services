#from pyon.ion.endpoint import ProcessRPCClient
from ion.services.sa.test.helpers import UnitTestGenerator
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

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.coverage import GridDomain, GridShape, CRS
from coverage_model.basic_types import MutabilityEnum, AxisTypeEnum
from ion.util.parameter_yaml_IO import get_param_dict


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


    @unittest.skip('not working')
    def test_createDataProduct_and_DataProducer_success(self):
        # setup
        self.clients.resource_registry.find_resources.return_value = ([], 'do not care')
        self.clients.resource_registry.find_associations.return_value = []
        self.clients.resource_registry.create.return_value = ('SOME_RR_ID1', 'Version_1')
        self.clients.data_acquisition_management.assign_data_product.return_value = None
        self.clients.pubsub_management.create_stream.return_value = "stream_id"


        # Construct temporal and spatial Coordinate Reference System objects
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT])

        # Construct temporal and spatial Domain objects
        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE) # 1d (timeline)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # 1d spatial topology (station/trajectory)

        sdom = sdom.dump()
        tdom = tdom.dump()

        #@TODO: DO NOT DO THIS, WHEN THIS TEST IS REWRITTEN GET RID OF THIS, IT WILL FAIL, thanks -Luke
        parameter_dictionary = get_param_dict('ctd_parsed_param_dict')

        parameter_dictionary = parameter_dictionary.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        # test call
        dp_id = self.data_product_management_service.create_data_product(data_product=dp_obj,
                stream_definition_id='a stream def id')



    @unittest.skip('not working')
    def test_createDataProduct_and_DataProducer_with_id_NotFound(self):
        # setup
        self.clients.resource_registry.find_resources.return_value = ([], 'do not care')
        self.clients.resource_registry.create.return_value = ('SOME_RR_ID1', 'Version_1')
        self.clients.pubsub_management.create_stream.return_value = 'stream1'

        # Data Product
        dpt_obj = IonObject(RT.DataProduct, name='DPT_X', description='some new data product')

        # test call
        with self.assertRaises(NotFound) as cm:
            dp_id = self.data_product_management_service.create_data_product(dpt_obj, 'stream_def_id')

        # check results
        self.clients.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, dpt_obj.name, True)
        self.clients.resource_registry.create.assert_called_once_with(dpt_obj)
        #todo: what are errors to check in create stream?


    def test_findDataProduct_success(self):
        # setup
        # Data Product
        dp_obj = IonObject(RT.DataProduct,
                           name='DP_X',
                           description='some existing dp')
        self.clients.resource_registry.find_resources.return_value = ([dp_obj], [])

        # test call
        result = self.data_product_management_service.find_data_products()

        # check results
        self.assertEqual(result, [dp_obj])
        self.clients.resource_registry.find_resources.assert_called_once_with(RT.DataProduct, None, None, False)



utg = UnitTestGenerator(TestDataProductManagementServiceUnit,
                        DataProductManagementService)

#utg.test_all_in_one(True)

utg.add_resource_unittests(RT.DataProduct, "data_product", {})
utg.add_resource_unittests(RT.DataProductCollection, "data_product_collection", {})

#remove some tests that don't work
delattr(TestDataProductManagementServiceUnit, "test_data_product_create_d14a028")
delattr(TestDataProductManagementServiceUnit, "test_data_product_collection_create_d14a028")
delattr(TestDataProductManagementServiceUnit, "test_data_product_collection_create_bad_dupname_d14a028")
delattr(TestDataProductManagementServiceUnit, "test_data_product_collection_create_bad_noname_d14a028")
delattr(TestDataProductManagementServiceUnit, "test_data_product_collection_create_bad_wrongtype_d14a028")

