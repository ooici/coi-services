#!/usr/bin/env python

"""Integration and Unit tests for Discovery Service"""

__author__ = 'Luke Campbell <LCampbell@ASAScience.com>, Michael Meisinger'

from unittest.case import skipIf, skip, SkipTest
import dateutil.parser
import time
import calendar
from nose.plugins.attrib import attr
from mock import Mock, patch, sentinel
from datetime import date, timedelta

from pyon.util.unit_test import IonUnitTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import PRED, CFG, RT, OT, LCS, BadRequest, NotFound, IonObject, DotDict, ResourceQuery, EventQuery, log

from ion.services.dm.presentation.discovery_service import DiscoveryService
from ion.util.geo_utils import GeoUtils
from ion.util.testing_utils import create_dummy_resources, create_dummy_events

from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.dm.iindex_management_service import IndexManagementServiceClient
from interface.services.dm.icatalog_management_service import CatalogManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from interface.objects import InstrumentDevice, Site, PlatformDevice, BankAccount, DataProduct, Transform, ProcessDefinition, \
    DataProcess, UserInfo, ContactInformation, Dataset, GeospatialIndex, GeospatialBounds, TemporalBounds, View, CustomAttribute


@attr('UNIT', group='dm')
class DiscoveryUnitTest(IonUnitTestCase):
    def setUp(self):
        super(DiscoveryUnitTest, self).setUp()
        mock_clients = self._create_service_mock('discovery')
        self.discovery = DiscoveryService()
        self.discovery.on_start()
        self.discovery.clients = mock_clients
        self.ds_mock = Mock()
        self.discovery.ds_discovery._get_datastore = Mock(return_value=self.ds_mock)
        container_mock = Mock()
        self.discovery.ds_discovery.container = container_mock
        container_mock.resource_registry = Mock()
        container_mock.resource_registry.get_superuser_actors = Mock(return_value={})

        self.rr_create = mock_clients.resource_registry.create
        self.rr_read = mock_clients.resource_registry.read
        self.rr_update = mock_clients.resource_registry.update
        self.rr_delete = mock_clients.resource_registry.delete
        self.rr_find_assoc = mock_clients.resource_registry.find_associations
        self.rr_find_res = mock_clients.resource_registry.find_resources
        self.rr_find_obj = mock_clients.resource_registry.find_objects
        self.rr_find_assocs_mult = mock_clients.resource_registry.find_objects_mult
        self.rr_create_assoc = mock_clients.resource_registry.create_association
        self.rr_delete_assoc = mock_clients.resource_registry.delete_association

        self.cms_create = mock_clients.catalog_management.create_catalog
        self.cms_list_indexes = mock_clients.catalog_management.list_indexes

    # --- Unit test View management

    def test_create_view(self):
        view_obj = View(name='fake_resource')
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.rr_find_res.return_value = ([], [])
        retval = self.discovery.create_view(view_obj)
        self.assertEquals(retval, 'res_id')
        self.rr_create.assert_called_once_with(view_obj)

    def create_catalog_view(self):
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.cms_create.return_value = 'catalog_id'
        retval = self.discovery.create_catalog_view('mock_view', fields=['name'])
        self.assertTrue(retval=='res_id', 'Improper resource creation')

    def test_create_catalog_view_exists(self):
        self.rr_find_res.return_value = ([1], [1])
        with self.assertRaises(BadRequest):
            self.discovery.create_catalog_view('doesnt matter', fields=['name'])

    def test_create_catalog_view_no_fields(self):
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.cms_create.return_value = 'catalog_id'
        with self.assertRaises(BadRequest):
            self.discovery.create_catalog_view('mock_view')

    def test_create_catalog_view_order(self):
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.cms_create.return_value = 'catalog_id'
        with self.assertRaises(BadRequest):
            self.discovery.create_catalog_view('movk_view', fields=['name'], order=['age'])

    def test_read_view(self):
        self.rr_read.return_value = View(name='fake_resource')
        retval = self.discovery.read_view('mock_view_id')
        self.assertEquals(retval.name, 'fake_resource')

    def test_update_view(self):
        view_obj = View(name='fake_resource')
        retval = self.discovery.update_view(view_obj)
        self.assertTrue(retval)
        self.rr_update.assert_called_once_with(view_obj)

    def test_delete_view(self):
        retval = self.discovery.delete_view('view_id')
        self.assertTrue(retval)
        self.rr_delete.assert_called_once_with('view_id')

    # --- Unit test queries

    def test_bad_query(self):
        query = DotDict()
        query.unknown = 'yup'

        with self.assertRaises(BadRequest):
            self.discovery.query(query)
    
    def test_bad_requests(self):
        #================================
        # Battery of broken requests
        #================================
        self.discovery.ds_discovery = Mock()
        bad_requests = [
            {},
            {'field':"f"},
            {'and':[]},
            {'or':[]},
        ]
        for req in bad_requests:
            with self.assertRaises(BadRequest):
                self.discovery._discovery_request(req)

    @patch('ion.services.dm.presentation.discovery_service.QueryLanguage')
    def test_parse_mock(self, mock_parser):
        mock_parser().parse.return_value = 'arg'
        self.discovery._discovery_request = Mock()
        self.discovery._discovery_request.return_value = 'correct_value'
        retval = self.discovery.parse('blah blah', id_only=sentinel.id_only)
        self.discovery._discovery_request.assert_called_once_with('arg', search_args=None, query_params=None, id_only=sentinel.id_only)
        self.assertTrue(retval=='correct_value', '%s' % retval)

    def test_parse(self):
        self.ds_mock.find_by_query = Mock(return_value=["FOO"])

        search_string = "search 'serial_number' is 'abc' from 'resources_index'"
        retval = self.discovery.parse(search_string)
        self.discovery.ds_discovery._get_datastore.assert_called_once_with("resources")
        self.assertEquals(retval, ["FOO"])


    def test_tier1_request(self):
        self.ds_mock.find_by_query = Mock(return_value=["FOO"])

        query = {'query':{'field': 'name', 'value': 'foo'}}
        retval = self.discovery._discovery_request(query)
        self.discovery.ds_discovery._get_datastore.assert_called_once_with("resources")
        self.assertEquals(retval, ["FOO"])

    def test_tier2_request(self):
        self.ds_mock.find_by_query = Mock(return_value=["FOO"])

        query = {'query':{'field': 'name', 'value': 'foo'}, 'and':[{'field': 'lcstate', 'value': 'foo2'}]}
        retval = self.discovery._discovery_request(query)
        self.discovery.ds_discovery._get_datastore.assert_called_once_with("resources")
        self.assertEquals(retval, ["FOO"])

        query = {'query':{'field': 'name', 'value': 'foo'}, 'or':[{'field': 'lcstate', 'value': 'foo2'}]}
        retval = self.discovery._discovery_request(query)
        self.assertEquals(retval, ["FOO"])


@attr('INT', group='dm')
class DiscoveryQueryTest(IonIntegrationTestCase):
    """Tests discovery in a somewhat integration environment. Only a container and a DiscoveryService instance
    but no r2deploy and process"""

    def setUp(self):
        self._start_container()

        self.discovery = DiscoveryService()
        self.discovery.container = self.container
        self.discovery.on_start()

        self.rr = self.container.resource_registry

    def _geopt(self, x1, y1):
        return GeospatialIndex(lat=float(x1), lon=float(y1))

    def _geobb(self, x1, y1, x2=None, y2=None, z1=0.0, z2=None):
        if x2 is None: x2 = x1
        if y2 is None: y2 = y1
        if z2 is None: z2 = z1
        return GeospatialBounds(geospatial_latitude_limit_north=float(y2),
                                geospatial_latitude_limit_south=float(y1),
                                geospatial_longitude_limit_west=float(x1),
                                geospatial_longitude_limit_east=float(x2),
                                geospatial_vertical_min=float(z1),
                                geospatial_vertical_max=float(z2))

    def _temprng(self, t1="", t2=None):
        if t2 is None: t2 = t1
        return TemporalBounds(start_datetime=str(t1), end_datetime=str(t2))


    def _geodp(self, x1, y1, x2=None, y2=None, z1=0.0, z2=None, t1="", t2=None):
        if x2 is None: x2 = x1
        if y2 is None: y2 = y1
        if z2 is None: z2 = z1
        if t2 is None: t2 = t1
        bounds = self._geobb(x1, y1, x2, y2, z1, z2)
        attrs = dict(geospatial_point_center=GeoUtils.calc_geospatial_point_center(bounds),
                     geospatial_bounds=bounds,
                     nominal_datetime=self._temprng(t1, t2))
        return attrs


    def test_basic_searching(self):
        t0 = 1363046400
        hour = 60*24
        day = 60*60*24
        resources = [
            ("ID1", InstrumentDevice(name='sonobuoy1', firmware_version='A1')),
            ("ID2", InstrumentDevice(name='sonobuoy2', firmware_version='A2')),
            ("ID3", InstrumentDevice(name='sonobuoy3', firmware_version='A3')),

            ("DP1", DataProduct(name='testData1', **self._geodp(5, 5, 15, 15, 0, 100, t0, t0+day))),
            ("DP2", DataProduct(name='testData2', **self._geodp(25, 5, 35, 15, 0, 100, t0+hour+day, t0+2*day))),
            ("DP3", DataProduct(name='testData3', **self._geodp(30, 10, 40, 20, 50, 200, t0+100*day, t0+110*day))),
            ("DP4", DataProduct(name='testData4', **self._geodp(30, 5, 32, 10, 5, 20, t0+100*day, t0+110*day))),
        ]
        # create a large range of resources to test skip(offset)
        for i in range(200):
            resources.append(("INS%03d" % i, InstrumentDevice(name='range%03d' % i)))
        res_by_alias = {}
        for (alias, resource) in resources:
            rid,_ = self.rr.create(resource)
            res_by_alias[alias] = rid

        # ----------------------------------------------------
        # Resource attribute search

        # Resource attribute equals
        # Variant 1: Test via query DSL
        search_string = "search 'firmware_version' is 'A2' from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)
        self.assertIsInstance(result[0], InstrumentDevice)
        self.assertTrue(result[0].name == 'sonobuoy2')
        self.assertTrue(result[0].firmware_version == 'A2')

        # Variant 2: Test via query DSL expression
        query_str = "{'and': [], 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A2'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Variant 3: Test the query expression
        query_str = """{'QUERYEXP': 'qexp_v1.0',
            'query_args': {'datastore': 'resources', 'id_only': False, 'limit': 0, 'profile': 'RESOURCES', 'skip': 0},
            'where': ['xop:attilike', ('firmware_version', 'A2')],
            'order_by': {}}"""
        query_obj = eval(query_str)
        result2 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result2))
        self.assertEquals(result, result2)

        search_string = "search 'firmware_version' is 'A2' from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=False, search_args=dict(attribute_filter=["firmware_version"]))
        self.assertTrue(all(isinstance(eo, dict) for eo in result))
        self.assertTrue(all("firmware_version" in eo for eo in result))
        self.assertTrue(all(len(eo) <= 4 for eo in result))

        result = self.discovery.query(query_obj, id_only=False, search_args=dict(attribute_filter=["firmware_version"]))
        self.assertTrue(all(isinstance(eo, dict) for eo in result))
        self.assertTrue(all("firmware_version" in eo for eo in result))
        self.assertTrue(all(len(eo) <= 4 for eo in result))

        # Resource attribute match
        search_string = "search 'firmware_version' is 'A*' from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 3)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A*'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Resource attribute match with limit
        search_string = "search 'firmware_version' is 'A*' from 'resources_index' limit 2"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 2)

        query_str = "{'and': [], 'limit': 2, 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A*'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Resource attribute match with limit and skip (offset)
        # TODO these tests are unlikely to always work until order_by is implemented (1/200 chance fails)
        
        # -- limit 1 without skip (using QueryLanguage)
        search_string = "search 'name' is 'range*' from 'resources_index' limit 1"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)
        # -- limit 1 with skip (using QueryLanguage)
        search_string = "search 'name' is 'range*' from 'resources_index' SKIP 100 limit 1"
        result1 = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result1), 1)
        # check same length and not equal (one uses SKIP 100, other doesn't)
        self.assertEquals(len(result), len(result1))
        self.assertNotEquals(result, result1)

        # -- limit 1 without skip (using Discovery Intermediate Format)
        query_str = "{'and': [], 'limit': 1, 'or': [], 'query': {'field': 'name', 'index': 'resources_index', 'value': 'range*'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        # -- limit 1 with skip (using Discovery Intermediate Format)
        query_str = "{'and': [], 'limit': 1, 'skip': 100, 'or': [], 'query': {'field': 'name', 'index': 'resources_index', 'value': 'range*'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        # check same length and not equal (one uses SKIP 100, other doesn't)
        self.assertEquals(len(result), len(result1))
        self.assertNotEquals(result, result1)

        # Resource attribute match only count (results should return single value, a count of available results)
        search_args_str = "{'count': True}"
        search_args = eval(search_args_str)
        search_string = "search 'firmware_version' is 'A*' from 'resources_index' limit 2"
        result = self.discovery.parse(search_string, id_only=False, search_args=search_args)
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'limit': 2, 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A*'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False, search_args=search_args)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Check data products
        search_string = "search 'name' is 'testData*' from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 4)

        search_string = "search 'type_' is 'DataProduct' from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 4)

        # ----------------------------------------------------
        # Geospatial search

        # Geospatial search - query bbox fully overlaps
        search_string = "search 'geospatial_point_center' geo box top-left lat 180 lon -180 bottom-right lat -180 lon 180 from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=True)
        self.assertGreaterEqual(len(result), 4)
        for dp in ["DP1", "DP2", "DP3", "DP4"]:
            self.assertIn(res_by_alias[dp], result)

        search_string = "search 'geospatial_point_center' geo box top-left lat 20 lon 0 bottom-right lat 0 lon 20 from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)

        # Note that in Discovery intermediate format top_left=x1,y2 and bottom_right=x2,y1 contrary to naming
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'index_location', 'index': 'resources_index'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Geospatial bbox operators - overlaps (this is the default and should be the same as above)
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result2 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result1), len(result2))
        self.assertEquals(result1, result2)

        # Geospatial bbox operators - contains (the resource contains the query)
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 0)

        query_str = "{'and': [], 'or': [], 'query': {'top_left': [8.0, 11.0], 'bottom_right': [12.0, 9.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 1)

        # Geospatial bbox operators - within (the resource with the query)
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 1)

        query_str = "{'and': [], 'or': [], 'query': {'top_left': [15.0, 5.0], 'bottom_right': [5.0, 15.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 1)

        query_str = "{'and': [], 'or': [], 'query': {'top_left': [14.0, 5.0], 'bottom_right': [5.0, 15.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result3 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 0)

        # Geospatial search - query bbox partial overlaps
        search_string = "search 'geospatial_bounds' geo box top-left lat 11 lon 9 bottom-right lat 9 lon 11 from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)


        # Geospatial - WKT (a box 4,4 to 4,14 to 14,14 to 14,4, to 4,4 overlaps DP1 but is not contained by it or does not have it within)
        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POLYGON((4 4,4 14,14 14,14 4,4 4))', 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POLYGON((4 4,4 14,14 14,14 4,4 4))', 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 0)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POLYGON((4 4,4 14,14 14,14 4,4 4))', 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 0)

        # -- with buffer (eg. point with radius CIRCLE)
        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': 1.0, 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': 1.0, 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 0)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': '15000m', 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': '15000m', 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 0)


        # ----------------------------------------------------
        # Vertical search

        search_string = "search 'geospatial_bounds' vertical from 0 to 500 from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=True)
        self.assertGreaterEqual(len(result), 4)
        for dp in ["DP1", "DP2", "DP3", "DP4"]:
            self.assertIn(res_by_alias[dp], result)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 0.0, 'to': 500.0}, 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result1), 4)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 1.0, 'to': 2.0}, 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result1), 2)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 110.0, 'to': 120.0}, 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=True)
        self.assertEquals(len(result1), 1)
        self.assertEquals(res_by_alias["DP3"], result1[0])

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 5.0, 'to': 30.0}, 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=True)
        self.assertEquals(len(result1), 1)
        self.assertEquals(res_by_alias["DP4"], result1[0])

        # ----------------------------------------------------
        # Temporal search

        search_string = "search 'nominal_datetime' timebounds from '%s' to '%s' from 'resources_index'" %('2013-03-12','2013-03-19')
        result = self.discovery.parse(search_string, id_only=True)
        self.assertEquals(len(result), 2)
        for dp in ["DP1", "DP2"]:
            self.assertIn(res_by_alias[dp], result)

        search_string = "search 'nominal_datetime' timebounds from '%s' to '%s' from 'resources_index'" %('2013-03-12','2013-11-19')
        result = self.discovery.parse(search_string, id_only=True)
        self.assertEquals(len(result), 4)
        for dp in ["DP1", "DP2", "DP3", "DP4"]:
            self.assertIn(res_by_alias[dp], result)

        search_string = "search 'nominal_datetime' timebounds from '%s' to '%s' from 'resources_index'" %('2013-03-12','2013-03-13')
        result = self.discovery.parse(search_string, id_only=True)
        self.assertEquals(len(result), 1)
        for dp in ["DP1"]:
            self.assertIn(res_by_alias[dp], result)

    def test_event_search(self):
        from interface.objects import ResourceOperatorEvent, ResourceCommandEvent
        t0 = 136304640000

        events = [
            ("RME1", ResourceCommandEvent(origin="O1", origin_type="OT1", sub_type="ST1", ts_created=str(t0))),
            ("RME2", ResourceCommandEvent(origin="O2", origin_type="OT1", sub_type="ST2", ts_created=str(t0+1))),
            ("RME3", ResourceCommandEvent(origin="O2", origin_type="OT2", sub_type="ST3", ts_created=str(t0+2))),

            ("RLE1", ResourceOperatorEvent(origin="O1", origin_type="OT3", sub_type="ST4", ts_created=str(t0+3))),
            ("RLE2", ResourceOperatorEvent(origin="O3", origin_type="OT3", sub_type="ST5", ts_created=str(t0+4))),
            ("RLE3", ResourceOperatorEvent(origin="O3", origin_type="OT2", sub_type="ST6", ts_created=str(t0+5))),

        ]
        ev_by_alias = {}
        for (alias, event) in events:
            evid, _ = self.container.event_repository.put_event(event)
            ev_by_alias[alias] = evid

        # ----------------------------------------------------

        search_string = "search 'origin' is 'O1' from 'events_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 2)

        search_string = "search 'origin_type' is 'OT2' from 'events_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 2)

        search_string = "search 'sub_type' is 'ST6' from 'events_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)

        search_string = "search 'ts_created' values from 136304640000 to 136304640000 from 'events_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)

        search_string = "search 'type_' is 'ResourceCommandEvent' from 'events_index' order by 'ts_created'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 3)

        #from pyon.util.breakpoint import breakpoint
        #breakpoint()

    def test_query_view(self):
        res_objs = [
            (IonObject(RT.ActorIdentity, name="Act1"), ),
            (IonObject(RT.ActorIdentity, name="Act2"), ),

            (IonObject(RT.InstrumentDevice, name="ID1", lcstate=LCS.DEPLOYED, firmware_version='A1'), "Act1"),
            (IonObject(RT.InstrumentDevice, name="ID2", lcstate=LCS.INTEGRATED, firmware_version='A2'), "Act2"),

            (IonObject(RT.PlatformDevice, name="PD1"), ),
            (IonObject(RT.PlatformDevice, name="PD2"), ),

            (IonObject(RT.Stream, name="Stream1"), ),
        ]
        assocs = [
            ("PD1", PRED.hasDevice, "ID1"),
            ("PD2", PRED.hasDevice, "ID2"),

        ]
        res_by_name = create_dummy_resources(res_objs, assocs)

        # ----------------------------------------------------
        # Resource attribute search

        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.InstrumentDevice))
        view_obj = View(name="All InstrumentDevice resources", view_definition=rq.get_query())
        view_id = self.discovery.create_view(view_obj)

        # TEST: View by ID
        result = self.discovery.query_view(view_id, id_only=False)
        self.assertEquals(len(result), 2)
        self.assertTrue(all(True for ro in result if ro.type_ == RT.InstrumentDevice))

        # TEST: View by Name
        result = self.discovery.query_view(view_name="All InstrumentDevice resources", id_only=False)
        self.assertEquals(len(result), 2)
        self.assertTrue(all(True for ro in result if ro.type_ == RT.InstrumentDevice))

        # TEST: View plus ext_query
        rq = ResourceQuery()
        rq.set_filter(rq.filter_name("ID1"))
        result = self.discovery.query_view(view_id, id_only=False, ext_query=rq.get_query())
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID1")

        # TEST: View with params (anonymous)
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.InstrumentDevice),
                      rq.filter_attribute("firmware_version", "$(firmware_version)"))
        view_obj = View(name="InstrumentDevice resources with a specific firmware - parameterized",
                        view_definition=rq.get_query())
        view_id = self.discovery.create_view(view_obj)

        view_params = {"firmware_version": "A2"}
        result = self.discovery.query_view(view_id, id_only=False, search_args=view_params)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID2")

        # TEST: View with params (anonymous) - no values provided
        result = self.discovery.query_view(view_id, id_only=False)
        self.assertEquals(len(result), 0)

        # View with params (with definitions and defaults)
        view_param_def = [CustomAttribute(name="firmware_version",
                                          type="str",
                                          default="A1")]
        view_obj = View(name="InstrumentDevice resources with a specific firmware - parameterized with defaults",
                        view_definition=rq.get_query(),
                        view_parameters=view_param_def)
        view_id = self.discovery.create_view(view_obj)

        # TEST: Definition defaults
        result = self.discovery.query_view(view_id, id_only=False)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID1")

        # TEST: Parameterized values
        result = self.discovery.query_view(view_id, id_only=False, search_args=view_params)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID2")

        # TEST: Parameterized association query for resource owner
        rq = ResourceQuery()
        rq.set_filter(rq.filter_associated_with_subject("$(owner)"))
        view_obj = View(name="Resources owned by actor - parameterized", view_definition=rq.get_query())
        view_id = self.discovery.create_view(view_obj)
        view_params = {"owner": res_by_name["Act2"]}
        result = self.discovery.query_view(view_id, id_only=False, search_args=view_params)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID2")

        # TEST: Parameterized association query for resource owner with parameter value
        view_params = {"owner": res_by_name["Act2"], "query_info": True}
        result = self.discovery.query_view(view_id, id_only=False, search_args=view_params)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0].name, "ID2")
        self.assertIn("_query_info", result[1])
        self.assertIn("statement_sql", result[1])

        # TEST: Builtin views
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.Stream))
        result = self.discovery.query_view(view_name="resources_index", id_only=False, ext_query=rq.get_query())
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "Stream1")

        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.Stream))
        result = self.discovery.query_view(view_name="data_products_index", id_only=False, ext_query=rq.get_query())
        self.assertEquals(len(result), 0)


        # --- Events setup

        from interface.objects import ResourceOperatorEvent, ResourceCommandEvent
        t0 = 136304640000
        events = [
            ("RME1", ResourceCommandEvent(origin="O1", origin_type="OT1", sub_type="ST1", ts_created=str(t0))),
            ("RME2", ResourceCommandEvent(origin="O2", origin_type="OT1", sub_type="ST2", ts_created=str(t0+1))),
            ("RME3", ResourceCommandEvent(origin="O2", origin_type="OT2", sub_type="ST3", ts_created=str(t0+2))),

            ("RLE1", ResourceOperatorEvent(origin="O1", origin_type="OT3", sub_type="ST4", ts_created=str(t0+3))),
            ("RLE2", ResourceOperatorEvent(origin="O3", origin_type="OT3", sub_type="ST5", ts_created=str(t0+4))),
            ("RLE3", ResourceOperatorEvent(origin="O3", origin_type="OT2", sub_type="ST6", ts_created=str(t0+5))),

        ]
        ev_by_alias = create_dummy_events(events)

        # TEST: Event query with views
        eq = EventQuery()
        eq.set_filter(eq.filter_type(OT.ResourceCommandEvent))
        view_obj = View(name="All ResourceCommandEvent events", view_definition=eq.get_query())
        view_id = self.discovery.create_view(view_obj)
        result = self.discovery.query_view(view_id, id_only=False)
        self.assertEquals(len(result), 3)
        self.assertTrue(all(True for eo in result if eo.type_ == OT.ResourceCommandEvent))

        # TEST: Event query with views - stripped format
        result = self.discovery.query_view(view_id, id_only=False, search_args=dict(attribute_filter=["origin"]))
        self.assertEquals(len(result), 3)
        self.assertTrue(all(True for eo in result if isinstance(eo, dict)))
        self.assertTrue(all(True for eo in result if "origin" in eo))
        self.assertTrue(all(True for eo in result if len(eo) <= 4))

        # TEST: Builtin views
        eq = EventQuery()
        eq.set_filter(eq.filter_type(OT.ResourceCommandEvent))
        result = self.discovery.query_view(view_name="events_index", id_only=False, ext_query=eq.get_query())
        self.assertEquals(len(result), 3)

    def test_complex_queries(self):
        res_objs = [
            dict(res=IonObject(RT.ActorIdentity, name="Act1")),
            dict(res=IonObject(RT.ActorIdentity, name="Act2")),

            dict(res=IonObject(RT.Org, name="Org1"), act="Act1"),
            dict(res=IonObject(RT.Observatory, name="Obs1"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.PlatformSite, name="PS1"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.PlatformSite, name="PSC1"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.InstrumentSite, name="IS1"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.PlatformModel, name="PM1", manufacturer="CGSN"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.PlatformModel, name="PMC1", manufacturer="Bluefin"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.PlatformModel, name="PM2", manufacturer="Webb"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.InstrumentModel, name="IM1", manufacturer="SeaBird"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.InstrumentModel, name="IM2", manufacturer="Teledyne"), act="Act1", org="Org1"),
            dict(res=IonObject(RT.PlatformDevice, name="PD1", lcstate=LCS.DEPLOYED), act="Act1", org="Org1"),
            dict(res=IonObject(RT.PlatformDevice, name="PDC1", lcstate=LCS.INTEGRATED), act="Act1", org="Org1"),
            dict(res=IonObject(RT.InstrumentDevice, name="ID1", lcstate=LCS.DEPLOYED, firmware_version='A1'), act="Act1", org="Org1"),
            dict(res=IonObject(RT.InstrumentDevice, name="ID2", lcstate=LCS.INTEGRATED, firmware_version='A2'), act="Act1", org="Org1"),

            dict(res=IonObject(RT.Org, name="Org2"), act="Act2"),
            dict(res=IonObject(RT.Observatory, name="Obs2"), act="Act2", org="Org2"),
            dict(res=IonObject(RT.PlatformSite, name="PS2"), act="Act2", org="Org2"),
            dict(res=IonObject(RT.PlatformDevice, name="PD2"), act="Act2", org="Org2"),
            dict(res=IonObject(RT.InstrumentDevice, name="ID3", lcstate=LCS.DEPLOYED, firmware_version='A3'), act="Act2", org="Org2"),
            dict(res=IonObject(RT.Stream, name="Stream1")),
        ]
        assocs = [
            ("Obs1", PRED.hasSite, "PS1"),
            ("PS1", PRED.hasSite, "PSC1"),
            ("PSC1", PRED.hasSite, "IS1"),
            ("PS1", PRED.hasDevice, "PD1"),
            ("PSC1", PRED.hasDevice, "PDC1"),
            ("IS1", PRED.hasDevice, "ID1"),
            ("PD1", PRED.hasDevice, "PDC1"),
            ("PDC1", PRED.hasDevice, "ID1"),

            ("PS1", PRED.hasModel, "PM1"),
            ("PSC1", PRED.hasModel, "PMC1"),
            ("IS1", PRED.hasModel, "IM1"),
            ("PD1", PRED.hasModel, "PM1"),
            ("PDC1", PRED.hasModel, "PMC1"),
            ("ID1", PRED.hasModel, "IM1"),
            ("PD2", PRED.hasModel, "PM2"),
            ("ID2", PRED.hasModel, "IM2"),

        ]
        res_by_name = create_dummy_resources(res_objs, assocs)

        log.info("TEST: Query for all resources owned by actor Act1")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_associated_with_subject(res_by_name["Act1"], None, "hasOwner"))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 14)

        log.info("TEST: Query for all Site descendants of Observatory Obs1")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_object_descendants(res_by_name["Obs1"], [RT.PlatformSite, RT.InstrumentSite], PRED.hasSite))
        result = self.discovery.query(rq.get_query(), id_only=False)
        # import pprint
        # pprint.pprint(rq.get_query())
        self.assertEquals(len(result), 3)

        log.info("TEST: Query for all resources belonging to Org Org1")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_associated_with_object(res_by_name["Org1"], None, "hasResource"))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 13)

        log.info("TEST: Query for all resources belonging to Org Org1 AND of type InstrumentDevice")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_associated_with_object(res_by_name["Org1"], None, "hasResource"),
                      rq.filter_type(RT.InstrumentDevice))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 2)

        log.info("TEST: Query for instruments whose platform parent has a name of PDC1")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.InstrumentDevice),
                      rq.filter_associated_with_object(subject_type=RT.PlatformDevice, predicate=PRED.hasDevice, target_filter=rq.filter_name("PDC1")))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID1")

        log.info("TEST: Query for instruments in Org1 whose platform parent has a specific attribute set")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.InstrumentDevice),
                      rq.filter_associated_with_object(res_by_name["Org1"], None, "hasResource"),
                      rq.filter_associated_with_object(subject_type=RT.PlatformDevice, predicate=PRED.hasDevice, target_filter=rq.filter_name("PDC1")))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID1")

        log.info("TEST: Query for instruments in Org1 that are lcstate INTEGRATED")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_type(RT.InstrumentDevice),
                      rq.filter_lcstate(LCS.INTEGRATED),
                      rq.filter_associated_with_object(res_by_name["Org1"], None, "hasResource"))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0].name, "ID2")

        log.info("TEST: Query for instruments in Org1 that are lcstate INTEGRATED OR platforms in Org1 that are lcstate DEPLOYED")
        rq = ResourceQuery()
        rq.set_filter(rq.filter_or(rq.filter_and(rq.filter_type(RT.InstrumentDevice),
                                                 rq.filter_lcstate(LCS.INTEGRATED)),
                                   rq.filter_and(rq.filter_type(RT.PlatformDevice),
                                                 rq.filter_lcstate(LCS.DEPLOYED))),
                      rq.filter_associated_with_object(res_by_name["Org1"], None, "hasResource"))
        result = self.discovery.query(rq.get_query(), id_only=False)
        self.assertEquals(len(result), 2)
        #self.assertEquals(result[0].name, "ID2")


@attr('INT', group='dm')
class DiscoveryIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(DiscoveryIntTest, self).setUp()
        config = DotDict()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml', config)

        self.discovery              = DiscoveryServiceClient()
        self.catalog                = CatalogManagementServiceClient()
        self.ims                    = IndexManagementServiceClient()
        self.rr                     = ResourceRegistryServiceClient()
        self.dataset_management     = DatasetManagementServiceClient()
        self.pubsub_management      = PubsubManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()

    def test_discovery_search(self):
        # ----------------------------------------------------
        # Resource setup
        
        resources = [
            ("ID1", InstrumentDevice(name='sonobuoy1', firmware_version='A1')),
            ("ID2", InstrumentDevice(name='sonobuoy2', firmware_version='A2')),
            ("ID3", InstrumentDevice(name='sonobuoy3', firmware_version='A3')),
        ]
        res_by_alias = {}
        for (alias, resource) in resources:
            rid,_ = self.rr.create(resource)
            res_by_alias[alias] = rid

        # ----------------------------------------------------
        # Resource attribute search

        # Resource search via query DSL
        search_string = "search 'firmware_version' is 'A2' from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)
        self.assertIsInstance(result[0], InstrumentDevice)
        self.assertTrue(result[0].name == 'sonobuoy2')
        self.assertTrue(result[0].firmware_version == 'A2')

        # Resource search via query object
        query_str = "{'and': [], 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A2'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        search_string = "search 'firmware_version' is 'A2' from 'resources_index'"
        result = self.discovery.parse(search_string, id_only=True)
        self.assertEquals(len(result), 1)
        self.assertIsInstance(result[0], str)

        query_str = """{'QUERYEXP': 'qexp_v1.0',
            'query_args': {'datastore': 'resources', 'id_only': False, 'limit': 0, 'profile': 'RESOURCES', 'skip': 0},
            'where': ['xop:attilike', ('firmware_version', 'A2')],
            'order_by': {}}"""
        query_obj = eval(query_str)
        result = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)
        self.assertTrue(all(isinstance(eo, InstrumentDevice) for eo in result))

        result = self.discovery.query(query_obj, id_only=False, search_args=dict(attribute_filter=["firmware_version"]))
        self.assertEquals(len(result), 1)
        self.assertTrue(all(isinstance(eo, dict) for eo in result))
        self.assertTrue(all("firmware_version" in eo for eo in result))
        self.assertTrue(all(len(eo) <= 4 for eo in result))


        # ----------------------------------------------------
        # Events setup

        from interface.objects import ResourceOperatorEvent, ResourceCommandEvent
        t0 = 136304640000

        events = [
            ("RME1", ResourceCommandEvent(origin="O1", origin_type="OT1", sub_type="ST1", ts_created=str(t0))),
            ("RME2", ResourceCommandEvent(origin="O2", origin_type="OT1", sub_type="ST2", ts_created=str(t0+1))),
            ("RME3", ResourceCommandEvent(origin="O2", origin_type="OT2", sub_type="ST3", ts_created=str(t0+2))),

            ("RLE1", ResourceOperatorEvent(origin="O1", origin_type="OT3", sub_type="ST4", ts_created=str(t0+3))),
            ("RLE2", ResourceOperatorEvent(origin="O3", origin_type="OT3", sub_type="ST5", ts_created=str(t0+4))),
            ("RLE3", ResourceOperatorEvent(origin="O3", origin_type="OT2", sub_type="ST6", ts_created=str(t0+5))),
        ]
        ev_by_alias = {}
        for (alias, event) in events:
            evid, _ = self.container.event_repository.put_event(event)
            ev_by_alias[alias] = evid

        # ----------------------------------------------------
        # Events attribute search

        # Event search via query DSL
        search_string = "search 'origin' is 'O1' from 'events_index'"
        result = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 2)

        # Event search via query DSL
        query_str = "{'and': [], 'or': [], 'query': {'field': 'origin', 'index': 'events_index', 'value': 'O1'}}"
        query_obj = eval(query_str)
        result1 = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result1), 2)


    @skip("Not yet ported to Postgres")
    def test_geo_distance_search(self):

        pd = PlatformDevice(name='test_dev')
        pd_id, _ = self.rr.create(pd)

        search_string = "search 'index_location' geo distance 20 km from lat 0 lon 0 from 'devices_index'"
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == pd_id)
        self.assertTrue(results[0]['_source'].name == 'test_dev')

    @skip("Not yet ported to Postgres")
    def test_collections_searching(self):
        site_id, _ = self.rr.create(Site(name='black_mesa'))
        view_id   = self.discovery.create_catalog_view('big', fields=['name'])

        # Add the site to a new collection
        collection_id = self.ims.create_collection('resource_collection', [site_id])
        search_string = "search 'name' is '*' from '%s' and in '%s'" %(view_id, collection_id)
        results = self.discovery.parse(search_string, id_only=True)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0] == site_id, '%s' % results)

    @skip("Not yet ported to Postgres")
    def test_time_search(self):
        today    = date.today()
        past = today - timedelta(days=2)
        future = today + timedelta(days=2)

        data_product = DataProduct()
        dp_id, _ = self.rr.create(data_product)
        
        search_string = "search 'type_' is 'DataProduct' from 'data_products_index' and search 'ts_created' time from '%s' to '%s' from 'data_products_index'" % (past, future)
        results = self.discovery.parse(search_string, id_only=True)
        self.assertIsNotNone(results,'Results not found')
        self.assertIn(dp_id, results)
        
        search_string = "search 'type_' is 'DataProduct' from 'data_products_index' and search 'ts_created' time from '%s' from 'data_products_index'" % past
        results = self.discovery.parse(search_string, id_only=True)
        self.assertIsNotNone(results,'Results not found')
        self.assertIn(dp_id, results)

    @skip("Not yet ported to Postgres")
    def test_user_search(self):
        user = UserInfo()
        user.name = 'test'
        user.contact.phones.append('5551212')

        user_id, _ = self.rr.create(user)
        search_string = 'search "name" is "test" from "users_index"'
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == user_id)
        self.assertTrue(results[0]['_source'].name == 'test')

        search_string = 'search "contact.phones" is "5551212" from "users_index"'
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == user_id)
        self.assertTrue(results[0]['_source'].name == 'test')

    @skip("Not yet ported to Postgres")
    def test_subobject_search(self):
        contact = ContactInformation()
        contact.email = 'test@gmail.com'
        contact.individual_name_family = 'Tester'
        contact.individual_names_given = 'Intern'

        dp = DataProduct(name='example')
        dp.contacts.append(contact)
        dp_id,_ = self.rr.create(dp)

        #--------------------------------------------------------------------------------
        # Example using the full field name
        #--------------------------------------------------------------------------------
        search_string = 'search "contacts.email" is "test@gmail.com" from "data_products"'
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')

        #--------------------------------------------------------------------------------
        # Example using a sub-object's field name (ambiguous searching)
        #--------------------------------------------------------------------------------
        search_string = 'search "individual_names_given" is "Intern" from "data_products"'
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')

    @skip("Not yet ported to Postgres")
    def test_descriptive_phrase_search(self):
        dp = DataProduct(name='example', description='This is simply a description for this data product')
        dp_id, _ = self.rr.create(dp)

        search_string = 'search "description" like "description for" from "data_products"'
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')
    
    @skip("Not yet ported to Postgres")
    def test_match_search(self):
        dp = DataProduct(name='example', description='This is simply a description for this data product')
        dp_id, _ = self.rr.create(dp)

        search_string = 'search "description" match "this data product" from "data_products"'
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')

    @skip("Not yet ported to Postgres")
    def test_expected_match_results(self):
        names = [
            'Instrument for site1',
            'Instrument for simulator',
            'CTD1',
            'SBE37',
            'SSN-719',
            'Submerssible Expendable Bathyothermograph',
            'VELPT',
            'VELO',
            'Safire2 169'
            ]
        for name in names:
            res_id, _ = self.rr.create(InstrumentDevice(name=name))
            self.addCleanup(self.rr.delete, res_id)

        search_string = 'search "name" match "expendable" from "devices"'
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertEquals(len(results),1)
        self.assertEquals(results[0]['_source'].name, 'Submerssible Expendable Bathyothermograph')

        search_string = 'search "name" match "instrument for" from "devices"'
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertEquals(len(results),2)
        self.assertTrue('Instrument for' in results[0]['_source'].name)
        self.assertTrue('Instrument for' in results[1]['_source'].name)

        search_string = 'search "name" match "velo for" from "devices"'
        results = self.discovery.parse(search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertEquals(len(results),1)
        self.assertEquals(results[0]['_source'].name, 'VELO')

    @skip("Not yet ported to Postgres")
    def test_ownership_searching(self):
        # Create two data products so that there is competition to the search, one is parsed 
        # (with conductivity as a parameter) and the other is raw
        dp = DataProduct(name='example dataproduct')
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_def_id = self.pubsub_management.create_stream_definition('ctd parsed', parameter_dictionary_id=pdict_id)
        dp_id = self.data_product_management.create_data_product(dp, stream_definition_id=stream_def_id, exchange_point='xp1')

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_raw_param_dict')
        stream_def_id = self.pubsub_management.create_stream_definition('ctd raw', parameter_dictionary_id=pdict_id)
        dp = DataProduct(name='WRONG')
        self.data_product_management.create_data_product(dp, stream_definition_id=stream_def_id, exchange_point='xp1')

        parameter_search = 'search "name" is "conductivity" from "resources_index"'
        results = self.discovery.parse(parameter_search)
        param_id = results[0]['_id']

        data_product_search = 'search "name" is "*" from "data_products_index" and has "%s"' % param_id
        results = self.discovery.parse(data_product_search, id_only=True)
        self.assertIn(dp_id, results)
        #self.assertEquals(results[0], dp_id)

    @skip("Not yet ported to Postgres")
    def test_associative_searching(self):
        dp_id,_ = self.rr.create(DataProduct('test_foo'))
        ds_id,_ = self.rr.create(Dataset('test_bar', registered=True))
        self.rr.create_association(subject=dp_id, object=ds_id, predicate='hasDataset')

        search_string = "search 'type_' is 'Dataset' from 'resources_index' and belongs to '%s'" % dp_id
        results = self.discovery.parse(search_string, id_only=True)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(ds_id in results)

    @skip("Not yet ported to Postgres")
    def test_iterative_associative_searching(self):
        #--------------------------------------------------------------------------------
        # Tests the ability to limit the iterations
        #--------------------------------------------------------------------------------
        dp       = DataProcess()
        transform = Transform()
        pd       = ProcessDefinition()

        dp_id, _       = self.rr.create(dp)
        transform_id, _ = self.rr.create(transform)
        pd_id, _       = self.rr.create(pd)

        self.rr.create_association(subject=dp_id, object=transform_id, predicate=PRED.hasTransform)
        self.rr.create_association(subject=transform_id, object=pd_id, predicate=PRED.hasProcessDefinition)

        search_string = "belongs to '%s' depth 1" % dp_id
        results = self.discovery.parse(search_string)
        results = list([i._id for i in results])
        correct = [transform_id]
        self.assertTrue(results == correct, '%s' % results)

        search_string = "belongs to '%s' depth 2" % dp_id
        results = self.discovery.parse(search_string)
        results = list([i._id for i in results])
        results.sort()
        correct = [transform_id, pd_id]
        correct.sort()
        self.assertTrue(results == correct)
