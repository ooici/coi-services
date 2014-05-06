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
from pyon.public import PRED, CFG, RT, BadRequest, NotFound, DotDict
from pyon.util.poller import poll_wrapper

from ion.services.dm.presentation.discovery_service import DiscoveryService
from ion.services.dm.inventory.index_management_service import IndexManagementService
from ion.util.geo_utils import GeoUtils

from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.dm.iindex_management_service import IndexManagementServiceClient
from interface.services.dm.icatalog_management_service import CatalogManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from interface.objects import InstrumentDevice, Site, PlatformDevice, BankAccount, DataProduct, Transform, ProcessDefinition, \
    DataProcess, UserInfo, ContactInformation, Dataset, GeospatialIndex, GeospatialBounds, TemporalBounds


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

    def test_create_view(self):
        # Mocks
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.cms_create.return_value = 'catalog_id'

        retval = self.discovery.create_view('mock_view',fields=['name'])

        self.assertTrue(retval=='res_id', 'Improper resource creation')

    def test_create_view_exists(self):
        self.rr_find_res.return_value = ([1], [1])
        with self.assertRaises(BadRequest):
            self.discovery.create_view('doesnt matter', fields=['name'])

    def test_create_view_no_fields(self):
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.cms_create.return_value = 'catalog_id'

        with self.assertRaises(BadRequest):
            self.discovery.create_view('mock_view')

    def test_create_view_order(self):
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.cms_create.return_value = 'catalog_id'

        with self.assertRaises(BadRequest):
            self.discovery.create_view('movk_view',fields=['name'],order=['age'])


    def test_read_view(self):
        # Mocks
        self.rr_read.return_value = 'fake_resource'
        retval = self.discovery.read_view('mock_view_id')
        self.assertTrue(retval == 'fake_resource')

    def test_update_view(self):
        retval = self.discovery.update_view({})
        self.assertTrue(retval)
        self.rr_update.assert_called_once_with({})

    def test_delete_view(self):
        catalog_assoc = DotDict(_id=0)
        self.rr_find_assocs_mult.return_value = ([],[catalog_assoc])
        retval = self.discovery.delete_view('view_id')
        self.assertTrue(retval)
        self.rr_delete.assert_called_once_with('view_id')
        self.assertTrue(self.rr_delete_assoc.call_count == 1)


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
                self.discovery.request(req)

    @patch('ion.services.dm.presentation.discovery_service.QueryLanguage')
    def test_parse_mock(self, mock_parser):
        mock_parser().parse.return_value = 'arg'
        self.discovery.request = Mock()
        self.discovery.request.return_value = 'correct_value'
        retval = self.discovery.parse('blah blah', id_only=sentinel.id_only)
        self.discovery.request.assert_called_once_with('arg', search_args=None, id_only=sentinel.id_only)
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
        retval = self.discovery.request(query)
        self.discovery.ds_discovery._get_datastore.assert_called_once_with("resources")
        self.assertEquals(retval, ["FOO"])

    def test_tier2_request(self):
        self.ds_mock.find_by_query = Mock(return_value=["FOO"])

        query = {'query':{'field': 'name', 'value': 'foo'}, 'and':[{'field': 'lcstate', 'value': 'foo2'}]}
        retval = self.discovery.request(query)
        self.discovery.ds_discovery._get_datastore.assert_called_once_with("resources")
        self.assertEquals(retval, ["FOO"])

        query = {'query':{'field': 'name', 'value': 'foo'}, 'or':[{'field': 'lcstate', 'value': 'foo2'}]}
        retval = self.discovery.request(query)
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
        search_string = "search 'firmware_version' is 'A2' from 'resources_index'"
        result  = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)
        self.assertIsInstance(result[0], InstrumentDevice)
        self.assertTrue(result[0].name == 'sonobuoy2')
        self.assertTrue(result[0].firmware_version == 'A2')

        query_str = "{'and': [], 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A2'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Resource attribute match
        search_string = "search 'firmware_version' is 'A*' from 'resources_index'"
        result  = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 3)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A*'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Resource attribute match with limit
        search_string = "search 'firmware_version' is 'A*' from 'resources_index' limit 2"
        result  = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 2)

        query_str = "{'and': [], 'limit': 2, 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A*'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Resource attribute match with limit and skip (offset)
        # TODO these tests are unlikely to always work until order_by is implemented (1/200 chance fails)
        
        # -- limit 1 without skip (using QueryLanguage)
        search_string = "search 'name' is 'range*' from 'resources_index' limit 1"
        result  = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)
        # -- limit 1 with skip (using QueryLanguage)
        search_string = "search 'name' is 'range*' from 'resources_index' SKIP 100 limit 1"
        result1  = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result1), 1)
        # check same length and not equal (one uses SKIP 100, other doesn't)
        self.assertEquals(len(result), len(result1))
        self.assertNotEquals(result, result1)

        # -- limit 1 without skip (using Discovery Intermediate Format)
        query_str = "{'and': [], 'limit': 1, 'or': [], 'query': {'field': 'name', 'index': 'resources_index', 'value': 'range*'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        # -- limit 1 with skip (using Discovery Intermediate Format)
        query_str = "{'and': [], 'limit': 1, 'skip': 100, 'or': [], 'query': {'field': 'name', 'index': 'resources_index', 'value': 'range*'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=False)
        # check same length and not equal (one uses SKIP 100, other doesn't)
        self.assertEquals(len(result), len(result1))
        self.assertNotEquals(result, result1)

        # Resource attribute match only count (results should return single value, a count of available results)
        search_args_str = "{'count': True}"
        search_args = eval(search_args_str)
        search_string = "search 'firmware_version' is 'A*' from 'resources_index' limit 2"
        result  = self.discovery.parse(search_string, id_only=False, search_args=search_args)
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'limit': 2, 'or': [], 'query': {'field': 'firmware_version', 'index': 'resources_index', 'value': 'A*'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=False, search_args=search_args)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Check data products
        search_string = "search 'name' is 'testData*' from 'resources_index'"
        result  = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 4)

        search_string = "search 'type_' is 'DataProduct' from 'resources_index'"
        result  = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 4)

        # ----------------------------------------------------
        # Geospatial search

        # Geospatial search - query bbox fully overlaps
        search_string = "search 'geospatial_point_center' geo box top-left lat 180 lon -180 bottom-right lat -180 lon 180 from 'resources_index'"
        result  = self.discovery.parse(search_string, id_only=True)
        self.assertGreaterEqual(len(result), 4)
        for dp in ["DP1", "DP2", "DP3", "DP4"]:
            self.assertIn(res_by_alias[dp], result)

        search_string = "search 'geospatial_point_center' geo box top-left lat 20 lon 0 bottom-right lat 0 lon 20 from 'resources_index'"
        result  = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)

        # Note that in Discovery intermediate format top_left=x1,y2 and bottom_right=x2,y1 contrary to naming
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'index_location', 'index': 'resources_index'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), len(result1))
        self.assertEquals(result, result1)

        # Geospatial bbox operators - overlaps (this is the default and should be the same as above)
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result2  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result1), len(result2))
        self.assertEquals(result1, result2)

        # Geospatial bbox operators - contains (the resource contains the query)
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result3  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 0)

        query_str = "{'and': [], 'or': [], 'query': {'top_left': [8.0, 11.0], 'bottom_right': [12.0, 9.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result3  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 1)

        # Geospatial bbox operators - within (the resource with the query)
        query_str = "{'and': [], 'or': [], 'query': {'top_left': [0.0, 20.0], 'bottom_right': [20.0, 0.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result3  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 1)

        query_str = "{'and': [], 'or': [], 'query': {'top_left': [15.0, 5.0], 'bottom_right': [5.0, 15.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result3  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 1)

        query_str = "{'and': [], 'or': [], 'query': {'top_left': [14.0, 5.0], 'bottom_right': [5.0, 15.0], 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result3  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result3), 0)

        # Geospatial search - query bbox partial overlaps
        search_string = "search 'geospatial_bounds' geo box top-left lat 11 lon 9 bottom-right lat 9 lon 11 from 'resources_index'"
        result  = self.discovery.parse(search_string, id_only=False)
        self.assertEquals(len(result), 1)


        # Geospatial - WKT (a box 4,4 to 4,14 to 14,14 to 14,4, to 4,4 overlaps DP1 but is not contained by it or does not have it within)
        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POLYGON((4 4,4 14,14 14,14 4,4 4))', 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        print result
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POLYGON((4 4,4 14,14 14,14 4,4 4))', 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        print result
        self.assertEquals(len(result), 0)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POLYGON((4 4,4 14,14 14,14 4,4 4))', 'field': 'geospatial_bounds', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        print result
        self.assertEquals(len(result), 0)

        # -- with buffer (eg. point with radius CIRCLE)
        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': 1.0, 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 1)

        query_str = "{'and': [], 'or': [], 'query': {'wkt': 'POINT(10.0 10.0)', 'buffer': 1.0, 'field': 'geospatial_point_center', 'index': 'resources_index', 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result), 0)


        # ----------------------------------------------------
        # Vertical search

        search_string = "search 'geospatial_bounds' vertical from 0 to 500 from 'resources_index'"
        result  = self.discovery.parse(search_string, id_only=True)
        self.assertGreaterEqual(len(result), 4)
        for dp in ["DP1", "DP2", "DP3", "DP4"]:
            self.assertIn(res_by_alias[dp], result)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 0.0, 'to': 500.0}, 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result1), 4)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 1.0, 'to': 2.0}, 'cmpop': 'overlaps'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=False)
        self.assertEquals(len(result1), 2)

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 110.0, 'to': 120.0}, 'cmpop': 'contains'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=True)
        self.assertEquals(len(result1), 1)
        self.assertEquals(res_by_alias["DP3"], result1[0])

        query_str = "{'and': [], 'or': [], 'query': {'field': 'geospatial_bounds', 'index': 'resources_index', 'vertical_bounds': {'from': 5.0, 'to': 30.0}, 'cmpop': 'within'}}"
        query_obj = eval(query_str)
        result1  = self.discovery.query(query_obj, id_only=True)
        self.assertEquals(len(result1), 1)
        self.assertEquals(res_by_alias["DP4"], result1[0])

        # ----------------------------------------------------
        # Temporal search

        search_string = "search 'nominal_datetime' timebounds from '%s' to '%s' from 'resources_index'" %('2013-03-12','2013-03-19')
        result  = self.discovery.parse(search_string, id_only=True)
        self.assertEquals(len(result), 2)
        for dp in ["DP1", "DP2"]:
            self.assertIn(res_by_alias[dp], result)

        search_string = "search 'nominal_datetime' timebounds from '%s' to '%s' from 'resources_index'" %('2013-03-12','2013-11-19')
        result  = self.discovery.parse(search_string, id_only=True)
        self.assertEquals(len(result), 4)
        for dp in ["DP1", "DP2", "DP3", "DP4"]:
            self.assertIn(res_by_alias[dp], result)

        search_string = "search 'nominal_datetime' timebounds from '%s' to '%s' from 'resources_index'" %('2013-03-12','2013-03-13')
        result  = self.discovery.parse(search_string, id_only=True)
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


@attr('INT', group='dm')
class DiscoveryIntTest(IonIntegrationTestCase):
    def setUp(self):
        raise SkipTest("Not yet ported to Postgres")

        super(DiscoveryIntTest, self).setUp()
        config = DotDict()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml', config)

        self.discovery               = DiscoveryServiceClient()
        self.catalog                 = CatalogManagementServiceClient()
        self.ims                     = IndexManagementServiceClient()
        self.rr                      = ResourceRegistryServiceClient()
        self.dataset_management      = DatasetManagementServiceClient()
        self.pubsub_management       = PubsubManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()


    def test_geo_distance_search(self):

        pd = PlatformDevice(name='test_dev')

        pd_id, _ = self.rr.create(pd)

        search_string = "search 'index_location' geo distance 20 km from lat 0 lon 0 from 'devices_index'"

        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')

        self.assertTrue(results[0]['_id'] == pd_id)
        self.assertTrue(results[0]['_source'].name == 'test_dev')


    def test_ranged_value_searching(self):
        discovery = self.discovery
        rr        = self.rr
        
        view_id = discovery.create_view('bank_view', fields=['cash_balance'])
        bank_id, _ = rr.create(BankAccount(name='broke', cash_balance=10))

        search_string = "search 'cash_balance' values from 0 to 100 from '%s'" % view_id

        results = self.poll(5, discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == bank_id)
        
        bank_id, _ = rr.create(BankAccount(name='broke', cash_balance=90))

        search_string = "search 'cash_balance' values from 80 from '%s'" % view_id

        results = self.poll(5, discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == bank_id)

    def test_collections_searching(self):
        site_id, _ = self.rr.create(Site(name='black_mesa'))
        view_id    = self.discovery.create_view('big', fields=['name'])

        # Add the site to a new collection
        collection_id = self.ims.create_collection('resource_collection', [site_id])

        search_string = "search 'name' is '*' from '%s' and in '%s'" %(view_id, collection_id)

        results = self.poll(9, self.discovery.parse,search_string,id_only=True)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0] == site_id, '%s' % results)

    def test_search_by_name_index(self):
        inst_dev = InstrumentDevice(name='test_dev',serial_number='ABC123')

        dev_id, _ = self.rr.create(inst_dev)
        search_string = "search 'serial_number' is 'abc*' from 'resources_index'"
        
        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dev_id)

        bank_acc = BankAccount(name='blah', cash_balance=10)
        res_id , _ = self.rr.create(bank_acc)

        search_string = "search 'cash_balance' values from 0 to 100 from 'resources_index'"

        results = self.poll(9, self.discovery.parse,search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == res_id)

    def test_data_product_search(self):
        # Create the dataproduct
        dp = DataProduct(name='test_product')
        dp.data_format.name = 'test_signal'
        dp.data_format.description = 'test signal'
        dp.data_format.character_set = 'utf8'
        dp.data_format.nominal_sampling_rate_maximum = '44000'
        dp.data_format.nominal_sampling_rate_minimum = '44000'
        dp.CDM_data_type = 'basic'
        dp_id, _ = self.rr.create(dp)

        search_string = "search 'data_format.name' is 'test_signal' from 'data_products_index'"
        results = self.poll(9, self.discovery.parse, search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)

        search_string = "search 'CDM_data_type' is 'basic' from 'data_products_index'"
        results = self.poll(9, self.discovery.parse, search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        
        search_string = "search 'data_format.character_set' is 'utf8' from 'data_products_index'"
        results = self.poll(9, self.discovery.parse, search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)


    def test_events_search(self):
        # Create a resource to force a new event

        dp = DataProcess()
        dp_id, rev = self.rr.create(dp)

        search_string = "SEARCH 'origin' IS '%s' FROM 'events_index'" % dp_id

        results = self.poll(9, self.discovery.parse,search_string)
        origin_type = results[0]['_source'].origin_type
        origin_id = results[0]['_source'].origin

        self.assertTrue(origin_type == RT.DataProcess)
        self.assertTrue(origin_id == dp_id)



    def test_time_search(self):
        today     = date.today()
        past = today - timedelta(days=2)
        future  = today + timedelta(days=2)

        data_product = DataProduct()
        dp_id, _ = self.rr.create(data_product)
        
        search_string = "search 'type_' is 'DataProduct' from 'data_products_index' and search 'ts_created' time from '%s' to '%s' from 'data_products_index'" % (past, future)

        results = self.poll(9, self.discovery.parse,search_string,id_only=True)

        self.assertIsNotNone(results,'Results not found')

        self.assertIn(dp_id, results)
        
        search_string = "search 'type_' is 'DataProduct' from 'data_products_index' and search 'ts_created' time from '%s' from 'data_products_index'" % past

        results = self.poll(9, self.discovery.parse,search_string,id_only=True)

        self.assertIsNotNone(results,'Results not found')

        self.assertIn(dp_id, results)

        
    def test_user_search(self):
        user = UserInfo()
        user.name = 'test'
        user.contact.phones.append('5551212')

        user_id, _ = self.rr.create(user)

        search_string = 'search "name" is "test" from "users_index"'

        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')

        self.assertTrue(results[0]['_id'] == user_id)
        self.assertTrue(results[0]['_source'].name == 'test')

        search_string = 'search "contact.phones" is "5551212" from "users_index"'
        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')

        self.assertTrue(results[0]['_id'] == user_id)
        self.assertTrue(results[0]['_source'].name == 'test')


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
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')

        #--------------------------------------------------------------------------------
        # Example using a sub-object's field name (ambiguous searching)
        #--------------------------------------------------------------------------------
        search_string = 'search "individual_names_given" is "Intern" from "data_products"'
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')

    def test_descriptive_phrase_search(self):
        dp = DataProduct(name='example', description='This is simply a description for this data product')
        dp_id, _ = self.rr.create(dp)

        search_string = 'search "description" like "description for" from "data_products"'
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')
    
    def test_match_search(self):
        dp = DataProduct(name='example', description='This is simply a description for this data product')
        dp_id, _ = self.rr.create(dp)

        search_string = 'search "description" match "this data product" from "data_products"'
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')

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
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')

        self.assertEquals(len(results),1)
        self.assertEquals(results[0]['_source'].name, 'Submerssible Expendable Bathyothermograph')


        search_string = 'search "name" match "instrument for" from "devices"'
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')

        self.assertEquals(len(results),2)
        self.assertTrue('Instrument for' in results[0]['_source'].name)
        self.assertTrue('Instrument for' in results[1]['_source'].name)


        search_string = 'search "name" match "velo for" from "devices"'
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')

        self.assertEquals(len(results),1)
        self.assertEquals(results[0]['_source'].name, 'VELO')


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
        results = self.poll(9, self.discovery.parse, parameter_search)
        param_id = results[0]['_id']

        data_product_search = 'search "name" is "*" from "data_products_index" and has "%s"' % param_id
        results = self.poll(9, self.discovery.parse, data_product_search,id_only=True)
        self.assertIn(dp_id, results)
        #self.assertEquals(results[0], dp_id)

    def test_associative_searching(self):
        dp_id,_ = self.rr.create(DataProduct('test_foo'))
        ds_id,_ = self.rr.create(Dataset('test_bar', registered=True))
        self.rr.create_association(subject=dp_id, object=ds_id, predicate='hasDataset')

        search_string = "search 'type_' is 'Dataset' from 'resources_index' and belongs to '%s'" % dp_id

        results = self.poll(5, self.discovery.parse, search_string, id_only=True)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(ds_id in results)

    def test_iterative_associative_searching(self):
        #--------------------------------------------------------------------------------
        # Tests the ability to limit the iterations
        #--------------------------------------------------------------------------------
        dp        = DataProcess()
        transform = Transform()
        pd        = ProcessDefinition()

        dp_id, _        = self.rr.create(dp)
        transform_id, _ = self.rr.create(transform)
        pd_id, _        = self.rr.create(pd)

        self.rr.create_association(subject=dp_id, object=transform_id, predicate=PRED.hasTransform)
        self.rr.create_association(subject=transform_id, object=pd_id, predicate=PRED.hasProcessDefinition)

        search_string = "belongs to '%s' depth 1" % dp_id
        results = self.poll(5, self.discovery.parse,search_string)
        results = list([i._id for i in results])
        correct = [transform_id]
        self.assertTrue(results == correct, '%s' % results)

        search_string = "belongs to '%s' depth 2" % dp_id
        results = self.poll(5, self.discovery.parse,search_string)
        results = list([i._id for i in results])
        results.sort()
        correct = [transform_id, pd_id]
        correct.sort()
        self.assertTrue(results == correct)
