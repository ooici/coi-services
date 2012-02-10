'''
@author Luke Campbell <luke.s.campbell@gmail.com>
@file ion/services/dm/inventory/test/dataset_management_test.py
@description Unit and Integration test implementations for the data set management service class.
'''
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from interface.objects import DataSet
from pyon.datastore.datastore import DataStore
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr


@attr('UNIT',group='DM')
class DatasetManagementTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('dataset_management')
        self.dataset_management = DatasetManagementService()
        self.dataset_management.clients = mock_clients

        self.mock_rr_create = self.dataset_management.clients.resource_registry.create
        self.mock_rr_read = self.dataset_management.clients.resource_registry.read
        self.mock_rr_update = self.dataset_management.clients.resource_registry.update
        self.mock_rr_delete = self.dataset_management.clients.resource_registry.delete

    def test_create_dataset(self):
        # mocks
        self.mock_rr_create.return_value = ('dataset_id','rev')

        # execution
        dataset_id = self.dataset_management.create_dataset(name='123')


        # assertions
        self.assertEquals(dataset_id,'dataset_id')
        self.assertTrue(self.mock_rr_create.called)

    def test_update_dataset(self):
        # mocks
        mock_dataset = DotDict({'_id':'dataset_id'})


        # execution
        self.dataset_management.update_dataset(mock_dataset)


        # assertions
        self.mock_rr_update.assert_called_with(mock_dataset)

    def test_delete_dataset(self):
        # mocks

        # execution
        self.dataset_management.delete_dataset('123')

        # assertions
        self.mock_rr_delete.assert_called_with('123')


@attr('INT', group='DM')
class DatasetManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        import couchdb
        super(DatasetManagementIntTest,self).setUp()
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2dm.yml')


        self.db = self.container.datastore_manager.get_datastore('scidata', DataStore.DS_PROFILE.SCIDATA)
        self.db_raw = self.db.server

        self.dataset_management_client = DatasetManagementServiceClient(node=self.container.node)

    def _generate_points(self,number):

        import random
        import time

        random_temps = list(random.normalvariate(48.0,8) for x in xrange(80))



        points = []
        for d in xrange(number):
            sci_point = {
                "type_": "SciData",
                "temp": random.normalvariate(48.0,8),
                "depth": ((random.random() * 20) + 50),
                "origin_id": 1,
                "area": 1,
                "lattitude": ((random.random() * 10)+30),
                "longitude": ((random.random() * 10)+70),
                "lattitude_hemisphere": "N",
                "longitude_hemisphere": "W",
                "lattitude_precision": 8,
                "longitude_precision": 8,
                "time": time.strftime("%Y-%m-%dT%H:%M:%S-05"),
                "submitting_entity": "OOICI-DM",
                "instutition": "OOICI",
                "organization": "ASA",
                "platform": None,
                "depth_units": "ft"
            }
            points.append(sci_point)

        return points

    def _populate_with_mock_scidata(self):
        for point in self._generate_points(100):
            self.db_raw[self.db.datastore_name].create(point)


    def test_get_dataset_bounds(self):
        self._populate_with_mock_scidata()

        bounds = self.dataset_management_client.get_dataset_bounds()
        self.assertTrue(bounds['min_lat'] > 30 and bounds['max_lat'] < 40, '%s' % bounds)
        self.assertTrue(bounds['min_lon'] > 70 and bounds['min_lon'] < 80, '%s' % bounds)
        self.assertTrue(bounds['min_depth'] > 50 and bounds['max_depth'] < 70, '%s' % bounds)
