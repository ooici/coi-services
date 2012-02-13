'''
@author Luke Campbell <luke.s.campbell@gmail.com>
@file ion/services/dm/inventory/test/dataset_management_test.py
@description Unit and Integration test implementations for the data set management service class.
'''
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from prototype.sci_data.ctd_stream import ctd_stream_packet
from pyon.datastore.datastore import DataStore
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import random


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

    def _random_data(self, entropy):
        random_pressures = [(random.random()*100) for i in xrange(entropy)]
        random_salinity = [(random.random()*28) for i in xrange(entropy)]
        random_temperature = [(random.random()*10)+32 for i in xrange(entropy)]
        random_times = [random.randrange(1328205227, 1328896395) for i in xrange(entropy)]
        random_lat = [(random.random()*10)+30 for i in xrange(entropy)]
        random_lon = [(random.random()*10)+70 for i in xrange(entropy)]
        return [random_pressures, random_salinity, random_temperature, random_times, random_lat, random_lon]

    def _generate_point(self, entropy=5):
        points = []
        random_values = self._random_data(entropy)
        point = ctd_stream_packet(stream_id='test_data', p=random_values[0], c=random_values[1], t=random_values[2],time=random_values[3], lat=random_values[4], lon=random_values[5], create_hdf=False)
        return point


    def test_get_dataset_bounds(self):
        for i in xrange(3):
            point = self._generate_point()
            self.db.create(point)

        dataset_id = self.dataset_management_client.create_dataset(stream_id='test_data')


        bounds = self.dataset_management_client.get_dataset_bounds(dataset_id=dataset_id)

        self.assertTrue(bounds['latitude_bounds'][0] > 30.0)
        self.assertTrue(bounds['latitude_bounds'][1] < 40.0)
        self.assertTrue(bounds['longitude_bounds'][0] > 70.0)
        self.assertTrue(bounds['longitude_bounds'][1] < 80.0)

        self.dataset_management_client.delete_dataset(dataset_id)