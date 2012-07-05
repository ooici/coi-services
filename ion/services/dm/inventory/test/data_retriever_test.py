#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file data_retriever_test_a.py
@date 06/14/12 15:06
@description DESCRIPTION
'''
from nose.plugins.attrib import attr
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG
from pyon.core.exception import BadRequest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.processes.data.replay.replay_process import ReplayProcess
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService
from pyon.datastore.datastore import DataStore
from pyon.core.bootstrap import get_sys_name
import unittest
import os
from pyon.util.unit_test import PyonTestCase
@attr('UNIT',group='dm')
class DataRetrieverUnitTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('data_retriever')
        self.data_retriever = DataRetrieverService()
        self.data_retriever.clients = mock_clients
        self.pubsub_create_stream = mock_clients.pubsub_management.create_stream
        self.rr_create = mock_clients.resource_registry.create
        self.rr_read = mock_clients.resource_registry.read
        self.rr_delete = mock_clients.resource_registry.delete
        self.rr_delete_association = mock_clients.resource_registry.delete_association
        self.rr_find_assocs = mock_clients.resource_registry.find_associations

        self.pd_schedule = mock_clients.process_dispatcher.schedule_process

    def test_define_replay(self):
        with self.assertRaises(BadRequest):
            self.data_retriever.define_replay()
        self.data_retriever.process_definition_id = 'fakeprocid'

        dataset = DotDict()
        dataset.datastore_name = 'testdatastore'
        dataset.view_name = 'manifest/by_dataset' # use real name so that when I refactor I know it needs to be changed here too
        dataset.primary_view_key = 'timestamp'

        stream_id = 'stream_id'

        self.pubsub_create_stream.return_value = stream_id
        self.rr_read.return_value = dataset
        self.rr_create.return_value = ('replay_id', 'rev')
        self.pd_schedule.return_value = 'pid'

        retval = self.data_retriever.define_replay(dataset_id='dataset_id')

        self.assertTrue(retval == ('replay_id','stream_id'))

    def test_delete_replay(self):
        self.rr_find_assocs.return_value = ['assoc']
        self.data_retriever.delete_replay('replay_id')

        self.rr_delete.assert_called_once_with('replay_id')
        self.rr_delete_association.assert_called_once_with('assoc')


    def test_start_replay(self):
        pass

    def test_stop_replay(self):
        pass

@attr('INT', group='dm')
class DataRetrieverIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(DataRetrieverIntTest,self).setUp()


        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')


        self.datastore_name = 'test_datasets'
        self.datastore      = self.container.datastore_manager.get_datastore(self.datastore_name, profile=DataStore.DS_PROFILE.SCIDATA)

        self.data_retriever     = DataRetrieverServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.resource_registry  = ResourceRegistryServiceClient()
        self.pubsub_management  = PubsubManagementServiceClient()

        xs_dot_xp = CFG.core_xps.science_data

        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)
    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_define_replay(self):
        fake_stream = self.pubsub_management.create_stream()

        # Create a dataset to work with
        dataset_id = self.dataset_management.create_dataset(fake_stream, self.datastore_name)

        replay_id, stream_id = self.data_retriever.define_replay(dataset_id=dataset_id)

        # Verify that the replay instance was created
        replay = self.resource_registry.read(replay_id)

        pid = replay.process_id

        process = self.container.proc_manager.procs[pid]

        self.assertIsInstance(process,ReplayProcess, 'Incorrect process launched')
