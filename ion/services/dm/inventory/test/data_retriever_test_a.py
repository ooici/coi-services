#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file data_retriever_test_a.py
@date 06/14/12 15:06
@description DESCRIPTION
'''
from nose.plugins.attrib import attr
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.processes.data.replay.replay_process_a import ReplayProcess
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG
from pyon.datastore.datastore import DataStore
from pyon.core.bootstrap import get_sys_name
import unittest
import os

@attr('INT', group='dm')
class DataRetrieverIntTestAlpha(IonIntegrationTestCase):
    def setUp(self):
        super(DataRetrieverIntTestAlpha,self).setUp()


        self._start_container()
        config = DotDict()
        config.bootstrap.processes.replay.module    = 'ion.processes.data.replay.replay_process_a'
        self.container.start_rel_from_url('res/deploy/r2dm.yml', config)


        self.datastore_name = 'test_datasets'
        self.datastore      = self.container.datastore_manager.get_datastore(self.datastore_name, profile=DataStore.DS_PROFILE.SCIDATA)

        self.data_retriever     = DataRetrieverServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.resource_registry  = ResourceRegistryServiceClient()

        xs_dot_xp = CFG.core_xps.science_data

        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)
    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_define_replay(self):
        # Create a dataset to work with
        dataset_id = self.dataset_management.create_dataset('fakestream', self.datastore_name)

        replay_id, stream_id = self.data_retriever.define_replay(dataset_id=dataset_id)

        # Verify that the replay instance was created
        replay = self.resource_registry.read(replay_id)

        pid = replay.process_id

        process = self.container.proc_manager.procs[pid]

        self.assertIsInstance(process,ReplayProcess, 'Incorrect process launched')
