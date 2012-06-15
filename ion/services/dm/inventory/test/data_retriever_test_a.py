#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file data_retriever_test_a.py
@date 06/14/12 15:06
@description DESCRIPTION
'''
from nose.plugins.attrib import attr
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG
from pyon.datastore.datastore import DataStore
from pyon.core.bootstrap import get_sys_name

@attr('INT', group='dm')
class DataRetrieverIntTestAlpha(IonIntegrationTestCase):
    def setUp(self):
        super(DataRetrieverIntTestAlpha,self).setUp()


        self._start_container()

        CFG.processes.ingestion_module = 'ion.processes.data.ingestion.ingestion_worker_a'
        CFG.processes.replay_module    = 'ion.processes.data.replay.replay_process_a'

        self.container.start_rel_from_url('res/deploy/r2dm.yml')


        self.datastore_name = 'test_datasets'
        self.datastore      = self.container.datastore_manager.get_datastore(self.datastore_name, profile=DataStore.DS_PROFILE.SCIDATA)

        self.data_retriever = DataRetrieverServiceClient()

        xs_dot_xp = CFG.core_xps.science_data

        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)

    def test_define_replay(self):
        pass