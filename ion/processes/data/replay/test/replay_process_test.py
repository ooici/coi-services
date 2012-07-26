#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file replay_process_test
@date 06/14/12 10:48
@description DESCRIPTION
'''
from pyon.core.interceptor.encode import encode_ion
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from ion.processes.data.replay.replay_process import ReplayProcess
from pyon.datastore.datastore import DataStore
from mock import Mock, patch
from nose.plugins.attrib import attr
import msgpack
import unittest

@attr('UNIT',group='dm')
class ReplayProcessUnitTest(PyonTestCase):
    def setUp(self):
        self.replay = ReplayProcess()
        self.replay.dataset_id = 'dataset'
        self.replay.dataset = DotDict()
        self.replay.dataset.datastore_name='datasets'
        self.replay.dataset.primary_view_key = 'stream_id'
        self.replay.deliver_format = {}
        self.replay.start_time = None
        self.replay.end_time = None
        self.replay.output =  DotDict()
        self.replay.output.publish = Mock()


    @patch('ion.processes.data.replay.replay_process.DatasetManagementService')
    @patch('ion.processes.data.replay.replay_process.CoverageCraft')
    def test_execute_retrieve(self,mock_bb, mock_dsm):
        mock_dsm._get_coverage = Mock()
        mock_bb.sync_rdt_with_coverage = Mock()
        mock_bb().to_granule = Mock()
        mock_bb().to_granule.return_value = {'test':1}

        self.replay.start_time = 0
        self.replay.end_time = 0

        retval = self.replay.execute_retrieve()

        self.assertEquals(retval,{'test':1})

    @patch('ion.processes.data.replay.replay_process.gevent')
    def test_execute_replay(self,mock_gevent):
        mock_gevent.spawn = Mock()
        self.replay.publishing = DotDict()
        self.replay.publishing.is_set = Mock()
        self.replay.publishing.return_value = False

        retval = self.replay.execute_replay
        self.assertTrue(retval)
        
    def test_replay(self):
        self.replay.publishing = DotDict()
        self.replay.publishing.set = Mock()
        self.replay.publishing['clear'] = Mock()
        self.replay.execute_retrieve = Mock()
        self.replay.execute_retrieve.return_value = {'test':True}
        self.replay.output = DotDict()
        self.replay.output.publish = Mock()

        retval = self.replay.replay()
        self.assertTrue(retval)

    @patch('ion.processes.data.replay.replay_process.DatasetManagementService')
    @patch('ion.processes.data.replay.replay_process.DatasetManagementServiceClient')
    @patch('ion.processes.data.replay.replay_process.CoverageCraft')
    def test_get_last_granule(self, mock_bb, dsm_cli, dsm):

        mock_bb().sync_rdt_with_coverage = Mock()
        mock_bb().to_granule.return_value = {'test':True}

        dsm_cli().read_dataset = Mock()
        dataset = DotDict()
        dataset.datastore_name = 'test'
        dataset.view_name = 'bogus/view'

        dsm._get_coverage = Mock()
        dsm._get_coverage.return_value = {}
        
        datastore = DotDict()
        datastore.query_view = Mock()
        datastore.query_view.return_value = [{'doc':{'ts_create':0}}]

        
        container = DotDict()
        container.datastore_manager.get_datastore = Mock()
        container.datastore_manager.get_datastore.return_value = datastore

        retval = self.replay.get_last_granule(container,'dataset_id')

        self.assertEquals(retval,{'test':True})

    
    @patch('ion.processes.data.replay.replay_process.DatasetManagementService')
    @patch('ion.processes.data.replay.replay_process.CoverageCraft')
    def test_get_last_values(self, craft, dsm):
        dsm._get_coverage = Mock()

        craft().sync_rdt_with_coverage = Mock()
        craft().to_granule = Mock()
        craft().to_granule.return_value = {'test':True}

        retval = self.replay.get_last_values('dataset_id')
        self.assertEquals(retval,{'test':True})
