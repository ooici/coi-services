#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/replay/test/test_binary_replay.py
@date Thu Aug 23 10:36:09 EDT 2012
@brief Tests for Binary Replay
'''
from pyon.util.unit_test import PyonTestCase
from ion.processes.data.replay.binary_replay import BinaryReplayProcess
from nose.plugins.attrib import attr
from pyon.util.containers import DotDict
from mock import Mock, patch
@attr('UNIT',group='dm')
class BinaryReplayProcessUnitTest(PyonTestCase):
    def setUp(self):
        self.binary_replay = BinaryReplayProcess()

    @patch('ion.processes.data.replay.binary_replay.PreservationManagementServiceClient')
    def test_on_start(self, psmc_mock):
        self.binary_replay.CFG = DotDict()
        self.binary_replay.on_start()

    def test_execute_retrieve(self):
        container = DotDict()
        self.binary_replay.container = container
        
        datastore = DotDict()
        container.datastore_manager.get_datastore = Mock()
        container.datastore_manager.get_datastore.return_value = datastore

        datastore.query_view = Mock()
        datastore.query_view.return_value = []

        self.binary_replay.stream_id = 'stream_id'
        self.binary_replay.file_id   = 'file_id'
        self.binary_replay.file_name = None

        for i in self.binary_replay.execute_retrieve():
            pass
        datastore.query_view.assert_called_once_with('catalog/file_by_owner', opts={'start_key':['stream_id','file_id',0], 'end_key':['stream_id','file_id',{}]})

