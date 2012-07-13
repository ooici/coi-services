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

    @patch('ion.processes.data.replay.replay_process.msgpack')
    def test_granule_from_doc(self, msgpack_mock):
        self.replay.read_persisted_cache = Mock()
        self.replay.read_persisted_cache.return_value = 'bytestring'
        self.replay.deserializer = DotDict()
        self.replay.deserializer.deserialize = Mock()
        self.replay.deserializer.deserialize.return_value = {1:1}
        msgpack_mock.unpackb = Mock()
        msgpack_mock.unpackb.return_value = None
        retval = self.replay.granule_from_doc({1:1})
        self.assertTrue(retval == {1:1}, '%s' % retval)

    def test_execute_retrieve(self):
        datastore = DotDict()
        container = DotDict()
        datastore.profile = DataStore.DS_PROFILE.SCIDATA
        datastore.query_view = Mock()
        container.datastore_manager.get_datastore = Mock()
        container.datastore_manager.get_datastore.return_value = datastore
        mock_data = msgpack.packb({'test':'test'},default=encode_ion)

        self.received_packet = False
        self.end_stream      = False

        result = DotDict()
        result.doc.sha1 = 'sha1'
        result.doc.encoding = 'test'

        datastore.query_view.return_value = [result]
        self.replay.container = container
        self.replay.read_persisted_cache = Mock()
        self.replay.read_persisted_cache.return_value = mock_data


        retval = self.replay.execute_retrieve()
        self.assertTrue(retval=={'test':'test'})


    def test_execute_replay(self):
        datastore = DotDict()
        container = DotDict()
        datastore.query_view = Mock()
        datastore.profile = DataStore.DS_PROFILE.SCIDATA
        container.datastore_manager.get_datastore = Mock()
        container.datastore_manager.get_datastore.return_value = datastore
        mock_data = msgpack.packb({'test':'test'},default=encode_ion)

        self.received_packet = False
        self.end_stream      = False

        def output_test(packet):
            self.assertIsInstance(packet,dict)
            if packet == {'test':'test'}:
                self.received_packet = True
            if packet == {}:
                self.end_stream = True

        self.replay.output.publish.side_effect = output_test
        self.replay.publishing = DotDict()
        self.replay.publishing.set = Mock()
        self.replay.publishing['clear'] = Mock() # Really bad


        result = DotDict()
        result.doc.sha1 = 'sha1'
        result.doc.encoding = 'test'

        datastore.query_view.return_value = [result]
        self.replay.container = container
        self.replay.read_persisted_cache = Mock()
        self.replay.read_persisted_cache.return_value = mock_data


        retval = self.replay.replay()

        self.assertTrue(retval)
        self.assertTrue(self.received_packet)
        self.assertTrue(self.end_stream)

    @patch('ion.processes.data.replay.replay_process.DatasetManagementServiceClient')
    @patch('ion.processes.data.replay.replay_process.IonObjectDeserializer')
    def test_get_last_granule(self, deserializer_mock, dsm_cli_mock):
        def check(*args, **kwargs):
            return 'abc'

        test_val = [{'doc':{'persisted_sha1':'abc123', 'encoding_type':'mocktest'}}]
        ReplayProcess.read_persisted_cache = Mock()
        ReplayProcess.read_persisted_cache.side_effect = check

        deserializer_mock().deserialize.return_value = {'test':'test'}
        
        dataset = DotDict()
        dataset.primary_view_key = 'view-key'
        dataset.datastore_name = 'datastore-name'
        read_dataset = Mock()
        read_dataset.return_value = dataset
        dsm_cli_mock().read_dataset = Mock()

        datastore = DotDict()
        datastore.query_view = Mock()
        datastore.query_view.return_value = test_val
        
        container = DotDict()
        container.datastore_manager.get_datastore = Mock()
        container.datastore_manager.get_datastore.return_value = datastore

        retval = self.replay.get_last_granule(container, 'dataset_id')

        self.assertTrue(retval == {'test':'test'})



