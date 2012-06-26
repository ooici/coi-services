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
from ion.processes.data.replay.replay_process_a import ReplayProcess
from mock import Mock
from nose.plugins.attrib import attr
import msgpack

@attr('UNIT',group='dm')
class ReplayProcessUnitTest(PyonTestCase):
    def setUp(self):
        self.replay = ReplayProcess()
        self.replay.dataset_id = 'dataset'
        self.replay.dataset = DotDict()
        self.replay.dataset.datastore_name='datasets'
        self.replay.deliver_format = {}
        self.replay.start_time = None
        self.replay.end_time = None
        self.replay.output =  DotDict()
        self.replay.output.publish = Mock()

    def test_execute_replay(self):
        datastore = DotDict()
        container = DotDict()
        datastore.query_view = Mock()
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


        result = DotDict()
        result.doc.sha1 = 'sha1'
        result.doc.encoding = 'test'

        datastore.query_view.return_value = [result]
        self.replay.container = container
        self.replay.read_persisted_cache = Mock()
        self.replay.read_persisted_cache.return_value = mock_data


        retval = self.replay.execute_replay()

        self.assertTrue(retval)
        self.assertTrue(self.received_packet)
        self.assertTrue(self.end_stream)


