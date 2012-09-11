#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Wed Aug 15 10:13:02 EDT 2012
@file test_mux.py
@brief Test for the mux/demux transforms
'''

from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.ion.stream import StandaloneStreamSubscriber, StandaloneStreamPublisher
from ion.processes.data.transforms.mux import DemuxTransform
from mock import Mock
from nose.plugins.attrib import attr
from pyon.ion.exchange import ExchangeNameQueue
from pyon.util.containers import DotDict
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
import gevent

@attr('UNIT',group='dm')
class DemuxTransformUnitTest(PyonTestCase):
    def setUp(self):
        self.demuxer = DemuxTransform()

    def test_on_start(self):
        pass #No cover because of complexity (INT test covers it)

    def test_on_quit(self):
        pass # No cover because of complexity

    def test_recv_packet(self):
        self.demuxer.publish = Mock()

        self.demuxer.recv_packet('incoming',{})
        self.demuxer.publish.assert_called_once_with('incoming',None)

    def test_publish(self):
        mock_publish = DotDict(publish=Mock())
        self.demuxer.publishers = [mock_publish]
        self.demuxer.publish('incoming',None)
        mock_publish.publish.assert_called_once_with('incoming')

@attr('INT',group='dm')
class DemuxTransformIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.pubsub_client = PubsubManagementServiceClient()
        self.queue_cleanup = list()


    def tearDown(self):
        for queue in self.queue_cleanup:
            if isinstance(queue,ExchangeNameQueue):
                queue.delete()
            elif isinstance(queue,basestring):
                xn = self.container.ex_manager.create_xn_queue(queue)
                xn.delete()


    

    def test_demux(self):
        self.stream0, self.route0 = self.pubsub_client.create_stream('stream0', exchange_point='test')
        self.stream1, self.route1 = self.pubsub_client.create_stream('stream1', exchange_point='main_data')
        self.stream2, self.route2 = self.pubsub_client.create_stream('stream2', exchange_point='alt_data')
        
        self.r_stream1 = gevent.event.Event()
        self.r_stream2 = gevent.event.Event()

        def process(msg, stream_route, stream_id):
            if stream_id == self.stream1:
                self.r_stream1.set()
            elif stream_id == self.stream2:
                self.r_stream2.set()
        
        self.container.spawn_process('demuxer', 'ion.processes.data.transforms.mux', 'DemuxTransform', {'process':{'output_streams':[self.stream1, self.stream2]}}, 'demuxer_pid')
        self.queue_cleanup.append('demuxer_pid') 
        
        sub1 = StandaloneStreamSubscriber('sub1', process)
        sub2 = StandaloneStreamSubscriber('sub2', process)
        sub1.xn.bind('%s.data' % self.stream1, self.container.ex_manager.create_xp('main_data'))
        sub2.xn.bind('%s.data' % self.stream2, self.container.ex_manager.create_xp('alt_data'))
        sub1.start()
        sub2.start()

        self.queue_cleanup.append(sub1.xn)
        self.queue_cleanup.append(sub2.xn)
        xn = self.container.ex_manager.create_xn_queue('demuxer_pid')
        xn.bind('%s.data' % self.stream0, self.container.ex_manager.create_xp('input_data'))
        domino = StandaloneStreamPublisher(self.stream0, self.route0)
        domino.publish('test')

        self.assertTrue(self.r_stream1.wait(2))
        self.assertTrue(self.r_stream2.wait(2))

        self.container.proc_manager.terminate_process('demuxer_pid')
        
        sub1.stop()
        sub2.stop()
