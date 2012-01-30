'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/inventory/test/data_retriever_test.py
@description Testing Platform for Data Retriver Service
'''
import gevent
from mock import Mock
from interface.objects import Replay, Query, StreamQuery
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService
from pyon.core.exception import NotFound
from pyon.ion.endpoint import StreamSubscriber
from pyon.ion.resource import PRED, RT
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
@attr('UNIT',group='dm')
class DataRetrieverServiceTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('data_retriever')
        self.data_retriever_service = DataRetrieverService()
        self.data_retriever_service.clients = mock_clients
        self.mock_rr_create = self.data_retriever_service.clients.resource_registry.create
        self.mock_rr_create_assoc = self.data_retriever_service.clients.resource_registry.create_association
        self.mock_rr_read = self.data_retriever_service.clients.resource_registry.read
        self.mock_rr_update = self.data_retriever_service.clients.resource_registry.update
        self.mock_rr_delete = self.data_retriever_service.clients.resource_registry.delete
        self.mock_rr_delete_assoc = self.data_retriever_service.clients.resource_registry.delete_association
        self.mock_rr_find_assoc = self.data_retriever_service.clients.resource_registry.find_associations
        self.mock_ps_create_stream = self.data_retriever_service.clients.pubsub_management.create_stream
        self.data_retriever_service.container = DotDict({'id':'123','spawn_process':Mock(),'proc_manager':DotDict({'terminate_process':Mock(),'procs':[]})})
        self.mock_cc_spawn = self.data_retriever_service.container.spawn_process
        self.mock_cc_terminate = self.data_retriever_service.container.proc_manager.terminate_process

    def test_define_replay(self):
        #mocks
        self.mock_ps_create_stream.return_value = 'stream_id'
        self.mock_rr_create.return_value = ('replay_id','garbage')
        self.mock_cc_spawn.return_value = 'agent_id'


        # execution
        r,s = self.data_retriever_service.define_replay('dataset_id')

        # assertions
        self.mock_ps_create_stream.assert_called_with('', True, '', '', '', '')
        self.assertTrue(self.mock_rr_create.called)
        self.mock_rr_create_assoc.assert_called_with('replay_id',PRED.hasStream,'stream_id',None)
        self.assertTrue(self.mock_cc_spawn.called)
        self.assertTrue(self.mock_rr_update.called)
        self.assertEquals(r,'replay_id')
        self.assertEquals(s,'stream_id')



    @unittest.skip('Can\'t do unit test here')
    def test_start_replay(self):
        pass


    def test_cancel_replay(self):
        #mocks
        self.mock_rr_find_assoc.return_value = [1,2,3]

        replay = Replay()
        replay.process_id = '1'
        self.mock_rr_read.return_value = replay

        #execution
        self.data_retriever_service.cancel_replay('replay_id')

        #assertions
        self.assertEquals(self.mock_rr_delete_assoc.call_count,3)
        self.mock_rr_delete.assert_called_with('replay_id')

        self.mock_cc_terminate.assert_called_with('1')




@attr('INT', group='dm')
class DataRetrieverServiceIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(DataRetrieverServiceIntTest,self).setUp()
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.dr_cli = DataRetrieverServiceClient(node=self.container.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.container.node)
        self.ps_cli = PubsubManagementServiceClient(node=self.container.node)


    def tearDown(self):
        super(DataRetrieverServiceIntTest,self).tearDown()


    def test_define_replay(self):
        replay_id, stream_id = self.dr_cli.define_replay('123')

        # assert resources created
        replay = self.rr_cli.read(replay_id)
        self.assertTrue(replay._id == replay_id)

        stream = self.rr_cli.read(stream_id)
        self.assertTrue(stream._id == stream_id)

        # assert association created
        assocs = self.rr_cli.find_associations(replay_id,PRED.hasStream, id_only=True)
        self.assertTrue(len(assocs)>0)

        # assert process exists
        self.assertTrue(self.container.proc_manager.procs[replay.process_id])



    def test_cancel_replay(self):
        replay_id, stream_id = self.dr_cli.define_replay('123')
        replay = self.rr_cli.read(replay_id)

        # assert that the process was created

        self.assertTrue(self.container.proc_manager.procs[replay.process_id])

        # delete the process
        self.dr_cli.cancel_replay(replay_id)

        # assert that the resource was deleted
        with self.assertRaises(NotFound):
            self.rr_cli.read(replay_id)

        # assert the process has stopped
        proc = self.container.proc_manager.procs.get(replay.process_id,None)
        self.assertTrue(not proc)

    def test_start_replay(self):
        replay_id, stream_id = self.dr_cli.define_replay('123')
        replay = self.rr_cli.read(replay_id)


        # assert that the process was created

        self.assertTrue(self.container.proc_manager.procs[replay.process_id])

        # pattern from Tim G
        ar = gevent.event.AsyncResult()
        def consume(message, headers):
            ar.set(message)

        subscriber = StreamSubscriber(node=self.container.node,
            process=self,
            name=('science_data','test_queue'),
            callback=lambda m,h: consume(m,h))
        subscriber.start()

        query = StreamQuery(stream_ids=[stream_id])
        subscription_id = self.ps_cli.create_subscription(query=query,exchange_name='test_queue')
        self.ps_cli.activate_subscription(subscription_id)

        self.dr_cli.start_replay(replay_id)
        self.assertEqual(ar.get(timeout=10),{'num':0})

        self.dr_cli.start_replay(replay_id)
        self.assertEqual(ar.get(timeout=10),{'num':1})


        subscriber.stop()