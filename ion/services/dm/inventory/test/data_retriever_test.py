'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/inventory/test/data_retriever_test.py
@description Testing Platform for Data Retriver Service
'''
import gevent
from mock import Mock
from interface.objects import Replay, StreamQuery, BlogPost, BlogAuthor, ProcessDefinition
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService
from prototype.sci_data.ctd_stream import ctd_stream_packet
from pyon.core.exception import NotFound
from pyon.datastore.datastore import DataStore
from pyon.public import  StreamSubscriberRegistrar
from pyon.public import PRED
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest, time
from pyon.public import log

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
        self.mock_pd_schedule = self.data_retriever_service.clients.process_dispatcher.schedule_process
        self.mock_pd_cancel = self.data_retriever_service.clients.process_dispatcher.cancel_process
        self.mock_ds_read = self.data_retriever_service.clients.dataset_management.read_dataset
        self.data_retriever_service.process_definition = ProcessDefinition()
        self.data_retriever_service.process_definition.executable['module'] = 'ion.processes.data.replay_process'
        self.data_retriever_service.process_definition.executable['class'] = 'ReplayProcess'

        self.data_retriever_service.process_definition_id = 'mock_procdef_id'

    def test_define_replay(self):
        #mocks
        self.mock_ps_create_stream.return_value = '12345'
        self.mock_rr_create.return_value = ('replay_id','garbage')
        self.mock_ds_read.return_value = DotDict({
            'datastore_name':'unittest',
            'view_name':'garbage',
            'primary_view_key':'primary key'})

        self.mock_pd_schedule.return_value = 'process_id'

        config = {'process':{
            'query':'myquery',
            'datastore_name':'unittest',
            'view_name':'garbage',
            'key_id':'primary key',
            'delivery_format':None,
            'publish_streams':{'output':'12345'}
        }}


        # execution
        r,s = self.data_retriever_service.define_replay(dataset_id='dataset_id', query='myquery')

        # assertions
        self.mock_ps_create_stream.assert_called_with('', True, None, '', '', '')
        self.assertTrue(self.mock_rr_create.called)
        self.mock_rr_create_assoc.assert_called_with('replay_id',PRED.hasStream,'12345',None)
        self.mock_pd_schedule.assert_called_with('mock_procdef_id',None,config )
        self.assertTrue(self.mock_rr_update.called)
        self.assertEquals(r,'replay_id')
        self.assertEquals(s,'12345')



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

        self.mock_pd_cancel.assert_called_with('1')




@attr('INT', group='dm')
class DataRetrieverServiceIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(DataRetrieverServiceIntTest,self).setUp()
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.couch = self.container.datastore_manager.get_datastore('test_data_retriever', profile=DataStore.DS_PROFILE.EXAMPLES)
        self.datastore_name = 'test_data_retriever'

        self.dr_cli = DataRetrieverServiceClient(node=self.container.node)
        self.dsm_cli = DatasetManagementServiceClient(node=self.container.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.container.node)
        self.ps_cli = PubsubManagementServiceClient(node=self.container.node)


    def tearDown(self):
        super(DataRetrieverServiceIntTest,self).tearDown()



    def test_define_replay(self):
        dataset_id = self.dsm_cli.create_dataset(
            stream_id='12345',
            datastore_name=self.datastore_name,
            view_name='posts/posts_join_comments',
            name='test define replay'
        )
        replay_id, stream_id = self.dr_cli.define_replay(dataset_id=dataset_id)

        replay = self.rr_cli.read(replay_id)

        # Assert that the process was created

        self.assertTrue(self.container.proc_manager.procs[replay.process_id])

        self.dr_cli.cancel_replay(replay_id)

    def test_cancel_replay(self):
        dataset_id = self.dsm_cli.create_dataset(
            stream_id='12345',
            datastore_name=self.datastore_name,
            view_name='posts/posts_join_comments',
            name='test define replay'
        )
        replay_id, stream_id = self.dr_cli.define_replay(dataset_id=dataset_id)

        replay = self.rr_cli.read(replay_id)

        # Assert that the process was created

        self.assertTrue(self.container.proc_manager.procs[replay.process_id])

        self.dr_cli.cancel_replay(replay_id)

        # assert that the process is no more
        self.assertFalse(replay.process_id in self.container.proc_manager.procs)

        # assert that the resource no longer exists
        with self.assertRaises(NotFound):
            self.rr_cli.read(replay_id)

    def test_start_replay(self):
        post = BlogPost(title='test blog post', post_id='12345', author=BlogAuthor(name='Jon Doe'), content='this is a blog post',
        updated=time.strftime("%Y-%m-%dT%H:%M%S-05"))

        dataset_id = self.dsm_cli.create_dataset(
            stream_id='12345',
            datastore_name=self.datastore_name,
            view_name='posts/posts_join_comments',
            name='blog posts test'
        )

        self.couch.create(post)

        replay_id, stream_id = self.dr_cli.define_replay(dataset_id)
        replay = self.rr_cli.read(replay_id)


        # assert that the process was created

        self.assertTrue(self.container.proc_manager.procs[replay.process_id])

        # pattern from Tim G
        ar = gevent.event.AsyncResult()
        def consume(message, headers):
            ar.set(message)

        stream_subscriber = StreamSubscriberRegistrar(process=self.container, node=self.container.node)
        subscriber = stream_subscriber.create_subscriber(exchange_name='test_queue', callback=consume)
        subscriber.start()

        query = StreamQuery(stream_ids=[stream_id])
        subscription_id = self.ps_cli.create_subscription(query=query,exchange_name='test_queue')
        self.ps_cli.activate_subscription(subscription_id)

        self.dr_cli.start_replay(replay_id)
        self.assertEqual(ar.get(timeout=10).post_id,post.post_id)

        subscriber.stop()

    def test_chop_chop(self):
        # Override couch

        self.couch = self.container.datastore_manager.get_datastore(
            ds_name='chopping_block',
            profile=DataStore.DS_PROFILE.SCIDATA
        )
        self.datastore_name = 'chopping_block'
        granule = ctd_stream_packet(
            stream_id='this_is_only_a_test',
            time='12345', #Same combo on my luggage
            create_hdf=False
        )

        self.couch.create(granule)
        log.debug("Granule: %s", granule)

        dataset_id = self.dsm_cli.create_dataset(
            stream_id='this_is_only_a_test',
            datastore_name=self.datastore_name,
            view_name='datasets/dataset_by_id',
            name='sci_data_granule_chop'
        )

        replay_id, stream_id = self.dr_cli.define_replay(
            dataset_id=dataset_id,
            delivery_format={'chop':True}
        )

        replay = self.rr_cli.read(replay_id)
        self.assertTrue(self.container.proc_manager.procs[replay.process_id])

        async_result = gevent.event.AsyncResult()
        def consume(message, headers):
            async_result.set(message)

        stream_subscriber = StreamSubscriberRegistrar(process=self.container, node=self.container.node)
        subscriber = stream_subscriber.create_subscriber(exchange_name = 'chopping_block', callback=consume)
        subscriber.start()

        query = StreamQuery(stream_ids=[stream_id])
        subscription_id = self.ps_cli.create_subscription(query=query, exchange_name='chopping_block')
        self.ps_cli.activate_subscription(subscription_id=subscription_id)
        self.dr_cli.start_replay(replay_id)

        for fields in xrange(4):
            self.assertTrue(async_result.get(timeout=10))


        subscriber.stop()
        self.dr_cli.cancel_replay(replay_id=replay_id)