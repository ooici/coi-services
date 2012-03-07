'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/inventory/test/data_retriever_test.py
@description Testing Platform for Data Retriver Service
'''
from pyon.core.exception import NotFound
from pyon.datastore.datastore import DataStore
from pyon.public import  StreamSubscriberRegistrar
from pyon.public import PRED, log
from pyon.util.containers import DotDict
from pyon.util.file_sys import FS, FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.net.endpoint import Subscriber
from pyon.public import CFG
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from ion.processes.data.replay_process import llog, ReplayProcess
from prototype.hdf.hdf_codec import HDFEncoder
from prototype.sci_data.constructor_apis import DefinitionTree, PointSupplementConstructor
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from interface.objects import Replay, StreamQuery, BlogPost, BlogAuthor, ProcessDefinition, StreamGranuleContainer
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService

from nose.plugins.attrib import attr
from mock import Mock
import unittest, time
import random
import hashlib
import numpy as np
import pyon.core.bootstrap as bootstrap
import gevent
import unittest
import os

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
        self.mock_ps_create_stream_definition = self.data_retriever_service.clients.pubsub_management.create_stream_definition
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
        self.assertTrue(self.mock_ps_create_stream_definition.called)
        self.assertTrue(self.mock_ps_create_stream.called)
        self.assertTrue(self.mock_rr_create.called)
        self.mock_rr_create_assoc.assert_called_with('replay_id',PRED.hasStream,'12345',None)
        self.assertTrue(self.mock_pd_schedule.called)
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

        self.couch = self.container.datastore_manager.get_datastore('test_data_retriever', profile=DataStore.DS_PROFILE.SCIDATA)
        self.datastore_name = 'test_data_retriever'

        self.dr_cli = DataRetrieverServiceClient(node=self.container.node)
        self.dsm_cli = DatasetManagementServiceClient(node=self.container.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.container.node)
        self.ps_cli = PubsubManagementServiceClient(node=self.container.node)
        self.tms_cli = TransformManagementServiceClient(node=self.container.node)
        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        xs_dot_xp = CFG.core_xps.science_data
        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([bootstrap.get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)

        self.thread_pool = list()

    def make_some_data(self):
        stream_id = 'I am very special'
        definition = SBE37_CDM_stream_definition()
        definition.stream_resource_id = stream_id

        self.couch.create(definition)

        total = 100
        n = 10 # at most n records per granule
        i = 0

        while i < total:
            r = random.randint(1,n)

            psc = PointSupplementConstructor(point_definition=definition, stream_id=stream_id)
            for x in xrange(r):
                i+=1
                point_id = psc.add_point(time=i, location=(0,0,0))
                psc.add_scalar_point_coverage(point_id=point_id, coverage_id='temperature', value=np.random.normal(loc=48.0,scale=4.0, size=1)[0])
                psc.add_scalar_point_coverage(point_id=point_id, coverage_id='pressure', value=np.float32(1.0))
                psc.add_scalar_point_coverage(point_id=point_id, coverage_id='conductivity', value=np.float32(2.0))
            granule = psc.close_stream_granule()
            hdf_string = granule.identifiables[definition.data_stream_id].values
            sha1 = hashlib.sha1(hdf_string).hexdigest().upper()
            with open(FileSystem.get_url(FS.CACHE, '%s.hdf5' % sha1),'w') as f:
                f.write(hdf_string)
            granule.identifiables[definition.data_stream_id].values = ''
            self.couch.create(granule)



    def start_listener(self, stream_id, callback):

        sub = Subscriber(name=(self.XP, 'replay_listener'), callback=callback)
        g = gevent.Greenlet(sub.listen, binding='%s.data' % stream_id)
        g.start()
        self.thread_pool.append(g)

    def tearDown(self):
        super(DataRetrieverServiceIntTest,self).tearDown()
        for greenlet in self.thread_pool:
            greenlet.kill()

    def test_define_replay(self):
        self.make_some_data()
        dsm_cli = self.dsm_cli
        dr_cli = self.dr_cli
        rr_cli = self.rr_cli
        assertions = self.assertTrue
        cc = self.container

        dataset_id = dsm_cli.create_dataset(stream_id='I am very special', datastore_name=self.datastore_name, view_name='datasets/dataset_by_id')
        replay_id, stream_id = dr_cli.define_replay(dataset_id=dataset_id)

        replay = rr_cli.read(replay_id)
        pid = replay.process_id
        assertions(cc.proc_manager.procs.has_key(pid), 'Process was not spawned correctly.')
        assertions(isinstance(cc.proc_manager.procs[pid], ReplayProcess))



    def test_cancel_replay(self):
        self.make_some_data()
        dsm_cli = self.dsm_cli
        dr_cli = self.dr_cli
        rr_cli = self.rr_cli
        assertions = self.assertTrue
        cc = self.container

        dataset_id = dsm_cli.create_dataset(stream_id='I am very special', datastore_name=self.datastore_name, view_name='datasets/dataset_by_id')
        replay_id, stream_id = dr_cli.define_replay(dataset_id=dataset_id)

        replay = rr_cli.read(replay_id)
        pid = replay.process_id
        assertions(cc.proc_manager.procs.has_key(pid), 'Process was not spawned correctly.')
        assertions(isinstance(cc.proc_manager.procs[pid], ReplayProcess))

        dr_cli.cancel_replay(replay_id=replay_id)

        assertions(not cc.proc_manager.procs.has_key(pid),'Process was not terminated correctly.')

    def test_start_replay(self):
        self.make_some_data()
        dsm_cli = self.dsm_cli
        dr_cli = self.dr_cli
        rr_cli = self.rr_cli
        assertions = self.assertTrue
        cc = self.container

        dataset_id = dsm_cli.create_dataset(stream_id='I am very special', datastore_name=self.datastore_name, view_name='datasets/dataset_by_id')
        replay_id, stream_id = dr_cli.define_replay(dataset_id=dataset_id)

        replay = rr_cli.read(replay_id)
        pid = replay.process_id
        assertions(cc.proc_manager.procs.has_key(pid), 'Process was not spawned correctly.')
        assertions(isinstance(cc.proc_manager.procs[pid], ReplayProcess))

        dr_cli.start_replay(replay_id=replay_id)

        time.sleep(0.5)

        dr_cli.cancel_replay(replay_id=replay_id)
        assertions(not cc.proc_manager.procs.has_key(pid),'Process was not terminated correctly.')


    def test_fields_replay(self):
        self.make_some_data()
        dsm_cli = self.dsm_cli
        dr_cli = self.dr_cli
        rr_cli = self.rr_cli
        assertions = self.assertTrue
        cc = self.container

        dataset_id = dsm_cli.create_dataset(stream_id='I am very special', datastore_name=self.datastore_name, view_name='datasets/dataset_by_id')
        replay_id, stream_id = dr_cli.define_replay(dataset_id=dataset_id, delivery_format={'fields':['temperature']})

        replay = rr_cli.read(replay_id)
        pid = replay.process_id
        assertions(cc.proc_manager.procs.has_key(pid), 'Process was not spawned correctly.')
        assertions(isinstance(cc.proc_manager.procs[pid], ReplayProcess))

        result = gevent.event.AsyncResult()

        def check_msg(msg, header):
            assertions(isinstance(msg, StreamGranuleContainer), 'Msg is not a container')
            hdf_string = msg.identifiables[msg.data_stream_id].values
            sha1 = hashlib.sha1(hdf_string).hexdigest().upper()
            log.debug('Sha1 matches')
            log.debug('Dumping file so you can inspect it.')
            log.debug('Records: %d' % msg.identifiables['record_count'].value)
            with open(FileSystem.get_url(FS.TEMP,'%s.cap.hdf5' % sha1[:8]),'w') as f:
                f.write(hdf_string)
                log.debug('Stream Capture: %s', f.name)
            result.set(True)

        self.start_listener(stream_id=stream_id, callback=check_msg)

        dr_cli.start_replay(replay_id=replay_id)

        assertions(result.get(timeout=3), 'Did not receive a msg from replay')

        dr_cli.cancel_replay(replay_id=replay_id)
        assertions(not cc.proc_manager.procs.has_key(pid),'Process was not terminated correctly.')

    def test_advanced_replay(self):
        self.make_some_data()
        dsm_cli = self.dsm_cli
        dr_cli = self.dr_cli
        rr_cli = self.rr_cli
        assertions = self.assertTrue
        cc = self.container

        dataset_id = dsm_cli.create_dataset(stream_id='I am very special', datastore_name=self.datastore_name, view_name='datasets/dataset_by_id')
        replay_id, stream_id = dr_cli.define_replay(dataset_id=dataset_id, delivery_format={'fields':['temperature'], 'time':(1,71),'records':10})

        replay = rr_cli.read(replay_id)
        pid = replay.process_id
        assertions(cc.proc_manager.procs.has_key(pid), 'Process was not spawned correctly.')
        assertions(isinstance(cc.proc_manager.procs[pid], ReplayProcess))

        result = gevent.event.AsyncResult()

        def check_msg(msg, header):
            assertions(isinstance(msg, StreamGranuleContainer), 'Msg is not a container')
            hdf_string = msg.identifiables[msg.data_stream_id].values
            sha1 = hashlib.sha1(hdf_string).hexdigest().upper()
            log.debug('Sha1 matches')
            log.debug('Dumping file so you can inspect it.')
            log.debug('Records: %d' % msg.identifiables['record_count'].value)
            with open(FileSystem.get_url(FS.TEMP,'%s.cap.hdf5' % sha1[:8]),'w') as f:
                f.write(hdf_string)
                log.debug('Stream Capture: %s', f.name)
            result.set(True)

        self.start_listener(stream_id=stream_id, callback=check_msg)

        dr_cli.start_replay(replay_id=replay_id)

        assertions(result.get(timeout=3), 'Did not receive a msg from replay')

        dr_cli.cancel_replay(replay_id=replay_id)
        assertions(not cc.proc_manager.procs.has_key(pid),'Process was not terminated correctly.')

