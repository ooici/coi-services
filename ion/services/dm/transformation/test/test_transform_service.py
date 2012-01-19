'''
@author Luke Campbell
@file ion/services/dm/transformation/test_transform_service.py
@description Unit Test for Transform Management Service
'''

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from pyon.core.exception import NotFound, BadRequest
from pyon.util.containers import DotDict
from pyon.public import IonObject, RT, log, AT
from pyon.util.unit_test import PyonTestCase
from mock import Mock
from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.dm.transformation.transform_management_service import TransformManagementService
from ion.services.dm.transformation.example.transform_example import TransformExample
import unittest

@attr('UNIT',group='dm')
class TransformManagementServiceTest(PyonTestCase):
    """Unit test for TransformManagementService

    """
    def setUp(self):
        mock_clients = self._create_service_mock('transform_management')
        self.transform_service = TransformManagementService()
        self.transform_service.clients = mock_clients
        self.transform_service.clients.pubsub_management = DotDict()
        self.transform_service.clients.pubsub_management['XP'] = 'science.data'
        self.transform_service.clients.pubsub_management['create_stream'] = Mock()
        self.transform_service.clients.pubsub_management['create_subscription'] = Mock()
        self.transform_service.clients.pubsub_management['register_producer'] = Mock()
        self.transform_service.clients.pubsub_management['activate_subscription'] = Mock()
        self.transform_service.clients.pubsub_management['read_subscription'] = Mock()
        self.transform_service.container = DotDict()
        self.transform_service.container['spawn_process'] = Mock()
        self.transform_service.container['id'] = 'mock_container_id'
        self.transform_service.container['proc_manager'] = DotDict()
        self.transform_service.container.proc_manager['terminate_process'] = Mock()
        # CRUD Shortcuts
        self.mock_rr_create = self.transform_service.clients.resource_registry.create
        self.mock_rr_read = self.transform_service.clients.resource_registry.read
        self.mock_rr_update = self.transform_service.clients.resource_registry.update
        self.mock_rr_delete = self.transform_service.clients.resource_registry.delete
        self.mock_rr_find = self.transform_service.clients.resource_registry.find_objects
        self.mock_rr_assoc = self.transform_service.clients.resource_registry.find_associations
        self.mock_rr_create_assoc = self.transform_service.clients.resource_registry.create_association
        self.mock_rr_del_assoc = self.transform_service.clients.resource_registry.delete_association

        self.mock_pd_create = self.transform_service.clients.process_dispatcher_service.create_process_definition
        self.mock_pd_read = self.transform_service.clients.process_dispatcher_service.read_process_definition
        self.mock_pd_update = self.transform_service.clients.process_dispatcher_service.update_process_definition
        self.mock_pd_delete = self.transform_service.clients.process_dispatcher_service.delete_process_definition
        self.mock_pd_schedule = self.transform_service.clients.process_dispatcher_service.schedule_process
        self.mock_pd_cancel = self.transform_service.clients.process_dispatcher_service.cancel_process

        self.mock_ps_create_stream = self.transform_service.clients.pubsub_management.create_stream
        self.mock_ps_create_sub = self.transform_service.clients.pubsub_management.create_subscription
        self.mock_ps_register = self.transform_service.clients.pubsub_management.register_producer
        self.mock_ps_activate = self.transform_service.clients.pubsub_management.activate_subscription
        self.mock_ps_read_sub = self.transform_service.clients.pubsub_management.read_subscription

        self.mock_cc_spawn = self.transform_service.container.spawn_process
        self.mock_cc_terminate = self.transform_service.container.proc_manager.terminate_process

    def test_create_transform(self):
        '''This is currently set up to test the example spawning.
        '''
        # mocks
        self.mock_cc_spawn.return_value = 'mock_pid'
        self.mock_ps_read_sub.return_value = DotDict({'exchange_name':'mock_exchange'})
        self.mock_rr_create.return_value = ('mock_transform_id','junk')

        # execution
        configuration = {'name':'mock_transform'}
        res = self.transform_service.create_transform('subscription_id','out_stream','process_definition_id',
                                                        configuration)

        # assertions
        self.mock_ps_read_sub.assert_called_once_with(subscription_id='subscription_id')
        #params are too complicated to mock and check for now
        self.assertTrue(self.mock_cc_spawn.called)
        self.assertTrue(self.mock_rr_create_assoc.called,3)
        self.assertEquals(res,'mock_transform_id')


    def test_update_transform(self):
        with self.assertRaises(NotImplementedError):
            self.transform_service.update_transform()


    def test_read_transform(self):
        # mocks
        self.mock_rr_read.return_value = 'mock_object'

        # execution
        ret = self.transform_service.read_transform('transform_id')

        # assertions
        self.mock_rr_read.assert_called_with('transform_id','')
        self.assertEquals(ret,'mock_object')


    def test_delete_transform(self):
        # mocks
        self.transform_service.read_transform = Mock()
        self.transform_service.read_transform.return_value = DotDict({'process_id':'pid'})
        find_list = ['process_definition','subscription_id','stream_id']
        def finds(*args, **kwargs):
            return ([find_list.pop(0)],'junk')
        self.mock_rr_find.side_effect = finds
        association_list = ['one','two','three']
        def associations(*args,**kwargs):
            return [association_list.pop(0)]
        self.mock_rr_assoc.side_effect = associations


        # execution
        ret = self.transform_service.delete_transform('mock_transform_id')

        # assertions
        self.transform_service.read_transform.assert_called_with(transform_id='mock_transform_id')
        self.mock_cc_terminate.assert_called_with('pid')
        self.assertEquals(self.mock_rr_find.call_count,3)
        self.assertEquals(self.mock_rr_del_assoc.call_count,3)
        self.assertEquals(self.mock_rr_delete.call_count,1)

            

    def test_activate_transform(self):
        # mocks
        self.mock_rr_find.return_value = [['id'],'garbage']

        # execution
        ret = self.transform_service.activate_transform('transform_id')

        # assertions
        self.mock_rr_find.assert_called_with('transform_id',AT.hasSubscription,RT.Subscription,True)
        self.mock_ps_activate.assert_called_with('id')

        # ---
    def test_activate_transform_nonexist(self):
        # mocks
        self.mock_rr_find.return_value = ([],'')

        # execution
        with self.assertRaises(NotFound):
            ret = self.transform_service.activate_transform('transform_id')



    def test_schedule_transform(self):
        # not implemented
        with self.assertRaises(NotImplementedError):
            self.transform_service.schedule_transform()

@attr('INT', group='dm')
class TransformManagementServiceIntTest(IonIntegrationTestCase):

    def setUp(self):
        # set up the container
        self._start_container()

        self.cc = ContainerAgentClient(node=self.container.node,name=self.container.name)

        self.cc.start_rel_from_url('res/deploy/r2deploy.yml')

        self.pubsub_cli = PubsubManagementServiceClient(node=self.cc.node)
        self.tms_cli = TransformManagementServiceClient(node=self.cc.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.cc.node)

        self.input_stream = IonObject(RT.Stream,name='ctd1 output', description='output from a ctd')
        self.input_stream.original = True
        self.input_stream.mimetype = 'hdf'
        self.input_stream_id = self.pubsub_cli.create_stream(self.input_stream)

        self.input_subscription = IonObject(RT.Subscription,name='ctd1 subscription', description='subscribe to this if you want ctd1 data')
        self.input_subscription.query['stream_id'] = self.input_stream_id
        self.input_subscription.exchange_name = 'a queue'
        self.input_subscription_id = self.pubsub_cli.create_subscription(self.input_subscription)

        self.output_stream = IonObject(RT.Stream,name='transform output', description='output from the transform process')
        self.output_stream.original = True
        self.output_stream.mimetype='raw'
        self.output_stream_id = self.pubsub_cli.create_stream(self.output_stream)


        self.process_definition = IonObject(RT.ProcessDefinition,name='transform_process')
        self.process_definition_id, _= self.rr_cli.create(self.process_definition)


        
    def test_create_transform(self):
        transform_id = self.tms_cli.create_transform(
              in_subscription_id=self.input_subscription_id,
              out_stream_id=self.output_stream_id,
              process_definition_id=self.process_definition_id,
              configuration= {'name':'basic transform'})

        # test transform creation in rr
        transform = self.rr_cli.read(transform_id)
        self.assertEquals(transform.name,'basic transform')


        # test associations
        predicates = [AT.hasSubscription, AT.hasOutStream, AT.hasProcessDefinition]
        assocs = []
        for p in predicates:
            assocs += self.rr_cli.find_associations(transform_id,p,id_only=True)
        self.assertEquals(len(assocs),3)

        # test process creation
        transform = self.tms_cli.read_transform(transform_id)
        pid = transform.process_id
        proc = self.container.proc_manager.procs[pid]
        self.assertTrue(isinstance(proc,TransformExample))

    def test_read_transform_exists(self):
        trans_obj = IonObject(RT.Transform,name='trans_obj')
        trans_id, _ = self.rr_cli.create(trans_obj)

        res = self.tms_cli.read_transform(trans_id)
        actual = self.rr_cli.read(trans_id)

        self.assertEquals(res._id,actual._id)

    def test_read_transform_nonexist(self):
        with self.assertRaises(NotFound) as e:
            res = self.tms_cli.read_transform('123')

    def test_activate_transform(self):
        transform_id = self.tms_cli.create_transform(
            in_subscription_id=self.input_subscription_id,
            out_stream_id=self.output_stream_id,
            process_definition_id=self.process_definition_id,
            configuration= {'name':'basic transform', 'exchange_name':self.input_subscription.exchange_name})

        self.tms_cli.activate_transform(transform_id)

    def test_activate_transform_nonexist(self):
        with self.assertRaises(NotFound):
            self.tms_cli.activate_transform('1234')

    def test_delete_transform(self):
        transform_id = self.tms_cli.create_transform(
            in_subscription_id=self.input_subscription_id,
            out_stream_id=self.output_stream_id,
            process_definition_id=self.process_definition_id,
            configuration= {'name':'basic transform', 'exchange_name':self.input_subscription.exchange_name})
        self.tms_cli.delete_transform(transform_id)

        # assertions
        with self.assertRaises(NotFound):
            self.rr_cli.read(transform_id)


    def test_delete_transform_nonexist(self):
        with self.assertRaises(NotFound):
            self.tms_cli.delete_transform('123')

