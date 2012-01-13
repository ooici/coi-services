'''
@author Luke Campbell
@file ion/services/dm/transformation/test_transform_service.py
@description Unit Test for Transform Management Service
'''

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from pyon.container.cc import Container
from pyon.core.exception import NotFound, BadRequest
from pyon.util.containers import DotDict
from pyon.public import IonObject, RT, log, AT
from pyon.util.unit_test import PyonTestCase
from mock import Mock
from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.dm.transformation.transform_management_service import TransformManagementService
import unittest

@attr('UNIT',group='dm')
class TransformManagementServiceTest(PyonTestCase):
    """Unit test for TransformManagementService

    """
    def setUp(self):
        mock_clients = self._create_service_mock('transform_management_service')
        self.transform_service = TransformManagementService()
        self.transform_service.clients = mock_clients
        self.transform_service.clients.pubsub_management = DotDict()
        self.transform_service.clients.pubsub_management['XP'] = 'science.data'
        self.transform_service.clients.pubsub_management['create_stream'] = Mock()
        self.transform_service.clients.pubsub_management['create_subscription'] = Mock()
        self.transform_service.clients.pubsub_management['register_producer'] = Mock()
        self.transform_service.clients.pubsub_management['activate_subscription'] = Mock()
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

    def test_create_transform(self):
        # mocks
        self.mock_pd_schedule.return_value = 'mock_pid'
        self.mock_rr_create.return_value = ('transform_id','rev')
        predicates = [AT.hasProcessDefinition, AT.hasSubscription, AT.hasOutStream]
        def create_assoc(*args, **kwargs):
            self.assertEquals(args[1],predicates.pop(0))
        self.mock_rr_create_assoc.side_effect = create_assoc

        # execution
        res = self.transform_service.create_transform('subscription_id','out_stream','process_definition_id',
                                                        {'name':'test_transform'})

        # assertions
        # it should create the association between the ids and the transform_id
        self.assertEquals(self.mock_rr_create_assoc.call_count,3)
        # it should launch the process
        self.assertTrue(self.mock_pd_schedule.called)


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
        mock_id_list = [(['proc_def_id'],'rev'),
                        (['sub_id'],'rev'),
                        (['stream_id'],'rev')]
        def mock_ids(*args,**kwargs):
            return mock_id_list.pop()
        self.mock_rr_find.side_effect = mock_ids

        self.transform_service.read_transform = Mock()
        self.transform_service.read_transform.return_value = DotDict({'process_id':'123'})

        associations=[['first'],['second'],['third']]
        def mock_association(*args,**kwargs):
            return associations.pop()
        self.mock_rr_assoc.side_effect = mock_association

        # execution
        self.transform_service.delete_transform('mock_transform_id')

        # assertions
        self.transform_service.read_transform.assert_called_with(transform_id='mock_transform_id')
        self.assertEquals(self.mock_rr_find.call_count,3)
        self.mock_pd_cancel.assert_called_with('123')
        self.assertEquals(self.mock_rr_del_assoc.call_count,3)
        self.assertEquals(self.mock_rr_delete.call_count,4)

        # ends case checking
        #@todo: fill this out

            

    def test_bind_transform(self):
        # mocks
        self.mock_rr_find.return_value = [['id'],'garbage']

        # execution
        ret = self.transform_service.bind_transform('transform_id')

        # assertions
        self.mock_rr_find.assert_called_with('transform_id',AT.hasSubscription,RT.Subscription,True)
        self.mock_ps_activate.assert_called_with('id')



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


        self.pubsub_cli = PubsubManagementServiceClient(node=self.cc.node)
        self.tms_cli = TransformManagementServiceClient(node=self.cc.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.cc.node)

        # Initialize the parameters that create_transform needs
        self.ctd_output_stream = IonObject(RT.Stream,name='ctd1 output', description='output from a ctd')
        self.ctd_output_stream.original = True
        self.ctd_output_stream.mimetype = 'hdf'
        self.ctd_output_stream.producers = ['science.data']
        self.ctd_output_stream_id = self.pubsub_cli.create_stream(self.ctd_output_stream)

        self.ctd_subscription = IonObject(RT.Subscription,name='ctd1 subscription', description='subscribe to this if you want ctd1 data')
        self.ctd_subscription.query['stream_id'] = self.ctd_output_stream_id
        self.ctd_subscription.exchange_name = 'science.data'
        self.ctd_subscription_id = self.pubsub_cli.create_subscription(self.ctd_subscription)



        self.data_product_stream = IonObject(RT.Stream,name='self.data_product_stream1', descriptoin='a simple data product stream test')
        self.data_product_stream.original = True
        self.data_product_stream.producers = ['science.data']
        self.data_product_stream_id = self.pubsub_cli.create_stream(self.data_product_stream)

        self.process_definition = IonObject(RT.ProcessDefinition, name='transform process definition')
        self.process_definition_id, _ = self.rr_cli.create(self.process_definition)

    def tearDown(self):
        # clean up resources alloced during set up
        self.pubsub_cli.delete_subscription(self.ctd_subscription_id)

        self.pubsub_cli.delete_stream(self.data_product_stream_id)
        self.rr_cli.delete(self.process_definition_id)
        IonIntegrationTestCase.tearDown(self)

        
    def test_create_transform(self):
        transform_id = self.tms_cli.create_transform(
              self.ctd_subscription_id,
              self.ctd_output_stream_id,
              self.process_definition_id,
                {'name':'basic transform'})

        # test transform creation in rr
        transform = self.rr_cli.read(transform_id)
        self.assertEquals(transform.name,'basic transform')


        # test associations
        predicates = [AT.hasSubscription, AT.hasOutStream, AT.hasProcessDefinition]
        assocs = []
        for p in predicates:
            assocs += self.rr_cli.find_associations(transform_id,p,id_only=True)
        self.assertEquals(len(assocs),3)

    def test_read_transform(self):
        transform_object = IonObject(RT.Transform,name='blank_transform')
        transform_id, rev = self.rr_cli.create(transform_object)
        res = self.tms_cli.read_transform(transform_id)

        # test the object
        self.assertEquals(res._id,transform_id)

        # clean up
        self.rr_cli.delete(res)

    @unittest.skip('not implemented yet')
    def test_bind_transform(self):
        transform_id = self.tms_cli.create_transform(
            self.ctd_subscription_id,
            self.ctd_output_stream_id,
            self.process_definition_id,
                {'name':'basic transform'})
        self.tms_cli.bind_transform(transform_id)
