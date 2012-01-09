'''
@author Luke Campbell
@file ion/services/dm/transformation/test_transform_service.py
@description Unit Test for Transform Management Service
'''
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
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
    pass
#    def setUp(self):
#        # start the container
#        self._start_container()
#
#        # Establish endpoint with container
#        self.container_client = ContainerAgentClient(node=self.container.node,name=self.container.name)
#        self.container_client.start_rel_from_url('res/deploy/r2deploy.yml')
#
#        self.tms_client = TransformManagementServiceClient(node=self.container.node)
#        self.pubsub_client = PubsubManagementServiceClient(node=self.container.node)
#
#    def test_createTransform(self):
#        # Make an output stream with pubsub
#        output_stream = IonObject(RT.Stream,name='test_product_stream_out')
#        output_stream.original = True
#        output_stream.producers = ['science.data']
#
#        output_stream_id = self.pubsub_client.create_stream(output_stream)
#
#        # Create the transform object
#        transform = IonObject(RT.Transform,name='test_transform')
#        transform.out_stream_id = output_stream_id
#        transform_id = self.tms_client.create_transform(transform)
#
#        # Bind the transform object
#        self.tms_client.bind_transform(transform_id)
#
#
#
