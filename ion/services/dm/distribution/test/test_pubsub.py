#!/usr/bin/env python

'''
@file ion/services/dm/distribution/test/test_pubsub.py
@author Jamie Chen
@test ion.services.dm.distribution.pubsub_management_service Unit test suite to cover all pub sub mgmt service code
'''
from mock import Mock
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from pyon.core.exception import NotFound
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.public import AT, RT, IonObject
from nose.plugins.attrib import attr
import unittest


@attr('UNIT', group='dm')
class PubSubTest(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('pubsub_management')
        self.pubsub_service = PubsubManagementService()
        self.pubsub_service.clients = mock_clients

        # save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_read = mock_clients.resource_registry.read
        self.mock_create_association = mock_clients.resource_registry.create_association
        self.mock_delete_association = mock_clients.resource_registry.delete_association
        self.mock_find_resources = mock_clients.resource_registry.find_resources
        self.mock_find_associations = mock_clients.resource_registry.find_associations

        # Stream
        self.stream_id = "stream_id"
        self.stream = Mock()
        self.stream.name = "SampleStream"
        self.stream.description = "Sample Stream In PubSub"
        self.stream.mimetype = ""
        self.stream.producers = ['producer1', 'producer2', 'producer3']

        #Subscription
        self.subscription_id = "subscription_id"
        self.subscription = Mock()
        self.subscription.name = "SampleSubscription"
        self.subscription.description = "Sample Subscription In PubSub"
        self.subscription.query = {"stream_id": self.stream_id}
        self.subscription.exchange_name = "ExchangeName"

        #Subscription Has Stream Association
        self.association_id = "association_id"
        self.subscription_to_stream_association = Mock()
        self.subscription_to_stream_association._id = self.association_id

        self.stream_route = Mock()
        self.stream_route.routing_key = self.stream_id + '.data'

    def test_create_stream(self):
        self.mock_create.return_value = [self.stream_id, 1]

        stream_id = self.pubsub_service.create_stream(encoding="",
            original=True,
            name="SampleStream",
            description="Sample Stream Description", url="")

        self.assertTrue(self.mock_create.called)
        self.assertEqual(stream_id, self.stream_id)

    def test_read_and_update_stream(self):
        self.mock_read.return_value = self.stream
        stream_obj = self.pubsub_service.read_stream(self.stream_id)

        self.mock_update.return_value = [self.stream_id, 2]
        stream_obj.name = "UpdatedSampleStream"
        ret = self.pubsub_service.update_stream(stream_obj)

        self.mock_update.assert_called_once_with(stream_obj)
        self.assertTrue(ret)

    def test_read_stream(self):
        self.mock_read.return_value = self.stream
        stream_obj = self.pubsub_service.read_stream(self.stream_id)

        assert stream_obj is self.mock_read.return_value
        self.mock_read.assert_called_once_with(self.stream_id, '')

    def test_read_stream_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.read_stream('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Stream notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_delete_stream(self):
        self.mock_read.return_value = self.stream

        ret = self.pubsub_service.delete_stream(self.stream_id)

        self.mock_read.assert_called_once_with(self.stream_id, '')
        self.mock_delete.assert_called_once_with(self.stream)
        self.assertTrue(ret)

    def test_delete_stream_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.delete_stream('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Stream notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_delete.call_count, 0)

    def test_find_stream(self):
        self.mock_find_resources.return_value = [self.stream]
        filter = {'name': 'SampleStream', 'description': 'Sample Stream In PubSub'}
        streams = self.pubsub_service.find_streams(filter)

        self.assertEqual(streams, [self.stream])

    def test_find_stream_not_found(self):
        self.mock_find_resources.return_value = [self.stream]
        filter = {'name': 'StreamNotFound', 'description': 'Sample Stream In PubSub'}
        streams = self.pubsub_service.find_streams(filter)

        self.assertEqual(streams, [])

    def test_find_stream_no_streams_registered(self):
        self.mock_find_resources.return_value = []
        filter = {'name': 'SampleStream', 'description': 'Sample Stream In PubSub'}
        streams = self.pubsub_service.find_streams(filter)

        self.assertEqual(streams, [])

    def test_find_streams_by_producer(self):
        self.mock_find_resources.return_value = [self.stream]
        streams = self.pubsub_service.find_streams_by_producer("producer1")

        self.mock_find_resources.assert_called_once_with(RT.Stream, None, None, False)
        self.assertEqual(streams, [self.stream])

    def test_find_streams_by_consumer(self):
        with self.assertRaises(NotImplementedError) as cm:
            self.pubsub_service.find_streams_by_consumer(None)

        ex = cm.exception
        self.assertEqual(ex.message, 'find_streams_by_consumer not implemented.')

    def test_create_subscription(self):
        self.mock_create.return_value = [self.subscription_id, 1]
        self.mock_create_association.return_value = [self.association_id, 1]

        id = self.pubsub_service.create_subscription(self.subscription)
        self.mock_create.assert_called_once_with(self.subscription)
        self.mock_create_association.assert_called_once_with(self.subscription_id, AT.hasStream, self.stream_id, None)
        self.assertEqual(id, self.subscription_id)

    def test_read_and_update_subscription(self):
        self.mock_read.return_value = self.subscription
        subscription_obj = self.pubsub_service.read_subscription(self.subscription_id)

        self.mock_update.return_value = [self.subscription_id, 2]
        subscription_obj.name = "UpdatedSampleSubscription"
        ret = self.pubsub_service.update_subscription(subscription_obj)

        self.mock_update.assert_called_once_with(subscription_obj)
        self.assertTrue(ret)

    def test_read_subscription(self):
        self.mock_read.return_value = self.subscription
        subscription_obj = self.pubsub_service.read_subscription(self.subscription_id)

        assert subscription_obj is self.mock_read.return_value
        self.mock_read.assert_called_once_with(self.subscription_id, '')

    def test_read_subscription_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.read_subscription('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Subscription notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_delete_subscription(self):
        self.mock_read.return_value = self.subscription
        self.mock_find_associations.return_value = [self.subscription_to_stream_association]
        ret = self.pubsub_service.delete_subscription(self.subscription_id)

        self.assertEqual(ret, True)
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_associations.assert_called_once_with(self.subscription_id, AT.hasStream, '', False)
        self.mock_delete_association.assert_called_once_with(self.association_id)
        self.mock_delete.assert_called_once_with(self.subscription)

    def test_delete_subscription_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.delete_subscription('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Subscription notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_delete_association.call_count, 0)
        self.assertEqual(self.mock_delete.call_count, 0)

    def test_delete_subscription_association_not_found(self):
        self.mock_read.return_value = self.subscription
        self.mock_find_associations.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.delete_subscription(self.subscription_id)

        ex = cm.exception
        self.assertEqual(ex.message, 'Subscription to Stream association for subscription id subscription_id does not exist')
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_associations.assert_called_once_with(self.subscription_id, AT.hasStream, '', False)
        self.assertEqual(self.mock_delete_association.call_count, 0)
        self.assertEqual(self.mock_delete.call_count, 0)

    @unittest.skip('Nothing to test')
    def test_activate_subscription(self):
        self.mock_read.return_value = self.subscription
        ret = self.pubsub_service.activate_subscription(self.subscription_id)

        self.assertTrue(ret)

    def test_activate_subscription_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.activate_subscription('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Subscription notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    @unittest.skip('Nothing to test')
    def test_deactivate_subscription(self):
        self.mock_read.return_value = self.subscription
        ret = self.pubsub_service.deactivate_subscription(self.subscription_id)

        self.assertTrue(ret)

    def test_deactivate_subscription_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.deactivate_subscription('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Subscription notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_register_consumer(self):
        with self.assertRaises(NotImplementedError) as cm:
            self.pubsub_service.register_consumer(None)

        ex = cm.exception
        self.assertEqual(ex.message, 'register_consumer not implemented.')

    def test_unregister_consumer(self):
        with self.assertRaises(NotImplementedError) as cm:
            self.pubsub_service.unregister_consumer(None)

        ex = cm.exception
        self.assertEqual(ex.message, 'unregister_consumer not implemented.')

    def test_find_consumers_by_stream(self):
        with self.assertRaises(NotImplementedError) as cm:
            self.pubsub_service.find_consumers_by_stream(None)

        ex = cm.exception
        self.assertEqual(ex.message, 'find_consumers_by_stream not implemented.')

    def test_register_producer(self):
        self.mock_read.return_value = self.stream
        self.mock_update.return_value = [self.stream_id, 2]
        ret = self.pubsub_service.register_producer("Test Producer", self.stream_id)

        self.assert_("Test Producer" in self.stream.producers)
        self.assertEqual(ret.routing_key, self.stream_route.routing_key)
        self.mock_read.assert_called_once_with(self.stream_id, '')

    def test_register_producer_stream_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.register_producer('Test Producer', 'notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Stream notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_unregister_producer(self):
        self.mock_read.return_value = self.stream
        self.mock_update.return_value = [self.stream_id, 2]
        ret = self.pubsub_service.unregister_producer('producer1', self.stream_id)

        self.assert_('producer1' not in self.stream.producers)
        self.assertEqual(ret, True)
        self.mock_read.assert_called_once_with(self.stream_id, '')

    def test_unregister_producer_stream_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.register_producer('Test Producer', 'notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Stream notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_unregister_producer_not_found(self):
        self.mock_read.return_value = self.stream

        # TEST: Execute the service operation call
        with self.assertRaises(ValueError) as cm:
            self.pubsub_service.unregister_producer('Test Producer', self.stream_id)

        ex = cm.exception
        self.assertEqual(ex.message, 'Producer Test Producer not found in stream stream_id')
        self.mock_read.assert_called_once_with(self.stream_id, '')
        self.assertEqual(self.mock_update.call_count, 0)

    def test_find_producers_by_stream(self):
        self.mock_read.return_value = self.stream
        producers = self.pubsub_service.find_producers_by_stream(self.stream_id)

        assert producers is self.stream.producers
        self.mock_read.assert_called_once_with(self.stream_id, '')

    def test_find_producers_by_stream_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.find_producers_by_stream('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Stream notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')


#@attr('INT', group='dm')
#class PublishSubscribeIntTest(IonIntegrationTestCase):
#
#    def setUp(self):
#        self._start_container()
#
#        # Establish endpoint with container
#        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
#        container_client.start_rel_from_url('res/deploy/r2deploy.yml')
#
#        # Now create client to bank service
#        self.client = PubsubManagementServiceClient(node=self.container.node)
#
#        self.container.spawn_process('test_process', 'pyon.ion.streamproc','StreamProcess',
#            config={'process':{'type':'stream_process','listen_name':'ctd_data'}})
#
#
#
#    def test_create_publisher(self):
#
#        stream = IonObject(RT.Stream, name='test stream')
#        id = self.client.create_stream(stream)
#
#        #self.publisher_registrar.create_publisher(stream_id=id)

@attr('INT', group='dm1')
class PubSubIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()

        self.cc = ContainerAgentClient(node=self.container.node,name=self.container.name)

        self.cc.start_rel_from_url('res/deploy/r2deploy.yml')

        self.pubsub_cli = PubsubManagementServiceClient(node=self.cc.node)

        self.ctd_output_stream_id = self.pubsub_cli.create_stream(encoding="",
            original=True,
            name="SampleStream",
            description="Sample Stream Description", url="",
            stream_definition_type="")

        self.ctd_subscription = IonObject(RT.Subscription, name='SampleSubscription', description='Sample Subscription Description')
        self.ctd_subscription.query['stream_id'] = self.ctd_output_stream_id
        self.ctd_subscription.exchange_name = 'a_queue'
        self.ctd_subscription_id = self.pubsub_cli.create_subscription(self.ctd_subscription)

    def tearDown(self):
        #self.pubsub_cli.delete_subscription(self.ctd_subscription_id)
        #self.pubsub_cli.delete_stream(self.ctd_output_stream_id)
        self._stop_container()

    def test_bind_subscription(self):
        self.pubsub_cli.activate_subscription(self.ctd_subscription_id)

    @unittest.skip("Nothing to test")
    def test_unbind_subscription(self):
        self.test_bind_subscription()
        self.pubsub_cli.deactivate_subscription(self.ctd_subscription_id)
        pass

    @unittest.skip("Nothing to test")
    def test_bind_already_bound_subscription(self):
        self.pubsub_cli.activate_subscription(self.ctd_subscription_id)
        self.pubsub_cli.activate_subscription(self.ctd_subscription_id)

    @unittest.skip("Nothing to test")
    def test_unbind_unbound_subscription(self):
        pass
