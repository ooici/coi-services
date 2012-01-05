#!/usr/bin/env python

'''
@file ion/services/dm/distribution/test/test_pubsub.py
@author Jamie Chen
@test ion.services.dm.distribution.pubsub_management_service Unit test suite to cover all pub sub mgmt service code
'''

from mock import Mock, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from nose.plugins.attrib import attr
from pyon.core.exception import NotFound
from pyon.public import log, AT
import unittest
from pyon.public import CFG, IonObject, log, RT, AT, LCS

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
        self.mock_find_subjects = mock_clients.resource_registry.find_subjects

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
        self.subscription.query = {"stream_id" : self.stream_id}
        self.subscription.exchange_name = "ExchangeName"

        #Subscription Has Stream Association
        self.association_id = "association_id"
        self.subscription_to_stream_association = Mock()
        self.subscription_to_stream_association._id = self.association_id

        self.stream_route = Mock()
        self.stream_route.routing_key = self.stream_id + '.data'

    def test_create_stream(self):
        self.mock_create.return_value = [self.stream_id, 1]

        stream_id = self.pubsub_service.create_stream(self.stream)

        self.mock_create.assert_called_once_with(self.stream)
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
        filter = {'name' : 'SampleStream', 'description' : 'Sample Stream In PubSub'}
        streams = self.pubsub_service.find_streams(filter)

        self.assertEqual(streams, [self.stream])

    def test_find_stream_not_found(self):
        self.mock_find_resources.return_value = [self.stream]
        filter = {'name' : 'StreamNotFound', 'description' : 'Sample Stream In PubSub'}
        streams = self.pubsub_service.find_streams(filter)

        self.assertEqual(streams, [])

    def test_find_stream_no_streams_registered(self):
        self.mock_find_resources.return_value = []
        filter = {'name' : 'SampleStream', 'description' : 'Sample Stream In PubSub'}
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
        self.mock_find_subjects.return_value = ("", [self.subscription_to_stream_association])
        ret = self.pubsub_service.delete_subscription(self.subscription_id)

        self.assertEqual(ret, True)
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_subjects.assert_called_once_with(self.subscription_id, AT.hasStream, self.subscription.query['stream_id'], False)
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

    @unittest.skip('Nothing to test')
    def test_activate_subscription(self):
        self.pubsub_service.activate_subscription()

    @unittest.skip('Nothing to test')
    def test_activate_subscription_not_found(self):
        self.pubsub_service.activate_subscription()

    @unittest.skip('Nothing to test')
    def test_deactivate_subscription(self):
        self.pubsub_service.deactivate_subscription()

    @unittest.skip('Nothing to test')
    def test_deactivate_subscription_not_found(self):
        self.pubsub_service.deactivate_subscription()

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


@attr('INT', group='dm')
class PubSubIntTest(PyonTestCase):

    def setUp(self):
        #create binding
        pass

    def test_bind_subscription(self):
        pass

    def test_unbind_subscription(self):
        pass

    def test_bind_already_bound_subscription(self):
        pass

    def test_unbind_unbound_subscription(self):
        pass