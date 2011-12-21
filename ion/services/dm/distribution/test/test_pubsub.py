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

        #Subscription Has Stream Association
        self.association_id = "association_id"

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
        self.pubsub_service.update_stream(stream_obj)

        self.mock_update.assert_called_once_with(stream_obj)

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

        self.pubsub_service.delete_stream(self.stream_id)

        self.mock_read.assert_called_once_with(self.stream_id, '')
        self.mock_delete.assert_called_once_with(self.stream)

    def test_delete_stream_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.delete_stream('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Stream notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    @unittest.skip('Nothing to test')
    def test_find_stream(self):
        self.pubsub_service.find_streams()

    @unittest.skip('Nothing to test')
    def test_find_streams_by_producer(self):
        self.pubsub_service.find_streams_by_producer()

    @unittest.skip('Nothing to test')
    def test_find_streams_by_consumer(self):
        self.pubsub_service.find_streams_by_consumer()

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
        self.pubsub_service.update_stream(subscription_obj)

        self.mock_update.assert_called_once_with(subscription_obj)

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

    @unittest.skip('Nothing to test')
    def test_delete_subscription(self):
        self.mock_read.return_value = self.subscription

        self.pubsub_service.delete_subscription(self.subscription_id)

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
        self.mock_read.assert_called_once_with('notfound', '') #

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

    @unittest.skip('Nothing to test')
    def test_register_consumer(self):
        self.pubsub_service.register_consumer()

    @unittest.skip('Nothing to test')
    def test_unregister_consumer(self):
        self.pubsub_service.unregister_consumer()

    @unittest.skip('Nothing to test')
    def test_find_consumers_by_stream(self):
        self.pubsub_service.find_consumers_by_stream()

    def test_register_producer(self):
        self.mock_read.return_value = self.stream
        self.mock_update.return_value = [self.stream_id, 2]
        ret = self.pubsub_service.register_producer("Test Producer", self.stream_id)

        self.assert_("Test Producer" in self.stream.producers)
        self.assertEqual(ret, 'credentials')
        self.mock_read.assert_called_once_with(self.stream_id, '')

        ## Temporarily patch and unpatch read_stream function of
        ## self.pubsub_service
        #with patch.object(self.pubsub_service, 'read_stream',
        #        mocksignature=True) as mock_read_stream:
        #    mock_stream_obj = Mock()
        #    mock_stream_obj.producers = []
        #    mock_read_stream.return_value = mock_stream_obj
        #
        #    credentials = self.pubsub_service.register_producer("Test Producer", self.stream_id)
        #
        #    mock_read_stream.assert_called_once_with('id_2')
        #    # side effect
        #    self.assertEqual(mock_stream_obj.producers, ['Test Producer'])
        #    # @todo this is a placeholder. Change to the real thing
        #    self.assertEqual(credentials, 'credentials')

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
        ret = self.pubsub_service.unregister_producer('Test Producer', self.stream_id)

        self.assertEqual(ret, False)
        self.mock_read.assert_called_once_with(self.stream_id, '')

    def test_find_producers_by_stream(self):
        ## Temporarily patch and unpatch read_stream function of
        ## self.pubsub_service
        #with patch.object(self.pubsub_service, 'read_stream',
        #        mocksignature=True) as mock_read_stream:
        #    mock_read_stream.return_value = sentinel
        #
        #    producers = self.pubsub_service.find_producers_by_stream('id_2')
        #
        #    mock_read_stream.assert_called_once_with('id_2')
        #    self.assertEqual(producers, sentinel.producers)
        self.mock_read.return_value = self.stream
        producers = self.pubsub_service.find_producers_by_stream(self.stream_id)

        assert producers is self.stream.producers
        self.mock_read.assert_called_once_with(self.stream_id, '')

    def test_find_producers_by_stream_not_found(self):
        ## Temporarily patch and unpatch read_stream function of
        ## self.pubsub_service
        #with patch.object(self.pubsub_service, 'read_stream',
        #        mocksignature=True) as mock_read_stream:
        #    mock_read_stream.return_value = None
        #
        #    with self.assertRaises(NotFound) as cm:
        #        producers = self.pubsub_service.find_producers_by_stream('id_2')
        #
        #    mock_read_stream.assert_called_once_with('id_2')
        #    ex = cm.exception
        #    self.assertEqual(ex.message, 'Stream id_2 does not exist')
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.find_producers_by_stream('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Stream notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
