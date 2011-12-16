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
import unittest

@attr('UNIT', group='dm')
class PubSubTest(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.dm.distribution.pubsub_management_service.IonObject')
        mock_clients = self._create_service_mock('pubsub_management')
        self.pubsub_service = PubsubManagementService()
        self.pubsub_service.clients = mock_clients

        # save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete

    def test_create_stream(self):
        self.mock_create.return_value = ('id_2', 'I do not care')
        stream = {"mimetype": "", "name": "SampleStream", "description": "Sample Stream In PubSub",
                "producers":['producer1', 'producer2', 'producer3']}

        stream_id = self.pubsub_service.create_stream(stream)

        self.mock_ionobj.assert_called_once_with('Stream', stream)
        self.mock_create.assert_called_once_with(self.mock_ionobj.return_value)
        self.assertEqual(stream_id, 'id_2')

    def test_update_stream(self):
        self.pubsub_service.update_stream(sentinel.stream)

        self.mock_update.assert_called_once_with(sentinel.stream)

    def test_read_stream(self):
        stream_obj = self.pubsub_service.read_stream()

    def test_read_stream_not_found(self):
        stream_obj = self.pubsub_service.read_stream()

    def test_delete_stream(self):
        self.pubsub_service.delete_stream()

    def test_delete_stream_not_found(self):
        self.pubsub_service.delete_stream()

    @unittest.skip('Nothin to test')
    def test_find_stream(self):
        self.pubsub_service.find_streams()

    @unittest.skip('Nothing to test')
    def test_find_streams_by_producer(self):
        self.pubsub_service.find_streams_by_producer()

    @unittest.skip('Nothing to test')
    def test_find_streams_by_consumer(self):
        self.pubsub_service.find_streams_by_consumer()

    def test_create_subscription(self):
        self.mock_create.return_value = ('id_2', 'I do not care')
        subscription = {"name": "SampleSubscription",
                "description": "Sample Subscription In PubSub",
                "query": {"stream_id": 'id_5'}}

        id = self.pubsub_service.create_subscription(subscription)

        self.mock_ionobj.assert_called_once_with('Subscription',
                subscription)
        self.mock_create.assert_called_once_with(self.mock_ionobj.return_value)
        self.assertEqual(id, 'id_2')

    def test_update_subscription(self):
        self.pubsub_service.update_subscription(sentinel.subscription)

        self.mock_update.assert_called_once_with(sentinel.subscription)

    def test_read_subscription(self):
        subscription_obj = self.pubsub_service.read_subscription()

    def test_read_subscription_not_found(self):
        subscription_obj = self.pubsub_service.read_subscription()

    def test_delete_subscription(self):
        # Temporarily patch and unpatch read_subscription function of
        # self.pubsub_service
        with patch.object(self.pubsub_service, 'read_subscription',
                mocksignature=True) as mock_read_subscription:

            value = self.pubsub_service.delete_subscription('id_2')

            mock_read_subscription.assert_called_once_with('id_2')
            self.mock_delete.assert_called_once_with(mock_read_subscription.return_value)
            self.assertEqual(value, self.mock_delete.return_value)

    def test_delete_subscription_not_found(self):
        # Temporarily patch and unpatch read_subscription function of
        # self.pubsub_service
        with patch.object(self.pubsub_service, 'read_subscription',
                mocksignature=True) as mock_read_subscription:
            mock_read_subscription.return_value = None

            with self.assertRaises(NotFound) as cm:
                value = self.pubsub_service.delete_subscription('id_2')

            mock_read_subscription.assert_called_once_with('id_2')
            ex = cm.exception
            self.assertEqual(ex.message, 'Subscription id_2 does not exist')

    def test_activate_subscription(self):
        self.pubsub_service.activate_subscription()

    def test_activate_subscription_not_found(self):
        self.pubsub_service.activate_subscription()

    def test_deactivate_subscription(self):
        self.pubsub_service.deactivate_subscription()

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
        # Temporarily patch and unpatch read_stream function of
        # self.pubsub_service
        with patch.object(self.pubsub_service, 'read_stream',
                mocksignature=True) as mock_read_stream:
            mock_stream_obj = Mock()
            mock_stream_obj.producers = []
            mock_read_stream.return_value = mock_stream_obj

            credentials = self.pubsub_service.register_producer("Test Producer", 'id_2')

            mock_read_stream.assert_called_once_with('id_2')
            # side effect
            self.assertEqual(mock_stream_obj.producers, ['Test Producer'])
            # @todo this is a placeholder. Change to the real thing
            self.assertEqual(credentials, 'credentials')

    def test_register_producer_not_found(self):
        self.pubsub_service.register_producer("Test Producer", '')

    def test_unregister_producer(self):
        self.pubsub_service.unregister_producer("Test Producer", '')

    def test_unregister_producer_not_found(self):
        self.pubsub_service.unregister_producer("Test Producer", '')

    def test_find_producers_by_stream(self):
        # Temporarily patch and unpatch read_stream function of
        # self.pubsub_service
        with patch.object(self.pubsub_service, 'read_stream',
                mocksignature=True) as mock_read_stream:
            mock_read_stream.return_value = sentinel

            producers = self.pubsub_service.find_producers_by_stream('id_2')

            mock_read_stream.assert_called_once_with('id_2')
            self.assertEqual(producers, sentinel.producers)

    def test_find_producers_by_stream_not_found(self):
        # Temporarily patch and unpatch read_stream function of
        # self.pubsub_service
        with patch.object(self.pubsub_service, 'read_stream',
                mocksignature=True) as mock_read_stream:
            mock_read_stream.return_value = None

            with self.assertRaises(NotFound) as cm:
                producers = self.pubsub_service.find_producers_by_stream('id_2')

            mock_read_stream.assert_called_once_with('id_2')
            ex = cm.exception
            self.assertEqual(ex.message, 'Stream id_2 does not exist')
