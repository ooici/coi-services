from mock import Mock, sentinel
from pyon.util.unit_test import PyonTestCase
from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from nose.plugins.attrib import attr

@attr('UNIT', group='dm')
class PubSubTest(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.dm.distribution.pubsub_management_service.IonObject')
        self._create_service_mock('pubsub_management')
        self.pubsub_service = PubsubManagementService()
        self.pubsub_service.clients = self.clients

        self.mock_create = self.resource_registry.create
        self.mock_update = self.resource_registry.update

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

    def test_delete_stream(self):
        self.pubsub_service.delete_stream()

    def test_find_stream(self):
        self.pubsub_service.find_streams()

    def test_find_streams_by_producer(self):
        self.pubsub_service.find_streams_by_producer()

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

    def test_delete_subscription(self):
        self.pubsub_service.delete_subscription()

    def test_activate_subscription(self):
        self.pubsub_service.activate_subscription()

    def test_deactivate_subscription(self):
        self.pubsub_service.deactivate_subscription()

    def test_register_consumer(self):
        self.pubsub_service.register_consumer()

    def test_unregister_consumer(self):
        self.pubsub_service.unregister_consumer()

    def test_find_consumers_by_stream(self):
        self.pubsub_service.find_consumers_by_stream()

    def test_register_producer(self):
        self.pubsub_service.register_producer("Test Producer", '')

    def test_unregister_producer(self):
        self.pubsub_service.unregister_producer("Test Producer", '')

    def test_find_producers_by_stream(self):
        self.pubsub_service.find_producers_by_stream('id_2')
