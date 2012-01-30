#!/usr/bin/env python

'''
@file ion/services/dm/distribution/test/test_pubsub.py
@author Jamie Chen
@test ion.services.dm.distribution.pubsub_management_service Unit test suite to cover all pub sub mgmt service code
'''
import gevent
from mock import Mock
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.icontainer_agent import ContainerAgentClient
from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from pyon.core.exception import NotFound, BadRequest
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.public import PRED, RT, StreamPublisher, StreamSubscriber, log
from nose.plugins.attrib import attr
import unittest
from interface.objects import StreamQuery, ExchangeQuery, SubscriptionTypeEnum
from pyon.util.containers import DotDict


@attr('UNIT', group='dm')
class PubSubTest(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('pubsub_management')
        self.pubsub_service = PubsubManagementService()
        self.pubsub_service.clients = mock_clients
        self.pubsub_service.container = DotDict()
        self.pubsub_service.container.node = Mock()

        # save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_read = mock_clients.resource_registry.read
        self.mock_create_association = mock_clients.resource_registry.create_association
        self.mock_delete_association = mock_clients.resource_registry.delete_association
        self.mock_find_resources = mock_clients.resource_registry.find_resources
        self.mock_find_associations = mock_clients.resource_registry.find_associations
        self.mock_find_objects = mock_clients.resource_registry.find_objects

        # Stream
        self.stream_id = "stream_id"
        self.stream = Mock()
        self.stream.name = "SampleStream"
        self.stream.description = "Sample Stream In PubSub"
        self.stream.encoding = ""
        self.stream.original = True
        self.stream.stream_definition_type = ""
        self.stream.url = ""
        self.stream.producers = ['producer1', 'producer2', 'producer3']

        #Subscription
        self.subscription_id = "subscription_id"
        self.subscription_stream_query = Mock()
        self.subscription_stream_query.name = "SampleSubscriptionStreamQuery"
        self.subscription_stream_query.description = "Sample Subscription With StreamQuery"
        self.subscription_stream_query.query = StreamQuery([self.stream_id])
        self.subscription_stream_query.exchange_name = "ExchangeName"
        self.subscription_stream_query.subscription_type = SubscriptionTypeEnum.STREAM_QUERY

        self.subscription_exchange_query = Mock()
        self.subscription_exchange_query.name = "SampleSubscriptionExchangeQuery"
        self.subscription_exchange_query.description = "Sample Subscription With Exchange Query"
        self.subscription_exchange_query.query = ExchangeQuery()
        self.subscription_exchange_query.exchange_name = "ExchangeName"
        self.subscription_exchange_query.subscription_type = SubscriptionTypeEnum.EXCHANGE_QUERY

        #Subscription Has Stream Association
        self.association_id = "association_id"
        self.subscription_to_stream_association = Mock()
        self.subscription_to_stream_association._id = self.association_id

        self.stream_route = Mock()
        self.stream_route.routing_key = self.stream_id + '.data'

    def test_create_stream(self):
        self.mock_create.return_value = [self.stream_id, 1]

        stream_id = self.pubsub_service.create_stream(name=self.stream.name,
                                                      description=self.stream.description)

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
        self.mock_delete.assert_called_once_with(self.stream_id)
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

    def test_find_streams_by_producer_not_in_list(self):
        self.mock_find_resources.return_value = [self.stream]
        streams = self.pubsub_service.find_streams_by_producer("producer_not_found")

        self.mock_find_resources.assert_called_once_with(RT.Stream, None, None, False)
        self.assertEqual(streams, [])

    def test_find_streams_by_consumer(self):
        with self.assertRaises(NotImplementedError) as cm:
            self.pubsub_service.find_streams_by_consumer(None)

        ex = cm.exception
        self.assertEqual(ex.message, 'find_streams_by_consumer not implemented.')

    def test_create_subscription_stream_query(self):
        self.mock_create.return_value = [self.subscription_id, 1]

        id = self.pubsub_service.create_subscription(name=self.subscription_stream_query.name,
                                                     description=self.subscription_stream_query.description,
                                                     query=self.subscription_stream_query.query,
                                                     exchange_name=self.subscription_stream_query.exchange_name)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.subscription_id, PRED.hasStream, self.stream_id, None)
        self.assertEqual(id, self.subscription_id)

    def test_create_subscription_exchange_query(self):
        self.mock_create.return_value = [self.subscription_id, 1]

        id = self.pubsub_service.create_subscription(name=self.subscription_exchange_query.name,
                                                     description=self.subscription_exchange_query.description,
                                                     query=self.subscription_exchange_query.query,
                                                     exchange_name=self.subscription_exchange_query.exchange_name)

        self.assertTrue(self.mock_create.called)
        self.assertEqual(id, self.subscription_id)

    def test_create_subscription_invalid_query_type(self):
        self.mock_create.return_value = [self.subscription_id, 1]

        query = Mock()
        exchange_name = "Invalid Subscription"

        with self.assertRaises(BadRequest) as cm:
            id = self.pubsub_service.create_subscription(name="InvalidSubscription",
                                                         description="Invalid Subscription Description",
                                                         query=query,
                                                         exchange_name=exchange_name)

        ex = cm.exception
        self.assertEqual(ex.message, 'Query type does not exist')

    def test_read_and_update_subscription(self):
        self.mock_read.return_value = self.subscription_stream_query
        subscription_obj = self.pubsub_service.read_subscription(self.subscription_id)

        self.mock_update.return_value = [self.subscription_id, 2]
        subscription_obj.name = "UpdatedSampleStreamQuerySubscription"
        ret = self.pubsub_service.update_subscription(subscription_obj)

        self.mock_update.assert_called_once_with(subscription_obj)
        self.assertTrue(ret)

    def test_read_subscription(self):
        self.mock_read.return_value = self.subscription_stream_query
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
        self.mock_read.return_value = self.subscription_stream_query
        self.mock_find_associations.return_value = [self.subscription_to_stream_association]
        ret = self.pubsub_service.delete_subscription(self.subscription_id)

        self.assertEqual(ret, True)
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_associations.assert_called_once_with(self.subscription_id, PRED.hasStream, '', False)
        self.mock_delete_association.assert_called_once_with(self.association_id)
        self.mock_delete.assert_called_once_with(self.subscription_id)

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
        self.mock_read.return_value = self.subscription_stream_query
        self.mock_find_associations.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.delete_subscription(self.subscription_id)

        ex = cm.exception
        self.assertEqual(ex.message, 'Subscription to Stream association for subscription id subscription_id does not exist')
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_associations.assert_called_once_with(self.subscription_id, PRED.hasStream, '', False)
        self.assertEqual(self.mock_delete_association.call_count, 0)
        self.assertEqual(self.mock_delete.call_count, 0)

    def test_activate_subscription_stream_query(self):
        self.mock_read.return_value = self.subscription_stream_query
        self.mock_find_objects.return_value = [self.stream_id], 0

        ret = self.pubsub_service.activate_subscription(self.subscription_id)

        self.assertTrue(ret)
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_objects.assert_called_once_with(self.subscription_id, PRED.hasStream, RT.Stream, True)

    def test_activate_subscription_exchange_query(self):
        self.mock_read.return_value = self.subscription_exchange_query
        self.mock_find_objects.return_value = [self.stream_id], 0

        ret = self.pubsub_service.activate_subscription(self.subscription_id)

        self.assertTrue(ret)
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_objects.assert_called_once_with(self.subscription_id, PRED.hasStream, RT.Stream, True)

    def test_activate_subscription_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.activate_subscription('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Subscription notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_deactivate_subscription_stream_query(self):
        self.mock_read.return_value = self.subscription_stream_query
        self.mock_find_objects.return_value = [self.stream_id], 0

        ret = self.pubsub_service.deactivate_subscription(self.subscription_id)

        self.assertTrue(ret)
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_objects.assert_called_once_with(self.subscription_id, PRED.hasStream, RT.Stream, True)

    def test_deactivate_subscription_exchange_query(self):
        self.mock_read.return_value = self.subscription_exchange_query
        self.mock_find_objects.return_value = [self.stream_id], 0

        ret = self.pubsub_service.deactivate_subscription(self.subscription_id)

        self.assertTrue(ret)
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_objects.assert_called_once_with(self.subscription_id, PRED.hasStream, RT.Stream, True)

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
            self.pubsub_service.unregister_producer('Test Producer', 'notfound')

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
class PubSubIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()

        self.cc = ContainerAgentClient(node=self.container.node,name=self.container.name)

        self.cc.start_rel_from_url('res/deploy/r2dm.yml')

        self.pubsub_cli = PubsubManagementServiceClient(node=self.cc.node)

        self.ctd_stream1_id = self.pubsub_cli.create_stream(name="SampleStream1",
                                                            description="Sample Stream 1 Description")

        self.ctd_stream2_id = self.pubsub_cli.create_stream(name="SampleStream2",
                                                            description="Sample Stream 2 Description")

        # Make a subscription to two input streams
        exchange_name = "a_queue"
        query = StreamQuery([self.ctd_stream1_id, self.ctd_stream2_id])

        self.ctd_subscription_id = self.pubsub_cli.create_subscription(query,
                                                                       exchange_name,
                                                                       "SampleSubscription",
                                                                       "Sample Subscription Description")

        # Make a subscription to all streams on an exchange point
        exchange_name = "another_queue"
        query = ExchangeQuery()

        self.exchange_subscription_id = self.pubsub_cli.create_subscription(query,
            exchange_name,
            "SampleExchangeSubscription",
            "Sample Exchange Subscription Description")


        # cheat to make a publisher object to send messages in the test.
        # it is really hokey to pass process=self.cc but it works
        stream_route = self.pubsub_cli.register_producer(exchange_name='producer_doesnt_have_a_name1', stream_id=self.ctd_stream1_id)
        self.ctd_stream1_publisher = StreamPublisher(node=self.cc.node, name=('science_data',stream_route.routing_key), process=self.cc)

        stream_route = self.pubsub_cli.register_producer(exchange_name='producer_doesnt_have_a_name2', stream_id=self.ctd_stream2_id)
        self.ctd_stream2_publisher = StreamPublisher(node=self.cc.node, name=('science_data',stream_route.routing_key), process=self.cc)

    def tearDown(self):
        self.pubsub_cli.delete_subscription(self.ctd_subscription_id)
        self.pubsub_cli.delete_subscription(self.exchange_subscription_id)
        self.pubsub_cli.delete_stream(self.ctd_stream1_id)
        self.pubsub_cli.delete_stream(self.ctd_stream2_id)
        self._stop_container()

    def test_bind_stream_subscription(self):

        ar = gevent.event.AsyncResult()
        self.first = True
        def message_received(message, headers):
            ar.set(message)

        subscriber = StreamSubscriber(node=self.cc.node, name=('science_data','a_queue'), callback=message_received, process=self.cc)
        subscriber.start()

        self.pubsub_cli.activate_subscription(self.ctd_subscription_id)

        self.ctd_stream1_publisher.publish('message1')
        self.assertEqual(ar.get(timeout=10), 'message1')

        ar = gevent.event.AsyncResult()

        self.ctd_stream2_publisher.publish('message2')
        self.assertEqual(ar.get(timeout=10), 'message2')

        subscriber.stop()


    def test_bind_exchange_subscription(self):

        ar = gevent.event.AsyncResult()
        self.first = True
        def message_received(message, headers):
            ar.set(message)

        subscriber = StreamSubscriber(node=self.cc.node, name=('science_data','another_queue'), callback=message_received, process=self.cc)
        subscriber.start()

        self.pubsub_cli.activate_subscription(self.exchange_subscription_id)

        self.ctd_stream1_publisher.publish('message1')
        self.assertEqual(ar.get(timeout=10), 'message1')

        ar = gevent.event.AsyncResult()

        self.ctd_stream2_publisher.publish('message2')
        self.assertEqual(ar.get(timeout=10), 'message2')

        subscriber.stop()


    def test_unbind_stream_subscription(self):
        ar = gevent.event.AsyncResult()
        self.first = True
        def message_received(message, headers):
            ar.set(message)

        subscriber = StreamSubscriber(node=self.cc.node, name=('science_data','a_queue'), callback=message_received, process=self.cc)
        subscriber.start()

        self.pubsub_cli.activate_subscription(self.ctd_subscription_id)

        self.ctd_stream1_publisher.publish('message1')
        self.assertEqual(ar.get(timeout=10), 'message1')

        self.pubsub_cli.deactivate_subscription(self.ctd_subscription_id)

        ar = gevent.event.AsyncResult()

        self.ctd_stream2_publisher.publish('message2')
        p = None
        with self.assertRaises(gevent.Timeout) as cm:
            p = ar.get(timeout=2)

        subscriber.stop()
        ex = cm.exception
        self.assertEqual(str(ex), '2 seconds')
        self.assertEqual(p, None)


    def test_unbind_exchange_subscription(self):
        ar = gevent.event.AsyncResult()
        self.first = True
        def message_received(message, headers):
            ar.set(message)

        subscriber = StreamSubscriber(node=self.cc.node, name=('science_data','another_queue'), callback=message_received, process=self.cc)
        subscriber.start()

        self.pubsub_cli.activate_subscription(self.exchange_subscription_id)

        self.ctd_stream1_publisher.publish('message1')
        self.assertEqual(ar.get(timeout=10), 'message1')

        self.pubsub_cli.deactivate_subscription(self.exchange_subscription_id)

        ar = gevent.event.AsyncResult()

        self.ctd_stream2_publisher.publish('message2')
        p = None
        with self.assertRaises(gevent.Timeout) as cm:
            p = ar.get(timeout=2)

        subscriber.stop()
        ex = cm.exception
        self.assertEqual(str(ex), '2 seconds')
        self.assertEqual(p, None)


    @unittest.skip("Nothing to test")
    def test_bind_already_bound_subscription(self):
        pass

    @unittest.skip("Nothing to test")
    def test_unbind_unbound_subscription(self):
        pass
