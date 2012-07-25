#!/usr/bin/env python

'''
@file ion/services/dm/distribution/test/test_pubsub.py
@author Jamie Chen
@test ion.services.dm.distribution.pubsub_management_service Unit test suite to cover all pub sub mgmt service code
'''
from pyon.public import PRED, RT, StreamSubscriberRegistrar, StreamPublisherRegistrar
from pyon.net.endpoint import Subscriber,Publisher
from pyon.ion.stream import SimpleStreamSubscriber, SimpleStreamPublisher
import gevent
from mock import Mock
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from pyon.core.exception import NotFound, BadRequest
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from interface.objects import StreamQuery, ExchangeQuery, SubscriptionTypeEnum, StreamDefinition, StreamDefinitionContainer, Subscription
from pyon.util.containers import DotDict
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
import logging

@attr('UNIT', group='dm1')
class PubSubTest(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('pubsub_management')
        self.pubsub_service = PubsubManagementService()
        self.pubsub_service.clients = mock_clients
        self.pubsub_service.container = DotDict()
        self.pubsub_service.container.node = Mock()
        self.pubsub_service.container.ex_manager = Mock()

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

        #StreamDefinition
        self.stream_definition_id = "stream_definition_id"
        self.stream_definition = Mock()
        self.stream_definition.name = "SampleStreamDefinition"
        self.stream_definition.description = "Sample StreamDefinition In PubSub"
        self.stream_definition.container = StreamDefinitionContainer()

        # Stream
        self.stream_id = "stream_id"
        self.stream = Mock()
        self.stream.name = "SampleStream"
        self.stream.description = "Sample Stream In PubSub"
        self.stream.encoding = ""
        self.stream.original = True
        self.stream.stream_definition_id = self.stream_definition_id
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
        self.subscription_stream_query.is_active = False

        self.subscription_exchange_query = Mock()
        self.subscription_exchange_query.name = "SampleSubscriptionExchangeQuery"
        self.subscription_exchange_query.description = "Sample Subscription With Exchange Query"
        self.subscription_exchange_query.query = ExchangeQuery()
        self.subscription_exchange_query.exchange_name = "ExchangeName"
        self.subscription_exchange_query.subscription_type = SubscriptionTypeEnum.EXCHANGE_QUERY
        self.subscription_exchange_query.is_active = False
        self.subscription_exchange_point = 'test_exchange_point'

        #Subscription Has Stream Association
        self.association_id = "association_id"
        self.subscription_to_stream_association = Mock()
        self.subscription_to_stream_association._id = self.association_id

        self.stream_route = Mock()
        self.stream_route.routing_key = self.stream_id + '.data'

    def test_create_stream(self):
        self.mock_create.return_value = [self.stream_id, 1]

        stream_id = self.pubsub_service.create_stream(name=self.stream.name,
                                                      description=self.stream.description,
                                                      stream_definition_id=self.stream.stream_definition_id)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.stream_id, PRED.hasStreamDefinition, self.stream.stream_definition_id, "H2H")
        self.assertEqual(stream_id, self.stream_id)

    def test_read_and_update_stream(self):
        self.mock_read.return_value = self.stream
        stream_obj = self.pubsub_service.read_stream(self.stream_id)

        self.mock_update.return_value = [self.stream_id, 2]
        stream_obj.name = "UpdatedSampleStream"
        ret = self.pubsub_service.update_stream(stream_obj)

        self.mock_update.assert_called_once_with(stream_obj)
        self.assertEqual(None, ret)

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

    def test_delete_stream_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.delete_stream('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Stream notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_delete.call_count, 0)

    def test_create_stream_definition(self):
        self.mock_create.return_value = [self.stream_definition_id, 1]

        stream_definition_id = self.pubsub_service.create_stream_definition(container=self.stream_definition.container)

        self.assertTrue(self.mock_create.called)
        self.assertEqual(stream_definition_id, self.stream_definition_id)

    def test_read_and_update_stream_definition(self):
        self.mock_read.return_value = self.stream_definition
        stream_definition_obj = self.pubsub_service.read_stream_definition(self.stream_definition_id)

        self.mock_update.return_value = [self.stream_definition_id, 2]
        stream_definition_obj.name = "UpdatedSampleStreamDefinition"
        ret = self.pubsub_service.update_stream_definition(stream_definition_obj)

        self.mock_update.assert_called_once_with(stream_definition_obj)
        self.assertEqual(ret,None)

    def test_read_stream_definition(self):
        self.mock_read.return_value = self.stream_definition
        stream_definition_obj = self.pubsub_service.read_stream_definition(self.stream_definition_id)

        assert stream_definition_obj is self.mock_read.return_value
        self.mock_read.assert_called_once_with(self.stream_definition_id, '')

    def test_read_stream_definition_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.read_stream_definition('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'StreamDefinition notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_delete_stream_definition(self):
        self.mock_read.return_value = self.stream_definition

        ret = self.pubsub_service.delete_stream_definition(self.stream_definition_id)

        self.mock_read.assert_called_once_with(self.stream_definition_id, '')
        self.mock_delete.assert_called_once_with(self.stream_definition_id)

    def test_delete_stream_definition_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pubsub_service.delete_stream_definition('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'StreamDefinition notfound does not exist')
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
                                                     exchange_name=self.subscription_stream_query.exchange_name,
                                                     exchange_point=self.subscription_exchange_point
                                                     )

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.subscription_id, PRED.hasStream, self.stream_id, "H2H")
        self.assertEqual(id, self.subscription_id)

    def test_create_subscription_exchange_query(self):
        self.mock_create.return_value = [self.subscription_id, 1]

        id = self.pubsub_service.create_subscription(name=self.subscription_exchange_query.name,
                                                     description=self.subscription_exchange_query.description,
                                                     query=self.subscription_exchange_query.query,
                                                     exchange_name=self.subscription_exchange_query.exchange_name,
                                                     exchange_point=self.subscription_exchange_point
                                                     )

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
                                                         exchange_name=exchange_name,
                                                         exchange_point=self.subscription_exchange_point)

        ex = cm.exception
        self.assertEqual(ex.message, 'Query type does not exist')

    def test_read_and_update_subscription(self):
        # Mocks
        subscription_obj = Subscription()
        subscription_obj.query = StreamQuery(['789'])
        subscription_obj.is_active=False
        subscription_obj.subscription_type = SubscriptionTypeEnum.STREAM_QUERY
        self.mock_read.return_value = subscription_obj

        self.mock_find_objects.return_value = (['789'],['This here is an association'])
        self.mock_update.return_value = ('not important','even less so')
        # Execution
        query = StreamQuery(['123'])
        retval = self.pubsub_service.update_subscription('subscription_id', query)

        # Assertions
        self.mock_read.assert_called_once_with('subscription_id','')
        self.mock_find_objects.assert_called_once_with('subscription_id',PRED.hasStream,'',True)
        self.mock_delete_association.assert_called_once_with('This here is an association')
        self.mock_create_association.assert_called_once_with('subscription_id',PRED.hasStream,'123',"H2H")
        self.assertTrue(self.mock_update.call_count == 1, 'update was not called')

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
        self.mock_find_associations.assert_called_once_with(self.subscription_id, PRED.hasStream, '', None, False)
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
        self.mock_find_associations.assert_called_once_with(self.subscription_id, PRED.hasStream, '', None, False)
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
        self.mock_read.return_value.is_active = True

        self.mock_find_objects.return_value = [self.stream_id], 0

        ret = self.pubsub_service.deactivate_subscription(self.subscription_id)

        self.assertTrue(ret)
        self.mock_read.assert_called_once_with(self.subscription_id, '')
        self.mock_find_objects.assert_called_once_with(self.subscription_id, PRED.hasStream, RT.Stream, True)

    def test_deactivate_subscription_exchange_query(self):
        self.mock_read.return_value = self.subscription_exchange_query
        self.mock_read.return_value.is_active = True

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
        logging.disable(logging.ERROR)
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')
        logging.disable(logging.NOTSET)

        self.pubsub_cli = PubsubManagementServiceClient(node=self.container.node)

        self.ctd_stream1_id = self.pubsub_cli.create_stream(name="SampleStream1",
                                                            description="Sample Stream 1 Description")

        self.ctd_stream2_id = self.pubsub_cli.create_stream(name="SampleStream2",
                                                            description="Sample Stream 2 Description")

        # Make a subscription to two input streams
        self.exchange_name = "a_queue"
        self.exchange_point = 'an_exchange'
        query = StreamQuery([self.ctd_stream1_id, self.ctd_stream2_id])

        self.ctd_subscription_id = self.pubsub_cli.create_subscription(query=query,
                                                                       exchange_name=self.exchange_name,
                                                                       exchange_point=self.exchange_point,
                                                                       name="SampleSubscription",
                                                                       description="Sample Subscription Description")

        # Make a subscription to all streams on an exchange point
        self.exchange2_name = "another_queue"
        query = ExchangeQuery()

        self.exchange_subscription_id = self.pubsub_cli.create_subscription(query=query,
            exchange_name=self.exchange2_name,
            exchange_point=self.exchange_point,
            name="SampleExchangeSubscription",
            description="Sample Exchange Subscription Description")


        # Normally the user does not see or create the publisher, this is part of the containers business.
        # For the test we need to set it up explicitly
        self.ctd_stream1_publisher = SimpleStreamPublisher.new_publisher(self.container, self.exchange_point, stream_id=self.ctd_stream1_id)
        self.ctd_stream2_publisher = SimpleStreamPublisher.new_publisher(self.container, self.exchange_point, stream_id=self.ctd_stream2_id)


        self.purge_queues()


    def tearDown(self):
        self.pubsub_cli.delete_subscription(self.ctd_subscription_id)
        self.pubsub_cli.delete_subscription(self.exchange_subscription_id)
        self.pubsub_cli.delete_stream(self.ctd_stream1_id)
        self.pubsub_cli.delete_stream(self.ctd_stream2_id)
        super(PubSubIntTest,self).tearDown()

    def purge_queues(self):
        xn = self.container.ex_manager.create_xn_queue(self.exchange_name)
        xn.purge()
        xn = self.container.ex_manager.create_xn_queue(self.exchange2_name)
        xn.purge()
        


    def test_bind_stream_subscription(self):

        event = gevent.event.Event()

        def message_received(message, headers):
            self.assertIn('test_bind_stream_subscription',message)
            self.assertFalse(event.is_set())
            event.set()


        subscriber = SimpleStreamSubscriber.new_subscriber(self.container,self.exchange_name,callback=message_received)
        subscriber.start()

        self.pubsub_cli.activate_subscription(self.ctd_subscription_id)

        self.ctd_stream1_publisher.publish('test_bind_stream_subscription')
        self.assertTrue(event.wait(2))
        event.clear()

        self.ctd_stream2_publisher.publish('test_bind_stream_subscription')
        self.assertTrue(event.wait(2))
        event.clear()

        subscriber.stop()


    def test_bind_exchange_subscription(self):

        event = gevent.event.Event()

        def message_received(message, headers):
            self.assertIn('test_bind_exchange_subscription',message)
            self.assertFalse(event.is_set())
            event.set()



        subscriber = SimpleStreamSubscriber.new_subscriber(self.container,self.exchange2_name,callback=message_received)
        subscriber.start()

        self.pubsub_cli.activate_subscription(self.exchange_subscription_id)

        self.ctd_stream1_publisher.publish('test_bind_exchange_subscription')
        self.assertTrue(event.wait(10))
        event.clear()

        self.ctd_stream2_publisher.publish('test_bind_exchange_subscription')
        self.assertTrue(event.wait(10))
        event.clear()

        subscriber.stop()


    def test_unbind_stream_subscription(self):

        event = gevent.event.Event()

        def message_received(message, headers):
            self.assertIn('test_unbind_stream_subscription',message)
            self.assertFalse(event.is_set())
            event.set()


        subscriber = SimpleStreamSubscriber.new_subscriber(self.container,self.exchange_name,callback=message_received)
        subscriber.start()

        self.pubsub_cli.activate_subscription(self.ctd_subscription_id)

        self.ctd_stream1_publisher.publish('test_unbind_stream_subscription')
        self.assertTrue(event.wait(2))
        event.clear()

        self.pubsub_cli.deactivate_subscription(self.ctd_subscription_id)


        self.ctd_stream2_publisher.publish('test_unbind_stream_subscription')
        self.assertFalse(event.wait(1))

        subscriber.stop()


    def test_unbind_exchange_subscription(self):

        event = gevent.event.Event()

        def message_received(message, headers):
            self.assertIn('test_unbind_exchange_subscription',message)
            self.assertFalse(event.is_set())
            event.set()


        subscriber = SimpleStreamSubscriber.new_subscriber(self.container,'another_queue',callback=message_received)
        subscriber.start()

        self.pubsub_cli.activate_subscription(self.exchange_subscription_id)

        self.ctd_stream1_publisher.publish('test_unbind_exchange_subscription')
        self.assertTrue(event.wait(10))
        event.clear()


        self.pubsub_cli.deactivate_subscription(self.exchange_subscription_id)


        self.ctd_stream2_publisher.publish('test_unbind_exchange_subscription')
        self.assertFalse(event.wait(2))

        subscriber.stop()

    def test_update_stream_subscription(self):

        event = gevent.event.Event()

        def message_received(message, headers):
            self.assertIn('test_update_stream_subscription',message)
            self.assertFalse(event.is_set())
            event.set()

        subscriber = SimpleStreamSubscriber.new_subscriber(self.container,self.exchange_name,callback=message_received)
        subscriber.start()

        self.pubsub_cli.activate_subscription(self.ctd_subscription_id)


        self.ctd_stream1_publisher.publish('test_update_stream_subscription')
        self.assertTrue(event.wait(2))
        event.clear()

        self.ctd_stream2_publisher.publish('test_update_stream_subscription')
        self.assertTrue(event.wait(2))
        event.clear()



        # Update the subscription by removing a stream...
        subscription = self.pubsub_cli.read_subscription(self.ctd_subscription_id)
        stream_ids = list(subscription.query.stream_ids)
        stream_ids.remove(self.ctd_stream2_id)
        self.pubsub_cli.update_subscription(
            subscription_id=subscription._id,
            query=StreamQuery(stream_ids=stream_ids)
        )


        # Stream 2 is no longer received
        self.ctd_stream2_publisher.publish('test_update_stream_subscription')

        self.assertFalse(event.wait(0.5))


        # Stream 1 is as before
        self.ctd_stream1_publisher.publish('test_update_stream_subscription')
        self.assertTrue(event.wait(2))
        event.clear()


        # Now swith the active streams...

        # Update the subscription by removing a stream...
        self.pubsub_cli.update_subscription(
            subscription_id=self.ctd_subscription_id,
            query=StreamQuery([self.ctd_stream2_id])
        )


        # Stream 1 is no longer received
        self.ctd_stream1_publisher.publish('test_update_stream_subscription')
        self.assertFalse(event.wait(1))


        # Stream 2 is received
        self.ctd_stream2_publisher.publish('test_update_stream_subscription')
        self.assertTrue(event.wait(2))
        event.clear()




        subscriber.stop()



    def test_find_stream_definition(self):
        definition = SBE37_CDM_stream_definition()
        definition_id = self.pubsub_cli.create_stream_definition(container=definition)
        stream_id = self.pubsub_cli.create_stream(stream_definition_id=definition_id)

        res_id = self.pubsub_cli.find_stream_definition(stream_id=stream_id, id_only=True)
        self.assertTrue(res_id==definition_id, 'The returned id did not match the definition_id')

        res_obj = self.pubsub_cli.find_stream_definition(stream_id=stream_id, id_only=False)
        self.assertTrue(isinstance(res_obj.container, StreamDefinitionContainer),
            'The container object is not a stream definition.')

    def test_strem_def_not_found(self):

        with self.assertRaises(NotFound):
            self.pubsub_cli.find_stream_definition(stream_id='nonexistent')

        definition = SBE37_CDM_stream_definition()
        definition_id = self.pubsub_cli.create_stream_definition(container=definition)

        with self.assertRaises(NotFound):
            self.pubsub_cli.find_stream_definition(stream_id='nonexistent')

        stream_id = self.pubsub_cli.create_stream()

        with self.assertRaises(NotFound):
            self.pubsub_cli.find_stream_definition(stream_id=stream_id)


        

    @unittest.skip("Nothing to test")
    def test_bind_already_bound_subscription(self):
        pass

    @unittest.skip("Nothing to test")
    def test_unbind_unbound_subscription(self):
        pass
