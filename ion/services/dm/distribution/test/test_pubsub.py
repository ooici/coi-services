#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Tue Sep    4 10:03:46 EDT 2012
@file ion/services/dm/distribution/test/test_pubsub.py
@brief Publication / Subscription Management Service Test Cases
'''

from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import NotFound
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.public import PRED

@attr('UNIT',group='dm')
class PubsubManagementUnitTest(PyonTestCase):
    pass

@attr('INT', group='dm')
class PubsubManagementIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/pubsub.yml')
        self.pubsub_management = PubsubManagementServiceClient()
        self.resource_registry = ResourceRegistryServiceClient()

    def test_stream_def_crud(self):
        stream_definition_id = self.pubsub_management.create_stream_definition('test_definition', parameter_dictionary={1:1}, stream_type='stream')

        stream_definition = self.pubsub_management.read_stream_definition(stream_definition_id)

        self.assertEquals(stream_definition.name,'test_definition')

        self.pubsub_management.delete_stream_definition(stream_definition_id)

        with self.assertRaises(NotFound):
            self.pubsub_management.read_stream_definition(stream_definition_id)

    def test_stream_crud(self):
        stream_def_id = self.pubsub_management.create_stream_definition('test_definition', parameter_dictionary={1:1}, stream_type='stream')
      
        stream_id = self.pubsub_management.create_stream(name='test_stream', exchange_point='test_exchange', stream_definition_id=stream_def_id)

        defs, assocs = self.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        self.assertTrue(len(defs))

        stream = self.pubsub_management.read_stream(stream_id)
        self.assertEquals(stream.name,'test_stream')
        self.pubsub_management.delete_stream(stream_id)
        
        with self.assertRaises(NotFound):
            self.pubsub_management.read_stream(stream_id)

        defs, assocs = self.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        self.assertFalse(len(defs))


    def test_subscription_crud(self):
        stream_def_id = self.pubsub_management.create_stream_definition('test_definition', parameter_dictionary={1:1}, stream_type='stream')
      
        stream_id = self.pubsub_management.create_stream(name='test_stream', exchange_point='test_exchange', stream_definition_id=stream_def_id)

        subscription_id = self.pubsub_management.create_subscription(name='test subscription', stream_ids=[stream_id], exchange_name='test_queue')

        subs, assocs = self.resource_registry.find_objects(subject=stream_id,predicate=PRED.hasSubscription,id_only=True)
        self.assertEquals(subs,[subscription_id])

        subscription = self.pubsub_management.read_subscription(subscription_id)
        self.assertEquals(subscription.exchange_name, 'test_queue')

        self.pubsub_management.delete_subscription(subscription_id)
        
        subs, assocs = self.resource_registry.find_objects(subject=stream_id,predicate=PRED.hasSubscription,id_only=True)
        self.assertFalse(len(subs))

    def test_topic_crud(self):

        topic_id = self.pubsub_management.create_topic(name='test_topic', exchange_point='test_xp')

        topic = self.pubsub_management.read_topic(topic_id)

        self.assertEquals(topic.name,'test_topic')
        self.assertEquals(topic.exchange_point, 'test_xp')

        self.pubsub_management.delete_topic(topic_id)
        with self.assertRaises(NotFound):
            self.pubsub_management.read_topic(topic_id)

