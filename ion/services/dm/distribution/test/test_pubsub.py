#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Tue Sep    4 10:03:46 EDT 2012
@file ion/services/dm/distribution/test/test_pubsub.py
@brief Publication / Subscription Management Service Test Cases
'''
from pyon.core.exception import NotFound
from pyon.ion.stream import StandaloneStreamSubscriber, StandaloneStreamPublisher
from pyon.public import PRED, RT
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from gevent.event import Event
from gevent.queue import Queue, Empty
from nose.plugins.attrib import attr

import gevent

@attr('UNIT',group='dm')
class PubsubManagementUnitTest(PyonTestCase):
    pass

@attr('INT', group='dm')
class PubsubManagementIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.pubsub_management  = PubsubManagementServiceClient()
        self.resource_registry  = ResourceRegistryServiceClient()
        self.dataset_management = DatasetManagementServiceClient()


        self.queue_cleanup = list()
        self.exchange_cleanup = list()

    def tearDown(self):
        for queue in self.queue_cleanup:
            xn = self.container.ex_manager.create_xn_queue(queue)
            xn.delete()
        for exchange in self.exchange_cleanup:
            xp = self.container.ex_manager.create_xp(exchange)
            xp.delete()

    def test_stream_def_crud(self):

        # Test Creation
        pdict = DatasetManagementService.get_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_definition_id = self.pubsub_management.create_stream_definition('ctd parsed', parameter_dictionary_id=pdict.identifier)

        # Make sure there is an assoc
        self.assertTrue(self.resource_registry.find_associations(subject=stream_definition_id, predicate=PRED.hasParameterDictionary, object=pdict.identifier, id_only=True))

        # Test Reading
        stream_definition = self.pubsub_management.read_stream_definition(stream_definition_id)
        self.assertTrue(PubsubManagementService._compare_pdicts(pdict.dump(), stream_definition.parameter_dictionary))

        # Test Deleting
        self.pubsub_management.delete_stream_definition(stream_definition_id)
        self.assertFalse(self.resource_registry.find_associations(subject=stream_definition_id, predicate=PRED.hasParameterDictionary, object=pdict.identifier, id_only=True))


        # Test comparisons
        in_stream_definition_id = self.pubsub_management.create_stream_definition('L0 products', parameter_dictionary=pdict.identifier, available_fields=['time','temp','conductivity','pressure'])
        self.addCleanup(self.pubsub_management.delete_stream_definition, in_stream_definition_id)

        out_stream_definition_id = in_stream_definition_id
        self.assertTrue(self.pubsub_management.compare_stream_definition(in_stream_definition_id, out_stream_definition_id))
        self.assertTrue(self.pubsub_management.compatible_stream_definitions(in_stream_definition_id, out_stream_definition_id))

        out_stream_definition_id = self.pubsub_management.create_stream_definition('L2 Products', parameter_dictionary=pdict.identifier, available_fields=['time','salinity','density'])
        self.addCleanup(self.pubsub_management.delete_stream_definition, out_stream_definition_id)
        self.assertFalse(self.pubsub_management.compare_stream_definition(in_stream_definition_id, out_stream_definition_id))

        self.assertTrue(self.pubsub_management.compatible_stream_definitions(in_stream_definition_id, out_stream_definition_id))




    def publish_on_stream(self, stream_id, msg):
        stream = self.pubsub_management.read_stream(stream_id)
        stream_route = stream.stream_route
        publisher = StandaloneStreamPublisher(stream_id=stream_id, stream_route=stream_route)
        publisher.publish(msg)

    def test_stream_crud(self):
        stream_def_id = self.pubsub_management.create_stream_definition('test_definition', stream_type='stream')
        topic_id = self.pubsub_management.create_topic(name='test_topic', exchange_point='test_exchange')
        self.exchange_cleanup.append('test_exchange')
        topic2_id = self.pubsub_management.create_topic(name='another_topic', exchange_point='outside')
        stream_id, route = self.pubsub_management.create_stream(name='test_stream', topic_ids=[topic_id, topic2_id], exchange_point='test_exchange', stream_definition_id=stream_def_id)

        topics, assocs = self.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasTopic, id_only=True)
        self.assertEquals(topics,[topic_id])

        defs, assocs = self.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        self.assertTrue(len(defs))

        stream = self.pubsub_management.read_stream(stream_id)
        self.assertEquals(stream.name,'test_stream')
        self.pubsub_management.delete_stream(stream_id)
        
        with self.assertRaises(NotFound):
            self.pubsub_management.read_stream(stream_id)

        defs, assocs = self.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
        self.assertFalse(len(defs))

        topics, assocs = self.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasTopic, id_only=True)
        self.assertFalse(len(topics))

        self.pubsub_management.delete_topic(topic_id)
        self.pubsub_management.delete_topic(topic2_id)
        self.pubsub_management.delete_stream_definition(stream_def_id)


    def test_subscription_crud(self):
        stream_def_id = self.pubsub_management.create_stream_definition('test_definition', stream_type='stream')
        stream_id, route = self.pubsub_management.create_stream(name='test_stream', exchange_point='test_exchange', stream_definition_id=stream_def_id)
        subscription_id = self.pubsub_management.create_subscription(name='test subscription', stream_ids=[stream_id], exchange_name='test_queue')
        self.exchange_cleanup.append('test_exchange')

        subs, assocs = self.resource_registry.find_objects(subject=subscription_id,predicate=PRED.hasStream,id_only=True)
        self.assertEquals(subs,[stream_id])

        res, _ = self.resource_registry.find_resources(restype=RT.ExchangeName, name='test_queue', id_only=True)
        self.assertEquals(len(res),1)

        subs, assocs = self.resource_registry.find_subjects(object=subscription_id, predicate=PRED.hasSubscription, id_only=True)
        self.assertEquals(subs[0], res[0])

        subscription = self.pubsub_management.read_subscription(subscription_id)
        self.assertEquals(subscription.exchange_name, 'test_queue')

        self.pubsub_management.delete_subscription(subscription_id)
        
        subs, assocs = self.resource_registry.find_objects(subject=subscription_id,predicate=PRED.hasStream,id_only=True)
        self.assertFalse(len(subs))

        subs, assocs = self.resource_registry.find_subjects(object=subscription_id, predicate=PRED.hasSubscription, id_only=True)
        self.assertFalse(len(subs))


        self.pubsub_management.delete_stream(stream_id)
        self.pubsub_management.delete_stream_definition(stream_def_id)

    def test_move_before_activate(self):
        stream_id, route = self.pubsub_management.create_stream(name='test_stream', exchange_point='test_xp')

        #--------------------------------------------------------------------------------
        # Test moving before activate
        #--------------------------------------------------------------------------------

        subscription_id = self.pubsub_management.create_subscription('first_queue', stream_ids=[stream_id])

        xn_ids, _ = self.resource_registry.find_resources(restype=RT.ExchangeName, name='first_queue', id_only=True)
        subjects, _ = self.resource_registry.find_subjects(object=subscription_id, predicate=PRED.hasSubscription, id_only=True)
        self.assertEquals(xn_ids[0], subjects[0])

        self.pubsub_management.move_subscription(subscription_id, exchange_name='second_queue')

        xn_ids, _ = self.resource_registry.find_resources(restype=RT.ExchangeName, name='second_queue', id_only=True)
        subjects, _ = self.resource_registry.find_subjects(object=subscription_id, predicate=PRED.hasSubscription, id_only=True)

        self.assertEquals(len(subjects),1)
        self.assertEquals(subjects[0], xn_ids[0])

        self.pubsub_management.delete_subscription(subscription_id)
        self.pubsub_management.delete_stream(stream_id)

    def test_move_activated_subscription(self):

        stream_id, route = self.pubsub_management.create_stream(name='test_stream', exchange_point='test_xp')
        #--------------------------------------------------------------------------------
        # Test moving after activate
        #--------------------------------------------------------------------------------

        subscription_id = self.pubsub_management.create_subscription('first_queue', stream_ids=[stream_id])
        self.pubsub_management.activate_subscription(subscription_id)

        xn_ids, _ = self.resource_registry.find_resources(restype=RT.ExchangeName, name='first_queue', id_only=True)
        subjects, _ = self.resource_registry.find_subjects(object=subscription_id, predicate=PRED.hasSubscription, id_only=True)
        self.assertEquals(xn_ids[0], subjects[0])

        self.verified = Event()

        def verify(m,r,s):
            self.assertEquals(m,'verified')
            self.verified.set()

        subscriber = StandaloneStreamSubscriber('second_queue', verify)
        subscriber.start()

        self.pubsub_management.move_subscription(subscription_id, exchange_name='second_queue')

        xn_ids, _ = self.resource_registry.find_resources(restype=RT.ExchangeName, name='second_queue', id_only=True)
        subjects, _ = self.resource_registry.find_subjects(object=subscription_id, predicate=PRED.hasSubscription, id_only=True)

        self.assertEquals(len(subjects),1)
        self.assertEquals(subjects[0], xn_ids[0])

        publisher = StandaloneStreamPublisher(stream_id, route)
        publisher.publish('verified')

        self.assertTrue(self.verified.wait(2))

        self.pubsub_management.deactivate_subscription(subscription_id)

        self.pubsub_management.delete_subscription(subscription_id)
        self.pubsub_management.delete_stream(stream_id)

    def test_queue_cleanup(self):
        stream_id, route = self.pubsub_management.create_stream('test_stream','xp1')
        xn_objs, _ = self.resource_registry.find_resources(restype=RT.ExchangeName, name='queue1')
        for xn_obj in xn_objs:
            xn = self.container.ex_manager.create_xn_queue(xn_obj.name)
            xn.delete()
        subscription_id = self.pubsub_management.create_subscription('queue1',stream_ids=[stream_id])
        xn_ids, _ = self.resource_registry.find_resources(restype=RT.ExchangeName, name='queue1')
        self.assertEquals(len(xn_ids),1)

        self.pubsub_management.delete_subscription(subscription_id)

        xn_ids, _ = self.resource_registry.find_resources(restype=RT.ExchangeName, name='queue1')
        self.assertEquals(len(xn_ids),0)

    def test_activation_and_deactivation(self):
        stream_id, route = self.pubsub_management.create_stream('stream1','xp1')
        subscription_id = self.pubsub_management.create_subscription('sub1', stream_ids=[stream_id])

        self.check1 = Event()

        def verifier(m,r,s):
            self.check1.set()


        subscriber = StandaloneStreamSubscriber('sub1',verifier)
        subscriber.start()

        publisher = StandaloneStreamPublisher(stream_id, route)
        publisher.publish('should not receive')

        self.assertFalse(self.check1.wait(0.25))

        self.pubsub_management.activate_subscription(subscription_id)

        publisher.publish('should receive')
        self.assertTrue(self.check1.wait(2))

        self.check1.clear()
        self.assertFalse(self.check1.is_set())

        self.pubsub_management.deactivate_subscription(subscription_id)

        publisher.publish('should not receive')
        self.assertFalse(self.check1.wait(0.5))

        self.pubsub_management.activate_subscription(subscription_id)

        publisher.publish('should receive')
        self.assertTrue(self.check1.wait(2))

        subscriber.stop()

        self.pubsub_management.deactivate_subscription(subscription_id)
        self.pubsub_management.delete_subscription(subscription_id)
        self.pubsub_management.delete_stream(stream_id)

        

    def test_topic_crud(self):

        topic_id = self.pubsub_management.create_topic(name='test_topic', exchange_point='test_xp')
        self.exchange_cleanup.append('test_xp')

        topic = self.pubsub_management.read_topic(topic_id)

        self.assertEquals(topic.name,'test_topic')
        self.assertEquals(topic.exchange_point, 'test_xp')

        self.pubsub_management.delete_topic(topic_id)
        with self.assertRaises(NotFound):
            self.pubsub_management.read_topic(topic_id)

    def test_full_pubsub(self):

        self.sub1_sat = Event()
        self.sub2_sat = Event()

        def subscriber1(m,r,s):
            self.sub1_sat.set()

        def subscriber2(m,r,s):
            self.sub2_sat.set()

        sub1 = StandaloneStreamSubscriber('sub1', subscriber1)
        self.queue_cleanup.append(sub1.xn.queue)
        sub1.start()

        sub2 = StandaloneStreamSubscriber('sub2', subscriber2)
        self.queue_cleanup.append(sub2.xn.queue)
        sub2.start()

        log_topic = self.pubsub_management.create_topic('instrument_logs', exchange_point='instruments')
        science_topic = self.pubsub_management.create_topic('science_data', exchange_point='instruments')
        events_topic = self.pubsub_management.create_topic('notifications', exchange_point='events')


        log_stream, route = self.pubsub_management.create_stream('instrument1-logs', topic_ids=[log_topic], exchange_point='instruments')
        ctd_stream, route = self.pubsub_management.create_stream('instrument1-ctd', topic_ids=[science_topic], exchange_point='instruments')
        event_stream, route = self.pubsub_management.create_stream('notifications', topic_ids=[events_topic], exchange_point='events')
        raw_stream, route = self.pubsub_management.create_stream('temp', exchange_point='global.data')
        self.exchange_cleanup.extend(['instruments','events','global.data'])


        subscription1 = self.pubsub_management.create_subscription('subscription1', stream_ids=[log_stream,event_stream], exchange_name='sub1')
        subscription2 = self.pubsub_management.create_subscription('subscription2', exchange_points=['global.data'], stream_ids=[ctd_stream], exchange_name='sub2')

        self.pubsub_management.activate_subscription(subscription1)
        self.pubsub_management.activate_subscription(subscription2)

        self.publish_on_stream(log_stream, 1)
        self.assertTrue(self.sub1_sat.wait(4))
        self.assertFalse(self.sub2_sat.is_set())

        self.publish_on_stream(raw_stream,1)
        self.assertTrue(self.sub1_sat.wait(4))

        sub1.stop()
        sub2.stop()


    def test_topic_craziness(self):

        self.msg_queue = Queue()

        def subscriber1(m,r,s):
            self.msg_queue.put(m)

        sub1 = StandaloneStreamSubscriber('sub1', subscriber1)
        self.queue_cleanup.append(sub1.xn.queue)
        sub1.start()

        topic1 = self.pubsub_management.create_topic('topic1', exchange_point='xp1')
        topic2 = self.pubsub_management.create_topic('topic2', exchange_point='xp1', parent_topic_id=topic1)
        topic3 = self.pubsub_management.create_topic('topic3', exchange_point='xp1', parent_topic_id=topic1)
        topic4 = self.pubsub_management.create_topic('topic4', exchange_point='xp1', parent_topic_id=topic2)
        topic5 = self.pubsub_management.create_topic('topic5', exchange_point='xp1', parent_topic_id=topic2)
        topic6 = self.pubsub_management.create_topic('topic6', exchange_point='xp1', parent_topic_id=topic3)
        topic7 = self.pubsub_management.create_topic('topic7', exchange_point='xp1', parent_topic_id=topic3)

        # Tree 2
        topic8 = self.pubsub_management.create_topic('topic8', exchange_point='xp2')
        topic9 = self.pubsub_management.create_topic('topic9', exchange_point='xp2', parent_topic_id=topic8)
        topic10 = self.pubsub_management.create_topic('topic10', exchange_point='xp2', parent_topic_id=topic9)
        topic11 = self.pubsub_management.create_topic('topic11', exchange_point='xp2', parent_topic_id=topic9)
        topic12 = self.pubsub_management.create_topic('topic12', exchange_point='xp2', parent_topic_id=topic11)
        topic13 = self.pubsub_management.create_topic('topic13', exchange_point='xp2', parent_topic_id=topic11)
        self.exchange_cleanup.extend(['xp1','xp2'])
        
        stream1_id, route = self.pubsub_management.create_stream('stream1', topic_ids=[topic7, topic4, topic5], exchange_point='xp1')
        stream2_id, route = self.pubsub_management.create_stream('stream2', topic_ids=[topic8], exchange_point='xp2')
        stream3_id, route = self.pubsub_management.create_stream('stream3', topic_ids=[topic10,topic13], exchange_point='xp2')
        stream4_id, route = self.pubsub_management.create_stream('stream4', topic_ids=[topic9], exchange_point='xp2')
        stream5_id, route = self.pubsub_management.create_stream('stream5', topic_ids=[topic11], exchange_point='xp2')

        subscription1 = self.pubsub_management.create_subscription('sub1', topic_ids=[topic1])
        subscription2 = self.pubsub_management.create_subscription('sub2', topic_ids=[topic8], exchange_name='sub1')
        subscription3 = self.pubsub_management.create_subscription('sub3', topic_ids=[topic9], exchange_name='sub1')
        subscription4 = self.pubsub_management.create_subscription('sub4', topic_ids=[topic10,topic13, topic11], exchange_name='sub1')
        #--------------------------------------------------------------------------------
        self.pubsub_management.activate_subscription(subscription1)

        self.publish_on_stream(stream1_id,1)

        self.assertEquals(self.msg_queue.get(timeout=10), 1)
        with self.assertRaises(Empty):
            self.msg_queue.get(timeout=0.1)


        self.pubsub_management.deactivate_subscription(subscription1)
        self.pubsub_management.delete_subscription(subscription1)
        #--------------------------------------------------------------------------------
        self.pubsub_management.activate_subscription(subscription2)
        
        self.publish_on_stream(stream2_id,2)
        self.assertEquals(self.msg_queue.get(timeout=10), 2)
        with self.assertRaises(Empty):
            self.msg_queue.get(timeout=0.1)

        self.pubsub_management.deactivate_subscription(subscription2)
        self.pubsub_management.delete_subscription(subscription2)

        #--------------------------------------------------------------------------------
        self.pubsub_management.activate_subscription(subscription3)

        self.publish_on_stream(stream2_id, 3)
        with self.assertRaises(Empty):
            self.msg_queue.get(timeout=0.3)

        self.publish_on_stream(stream3_id, 4)
        self.assertEquals(self.msg_queue.get(timeout=10),4)


        self.pubsub_management.deactivate_subscription(subscription3)
        self.pubsub_management.delete_subscription(subscription3)

        #--------------------------------------------------------------------------------
        self.pubsub_management.activate_subscription(subscription4)

        self.publish_on_stream(stream4_id, 5)
        with self.assertRaises(Empty):
            self.msg_queue.get(timeout=0.3)

        self.publish_on_stream(stream5_id, 6)
        self.assertEquals(self.msg_queue.get(timeout=10),6)
        with self.assertRaises(Empty):
            self.msg_queue.get(timeout=0.3)

        self.pubsub_management.deactivate_subscription(subscription4)
        self.pubsub_management.delete_subscription(subscription4)
        
        #--------------------------------------------------------------------------------
        sub1.stop()

        self.pubsub_management.delete_topic(topic13)
        self.pubsub_management.delete_topic(topic12)
        self.pubsub_management.delete_topic(topic11)
        self.pubsub_management.delete_topic(topic10)
        self.pubsub_management.delete_topic(topic9)
        self.pubsub_management.delete_topic(topic8)
        self.pubsub_management.delete_topic(topic7)
        self.pubsub_management.delete_topic(topic6)
        self.pubsub_management.delete_topic(topic5)
        self.pubsub_management.delete_topic(topic4)
        self.pubsub_management.delete_topic(topic3)
        self.pubsub_management.delete_topic(topic2)
        self.pubsub_management.delete_topic(topic1)

        self.pubsub_management.delete_stream(stream1_id)
        self.pubsub_management.delete_stream(stream2_id)
        self.pubsub_management.delete_stream(stream3_id)
        self.pubsub_management.delete_stream(stream4_id)
        self.pubsub_management.delete_stream(stream5_id)
