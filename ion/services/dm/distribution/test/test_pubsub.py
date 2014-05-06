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
from ion.services.dm.utility.granule_utils import time_series_domain

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient

from interface.objects import DataProduct

from gevent.event import Event
from gevent.queue import Queue, Empty
from nose.plugins.attrib import attr

from coverage_model import ParameterContext, AxisTypeEnum, QuantityType, ConstantType, NumexprFunction, ParameterFunctionType, VariabilityEnum, PythonFunction
import numpy as np
import unittest

@attr('UNIT',group='dm')
class PubsubManagementUnitTest(PyonTestCase):
    pass

@attr('INT', group='dm')
class PubsubManagementIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.pubsub_management       = PubsubManagementServiceClient()
        self.resource_registry       = ResourceRegistryServiceClient()
        self.dataset_management      = DatasetManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()

        self.pdicts = {}
        self.queue_cleanup = list()
        self.exchange_cleanup = list()
        self.context_ids = set()

    def tearDown(self):
        for queue in self.queue_cleanup:
            xn = self.container.ex_manager.create_xn_queue(queue)
            xn.delete()
        for exchange in self.exchange_cleanup:
            xp = self.container.ex_manager.create_xp(exchange)
            xp.delete()

        self.cleanup_contexts()
    
    def test_stream_def_crud(self):

        # Test Creation
        pdict = DatasetManagementService.get_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_definition_id = self.pubsub_management.create_stream_definition('ctd parsed', parameter_dictionary_id=pdict.identifier)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_definition_id)

        # Make sure there is an assoc
        self.assertTrue(self.resource_registry.find_associations(subject=stream_definition_id, predicate=PRED.hasParameterDictionary, object=pdict.identifier, id_only=True))

        # Test Reading
        stream_definition = self.pubsub_management.read_stream_definition(stream_definition_id)
        self.assertTrue(PubsubManagementService._compare_pdicts(pdict.dump(), stream_definition.parameter_dictionary))


        # Test comparisons
        in_stream_definition_id = self.pubsub_management.create_stream_definition('L0 products', parameter_dictionary_id=pdict.identifier, available_fields=['time','temp','conductivity','pressure'])
        self.addCleanup(self.pubsub_management.delete_stream_definition, in_stream_definition_id)

        out_stream_definition_id = in_stream_definition_id
        self.assertTrue(self.pubsub_management.compare_stream_definition(in_stream_definition_id, out_stream_definition_id))
        self.assertTrue(self.pubsub_management.compatible_stream_definitions(in_stream_definition_id, out_stream_definition_id))

        out_stream_definition_id = self.pubsub_management.create_stream_definition('L2 Products', parameter_dictionary_id=pdict.identifier, available_fields=['time','salinity','density'])
        self.addCleanup(self.pubsub_management.delete_stream_definition, out_stream_definition_id)
        self.assertFalse(self.pubsub_management.compare_stream_definition(in_stream_definition_id, out_stream_definition_id))

        self.assertTrue(self.pubsub_management.compatible_stream_definitions(in_stream_definition_id, out_stream_definition_id))

    @unittest.skip('Needs to be refactored for cleanup')
    def test_validate_stream_defs(self):
        self.addCleanup(self.cleanup_contexts)
        #test no input 
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0'])
        outgoing_pdict_id = self._get_pdict(['DENSITY', 'PRACSAL', 'TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1'])
        available_fields_in = []
        available_fields_out = []
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_0', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_0', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertFalse(result)
    
        #test input with no output
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0'])
        outgoing_pdict_id = self._get_pdict(['DENSITY', 'PRACSAL', 'TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1'])
        available_fields_in = ['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0']
        available_fields_out = []
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_1', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_1', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertTrue(result)
        
        #test available field missing parameter context definition -- missing PRESWAT_L0
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0'])
        outgoing_pdict_id = self._get_pdict(['DENSITY', 'PRACSAL', 'TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1'])
        available_fields_in = ['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0']
        available_fields_out = ['DENSITY']
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_2', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_2', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertFalse(result)

        #test l1 from l0
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0'])
        outgoing_pdict_id = self._get_pdict(['TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1'])
        available_fields_in = ['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0']
        available_fields_out = ['TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1']
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_3', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_3', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertTrue(result)

        #test l2 from l0
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0'])
        outgoing_pdict_id = self._get_pdict(['TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1', 'DENSITY', 'PRACSAL'])
        available_fields_in = ['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0']
        available_fields_out = ['DENSITY', 'PRACSAL']
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_4', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_4', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertTrue(result)
        
        #test Ln from L0
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0'])
        outgoing_pdict_id = self._get_pdict(['DENSITY','PRACSAL','TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1'])
        available_fields_in = ['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0']
        available_fields_out = ['DENSITY', 'PRACSAL', 'TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1']
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_5', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_5', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertTrue(result)
        
        #test L2 from L1
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON', 'TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1'])
        outgoing_pdict_id = self._get_pdict(['DENSITY','PRACSAL','TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1'])
        available_fields_in = ['TIME', 'LAT', 'LON', 'TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1']
        available_fields_out = ['DENSITY', 'PRACSAL']
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_6', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_6', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertTrue(result)
        
        #test L1 from L0 missing L0
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON'])
        outgoing_pdict_id = self._get_pdict(['TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1'])
        available_fields_in = ['TIME', 'LAT', 'LON']
        available_fields_out = ['DENSITY', 'PRACSAL']
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_7', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_7', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertFalse(result)
        
        #test L2 from L0 missing L0
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON'])
        outgoing_pdict_id = self._get_pdict(['DENSITY', 'PRACSAL', 'TEMPWAT_L1', 'CONDWAT_L1', 'PRESWAT_L1'])
        available_fields_in = ['TIME', 'LAT', 'LON']
        available_fields_out = ['DENSITY', 'PRACSAL']
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_8', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_8', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertFalse(result)
        
        #test L2 from L0 missing L1
        incoming_pdict_id = self._get_pdict(['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0'])
        outgoing_pdict_id = self._get_pdict(['DENSITY', 'PRACSAL'])
        available_fields_in = ['TIME', 'LAT', 'LON', 'TEMPWAT_L0', 'CONDWAT_L0', 'PRESWAT_L0']
        available_fields_out = ['DENSITY', 'PRACSAL']
        incoming_stream_def_id = self.pubsub_management.create_stream_definition('in_sd_9', parameter_dictionary_id=incoming_pdict_id, available_fields=available_fields_in)
        self.addCleanup(self.pubsub_management.delete_stream_definition, incoming_stream_def_id)
        outgoing_stream_def_id = self.pubsub_management.create_stream_definition('out_sd_9', parameter_dictionary_id=outgoing_pdict_id, available_fields=available_fields_out)
        self.addCleanup(self.pubsub_management.delete_stream_definition, outgoing_stream_def_id)
        result = self.pubsub_management.validate_stream_defs(incoming_stream_def_id, outgoing_stream_def_id)
        self.assertFalse(result)
    
    def publish_on_stream(self, stream_id, msg):
        stream = self.pubsub_management.read_stream(stream_id)
        stream_route = stream.stream_route
        publisher = StandaloneStreamPublisher(stream_id=stream_id, stream_route=stream_route)
        publisher.publish(msg)

    def test_stream_crud(self):
        stream_def_id = self.pubsub_management.create_stream_definition('test_definition', stream_type='stream')
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        topic_id = self.pubsub_management.create_topic(name='test_topic', exchange_point='test_exchange')
        self.addCleanup(self.pubsub_management.delete_topic, topic_id)
        self.exchange_cleanup.append('test_exchange')
        topic2_id = self.pubsub_management.create_topic(name='another_topic', exchange_point='outside')
        self.addCleanup(self.pubsub_management.delete_topic, topic2_id)
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



    def test_data_product_subscription(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        stream_def_id = self.pubsub_management.create_stream_definition('ctd parsed', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)

        dp = DataProduct(name='ctd parsed')

        data_product_id = self.data_product_management.create_data_product(data_product=dp, stream_definition_id=stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)

        subscription_id = self.pubsub_management.create_subscription('validator', data_product_ids=[data_product_id])
        self.addCleanup(self.pubsub_management.delete_subscription, subscription_id)

        validated = Event()
        def validation(msg, route, stream_id):
            validated.set()

        stream_ids, _ = self.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        dp_stream_id = stream_ids.pop()

        validator = StandaloneStreamSubscriber('validator', callback=validation)
        validator.start()
        self.addCleanup(validator.stop)

        self.pubsub_management.activate_subscription(subscription_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, subscription_id)

        route = self.pubsub_management.read_stream_route(dp_stream_id)

        publisher = StandaloneStreamPublisher(dp_stream_id, route)
        publisher.publish('hi')
        self.assertTrue(validated.wait(10))
            

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
        self.addCleanup(subscriber.stop)

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
        sub1.start()
        self.addCleanup(sub1.stop)

        sub2 = StandaloneStreamSubscriber('sub2', subscriber2)
        sub2.start()
        self.addCleanup(sub2.stop)

        log_topic = self.pubsub_management.create_topic('instrument_logs', exchange_point='instruments')
        self.addCleanup(self.pubsub_management.delete_topic, log_topic)
        science_topic = self.pubsub_management.create_topic('science_data', exchange_point='instruments')
        self.addCleanup(self.pubsub_management.delete_topic, science_topic)
        events_topic = self.pubsub_management.create_topic('notifications', exchange_point='events')
        self.addCleanup(self.pubsub_management.delete_topic, events_topic)


        log_stream, route = self.pubsub_management.create_stream('instrument1-logs', topic_ids=[log_topic], exchange_point='instruments')
        self.addCleanup(self.pubsub_management.delete_stream, log_stream)
        ctd_stream, route = self.pubsub_management.create_stream('instrument1-ctd', topic_ids=[science_topic], exchange_point='instruments')
        self.addCleanup(self.pubsub_management.delete_stream, ctd_stream)
        event_stream, route = self.pubsub_management.create_stream('notifications', topic_ids=[events_topic], exchange_point='events')
        self.addCleanup(self.pubsub_management.delete_stream, event_stream)
        raw_stream, route = self.pubsub_management.create_stream('temp', exchange_point='global.data')
        self.addCleanup(self.pubsub_management.delete_stream, raw_stream)


        subscription1 = self.pubsub_management.create_subscription('subscription1', stream_ids=[log_stream,event_stream], exchange_name='sub1')
        self.addCleanup(self.pubsub_management.delete_subscription, subscription1)
        subscription2 = self.pubsub_management.create_subscription('subscription2', exchange_points=['global.data'], stream_ids=[ctd_stream], exchange_name='sub2')
        self.addCleanup(self.pubsub_management.delete_subscription, subscription2)

        self.pubsub_management.activate_subscription(subscription1)
        self.addCleanup(self.pubsub_management.deactivate_subscription, subscription1)
        self.pubsub_management.activate_subscription(subscription2)
        self.addCleanup(self.pubsub_management.deactivate_subscription, subscription2)

        self.publish_on_stream(log_stream, 1)
        self.assertTrue(self.sub1_sat.wait(4))
        self.assertFalse(self.sub2_sat.is_set())

        self.publish_on_stream(raw_stream,1)
        self.assertTrue(self.sub1_sat.wait(4))
    
    def test_topic_craziness(self):

        self.msg_queue = Queue()

        def subscriber1(m,r,s):
            self.msg_queue.put(m)

        sub1 = StandaloneStreamSubscriber('sub1', subscriber1)
        sub1.start()
        self.addCleanup(sub1.stop)

        topic1 = self.pubsub_management.create_topic('topic1', exchange_point='xp1')
        self.addCleanup(self.pubsub_management.delete_topic, topic1)
        topic2 = self.pubsub_management.create_topic('topic2', exchange_point='xp1', parent_topic_id=topic1)
        self.addCleanup(self.pubsub_management.delete_topic, topic2)
        topic3 = self.pubsub_management.create_topic('topic3', exchange_point='xp1', parent_topic_id=topic1)
        self.addCleanup(self.pubsub_management.delete_topic, topic3)
        topic4 = self.pubsub_management.create_topic('topic4', exchange_point='xp1', parent_topic_id=topic2)
        self.addCleanup(self.pubsub_management.delete_topic, topic4)
        topic5 = self.pubsub_management.create_topic('topic5', exchange_point='xp1', parent_topic_id=topic2)
        self.addCleanup(self.pubsub_management.delete_topic, topic5)
        topic6 = self.pubsub_management.create_topic('topic6', exchange_point='xp1', parent_topic_id=topic3)
        self.addCleanup(self.pubsub_management.delete_topic, topic6)
        topic7 = self.pubsub_management.create_topic('topic7', exchange_point='xp1', parent_topic_id=topic3)
        self.addCleanup(self.pubsub_management.delete_topic, topic7)

        # Tree 2
        topic8 = self.pubsub_management.create_topic('topic8', exchange_point='xp2')
        self.addCleanup(self.pubsub_management.delete_topic, topic8)
        topic9 = self.pubsub_management.create_topic('topic9', exchange_point='xp2', parent_topic_id=topic8)
        self.addCleanup(self.pubsub_management.delete_topic, topic9)
        topic10 = self.pubsub_management.create_topic('topic10', exchange_point='xp2', parent_topic_id=topic9)
        self.addCleanup(self.pubsub_management.delete_topic, topic10)
        topic11 = self.pubsub_management.create_topic('topic11', exchange_point='xp2', parent_topic_id=topic9)
        self.addCleanup(self.pubsub_management.delete_topic, topic11)
        topic12 = self.pubsub_management.create_topic('topic12', exchange_point='xp2', parent_topic_id=topic11)
        self.addCleanup(self.pubsub_management.delete_topic, topic12)
        topic13 = self.pubsub_management.create_topic('topic13', exchange_point='xp2', parent_topic_id=topic11)
        self.addCleanup(self.pubsub_management.delete_topic, topic13)
        self.exchange_cleanup.extend(['xp1','xp2'])
        
        stream1_id, route = self.pubsub_management.create_stream('stream1', topic_ids=[topic7, topic4, topic5], exchange_point='xp1')
        self.addCleanup(self.pubsub_management.delete_stream, stream1_id)
        stream2_id, route = self.pubsub_management.create_stream('stream2', topic_ids=[topic8], exchange_point='xp2')
        self.addCleanup(self.pubsub_management.delete_stream, stream2_id)
        stream3_id, route = self.pubsub_management.create_stream('stream3', topic_ids=[topic10,topic13], exchange_point='xp2')
        self.addCleanup(self.pubsub_management.delete_stream, stream3_id)
        stream4_id, route = self.pubsub_management.create_stream('stream4', topic_ids=[topic9], exchange_point='xp2')
        self.addCleanup(self.pubsub_management.delete_stream, stream4_id)
        stream5_id, route = self.pubsub_management.create_stream('stream5', topic_ids=[topic11], exchange_point='xp2')
        self.addCleanup(self.pubsub_management.delete_stream, stream5_id)

        subscription1 = self.pubsub_management.create_subscription('sub1', topic_ids=[topic1])
        self.addCleanup(self.pubsub_management.delete_subscription, subscription1)
        subscription2 = self.pubsub_management.create_subscription('sub2', topic_ids=[topic8], exchange_name='sub1')
        self.addCleanup(self.pubsub_management.delete_subscription, subscription2)
        subscription3 = self.pubsub_management.create_subscription('sub3', topic_ids=[topic9], exchange_name='sub1')
        self.addCleanup(self.pubsub_management.delete_subscription, subscription3)
        subscription4 = self.pubsub_management.create_subscription('sub4', topic_ids=[topic10,topic13, topic11], exchange_name='sub1')
        self.addCleanup(self.pubsub_management.delete_subscription, subscription4)
        #--------------------------------------------------------------------------------
        self.pubsub_management.activate_subscription(subscription1)

        self.publish_on_stream(stream1_id,1)

        self.assertEquals(self.msg_queue.get(timeout=10), 1)
        with self.assertRaises(Empty):
            self.msg_queue.get(timeout=0.1)


        self.pubsub_management.deactivate_subscription(subscription1)
        #--------------------------------------------------------------------------------
        self.pubsub_management.activate_subscription(subscription2)
        
        self.publish_on_stream(stream2_id,2)
        self.assertEquals(self.msg_queue.get(timeout=10), 2)
        with self.assertRaises(Empty):
            self.msg_queue.get(timeout=0.1)

        self.pubsub_management.deactivate_subscription(subscription2)

        #--------------------------------------------------------------------------------
        self.pubsub_management.activate_subscription(subscription3)

        self.publish_on_stream(stream2_id, 3)
        with self.assertRaises(Empty):
            self.msg_queue.get(timeout=0.3)

        self.publish_on_stream(stream3_id, 4)
        self.assertEquals(self.msg_queue.get(timeout=10),4)


        self.pubsub_management.deactivate_subscription(subscription3)

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
        
        #--------------------------------------------------------------------------------

    def test_lookup_values(self):
        stream_def_id = self.create_lookup_stream_def()

        self.assertEquals(self.pubsub_management.has_lookup_values(stream_def_id), ['offset_a'])

    def create_lookup_stream_def(self):
        contexts = self.create_lookup_contexts()
        context_ids = [c_id for c,c_id in contexts.itervalues()]
        pdict_id = self.dataset_management.create_parameter_dictionary(name='lookup_pdict', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)
        stream_def_id = self.pubsub_management.create_stream_definition('lookup', parameter_dictionary_id=pdict_id, available_fields=['offset_a'])
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        return stream_def_id

    def create_lookup_contexts(self):
        contexts = {}
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('float64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, t_ctxt_id)
        contexts['time'] = (t_ctxt, t_ctxt_id)

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        temp_ctxt.uom = 'deg_C'
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='temp', parameter_context=temp_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, temp_ctxt_id)
        contexts['temp'] = temp_ctxt, temp_ctxt_id

        offset_ctxt = ParameterContext('offset_a', param_type=QuantityType(value_encoding='float32'), fill_value=-9999)
        offset_ctxt.lookup_value = True
        offset_ctxt_id = self.dataset_management.create_parameter_context(name='offset_a', parameter_context=offset_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, offset_ctxt_id)
        contexts['offset_a'] = offset_ctxt, offset_ctxt_id

        func = NumexprFunction('calibrated', 'temp + offset', ['temp','offset'], param_map={'temp':'temp', 'offset':'offset_a'})
        func.lookup_values = ['LV_offset']
        calibrated = ParameterContext('calibrated', param_type=ParameterFunctionType(func, value_encoding='float32'), fill_value=-9999)
        calibrated_id = self.dataset_management.create_parameter_context(name='calibrated', parameter_context=calibrated.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, calibrated_id)
        contexts['calibrated'] = calibrated, calibrated_id

        return contexts
    
    def cleanup_contexts(self):
        for context_id in self.context_ids:
            self.dataset_management.delete_parameter_context(context_id)

    def add_context_to_cleanup(self, context_id):
        self.context_ids.add(context_id)

    def _get_pdict(self, filter_values):
        t_ctxt = ParameterContext('TIME', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1900'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='TIME', parameter_context=t_ctxt.dump(), parameter_type='quantity<int64>', units=t_ctxt.uom)
        self.add_context_to_cleanup(t_ctxt_id)

        lat_ctxt = ParameterContext('LAT', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='LAT', parameter_context=lat_ctxt.dump(), parameter_type='quantity<float32>', units=lat_ctxt.uom)
        self.add_context_to_cleanup(lat_ctxt_id)


        lon_ctxt = ParameterContext('LON', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt_id = self.dataset_management.create_parameter_context(name='LON', parameter_context=lon_ctxt.dump(), parameter_type='quantity<float32>', units=lon_ctxt.uom)
        self.add_context_to_cleanup(lon_ctxt_id)


        # Independent Parameters
         # Temperature - values expected to be the decimal results of conversion from hex
        temp_ctxt = ParameterContext('TEMPWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        temp_ctxt.uom = 'deg_C'
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='TEMPWAT_L0', parameter_context=temp_ctxt.dump(), parameter_type='quantity<float32>', units=temp_ctxt.uom)
        self.add_context_to_cleanup(temp_ctxt_id)


        # Conductivity - values expected to be the decimal results of conversion from hex
        cond_ctxt = ParameterContext('CONDWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        cond_ctxt.uom = 'S m-1'
        cond_ctxt_id = self.dataset_management.create_parameter_context(name='CONDWAT_L0', parameter_context=cond_ctxt.dump(), parameter_type='quantity<float32>', units=cond_ctxt.uom)
        self.add_context_to_cleanup(cond_ctxt_id)


        # Pressure - values expected to be the decimal results of conversion from hex
        press_ctxt = ParameterContext('PRESWAT_L0', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        press_ctxt.uom = 'dbar'
        press_ctxt_id = self.dataset_management.create_parameter_context(name='PRESWAT_L0', parameter_context=press_ctxt.dump(), parameter_type='quantity<float32>', units=press_ctxt.uom)
        self.add_context_to_cleanup(press_ctxt_id)


        # Dependent Parameters

        # TEMPWAT_L1 = (TEMPWAT_L0 / 10000) - 10
        tl1_func = '(T / 10000) - 10'
        tl1_pmap = {'T': 'TEMPWAT_L0'}
        expr = NumexprFunction('TEMPWAT_L1', tl1_func, ['T'], param_map=tl1_pmap)
        tempL1_ctxt = ParameterContext('TEMPWAT_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        tempL1_ctxt.uom = 'deg_C'
        tempL1_ctxt_id = self.dataset_management.create_parameter_context(name=tempL1_ctxt.name, parameter_context=tempL1_ctxt.dump(), parameter_type='pfunc', units=tempL1_ctxt.uom)
        self.add_context_to_cleanup(tempL1_ctxt_id)


        # CONDWAT_L1 = (CONDWAT_L0 / 100000) - 0.5
        cl1_func = '(C / 100000) - 0.5'
        cl1_pmap = {'C': 'CONDWAT_L0'}
        expr = NumexprFunction('CONDWAT_L1', cl1_func, ['C'], param_map=cl1_pmap)
        condL1_ctxt = ParameterContext('CONDWAT_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        condL1_ctxt.uom = 'S m-1'
        condL1_ctxt_id = self.dataset_management.create_parameter_context(name=condL1_ctxt.name, parameter_context=condL1_ctxt.dump(), parameter_type='pfunc', units=condL1_ctxt.uom)
        self.add_context_to_cleanup(condL1_ctxt_id)


        # Equation uses p_range, which is a calibration coefficient - Fixing to 679.34040721
        #   PRESWAT_L1 = (PRESWAT_L0 * p_range / (0.85 * 65536)) - (0.05 * p_range)
        pl1_func = '(P * p_range / (0.85 * 65536)) - (0.05 * p_range)'
        pl1_pmap = {'P': 'PRESWAT_L0', 'p_range': 679.34040721}
        expr = NumexprFunction('PRESWAT_L1', pl1_func, ['P', 'p_range'], param_map=pl1_pmap)
        presL1_ctxt = ParameterContext('PRESWAT_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        presL1_ctxt.uom = 'S m-1'
        presL1_ctxt_id = self.dataset_management.create_parameter_context(name=presL1_ctxt.name, parameter_context=presL1_ctxt.dump(), parameter_type='pfunc', units=presL1_ctxt.uom)
        self.add_context_to_cleanup(presL1_ctxt_id)


        # Density & practical salinity calucluated using the Gibbs Seawater library - available via python-gsw project:
        #       https://code.google.com/p/python-gsw/ & http://pypi.python.org/pypi/gsw/3.0.1

        # PRACSAL = gsw.SP_from_C((CONDWAT_L1 * 10), TEMPWAT_L1, PRESWAT_L1)
        owner = 'gsw'
        sal_func = 'SP_from_C'
        sal_arglist = ['C', 't', 'p']
        sal_pmap = {'C': NumexprFunction('CONDWAT_L1*10', 'C*10', ['C'], param_map={'C': 'CONDWAT_L1'}), 't': 'TEMPWAT_L1', 'p': 'PRESWAT_L1'}
        sal_kwargmap = None
        expr = PythonFunction('PRACSAL', owner, sal_func, sal_arglist, sal_kwargmap, sal_pmap)
        sal_ctxt = ParameterContext('PRACSAL', param_type=ParameterFunctionType(expr), variability=VariabilityEnum.TEMPORAL)
        sal_ctxt.uom = 'g kg-1'
        sal_ctxt_id = self.dataset_management.create_parameter_context(name=sal_ctxt.name, parameter_context=sal_ctxt.dump(), parameter_type='pfunc', units=sal_ctxt.uom)
        self.add_context_to_cleanup(sal_ctxt_id)


        # absolute_salinity = gsw.SA_from_SP(PRACSAL, PRESWAT_L1, longitude, latitude)
        # conservative_temperature = gsw.CT_from_t(absolute_salinity, TEMPWAT_L1, PRESWAT_L1)
        # DENSITY = gsw.rho(absolute_salinity, conservative_temperature, PRESWAT_L1)
        owner = 'gsw'
        abs_sal_expr = PythonFunction('abs_sal', owner, 'SA_from_SP', ['PRACSAL', 'PRESWAT_L1', 'LON','LAT'])
        cons_temp_expr = PythonFunction('cons_temp', owner, 'CT_from_t', [abs_sal_expr, 'TEMPWAT_L1', 'PRESWAT_L1'])
        dens_expr = PythonFunction('DENSITY', owner, 'rho', [abs_sal_expr, cons_temp_expr, 'PRESWAT_L1'])
        dens_ctxt = ParameterContext('DENSITY', param_type=ParameterFunctionType(dens_expr), variability=VariabilityEnum.TEMPORAL)
        dens_ctxt.uom = 'kg m-3'
        dens_ctxt_id = self.dataset_management.create_parameter_context(name=dens_ctxt.name, parameter_context=dens_ctxt.dump(), parameter_type='pfunc', units=dens_ctxt.uom)
        self.add_context_to_cleanup(dens_ctxt_id)

        
        ids = [t_ctxt_id, lat_ctxt_id, lon_ctxt_id, temp_ctxt_id, cond_ctxt_id, press_ctxt_id, tempL1_ctxt_id, condL1_ctxt_id, presL1_ctxt_id, sal_ctxt_id, dens_ctxt_id]
        contexts = [t_ctxt, lat_ctxt, lon_ctxt, temp_ctxt, cond_ctxt, press_ctxt, tempL1_ctxt, condL1_ctxt, presL1_ctxt, sal_ctxt, dens_ctxt]
        context_ids = [ids[i] for i,ctxt in enumerate(contexts) if ctxt.name in filter_values]
        pdict_name = '_'.join([ctxt.name for ctxt in contexts if ctxt.name in filter_values])

        try:
            self.pdicts[pdict_name]
            return self.pdicts[pdict_name]
        except KeyError:
            pdict_id = self.dataset_management.create_parameter_dictionary(pdict_name, parameter_context_ids=context_ids, temporal_context='time')
            self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)
            self.pdicts[pdict_name] = pdict_id
            return pdict_id

