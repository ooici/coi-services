#!/usr/bin/env python
'''
@author M Manning
@file ion/processes/data/transforms/test/test_transform_worker.py
'''

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.processes.data.ingestion.stream_ingestion_worker import retrieve_stream

import numpy as np

from pyon.ion.stream import StandaloneStreamPublisher, StreamSubscriber, StandaloneStreamSubscriber
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.file_sys import FileSystem, FS
from pyon.event.event import EventSubscriber
from pyon.public import OT, RT, PRED, CFG
from pyon.util.containers import DotDict
from pyon.core.object import IonObjectDeserializer
from pyon.core.bootstrap import get_obj_registry
from pyon.public import log, IonObject
from interface.objects import  DataProcessTypeEnum, DataProcessTypeEnum, TransformFunctionType

from nose.plugins.attrib import attr

from ion.services.dm.utility.granule_utils import SimplexCoverage, ParameterDictionary, GridDomain, ParameterContext
from coverage_model.parameter_types import QuantityType, ArrayType
from coverage_model import NumexprFunction, PythonFunction

from ion.services.dm.utility.granule_utils import time_series_domain
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from ion.processes.data.transforms.transform_worker import TransformWorker

from coverage_model.coverage import AbstractCoverage

from gevent.event import Event

import unittest
import os


@attr('INT', group='dm')
class TestTransformWorkerSubscriptions(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management_client = DatasetManagementServiceClient(node=self.container.node)
        self.pubsub_client = PubsubManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)

        self.wait_time = CFG.get_safe('endpoint.receive.timeout', 10)



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_multi_subscriptions(self):
        self.dp_list = []
        self.event1_verified = Event()
        self.event2_verified = Event()

        self.parameter_dict_id = self.dataset_management_client.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict', id_only=True)

        # create the StreamDefinition
        self.stream_def_id = self.pubsub_client.create_stream_definition(name='stream_def', parameter_dictionary_id=self.parameter_dict_id)
        self.addCleanup(self.pubsub_client.delete_stream_definition, self.stream_def_id)

        # create the DataProduct
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product_one', description='input test stream one')
        self.input_dp_one_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)

        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product_two', description='input test stream two')
        self.input_dp_two_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)

        #retrieve the Stream for this data product
        stream_ids, assoc_ids = self.rrclient.find_objects(self.input_dp_one_id, PRED.hasStream, RT.Stream, True)
        self.stream_one_id = stream_ids[0]

        stream_ids, assoc_ids = self.rrclient.find_objects(self.input_dp_two_id, PRED.hasStream, RT.Stream, True)
        self.stream_two_id = stream_ids[0]


        dpd_id = self.create_data_process_definition()
        dp1_func_output_dp_id, dp2_func_output_dp_id =  self.create_output_data_products()
        first_dp_id = self.create_data_process_one(dpd_id, dp1_func_output_dp_id)

        second_dp_id = self.create_data_process_two(dpd_id, self.input_dp_two_id, dp2_func_output_dp_id)

        #retrieve subscription from data process
        subscription_objs, _ = self.rrclient.find_objects(subject=first_dp_id, predicate=PRED.hasSubscription, object_type=RT.Subscription, id_only=False)
        log.debug('test_transform_worker subscription_obj:  %s', subscription_objs[0])

        #create subscription to stream ONE, create data process and publish granule on stream ONE

        #create a queue to catch the published granules of stream ONE
        self.subscription_one_id = self.pubsub_client.create_subscription(name='parsed_subscription_one', stream_ids=[self.stream_one_id], exchange_name=subscription_objs[0].exchange_name)
        self.addCleanup(self.pubsub_client.delete_subscription, self.subscription_one_id)

        self.pubsub_client.activate_subscription(self.subscription_one_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, self.subscription_one_id)

        stream_route_one = self.pubsub_client.read_stream_route(self.stream_one_id)
        self.publisher_one = StandaloneStreamPublisher(stream_id=self.stream_one_id, stream_route=stream_route_one )

        self.start_event_listener()

        #data process 1 adds conductivity + pressure and puts the result in salinity
        rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        rdt['time']         = [0] # time should always come first
        rdt['conductivity'] = [1]
        rdt['pressure']     = [2]
        rdt['salinity']     = [8]

        self.publisher_one.publish(msg=rdt.to_granule(), stream_id=self.stream_one_id)

        #retrieve subscription from data process
        subscription_objs, _ = self.rrclient.find_objects(subject=second_dp_id, predicate=PRED.hasSubscription, object_type=RT.Subscription, id_only=False)
        log.debug('test_transform_worker subscription_obj:  %s', subscription_objs[0])

        #create subscription to stream ONE and TWO, move TW subscription, create data process and publish granule on stream TWO

        #create a queue to catch the published granules of stream TWO
        self.subscription_two_id = self.pubsub_client.create_subscription(name='parsed_subscription_one_two', stream_ids=[self.stream_two_id], exchange_name=subscription_objs[0].exchange_name)
        self.addCleanup(self.pubsub_client.delete_subscription, self.subscription_two_id)

        self.pubsub_client.activate_subscription(self.subscription_two_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, self.subscription_two_id)

        stream_route_two = self.pubsub_client.read_stream_route(self.stream_two_id)
        self.publisher_two = StandaloneStreamPublisher(stream_id=self.stream_two_id, stream_route=stream_route_two )

        #data process 1 adds conductivity + pressure and puts the result in salinity
        rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        rdt['time']         = [0] # time should always come first
        rdt['conductivity'] = [1]
        rdt['pressure']     = [2]
        rdt['salinity']     = [8]

        self.publisher_one.publish(msg=rdt.to_granule(), stream_id=self.stream_one_id)

        #data process 2 adds salinity + pressure and puts the result in conductivity
        rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        rdt['time']         = [0] # time should always come first
        rdt['conductivity'] = [22]
        rdt['pressure']     = [4]
        rdt['salinity']     = [1]

        self.publisher_two.publish(msg=rdt.to_granule(), stream_id=self.stream_two_id)


        self.assertTrue(self.event2_verified.wait(self.wait_time))
        self.assertTrue(self.event1_verified.wait(self.wait_time))


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_two_transforms_inline(self):
        self.dp_list = []
        self.event1_verified = Event()
        self.event2_verified = Event()

        self.parameter_dict_id = self.dataset_management_client.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict', id_only=True)

        # create the StreamDefinition
        self.stream_def_id = self.pubsub_client.create_stream_definition(name='stream_def', parameter_dictionary_id=self.parameter_dict_id)
        self.addCleanup(self.pubsub_client.delete_stream_definition, self.stream_def_id)

        # create the DataProduct
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product_one', description='input test stream one')
        self.input_dp_one_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)


        dpd_id = self.create_data_process_definition()
        dp1_func_output_dp_id, dp2_func_output_dp_id =  self.create_output_data_products()

        first_dp_id = self.create_data_process_one(dpd_id, dp1_func_output_dp_id)
        second_dp_id = self.create_data_process_two(dpd_id, dp1_func_output_dp_id, dp2_func_output_dp_id)

        #retrieve subscription from data process one
        subscription_objs, _ = self.rrclient.find_objects(subject=first_dp_id, predicate=PRED.hasSubscription, object_type=RT.Subscription, id_only=False)
        log.debug('test_transform_worker subscription_obj:  %s', subscription_objs[0])

        #retrieve the Stream for these data product
        stream_ids, assoc_ids = self.rrclient.find_objects(self.input_dp_one_id, PRED.hasStream, RT.Stream, True)
        self.stream_one_id = stream_ids[0]
        #the input to data process two is the output from data process one
        stream_ids, assoc_ids = self.rrclient.find_objects(dp1_func_output_dp_id, PRED.hasStream, RT.Stream, True)
        self.stream_two_id = stream_ids[0]

        # Run provenance on the output dataproduct of the second data process to see all the links
        # are as expected
        output_data_product_provenance = self.dataproductclient.get_data_product_provenance(dp2_func_output_dp_id)

        # Do a basic check to see if there were 2 entries in the provenance graph. Parent and Child.
        self.assertTrue(len(output_data_product_provenance) == 3)
        # confirm that the linking from the output dataproduct to input dataproduct is correct
        self.assertTrue(dp1_func_output_dp_id in output_data_product_provenance[dp2_func_output_dp_id]['parents'])
        self.assertTrue(self.input_dp_one_id in output_data_product_provenance[dp1_func_output_dp_id]['parents'])

        #create subscription to stream ONE, create data process and publish granule on stream ONE

        #create a queue to catch the published granules of stream ONE
        subscription_id = self.pubsub_client.create_subscription(name='parsed_subscription', stream_ids=[self.stream_one_id, self.stream_two_id], exchange_name=subscription_objs[0].exchange_name)
        self.addCleanup(self.pubsub_client.delete_subscription, subscription_id)

        self.pubsub_client.activate_subscription(subscription_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, subscription_id)

        stream_route_one = self.pubsub_client.read_stream_route(self.stream_one_id)
        self.publisher_one = StandaloneStreamPublisher(stream_id=self.stream_one_id, stream_route=stream_route_one )


        #retrieve subscription from data process
        subscription_objs, _ = self.rrclient.find_objects(subject=second_dp_id, predicate=PRED.hasSubscription, object_type=RT.Subscription, id_only=False)
        log.debug('test_transform_worker subscription_obj:  %s', subscription_objs[0])

        #data process 1 adds conductivity + pressure and puts the result in salinity
        #data process 2 adds salinity + pressure and puts the result in conductivity

        self.start_event_listener()

        rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        rdt['time']         = [0] # time should always come first
        rdt['conductivity'] = [1]
        rdt['pressure']     = [2]
        rdt['salinity']     = [8]

        self.publisher_one.publish(msg=rdt.to_granule(), stream_id=self.stream_one_id)


        self.assertTrue(self.event2_verified.wait(self.wait_time))
        self.assertTrue(self.event1_verified.wait(self.wait_time))



    def create_data_process_definition(self):

        #two data processes using one transform and one DPD

        # Set up DPD and DP #2 - array add function
        tf_obj = IonObject(RT.TransformFunction,
            name='add_array_func',
            description='adds values in an array',
            function='add_arrays',
            module="ion_example.add_arrays",
            arguments=['arr1', 'arr2'],
            function_type=TransformFunctionType.TRANSFORM,
            uri='http://sddevrepo.oceanobservatories.org/releases/ion_example-0.1-py2.7.egg'

            )
        add_array_func_id, rev = self.rrclient.create(tf_obj)

        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='add_arrays',
            description='adds the values of two arrays',
            data_process_type=DataProcessTypeEnum.TRANSFORM_PROCESS,
            )
        add_array_dpd_id = self.dataprocessclient.create_data_process_definition(data_process_definition=dpd_obj, function_id=add_array_func_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(self.stream_def_id, add_array_dpd_id, binding='add_array_func' )

        return add_array_dpd_id

    def create_data_process_one(self, data_process_definition_id, output_dataproduct):

        # Create the data process
        #data process 1 adds conductivity + pressure and puts the result in salinity
        argument_map = {"arr1":"conductivity", "arr2":"pressure"}
        output_param = "salinity" 
        dp1_data_process_id = self.dataprocessclient.create_data_process(
                    data_process_definition_id=data_process_definition_id, 
                    inputs=[self.input_dp_one_id], 
                    outputs=[output_dataproduct], 
                    argument_map=argument_map, 
                    out_param_name=output_param)
        self.damsclient.register_process(dp1_data_process_id)
        self.addCleanup(self.dataprocessclient.delete_data_process, dp1_data_process_id)
        self.dp_list.append(dp1_data_process_id)

        return dp1_data_process_id


    def create_data_process_two(self, data_process_definition_id, input_dataproduct, output_dataproduct):

        # Create the data process
        #data process 2 adds salinity + pressure and puts the result in conductivity
        argument_map = {'arr1':'salinity', 'arr2':'pressure'}
        output_param = 'conductivity'
        dp2_func_data_process_id = self.dataprocessclient.create_data_process(
                    data_process_definition_id=data_process_definition_id, 
                    inputs=[input_dataproduct],
                    outputs=[output_dataproduct], 
                    argument_map=argument_map, 
                    out_param_name=output_param)
        self.damsclient.register_process(dp2_func_data_process_id)
        self.addCleanup(self.dataprocessclient.delete_data_process, dp2_func_data_process_id)
        self.dp_list.append(dp2_func_data_process_id)

        return  dp2_func_data_process_id


    def create_output_data_products(self):

        dp1_outgoing_stream_id = self.pubsub_client.create_stream_definition(name='dp1_stream', parameter_dictionary_id=self.parameter_dict_id)

        dp1_output_dp_obj = IonObject(  RT.DataProduct,
            name='data_process1_data_product',
            description='output of add array func')

        dp1_func_output_dp_id = self.dataproductclient.create_data_product(dp1_output_dp_obj,  dp1_outgoing_stream_id)
        self.addCleanup(self.dataproductclient.delete_data_product, dp1_func_output_dp_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product and add to granule logger
        stream_ids, _ = self.rrclient.find_objects(dp1_func_output_dp_id, PRED.hasStream, None, True)
        self._output_stream_one_id = stream_ids[0]


        dp2_func_outgoing_stream_id = self.pubsub_client.create_stream_definition(name='dp2_stream', parameter_dictionary_id=self.parameter_dict_id)

        dp2_func_output_dp_obj = IonObject(  RT.DataProduct,
            name='data_process2_data_product',
            description='output of add array func')

        dp2_func_output_dp_id = self.dataproductclient.create_data_product(dp2_func_output_dp_obj,  dp2_func_outgoing_stream_id)
        self.addCleanup(self.dataproductclient.delete_data_product, dp2_func_output_dp_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product and add to granule logger
        stream_ids, _ = self.rrclient.find_objects(dp2_func_output_dp_id, PRED.hasStream, None, True)
        self._output_stream_two_id = stream_ids[0]


        subscription_id = self.pubsub_client.create_subscription('validator', data_product_ids=[dp1_func_output_dp_id, dp2_func_output_dp_id])
        self.addCleanup(self.pubsub_client.delete_subscription, subscription_id)

        def on_granule(msg, route, stream_id):
            log.debug('recv_packet stream_id: %s route: %s   msg: %s', stream_id, route, msg)
            self.validate_output_granule(msg, route, stream_id)


        validator = StandaloneStreamSubscriber('validator', callback=on_granule)
        validator.start()
        self.addCleanup(validator.stop)

        self.pubsub_client.activate_subscription(subscription_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, subscription_id)

        return dp1_func_output_dp_id, dp2_func_output_dp_id


    def validate_event(self, *args, **kwargs):
        """
        This method is a callback function for receiving DataProcessStatusEvent.
        """
        data_process_event = args[0]
        log.debug("DataProcessStatusEvent: %s" ,  str(data_process_event.__dict__))

        #if data process already created, check origin
        if not 'data process assigned to transform worker' in data_process_event.description:
            self.assertIn( data_process_event.origin, self.dp_list)


    def validate_output_granule(self, msg, route, stream_id):
        self.assertTrue( stream_id in [self._output_stream_one_id, self._output_stream_two_id])

        rdt = RecordDictionaryTool.load_from_granule(msg)
        log.debug('validate_output_granule  stream_id: %s', stream_id)

        if stream_id == self._output_stream_one_id:
            sal_val = rdt['salinity']
            log.debug('validate_output_granule  sal_val: %s', sal_val)
            np.testing.assert_array_equal(sal_val, np.array([3]))
            self.event1_verified.set()
        else:
            cond_val = rdt['conductivity']
            log.debug('validate_output_granule  cond_val: %s', cond_val)
            np.testing.assert_array_equal(cond_val, np.array([5]))
            self.event2_verified.set()

    def start_event_listener(self):

        es = EventSubscriber(event_type=OT.DataProcessStatusEvent, callback=self.validate_event)
        es.start()

        self.addCleanup(es.stop)




