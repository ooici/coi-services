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
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.public import OT, RT, PRED, CFG
from pyon.util.containers import DotDict
from pyon.core.object import IonObjectDeserializer
from pyon.core.exception import BadRequest
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
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.services.dm.utility.test.parameter_helper import ParameterHelper

from coverage_model.coverage import AbstractCoverage

from gevent.event import Event

import unittest
import os


def validate_salinity_array(a, context={}):
    from pyon.agent.agent import ResourceAgentState
    from pyon.event.event import  EventPublisher
    from pyon.public import OT

    stream_id = context['stream_id']
    dataprocess_id = context['dataprocess_id']

    event_publisher = EventPublisher(OT.DeviceStatusAlertEvent)

    event_publisher.publish_event(  origin = stream_id, values=[dataprocess_id], description="Invalid value for salinity")



@attr('INT', group='dm')
class TestTransformWorker(IonIntegrationTestCase):

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

        self.time_dom, self.spatial_dom = time_series_domain()

        self.ph = ParameterHelper(self.dataset_management_client, self.addCleanup)

        self.wait_time = CFG.get_safe('endpoint.receive.timeout', 10)

    def push_granule(self, data_product_id):
        '''
        Publishes and monitors that the granule arrived
        '''
        datasets, _ = self.rrclient.find_objects(data_product_id, PRED.hasDataset, id_only=True)
        dataset_monitor = DatasetMonitor(datasets[0])

        rdt = self.ph.rdt_for_data_product(data_product_id)
        self.ph.fill_parsed_rdt(rdt)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)


        assert dataset_monitor.wait()
        dataset_monitor.stop()



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_transform_worker(self):

        # test that a data process (type: data-product-in / data-product-out) can be defined and launched.
        # verify that the output granule fields are correctly populated

        # test that the input and output data products are linked to facilitate provenance


        self.data_process_objs = []
        self._output_stream_ids = []
        self.event_verified = Event()

        self.parameter_dict_id = self.dataset_management_client.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict', id_only=True)

        # create the StreamDefinition
        self.stream_def_id = self.pubsub_client.create_stream_definition(name='stream_def', parameter_dictionary_id=self.parameter_dict_id)
        self.addCleanup(self.pubsub_client.delete_stream_definition, self.stream_def_id)

        # create the DataProduct that is the input to the data processes
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product', description='input test stream',
                                             temporal_domain = self.time_dom.dump(),  spatial_domain = self.spatial_dom.dump())
        self.input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)

        #retrieve the Stream for this data product
        stream_ids, assoc_ids = self.rrclient.find_objects(self.input_dp_id, PRED.hasStream, RT.Stream, True)
        self.stream_id = stream_ids[0]

        #create the DPD and two DPs
        dataprocessdef_id, dataprocess_id, dataproduct_id = self.create_data_process()
        self.dp_list = [dataprocess_id]

        #verify that in and out data products are linked
        assocs = self.rrclient.find_associations(dataproduct_id, PRED.hasDataProductParent, self.input_dp_id)
        self.assertTrue(len(assocs) == 1)

        #retrieve subscription from data process
        subscription_objs, _ = self.rrclient.find_objects(subject=dataprocess_id, predicate=PRED.hasSubscription, object_type=RT.Subscription, id_only=False)
        log.debug('test_transform_worker subscription_obj:  %s', subscription_objs[0])

        #create a queue to catch the published granules
        self.subscription_id = self.pubsub_client.create_subscription(name='parsed_subscription', stream_ids=[self.stream_id], exchange_name=subscription_objs[0].exchange_name)
        self.addCleanup(self.pubsub_client.delete_subscription, self.subscription_id)

        self.pubsub_client.activate_subscription(self.subscription_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, self.subscription_id)

        stream_route = self.pubsub_client.read_stream_route(self.stream_id)
        self.publisher = StandaloneStreamPublisher(stream_id=self.stream_id, stream_route=stream_route )

        self.start_event_listener()

        rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        rdt['time']         = [0] # time should always come first
        rdt['conductivity'] = [1]
        rdt['pressure']     = [2]
        rdt['salinity']     = [8]

        self.publisher.publish(rdt.to_granule())

        #validate that the output granule is received and the updated value is correct
        self.assertTrue(self.event_verified.wait(self.wait_time))

        #validate that the code from the transform funcation can be retrieve via inspect_data_process_definition
        src = self.dataprocessclient.inspect_data_process_definition(dataprocessdef_id)
        self.assertIn( 'def add_arrays(a, b)', src)



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_event_transform_worker(self):
        self.data_process_objs = []
        self._output_stream_ids = []
        self.event_verified = Event()


        # test that a data process (type: data-product-in / event-out) can be defined and launched.
        # verify that event fields are correctly populated


        self.parameter_dict_id = self.dataset_management_client.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict', id_only=True)

        # create the StreamDefinition
        self.stream_def_id = self.pubsub_client.create_stream_definition(name='stream_def', parameter_dictionary_id=self.parameter_dict_id)
        self.addCleanup(self.pubsub_client.delete_stream_definition, self.stream_def_id)

        # create the DataProduct
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product', description='input test stream',
                                             temporal_domain = self.time_dom.dump(),  spatial_domain = self.spatial_dom.dump())
        self.input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)

        #retrieve the Stream for this data product
        stream_ids, assoc_ids = self.rrclient.find_objects(self.input_dp_id, PRED.hasStream, RT.Stream, True)
        self.stream_id = stream_ids[0]

        #create the DPD and two DPs
        self.event_data_process_id = self.create_event_data_processes()

        #retrieve subscription from data process
        subscription_objs, _ = self.rrclient.find_objects(subject=self.event_data_process_id, predicate=PRED.hasSubscription, object_type=RT.Subscription, id_only=False)
        log.debug('test_event_transform_worker subscription_obj:  %s', subscription_objs[0])

        #create a queue to catch the published granules
        self.subscription_id = self.pubsub_client.create_subscription(name='parsed_subscription', stream_ids=[self.stream_id], exchange_name=subscription_objs[0].exchange_name)
        self.addCleanup(self.pubsub_client.delete_subscription, self.subscription_id)

        self.pubsub_client.activate_subscription(self.subscription_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, self.subscription_id)

        stream_route = self.pubsub_client.read_stream_route(self.stream_id)
        self.publisher = StandaloneStreamPublisher(stream_id=self.stream_id, stream_route=stream_route )

        self.start_event_transform_listener()

        self.data_modified = Event()
        #self.data_modified.wait(5)

        rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        rdt['time']         = [0] # time should always come first
        rdt['conductivity'] = [1]
        rdt['pressure']     = [2]
        rdt['salinity']     = [8]

        self.publisher.publish(rdt.to_granule())

        self.assertTrue(self.event_verified.wait(self.wait_time))



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_bad_argument_map(self):
        self._output_stream_ids = []

        # test that a data process (type: data-product-in / data-product-out) parameter mapping it validated during
        # data process creation and that the correct exception is raised for both input and output.


        self.parameter_dict_id = self.dataset_management_client.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict', id_only=True)

        # create the StreamDefinition
        self.stream_def_id = self.pubsub_client.create_stream_definition(name='stream_def', parameter_dictionary_id=self.parameter_dict_id)
        self.addCleanup(self.pubsub_client.delete_stream_definition, self.stream_def_id)

        # create the DataProduct that is the input to the data processes
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product', description='input test stream',
                                             temporal_domain = self.time_dom.dump(),  spatial_domain = self.spatial_dom.dump())
        self.input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)


        #two data processes using one transform and one DPD

        dp1_func_output_dp_id =  self.create_output_data_product()


        # Set up DPD and DP #2 - array add function
        tf_obj = IonObject(RT.TransformFunction,
            name='add_array_func',
            description='adds values in an array',
            function='add_arrays',
            module="ion_example.add_arrays",
            arguments=['arr1', 'arr2'],
            function_type=TransformFunctionType.TRANSFORM

            )
        add_array_func_id, rev = self.rrclient.create(tf_obj)

        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='add_arrays',
            description='adds the values of two arrays',
            data_process_type=DataProcessTypeEnum.TRANSFORM_PROCESS,
            uri='http://sddevrepo.oceanobservatories.org/releases/ion_example-0.1-py2.7.egg'
            )
        add_array_dpd_id = self.dataprocessclient.create_data_process_definition(data_process_definition=dpd_obj, function_id=add_array_func_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(self.stream_def_id, add_array_dpd_id, binding='add_array_func' )

        # Create the data process with invalid argument map
        argument_map = {"arr1": "foo", "arr2": "bar"}
        output_param = "salinity"
        with self.assertRaises(BadRequest) as cm:
            dp1_data_process_id = self.dataprocessclient.create_data_process(data_process_definition_id=add_array_dpd_id, inputs=[self.input_dp_id],
                                                                                 outputs=[dp1_func_output_dp_id], argument_map=argument_map, out_param_name=output_param)

        ex = cm.exception
        log.debug(' exception raised: %s', cm)
        self.assertEqual(ex.message, "Input data product does not contain the parameters defined in argument map")

        # Create the data process with invalid output parameter name
        argument_map = {"arr1": "conductivity", "arr2": "pressure"}
        output_param = "foo"
        with self.assertRaises(BadRequest) as cm:
            dp1_data_process_id = self.dataprocessclient.create_data_process(data_process_definition_id=add_array_dpd_id, inputs=[self.input_dp_id],
                                                                                 outputs=[dp1_func_output_dp_id], argument_map=argument_map, out_param_name=output_param)

        ex = cm.exception
        log.debug(' exception raised: %s', cm)
        self.assertEqual(ex.message, "Output data product does not contain the output parameter name provided")


    def create_event_data_processes(self):

        #two data processes using one transform and one DPD
        argument_map= {"a": "salinity"}


        # Set up DPD and DP #2 - array add function
        tf_obj = IonObject(RT.TransformFunction,
            name='validate_salinity_array',
            description='validate_salinity_array',
            function='validate_salinity_array',
            module="ion.processes.data.transforms.test.test_transform_worker",
            arguments=['a'],
            function_type=TransformFunctionType.TRANSFORM
            )

        add_array_func_id, rev = self.rrclient.create(tf_obj)

        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='validate_salinity_array',
            description='validate_salinity_array',
            data_process_type=DataProcessTypeEnum.TRANSFORM_PROCESS,
            )
        add_array_dpd_id = self.dataprocessclient.create_data_process_definition(data_process_definition=dpd_obj, function_id=add_array_func_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(self.stream_def_id, add_array_dpd_id, binding='validate_salinity_array' )

        # Create the data process
        dp1_data_process_id = self.dataprocessclient.create_data_process(data_process_definition_id=add_array_dpd_id, inputs=[self.input_dp_id],
                                                                             outputs=None, argument_map=argument_map)
        self.damsclient.register_process(dp1_data_process_id)
        self.addCleanup(self.dataprocessclient.delete_data_process, dp1_data_process_id)


        return dp1_data_process_id

    def create_data_process(self):

        #two data processes using one transform and one DPD

        dp1_func_output_dp_id =  self.create_output_data_product()
        argument_map = {"arr1": "conductivity", "arr2": "pressure"}
        output_param = "salinity"


        # Set up DPD and DP #2 - array add function
        tf_obj = IonObject(RT.TransformFunction,
            name='add_array_func',
            description='adds values in an array',
            function='add_arrays',
            module="ion_example.add_arrays",
            arguments=['arr1', 'arr2'],
            function_type=TransformFunctionType.TRANSFORM

            )
        add_array_func_id, rev = self.rrclient.create(tf_obj)

        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='add_arrays',
            description='adds the values of two arrays',
            data_process_type=DataProcessTypeEnum.TRANSFORM_PROCESS,
            uri='http://sddevrepo.oceanobservatories.org/releases/ion_example-0.1-py2.7.egg'
            )
        add_array_dpd_id = self.dataprocessclient.create_data_process_definition(data_process_definition=dpd_obj, function_id=add_array_func_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(self.stream_def_id, add_array_dpd_id, binding='add_array_func' )

        # Create the data process
        dp1_data_process_id = self.dataprocessclient.create_data_process(data_process_definition_id=add_array_dpd_id, inputs=[self.input_dp_id],
                                                                             outputs=[dp1_func_output_dp_id], argument_map=argument_map, out_param_name=output_param)
        self.damsclient.register_process(dp1_data_process_id)
        self.addCleanup(self.dataprocessclient.delete_data_process, dp1_data_process_id)

        return add_array_dpd_id, dp1_data_process_id, dp1_func_output_dp_id


    def create_output_data_product(self):

        dp1_outgoing_stream_id = self.pubsub_client.create_stream_definition(name='dp1_stream', parameter_dictionary_id=self.parameter_dict_id)

        dp1_output_dp_obj = IonObject(  RT.DataProduct,
            name='data_process1_data_product',
            description='output of add array func',
            temporal_domain = self.time_dom.dump(),
            spatial_domain = self.spatial_dom.dump())

        dp1_func_output_dp_id = self.dataproductclient.create_data_product(dp1_output_dp_obj,  dp1_outgoing_stream_id)
        self.addCleanup(self.dataproductclient.delete_data_product, dp1_func_output_dp_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product and add to granule logger
        stream_ids, _ = self.rrclient.find_objects(dp1_func_output_dp_id, PRED.hasStream, None, True)
        self._output_stream_ids.append(stream_ids[0])


        subscription_id = self.pubsub_client.create_subscription('validator', data_product_ids=[dp1_func_output_dp_id])
        self.addCleanup(self.pubsub_client.delete_subscription, subscription_id)

        def on_granule(msg, route, stream_id):
            log.debug('recv_packet stream_id: %s route: %s   msg: %s', stream_id, route, msg)
            self.validate_output_granule(msg, route, stream_id)
            self.event_verified.set()


        validator = StandaloneStreamSubscriber('validator', callback=on_granule)
        validator.start()
        self.addCleanup(validator.stop)

        self.pubsub_client.activate_subscription(subscription_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, subscription_id)

        return dp1_func_output_dp_id


    def validate_event(self, *args, **kwargs):
        """
        This method is a callback function for receiving DataProcessStatusEvent.
        """
        data_process_event = args[0]
        log.debug("DataProcessStatusEvent: %s" ,  str(data_process_event.__dict__))
        self.assertIn( data_process_event.origin, self.dp_list)


    def validate_output_granule(self, msg, route, stream_id):
        self.assertIn( stream_id, self._output_stream_ids)

        rdt = RecordDictionaryTool.load_from_granule(msg)
        log.debug('validate_output_granule  rdt: %s', rdt)
        sal_val = rdt['salinity']
        np.testing.assert_array_equal(sal_val, np.array([3]))

    def start_event_listener(self):

        es = EventSubscriber(event_type=OT.DataProcessStatusEvent, callback=self.validate_event)
        es.start()

        self.addCleanup(es.stop)

    def validate_transform_event(self, *args, **kwargs):
        """
        This method is a callback function for receiving DataProcessStatusEvent.
        """
        status_alert_event = args[0]

        np.testing.assert_array_equal(status_alert_event.origin, self.stream_id )
        np.testing.assert_array_equal(status_alert_event.values, np.array([self.event_data_process_id]))
        log.debug("DeviceStatusAlertEvent: %s" ,  str(status_alert_event.__dict__))
        self.event_verified.set()


    def start_event_transform_listener(self):

        es = EventSubscriber(event_type=OT.DeviceStatusAlertEvent, callback=self.validate_transform_event)
        es.start()

        self.addCleanup(es.stop)



    def test_download(self):
        egg_url = 'http://sddevrepo.oceanobservatories.org/releases/ion_example-0.1-py2.7.egg'
        egg_path = TransformWorker.download_egg(egg_url)

        import pkg_resources
        pkg_resources.working_set.add_entry(egg_path)

        from ion_example.add_arrays import add_arrays

        a = add_arrays(1,2)
        self.assertEquals(a,3)
