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
from interface.objects import  DataProcessTypeEnum, DataProcessTypeEnum, TransformFunctionType, AttachmentType

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
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient


from coverage_model.coverage import AbstractCoverage
from pyon.util.context import LocalContextMixin

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

class TransformWorkerTestProcess(LocalContextMixin):
    name = 'tranform_worker_test'
    id='tranform_worker_int_test'
    process_type = 'simple'

@attr('INT', group='dm')
class TestTransformWorker(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        # Instantiate a process to represent the test
        process=TransformWorkerTestProcess()

        self.dataset_management_client = DatasetManagementServiceClient(node=self.container.node)
        self.pubsub_client = PubsubManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceProcessClient(node=self.container.node, process = process)

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

        self.dp_list = []
        self.data_process_objs = []
        self._output_stream_ids = []
        self.granule_verified = Event()
        self.worker_assigned_event_verified = Event()
        self.dp_created_event_verified = Event()
        self.heartbeat_event_verified = Event()

        self.parameter_dict_id = self.dataset_management_client.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict', id_only=True)

        # create the StreamDefinition
        self.stream_def_id = self.pubsub_client.create_stream_definition(name='stream_def', parameter_dictionary_id=self.parameter_dict_id)
        self.addCleanup(self.pubsub_client.delete_stream_definition, self.stream_def_id)

        # create the DataProduct that is the input to the data processes
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product', description='input test stream')
        self.input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)

        # retrieve the Stream for this data product
        stream_ids, assoc_ids = self.rrclient.find_objects(self.input_dp_id, PRED.hasStream, RT.Stream, True)
        self.stream_id = stream_ids[0]

        self.start_event_listener()

        # create the DPD, DataProcess and output DataProduct
        dataprocessdef_id, dataprocess_id, dataproduct_id = self.create_data_process()
        self.dp_list.append(dataprocess_id)

        # validate the repository for data product algorithms persists the new resources  NEW SA-1
        # create_data_process call created one of each
        dpd_ids, _ = self.rrclient.find_resources(restype=OT.DataProcessDefinition, id_only=False)
        # there will be more than one becuase of the DPDs that reperesent the PFs in the data product above
        self.assertTrue(dpd_ids is not None)
        dp_ids, _ = self.rrclient.find_resources(restype=OT.DataProcess, id_only=False)
        # only one DP becuase the PFs that are in the code dataproduct above are not activated yet.
        self.assertEquals(len(dp_ids), 1)


        # validate the name and version label  NEW SA - 2
        dataprocessdef_obj = self.dataprocessclient.read_data_process_definition(dataprocessdef_id)
        self.assertEqual(dataprocessdef_obj.version_label, '1.0a')
        self.assertEqual(dataprocessdef_obj.name, 'add_arrays')

        # validate that the DPD has an attachment  NEW SA - 21
        attachment_ids, assoc_ids = self.rrclient.find_objects(dataprocessdef_id, PRED.hasAttachment, RT.Attachment, True)
        self.assertEqual(len(attachment_ids), 1)
        attachment_obj = self.rrclient.read_attachment(attachment_ids[0])
        log.debug('attachment: %s', attachment_obj)

        # validate that the data process resource has input and output data products associated
        # L4-CI-SA-RQ-364  and NEW SA-3
        outproduct_ids, assoc_ids = self.rrclient.find_objects(dataprocess_id, PRED.hasOutputProduct, RT.DataProduct, True)
        self.assertEqual(len(outproduct_ids), 1)
        inproduct_ids, assoc_ids = self.rrclient.find_objects(dataprocess_id, PRED.hasInputProduct, RT.DataProduct, True)
        self.assertEqual(len(inproduct_ids), 1)

        # Test for provenance. Get Data product produced by the data processes
        output_data_product_id,_ = self.rrclient.find_objects(subject=dataprocess_id,
            object_type=RT.DataProduct,
            predicate=PRED.hasOutputProduct,
            id_only=True)

        output_data_product_provenance = self.dataproductclient.get_data_product_provenance(output_data_product_id[0])

        # Do a basic check to see if there were 3 entries in the provenance graph. Parent and Child and the
        # DataProcessDefinition creating the child from the parent.
        self.assertTrue(len(output_data_product_provenance) == 2)
        self.assertTrue(self.input_dp_id in output_data_product_provenance[output_data_product_id[0]]['parents'])
        self.assertTrue(output_data_product_provenance[output_data_product_id[0]]['parents'][self.input_dp_id]['data_process_definition_id'] == dataprocessdef_id)


        # NEW SA - 4 | Data processing shall include the appropriate data product algorithm name and version number in
        # the metadata of each output data product created by the data product algorithm.
        output_data_product_obj,_ = self.rrclient.find_objects(subject=dataprocess_id,
            object_type=RT.DataProduct,
            predicate=PRED.hasOutputProduct,
            id_only=False)
        self.assertTrue(output_data_product_obj[0].name != None)
        self.assertTrue(output_data_product_obj[0]._rev != None)

        # retrieve subscription from data process
        subscription_objs, _ = self.rrclient.find_objects(subject=dataprocess_id, predicate=PRED.hasSubscription, object_type=RT.Subscription, id_only=False)
        log.debug('test_transform_worker subscription_obj:  %s', subscription_objs[0])

        # create a queue to catch the published granules
        self.subscription_id = self.pubsub_client.create_subscription(name='parsed_subscription', stream_ids=[self.stream_id], exchange_name=subscription_objs[0].exchange_name)
        self.addCleanup(self.pubsub_client.delete_subscription, self.subscription_id)

        self.pubsub_client.activate_subscription(self.subscription_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, self.subscription_id)

        stream_route = self.pubsub_client.read_stream_route(self.stream_id)
        self.publisher = StandaloneStreamPublisher(stream_id=self.stream_id, stream_route=stream_route )


        for n in range(1, 101):
            rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
            rdt['time']         = [0] # time should always come first
            rdt['conductivity'] = [1]
            rdt['pressure']     = [2]
            rdt['salinity']     = [8]

            self.publisher.publish(rdt.to_granule())

        # validate that the output granule is received and the updated value is correct
        self.assertTrue(self.granule_verified.wait(self.wait_time))


        # validate that the data process loaded into worker event is received    (L4-CI-SA-RQ-182)
        self.assertTrue(self.worker_assigned_event_verified.wait(self.wait_time))

        # validate that the data process create (with data product ids) event is received    (NEW SA -42)
        self.assertTrue(self.dp_created_event_verified.wait(self.wait_time))

        # validate that the data process heartbeat event is received (for every hundred granules processed) (L4-CI-SA-RQ-182)
        #this takes a while so set wait limit to large value
        self.assertTrue(self.heartbeat_event_verified.wait(200))

        # validate that the code from the transform function can be retrieve via inspect_data_process_definition
        src = self.dataprocessclient.inspect_data_process_definition(dataprocessdef_id)
        self.assertIn( 'def add_arrays(a, b)', src)

        # now delete the DPD and DP then verify that the resources are retired so that information required for provenance are still available
        self.dataprocessclient.delete_data_process(dataprocess_id)
        self.dataprocessclient.delete_data_process_definition(dataprocessdef_id)

        in_dp_objs, _ = self.rrclient.find_objects(subject=dataprocess_id, predicate=PRED.hasInputProduct, object_type=RT.DataProduct, id_only=True)
        self.assertTrue(in_dp_objs is not None)

        dpd_objs, _ = self.rrclient.find_subjects(subject_type=RT.DataProcessDefinition, predicate=PRED.hasDataProcess, object=dataprocess_id, id_only=True)
        self.assertTrue(dpd_objs is not None)

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_transform_worker_with_instrumentdevice(self):

        # test that a data process (type: data-product-in / data-product-out) can be defined and launched.
        # verify that the output granule fields are correctly populated

        # test that the input and output data products are linked to facilitate provenance

        self.data_process_objs = []
        self._output_stream_ids = []
        self.event_verified = Event()

        # Create CTD Parsed as the initial data product
        # create a stream definition for the data from the ctd simulator
        self.parameter_dict_id = self.dataset_management_client.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        self.stream_def_id = self.pubsub_client.create_stream_definition(name='stream_def', parameter_dictionary_id=self.parameter_dict_id)

        # create the DataProduct that is the input to the data processes
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product', description='input test stream')
        self.input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)

        # retrieve the Stream for this data product
        stream_ids, assoc_ids = self.rrclient.find_objects(self.input_dp_id, PRED.hasStream, RT.Stream, True)
        self.stream_id = stream_ids[0]

        log.debug('new ctd_parsed_data_product_id = %s' % self.input_dp_id)

        # only ever need one device for testing purposes.
        instDevice_obj,_ = self.rrclient.find_resources(restype=RT.InstrumentDevice, name='test_ctd_device')
        if instDevice_obj:
            instDevice_id = instDevice_obj[0]._id
        else:
            instDevice_obj = IonObject(RT.InstrumentDevice, name='test_ctd_device', description="test_ctd_device", serial_number="12345" )
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=self.input_dp_id)

        # create the DPD, DataProcess and output DataProduct
        dataprocessdef_id, dataprocess_id, dataproduct_id = self.create_data_process()

        self.addCleanup(self.dataprocessclient.delete_data_process, dataprocess_id)
        self.addCleanup(self.dataprocessclient.delete_data_process_definition, dataprocessdef_id)

        # Test for provenance. Get Data product produced by the data processes
        output_data_product_id,_ = self.rrclient.find_objects(subject=dataprocess_id,
            object_type=RT.DataProduct,
            predicate=PRED.hasOutputProduct,
            id_only=True)

        output_data_product_provenance = self.dataproductclient.get_data_product_provenance(output_data_product_id[0])

        # Do a basic check to see if there were 3 entries in the provenance graph. Parent and Child and the
        # DataProcessDefinition creating the child from the parent.
        self.assertTrue(len(output_data_product_provenance) == 3)
        self.assertTrue(self.input_dp_id in output_data_product_provenance[output_data_product_id[0]]['parents'])
        self.assertTrue(instDevice_id in output_data_product_provenance[self.input_dp_id]['parents'])
        self.assertTrue(output_data_product_provenance[instDevice_id]['type'] == 'InstrumentDevice')

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_transform_worker_with_platformdevice(self):

        # test that a data process (type: data-product-in / data-product-out) can be defined and launched.
        # verify that the output granule fields are correctly populated

        # test that the input and output data products are linked to facilitate provenance

        self.data_process_objs = []
        self._output_stream_ids = []
        self.event_verified = Event()

        # Create CTD Parsed as the initial data product
        # create a stream definition for the data from the ctd simulator
        self.parameter_dict_id = self.dataset_management_client.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        self.stream_def_id = self.pubsub_client.create_stream_definition(name='stream_def', parameter_dictionary_id=self.parameter_dict_id)

        # create the DataProduct that is the input to the data processes
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product', description='input test stream')
        self.input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)

        # retrieve the Stream for this data product
        stream_ids, assoc_ids = self.rrclient.find_objects(self.input_dp_id, PRED.hasStream, RT.Stream, True)
        self.stream_id = stream_ids[0]

        log.debug('new ctd_parsed_data_product_id = %s' % self.input_dp_id)

        # only ever need one device for testing purposes.
        platform_device_obj,_ = self.rrclient.find_resources(restype=RT.PlatformDevice, name='TestPlatform')
        if platform_device_obj:
            platform_device_id = platform_device_obj[0]._id
        else:
            platform_device_obj = IonObject(RT.PlatformDevice, name='TestPlatform', description="TestPlatform", serial_number="12345" )
            platform_device_id = self.imsclient.create_platform_device(platform_device=platform_device_obj)

        self.damsclient.assign_data_product(input_resource_id=platform_device_id, data_product_id=self.input_dp_id)

        # create the DPD, DataProcess and output DataProduct
        dataprocessdef_id, dataprocess_id, dataproduct_id = self.create_data_process()
        self.addCleanup(self.dataprocessclient.delete_data_process, dataprocess_id)
        self.addCleanup(self.dataprocessclient.delete_data_process_definition, dataprocessdef_id)

        # Test for provenance. Get Data product produced by the data processes
        output_data_product_id,_ = self.rrclient.find_objects(subject=dataprocess_id,
            object_type=RT.DataProduct,
            predicate=PRED.hasOutputProduct,
            id_only=True)

        output_data_product_provenance = self.dataproductclient.get_data_product_provenance(output_data_product_id[0])

        # Do a basic check to see if there were 3 entries in the provenance graph. Parent and Child and the
        # DataProcessDefinition creating the child from the parent.
        self.assertTrue(len(output_data_product_provenance) == 3)
        self.assertTrue(self.input_dp_id in output_data_product_provenance[output_data_product_id[0]]['parents'])
        self.assertTrue(platform_device_id in output_data_product_provenance[self.input_dp_id]['parents'])
        self.assertTrue(output_data_product_provenance[platform_device_id]['type'] == 'PlatformDevice')


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
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product', description='input test stream')
        self.input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)

        # retrieve the Stream for this data product
        stream_ids, assoc_ids = self.rrclient.find_objects(self.input_dp_id, PRED.hasStream, RT.Stream, True)
        self.stream_id = stream_ids[0]

        # create the DPD and two DPs
        self.event_data_process_id = self.create_event_data_processes()

        # retrieve subscription from data process
        subscription_objs, _ = self.rrclient.find_objects(subject=self.event_data_process_id, predicate=PRED.hasSubscription, object_type=RT.Subscription, id_only=False)
        log.debug('test_event_transform_worker subscription_obj:  %s', subscription_objs[0])

        # create a queue to catch the published granules
        self.subscription_id = self.pubsub_client.create_subscription(name='parsed_subscription', stream_ids=[self.stream_id], exchange_name=subscription_objs[0].exchange_name)
        self.addCleanup(self.pubsub_client.delete_subscription, self.subscription_id)

        self.pubsub_client.activate_subscription(self.subscription_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, self.subscription_id)

        stream_route = self.pubsub_client.read_stream_route(self.stream_id)
        self.publisher = StandaloneStreamPublisher(stream_id=self.stream_id, stream_route=stream_route )

        self.start_event_transform_listener()

        self.data_modified = Event()

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
        input_dp_obj = IonObject(  RT.DataProduct, name='input_data_product', description='input test stream')
        self.input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj,  stream_definition_id=self.stream_def_id)

        # two data processes using one transform and one DPD

        dp1_func_output_dp_id =  self.create_output_data_product()


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
            data_process_type=DataProcessTypeEnum.TRANSFORM_PROCESS
            )
        add_array_dpd_id = self.dataprocessclient.create_data_process_definition(data_process_definition=dpd_obj, function_id=add_array_func_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(self.stream_def_id, add_array_dpd_id, binding='add_array_func' )

        # create the data process with invalid argument map
        argument_map = {"arr1": "foo", "arr2": "bar"}
        output_param = "salinity"
        with self.assertRaises(BadRequest) as cm:
            dp1_data_process_id = self.dataprocessclient.create_data_process(data_process_definition_id=add_array_dpd_id, inputs=[self.input_dp_id],
                                                                                 outputs=[dp1_func_output_dp_id], argument_map=argument_map, out_param_name=output_param)

        ex = cm.exception
        log.debug(' exception raised: %s', cm)
        self.assertEqual(ex.message, "Input data product does not contain the parameters defined in argument map")

        # create the data process with invalid output parameter name
        argument_map = {"arr1": "conductivity", "arr2": "pressure"}
        output_param = "foo"
        with self.assertRaises(BadRequest) as cm:
            dp1_data_process_id = self.dataprocessclient.create_data_process(data_process_definition_id=add_array_dpd_id, inputs=[self.input_dp_id],
                                                                                 outputs=[dp1_func_output_dp_id], argument_map=argument_map, out_param_name=output_param)

        ex = cm.exception
        log.debug(' exception raised: %s', cm)
        self.assertEqual(ex.message, "Output data product does not contain the output parameter name provided")


    def create_event_data_processes(self):

        # two data processes using one transform and one DPD
        argument_map= {"a": "salinity"}


        # set up DPD and DP #2 - array add function
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

        # create the data process
        dp1_data_process_id = self.dataprocessclient.create_data_process(data_process_definition_id=add_array_dpd_id, inputs=[self.input_dp_id],
                                                                             outputs=None, argument_map=argument_map)
        self.damsclient.register_process(dp1_data_process_id)
        self.addCleanup(self.dataprocessclient.delete_data_process, dp1_data_process_id)

        return dp1_data_process_id

    def create_data_process(self):

        # two data processes using one transform and one DPD

        dp1_func_output_dp_id =  self.create_output_data_product()
        argument_map = {"arr1": "conductivity", "arr2": "pressure"}
        output_param = "salinity"


        # set up DPD and DP #2 - array add function
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
            version_label='1.0a'
            )
        add_array_dpd_id = self.dataprocessclient.create_data_process_definition(data_process_definition=dpd_obj, function_id=add_array_func_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(self.stream_def_id, add_array_dpd_id, binding='add_array_func' )

        # create the data process
        dp1_data_process_id = self.dataprocessclient.create_data_process(data_process_definition_id=add_array_dpd_id, inputs=[self.input_dp_id],
                                                                             outputs=[dp1_func_output_dp_id], argument_map=argument_map, out_param_name=output_param)
        self.damsclient.register_process(dp1_data_process_id)
        #self.addCleanup(self.dataprocessclient.delete_data_process, dp1_data_process_id)

        # add an attachment object to this DPD to test new SA-21
        import msgpack
        attachment_content = 'foo bar'
        attachment_obj = IonObject( RT.Attachment,
                                name='test_attachment',
                                attachment_type=AttachmentType.ASCII,
                                content_type='text/plain',
                                content=msgpack.packb(attachment_content))
        att_id = self.rrclient.create_attachment(add_array_dpd_id, attachment_obj)
        self.addCleanup(self.rrclient.delete_attachment, att_id)

        return add_array_dpd_id, dp1_data_process_id, dp1_func_output_dp_id


    def create_output_data_product(self):
        dp1_outgoing_stream_id = self.pubsub_client.create_stream_definition(name='dp1_stream', parameter_dictionary_id=self.parameter_dict_id)

        dp1_output_dp_obj = IonObject(  RT.DataProduct,
            name='data_process1_data_product',
            description='output of add array func')

        dp1_func_output_dp_id = self.dataproductclient.create_data_product(dp1_output_dp_obj,  dp1_outgoing_stream_id)
        self.addCleanup(self.dataproductclient.delete_data_product, dp1_func_output_dp_id)
        # retrieve the id of the OUTPUT stream from the out Data Product and add to granule logger
        stream_ids, _ = self.rrclient.find_objects(dp1_func_output_dp_id, PRED.hasStream, None, True)
        self._output_stream_ids.append(stream_ids[0])

        subscription_id = self.pubsub_client.create_subscription('validator', data_product_ids=[dp1_func_output_dp_id])
        self.addCleanup(self.pubsub_client.delete_subscription, subscription_id)

        def on_granule(msg, route, stream_id):
            log.debug('recv_packet stream_id: %s route: %s   msg: %s', stream_id, route, msg)
            self.validate_output_granule(msg, route, stream_id)
            self.granule_verified.set()

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

        # if data process already created, check origin
        if self.dp_list:
            self.assertIn( data_process_event.origin, self.dp_list)

            # if this is a heartbeat event then 100 granules have been processed
            if 'data process status update.' in data_process_event.description:
                self.heartbeat_event_verified.set()

        else:
            # else check that this is the assign event

            if 'Data process assigned to transform worker' in data_process_event.description:
                self.worker_assigned_event_verified.set()
            elif 'Data process created for data product' in data_process_event.description:
                self.dp_created_event_verified.set()


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
