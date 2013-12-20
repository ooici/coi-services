#!/usr/bin/env python

"""
@file ion/services/sa/process/test/test_int_data_process_management_service.py
@author Maurice Manning
@test ion.services.sa.process.DataProcessManagementService integration test
"""

import time
import numpy as np
from gevent.event import Event
from nose.plugins.attrib import attr
from mock import patch
import gevent
from sets import Set
import unittest
import os

from pyon.util.int_test import IonIntegrationTestCase
from pyon.ion.event import EventPublisher
from pyon.public import LCS
from pyon.public import log, IonObject
from pyon.public import CFG, RT, PRED, OT
from pyon.core.exception import BadRequest, NotFound
from pyon.util.context import LocalContextMixin
from pyon.util.poller import poll
from pyon.util.containers import DotDict
from pyon.ion.stream import StandaloneStreamPublisher, StandaloneStreamSubscriber

from ion.services.dm.utility.granule_utils import time_series_domain
from ion.util.stored_values import StoredValueManager
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.utility.test.parameter_helper import ParameterHelper

from coverage_model import ParameterContext, QuantityType, NumexprFunction, ParameterFunctionType

from interface.objects import ProcessStateEnum, TransformFunction, TransformFunctionType, DataProcessDefinition, DataProcessTypeEnum, AgentCommand, Parser
from interface.objects import LastUpdate, ComputedValueAvailability, DataProduct, DataProducer, DataProcessProducerContext, Attachment, AttachmentType, ReferenceAttachmentContext
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.coi.iresource_management_service import ResourceManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
DRV_URI_GOOD = CFG.device.sbe37.dvr_egg

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
class TestIntDataProcessManagementServiceMultiOut(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient()
        self.damsclient = DataAcquisitionManagementServiceClient()
        self.pubsubclient =  PubsubManagementServiceClient()
        self.ingestclient = IngestionManagementServiceClient()
        self.imsclient = InstrumentManagementServiceClient()
        self.dataproductclient = DataProductManagementServiceClient()
        self.dataprocessclient = DataProcessManagementServiceClient()
        self.datasetclient =  DatasetManagementServiceClient()
        self.dataset_management = self.datasetclient
        self.process_dispatcher = ProcessDispatcherServiceClient()

    def create_L0_transform_function(self):
        tf = TransformFunction(name='ctdbp_L0_all', module='ion.processes.data.transforms.ctdbp.ctdbp_L0', cls='ctdbp_L0_algorithm')
        tf_id = self.dataprocessclient.create_transform_function(tf)
        self.addCleanup(self.dataprocessclient.delete_transform_function, tf_id)
        return tf_id

    def test_createDataProcess(self):

        #---------------------------------------------------------------------------
        # Data Process Definition
        #---------------------------------------------------------------------------
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L0_all',
                            description='transform ctd package into three separate L0 streams',
                            module='ion.processes.data.transforms.ctd.ctd_L0_all',
                            class_name='ctd_L0_all')
        dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)

        # Make assertion on the newly registered data process definition
        data_process_definition = self.rrclient.read(dprocdef_id)
        self.assertEquals(data_process_definition.name, 'ctd_L0_all')
        self.assertEquals(data_process_definition.description, 'transform ctd package into three separate L0 streams')
        self.assertEquals(data_process_definition.module, 'ion.processes.data.transforms.ctd.ctd_L0_all')
        self.assertEquals(data_process_definition.class_name, 'ctd_L0_all')

        # Read the data process definition using data process management and make assertions
        dprocdef_obj = self.dataprocessclient.read_data_process_definition(dprocdef_id)
        self.assertEquals(dprocdef_obj.class_name,'ctd_L0_all')
        self.assertEquals(dprocdef_obj.module,'ion.processes.data.transforms.ctd.ctd_L0_all')

        #---------------------------------------------------------------------------
        # Create an input instrument
        #---------------------------------------------------------------------------

        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
        instrument_id, rev = self.rrclient.create(instrument_obj)

        # Register the instrument so that the data producer and stream object are created
        data_producer_id = self.damsclient.register_instrument(instrument_id)

        # create a stream definition for the data from the ctd simulator
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(name='Simulated CTD data', parameter_dictionary_id=pdict_id)

        self.dataprocessclient.assign_input_stream_definition_to_data_process_definition(ctd_stream_def_id, dprocdef_id )

        # Assert that the link between the stream definition and the data process definition was done
        assocs = self.rrclient.find_associations(subject=dprocdef_id, predicate=PRED.hasInputStreamDefinition, object=ctd_stream_def_id, id_only=True)

        self.assertIsNotNone(assocs)

        #---------------------------------------------------------------------------
        # Input Data Product
        #---------------------------------------------------------------------------
        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()


        input_dp_obj = IonObject(   RT.DataProduct,
                                    name='InputDataProduct',
                                    description='some new dp',
                                    temporal_domain = tdom,
                                    spatial_domain = sdom)

        input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj, stream_definition_id=ctd_stream_def_id, exchange_point='test')

        #Make assertions on the input data product created
        input_dp_obj = self.rrclient.read(input_dp_id)
        self.assertEquals(input_dp_obj.name, 'InputDataProduct')
        self.assertEquals(input_dp_obj.description, 'some new dp')

        self.damsclient.assign_data_product(instrument_id, input_dp_id)

        # Retrieve the stream via the DataProduct->Stream associations
        stream_ids, _ = self.rrclient.find_objects(input_dp_id, PRED.hasStream, None, True)

        self.in_stream_id = stream_ids[0]

        #---------------------------------------------------------------------------
        # Output Data Product
        #---------------------------------------------------------------------------

        outgoing_stream_conductivity_id = self.pubsubclient.create_stream_definition(name='conductivity', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_conductivity_id, dprocdef_id,binding='conductivity' )

        outgoing_stream_pressure_id = self.pubsubclient.create_stream_definition(name='pressure', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_pressure_id, dprocdef_id, binding='pressure' )

        outgoing_stream_temperature_id = self.pubsubclient.create_stream_definition(name='temperature', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_temperature_id, dprocdef_id, binding='temperature' )


        output_dp_obj = IonObject(RT.DataProduct,
            name='conductivity',
            description='transform output conductivity',
            temporal_domain = tdom,
            spatial_domain = sdom)

        conductivity_dp_id = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_conductivity_id)

        output_dp_obj = IonObject(RT.DataProduct,
            name='pressure',
            description='transform output pressure',
            temporal_domain = tdom,
            spatial_domain = sdom)

        pressure_dp_id = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_pressure_id)

        output_dp_obj = IonObject(RT.DataProduct,
            name='temperature',
            description='transform output ',
            temporal_domain = tdom,
            spatial_domain = sdom)

        temp_dp_id = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_temperature_id)


        ctd_L0_actor = self.create_L0_transform_function()


        routes = {
            input_dp_id : {
                conductivity_dp_id : ctd_L0_actor,
                pressure_dp_id : ctd_L0_actor,
                temp_dp_id : ctd_L0_actor
                }
        }

        config = DotDict()
        config.process.routes = routes
        config.process.params.lat = 45.
        config.process.params.lon = -71.


        #---------------------------------------------------------------------------
        # Create the data process
        #---------------------------------------------------------------------------

        dproc_id = self.dataprocessclient.create_data_process( in_data_product_ids = [input_dp_id],
                                                                out_data_product_ids = [conductivity_dp_id, pressure_dp_id,temp_dp_id],
                                                                configuration= config )
        self.addCleanup(self.dataprocessclient.delete_data_process, dproc_id)

        # Make assertions on the data process created
        data_process = self.dataprocessclient.read_data_process(dproc_id)

        # Assert that the data process has a process id attached
        self.assertIsNotNone(data_process.process_id)

        # Assert that the data process got the input data product's subscription id attached as its own input_susbcription_id attribute
        self.assertIsNotNone(data_process.input_subscription_id)

        output_data_product_ids = self.rrclient.find_objects(subject=dproc_id, predicate=PRED.hasOutputProduct, object_type=RT.DataProduct, id_only=True)

        self.assertEquals(Set(output_data_product_ids[0]), Set([conductivity_dp_id,pressure_dp_id,temp_dp_id]))


    @patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
    def test_createDataProcessUsingSim(self):
        #-------------------------------
        # Create InstrumentModel
        #-------------------------------
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel" )
        instModel_id = self.imsclient.create_instrument_model(instModel_obj)

        #-------------------------------
        # Create InstrumentAgent
        #-------------------------------
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_uri=DRV_URI_GOOD)
        instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        #-------------------------------
        # Create InstrumentDevice
        #-------------------------------
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)

        #-------------------------------
        # Create InstrumentAgentInstance to hold configuration information
        #-------------------------------


        port_agent_config = {
            'device_addr': 'sbe37-simulator.oceanobservatories.org',
            'device_port': 4001,
            'process_type': PortAgentProcessType.UNIX,
            'binary_path': "port_agent",
            'command_port': 4002,
            'data_port': 4003,
            'log_level': 5,
        }


        port_agent_config = {
            'device_addr':  CFG.device.sbe37.host,
            'device_port':  CFG.device.sbe37.port,
            'process_type': PortAgentProcessType.UNIX,
            'binary_path': "port_agent",
            'port_agent_addr': 'localhost',
            'command_port': CFG.device.sbe37.port_agent_cmd_port,
            'data_port': CFG.device.sbe37.port_agent_data_port,
            'log_level': 5,
            'type': PortAgentType.ETHERNET
        }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance",
                                          port_agent_config = port_agent_config)
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)


        #-------------------------------
        # Create CTD Parsed as the first data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(name='SBE32_CDM', parameter_dictionary_id=pdict_id)

        # Construct temporal and spatial Coordinate Reference System objects
        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()



        dp_obj = IonObject(RT.DataProduct,
            name='ctd_parsed',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)

        #-------------------------------
        # Create CTD Raw as the second data product
        #-------------------------------
        raw_stream_def_id = self.pubsubclient.create_stream_definition(name='SBE37_RAW', parameter_dictionary_id=pdict_id)

        dp_obj.name = 'ctd_raw'
        ctd_raw_data_product = self.dataproductclient.create_data_product(dp_obj, raw_stream_def_id)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_raw_data_product)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_raw_data_product, PRED.hasStream, None, True)

        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L0_all',
                            description='transform ctd package into three separate L0 streams',
                            module='ion.processes.data.transforms.ctd.ctd_L0_all',
                            class_name='ctd_L0_all')
        ctd_L0_all_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
            
        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Output Data Products
        #-------------------------------

        outgoing_stream_l0_conductivity_id = self.pubsubclient.create_stream_definition(name='L0_Conductivity', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_conductivity_id, ctd_L0_all_dprocdef_id, binding='conductivity' )

        outgoing_stream_l0_pressure_id = self.pubsubclient.create_stream_definition(name='L0_Pressure', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_pressure_id, ctd_L0_all_dprocdef_id, binding='pressure' )

        outgoing_stream_l0_temperature_id = self.pubsubclient.create_stream_definition(name='L0_Temperature', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_temperature_id, ctd_L0_all_dprocdef_id, binding='temperature' )


        ctd_l0_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
                                                        name='L0_Conductivity',
                                                        description='transform output conductivity',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)


        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj,
                                                                                outgoing_stream_l0_conductivity_id)

        ctd_l0_pressure_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Pressure',
            description='transform output pressure',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj,
                                                                                    outgoing_stream_l0_pressure_id)

        ctd_l0_temperature_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Temperature',
            description='transform output temperature',
            temporal_domain = tdom,
            spatial_domain = sdom)


        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj,
                                                                                    outgoing_stream_l0_temperature_id)

        #-------------------------------
        # Create listener for data process events and verify that events are received.
        #-------------------------------

        # todo: add this validate for Req: L4-CI-SA-RQ-367  Data processing shall notify registered data product consumers about data processing workflow life cycle events
        #todo (contd) ... I believe the capability does not exist yet now. ANS And SA are not yet publishing any workflow life cycle events (Swarbhanu)
        
        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------

        in_data_product_ids = [ctd_parsed_data_product]
        out_data_product_ids = [ctd_l0_conductivity_output_dp_id, ctd_l0_pressure_output_dp_id, ctd_l0_temperature_output_dp_id]

        ctd_L0_actor = self.create_L0_transform_function()

        route = {ctd_parsed_data_product: {ctd_l0_conductivity_output_dp_id: ctd_L0_actor,
                                           ctd_l0_pressure_output_dp_id : ctd_L0_actor,
                                           ctd_l0_temperature_output_dp_id : ctd_L0_actor
                                           }}
        config = DotDict()
        config.process.routes = route
        config.process.params.lat = 45.
        config.process.params.lon = -71.

        ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(    data_process_definition_id = ctd_L0_all_dprocdef_id,
                                                                                    in_data_product_ids = in_data_product_ids,
                                                                                    out_data_product_ids = out_data_product_ids,
                                                                                    configuration= config
                                                                                )

        log.debug("created the data process: %s", ctd_l0_all_data_process_id)

        results, _ = self.rrclient.find_objects(subject=ctd_l0_all_data_process_id,
                                                predicate=PRED.hasProcess,
                                                id_only = True)

        process_id = results[0]

        #-------------------------------
        # Wait until the process launched in the create_data_process() method is actually running, before proceeding further in this test
        #-------------------------------

        gate = ProcessStateGate(self.process_dispatcher.read_process, process_id, ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30), "The data process (%s) did not spawn in 30 seconds" % process_id)

        #-------------------------------
        # Retrieve a list of all data process defintions in RR and validate that the DPD is listed
        #-------------------------------

        # todo: Req: L4-CI-SA-RQ-366  Data processing shall manage data topic definitions
        # todo: data topics are being handled by pub sub at the level of streams
        self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)
        

        #todo: check that activate event is received L4-CI-SA-RQ-367
        #todo... (it looks like no event is being published when the data process is activated... so below, we just check for now
        # todo... that the subscription is indeed activated) (Swarbhanu)


        # todo: monitor process to see if it is active (sa-rq-182)
        rets, _ = self.rrclient.find_objects(subject=ctd_l0_all_data_process_id,
                                                predicate=PRED.hasSubscription,
                                                id_only=True)
        input_subscription_id = rets[0]

        subs = self.rrclient.read(input_subscription_id)
        self.assertTrue(subs.activated)

        # todo: This has not yet been completed by CEI, will prbly surface thru a DPMS call
        self.dataprocessclient.deactivate_data_process(ctd_l0_all_data_process_id)


        #-------------------------------
        # Retrieve the extended resources for data process definition and for data process
        #-------------------------------
        extended_process_definition = self.dataprocessclient.get_data_process_definition_extension(ctd_L0_all_dprocdef_id)
        self.assertEqual(1, len(extended_process_definition.data_processes))
        self.assertEqual(1, len(extended_process_definition.data_products))
        self.assertEqual(3, len(extended_process_definition.data_products[0]))

        log.debug("test_createDataProcess: extended_process_definition  %s", str(extended_process_definition))

        extended_process = self.dataprocessclient.get_data_process_extension(ctd_l0_all_data_process_id)
        self.assertEqual(1, len(extended_process.input_data_products))
        log.debug("test_createDataProcess: extended_process  %s", str(extended_process))

        ################################ Test the removal of data processes ##################################

        #-------------------------------------------------------------------
        # Test the deleting of the data process
        #-------------------------------------------------------------------

        # Before deleting, get the input streams, output streams and the subscriptions so that they can be checked after deleting
#        dp_obj_1 = self.rrclient.read(ctd_l0_all_data_process_id)
#        input_subscription_id = dp_obj_1.input_subscription_id
#        out_prods, _ = self.rrclient.find_objects(subject=ctd_l0_all_data_process_id, predicate=PRED.hasOutputProduct, id_only=True)
#        in_prods, _ = self.rrclient.find_objects(ctd_l0_all_data_process_id, PRED.hasInputProduct, id_only=True)
#        in_streams = []
#        for in_prod in in_prods:
#            streams, _ = self.rrclient.find_objects(in_prod, PRED.hasStream, id_only=True)
#            in_streams.extend(streams)
#        out_streams = []
#        for out_prod in out_prods:
#            streams, _ = self.rrclient.find_objects(out_prod, PRED.hasStream, id_only=True)
#            out_streams.extend(streams)

        # Deleting the data process
        self.dataprocessclient.delete_data_process(ctd_l0_all_data_process_id)

        # Check that the data process got removed. Check the lcs state. It should be retired
        dp_obj = self.rrclient.read(ctd_l0_all_data_process_id)
        self.assertEquals(dp_obj.lcstate, LCS.RETIRED)

        # Check for process defs still attached to the data process
        dpd_assn_ids = self.rrclient.find_associations(subject=ctd_l0_all_data_process_id,  predicate=PRED.hasProcessDefinition, id_only=True)
        self.assertEquals(len(dpd_assn_ids), 0)

        # Check for output data product still attached to the data process
        out_products, assocs = self.rrclient.find_objects(subject=ctd_l0_all_data_process_id, predicate=PRED.hasOutputProduct, id_only=True)
        self.assertEquals(len(out_products), 0)
        self.assertEquals(len(assocs), 0)

        ## Check for input data products still attached to the data process
        #inprod_associations = self.rrclient.find_associations(ctd_l0_all_data_process_id, PRED.hasInputProduct)
        #self.assertEquals(len(inprod_associations), 0)
        #
        ## Check for input data products still attached to the data process
        #inprod_associations = self.rrclient.find_associations(ctd_l0_all_data_process_id, PRED.hasInputProduct)
        #self.assertEquals(len(inprod_associations), 0)

        # Read the original subscription id of the data process and check that it has been deactivated
        with self.assertRaises(NotFound):
            self.pubsubclient.read_subscription(input_subscription_id)

        #-------------------------------------------------------------------
        # Delete the data process definition
        #-------------------------------------------------------------------

        # before deleting, get the process definition being associated to in order to be able to check later if the latter gets deleted as it should
        proc_def_ids, proc_def_asocs = self.rrclient.find_objects(ctd_l0_all_data_process_id, PRED.hasProcessDefinition)
        self.dataprocessclient.delete_data_process_definition(ctd_L0_all_dprocdef_id)

        # check that the data process definition has been retired
        dp_proc_def = self.rrclient.read(ctd_L0_all_dprocdef_id)
        self.assertEquals(dp_proc_def.lcstate, LCS.RETIRED)

        # Check for old associations of this data process definition
        proc_defs, proc_def_asocs = self.rrclient.find_objects(ctd_L0_all_dprocdef_id, PRED.hasProcessDefinition)
        self.assertEquals(len(proc_defs), 0)

        # find all associations where this is the subject
        _, obj_assns = self.rrclient.find_objects(subject= ctd_L0_all_dprocdef_id, id_only=True)
        self.assertEquals(len(obj_assns), 0)

        ################################ Test the removal of data processes ##################################
        # Try force delete... This should simply delete the associations and the data process object
        # from the resource registry

        #---------------------------------------------------------------------------------------------------------------
        # Force deleting a data process
        #---------------------------------------------------------------------------------------------------------------
        self.dataprocessclient.force_delete_data_process(ctd_l0_all_data_process_id)

        # find all associations where this is the subject
        _, obj_assns = self.rrclient.find_objects(subject=ctd_l0_all_data_process_id, id_only=True)

        # find all associations where this is the object
        _, sbj_assns = self.rrclient.find_subjects(object=ctd_l0_all_data_process_id, id_only=True)

        self.assertEquals(len(obj_assns), 0)
        self.assertEquals(len(sbj_assns), 0)
        
        with self.assertRaises(NotFound):
            self.rrclient.read(ctd_l0_all_data_process_id)

        #---------------------------------------------------------------------------------------------------------------
        # Force deleting a data process definition
        #---------------------------------------------------------------------------------------------------------------
        self.dataprocessclient.force_delete_data_process_definition(ctd_L0_all_dprocdef_id)

        # find all associations where this is the subject
        _, obj_assns = self.rrclient.find_objects(subject=ctd_l0_all_data_process_id, id_only=True)

        # find all associations where this is the object
        _, sbj_assns = self.rrclient.find_subjects(object=ctd_l0_all_data_process_id, id_only=True)

        self.assertEquals(len(obj_assns), 0)
        self.assertEquals(len(sbj_assns), 0)

        with self.assertRaises(NotFound):
            self.rrclient.read(ctd_l0_all_data_process_id)


    

@attr('INT',grou='sa')
class TestDataProcessManagementPrime(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.dataset_management          = DatasetManagementServiceClient()
        self.resource_registry           = self.container.resource_registry
        self.pubsub_management           = PubsubManagementServiceClient()
        self.data_process_management     = DataProcessManagementServiceClient()
        self.data_product_management     = DataProductManagementServiceClient()
        self.data_acquisition_management = DataAcquisitionManagementServiceClient()
        self.data_retriever              = DataRetrieverServiceClient()
        self.rmsclient =  ResourceManagementServiceClient()

        self.validators = 0


    def lc_preload(self):
        config = DotDict()
        config.op = 'load'
        config.scenario = 'BETA,LC_TEST'
        config.categories = 'ParameterFunctions,ParameterDefs,ParameterDictionary,Parser'
        config.path = 'res/preload/r2_ioc'
        
        self.container.spawn_process('preload','ion.processes.bootstrap.ion_loader','IONLoader', config)

    def ctd_plain_input_data_product(self):
        available_fields = [
                'internal_timestamp', 
                'temp', 
                'preferred_timestamp', 
                'time', 
                'port_timestamp', 
                'quality_flag', 
                'lat', 
                'conductivity', 
                'driver_timestamp', 
                'lon', 
                'pressure']
        return self.make_data_product('ctd_parsed_param_dict', 'ctd plain test', available_fields)


    def ctd_plain_salinity(self):
        available_fields = [
                'internal_timestamp', 
                'preferred_timestamp', 
                'time', 
                'port_timestamp', 
                'quality_flag', 
                'lat', 
                'driver_timestamp', 
                'lon', 
                'salinity']
        return self.make_data_product('ctd_parsed_param_dict', 'salinity', available_fields)

    def ctd_plain_density(self):
        available_fields = [
                'internal_timestamp', 
                'preferred_timestamp', 
                'time', 
                'port_timestamp', 
                'quality_flag', 
                'lat', 
                'driver_timestamp', 
                'lon', 
                'density']
        return self.make_data_product('ctd_parsed_param_dict', 'density', available_fields)

    def ctd_without_latlon_data_product(self):
        available_fields = [
                'internal_timestamp', 
                'temp', 
                'preferred_timestamp', 
                'time', 
                'port_timestamp', 
                'quality_flag', 
                'conductivity', 
                'driver_timestamp', 
                'pressure']
        return self.make_data_product('ctd_LC_TEST', 'ctd instrument', available_fields)

    def ctd_platform_data_product(self):
        available_fields = [
                'internal_timestamp', 
                'preferred_timestamp', 
                'time', 
                'port_timestamp', 
                'quality_flag', 
                'driver_timestamp', 
                'lat',
                'lon']
        return self.make_data_product('ctd_LC_TEST', 'ctd platform', available_fields)

    def ctd_l2_data_product(self):
        available_fields = [
                'qc_temp_global_range',
                'qc_pressure_global_range',
                'qc_conductivity_global_range',
                'time', 
                'salinity',
                'lookup_density']
        return self.make_data_product('ctd_LC_TEST', 'ctd l2', available_fields)

    def ctd_instrument_data_product(self):
        available_fields = [
                'internal_timestamp', 
                'temp', 
                'preferred_timestamp', 
                'time', 
                'port_timestamp', 
                'quality_flag', 
                'lat', 
                'conductivity', 
                'driver_timestamp', 
                'lon', 
                'pressure']
        return self.make_data_product('ctd_LC_TEST', 'ctd instrument', available_fields)
    
    def sbe37_data_product(self):
        available_fields = [
                'internal_timestamp', 
                'temp', 
                'preferred_timestamp', 
                'time', 
                'port_timestamp', 
                'quality_flag', 
                'lat', 
                'conductivity', 
                'driver_timestamp', 
                'lon', 
                'pressure']
        return self.make_data_product('parsed', 'ctd instrument', available_fields)

    def stream_def_for_data_product(self, data_product_id):
        stream_def_ids, _ = self.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition,id_only=True)
        return stream_def_ids[0]

    def dataset_for_data_product(self, data_product_id):
        dataset_ids, _ = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)
        return dataset_ids[0]

    def verify_dataset(self, dataset_id, verifier):
        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        return verifier(rdt)

    def make_data_product(self, pdict_name, dp_name, available_fields=None):
        if available_fields is None: available_fields = []
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name(pdict_name, id_only=True)
        stream_def_id = self.pubsub_management.create_stream_definition('%s stream_def' % dp_name, parameter_dictionary_id=pdict_id, available_fields=available_fields or None)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        tdom, sdom = time_series_domain()
        tdom = tdom.dump()
        sdom = sdom.dump()
        dp_obj = DataProduct(name=dp_name)
        dp_obj.temporal_domain = tdom
        dp_obj.spatial_domain = sdom
        data_product_id = self.data_product_management.create_data_product(dp_obj, stream_definition_id=stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)
        return data_product_id

    def google_dt_data_product(self):
        return self.make_data_product('google_dt', 'visual')

    def ctd_derived_data_product(self):
        return self.make_data_product('ctd_LC_TEST', 'ctd derived products')
        
    def publish_to_plain_data_product(self, data_product_id):
        stream_ids, _ = self.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        self.assertTrue(len(stream_ids))
        stream_id = stream_ids.pop()
        route = self.pubsub_management.read_stream_route(stream_id)
        stream_definition = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        stream_def_id = stream_definition._id
        publisher = StandaloneStreamPublisher(stream_id, route)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        now = time.time()
        ntp_now = now + 2208988800 # Do not use in production, this is a loose translation

        rdt['internal_timestamp'] = [ntp_now]
        rdt['temp'] = [20.0]
        rdt['preferred_timestamp'] = ['driver_timestamp']
        rdt['time'] = [ntp_now]
        rdt['port_timestamp'] = [ntp_now]
        rdt['quality_flag'] = [None]
        rdt['lat'] = [45]
        rdt['conductivity'] = [4.2914]
        rdt['driver_timestamp'] = [ntp_now]
        rdt['lon'] = [-71]
        rdt['pressure'] = [3.068]

        granule = rdt.to_granule()
        publisher.publish(granule)

    def publish_to_data_product(self, data_product_id, rdt=None):
        stream_ids, _ = self.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        self.assertTrue(len(stream_ids))
        stream_id = stream_ids.pop()
        route = self.pubsub_management.read_stream_route(stream_id)
        stream_definition = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        stream_def_id = stream_definition._id
        publisher = StandaloneStreamPublisher(stream_id, route)
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        if not rdt:
            rdt = ph.get_rdt(stream_def_id)
            ph.fill_parsed_rdt(rdt)

        granule = rdt.to_granule()
        publisher.publish(granule)

    def setup_subscriber(self, data_product_id, callback):
        stream_ids, _ = self.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        self.assertTrue(len(stream_ids))
        stream_id = stream_ids.pop()

        sub_id = self.pubsub_management.create_subscription('validator_%s'%self.validators, stream_ids=[stream_id])
        self.addCleanup(self.pubsub_management.delete_subscription, sub_id)


        self.pubsub_management.activate_subscription(sub_id)
        self.addCleanup(self.pubsub_management.deactivate_subscription, sub_id)

        subscriber = StandaloneStreamSubscriber('validator_%s' % self.validators, callback=callback)
        subscriber.start()
        self.addCleanup(subscriber.stop)
        self.validators+=1

        return subscriber

    def create_density_transform_function(self):
        tf = TransformFunction(name='ctdbp_l2_density', module='ion.processes.data.transforms.ctdbp.ctdbp_L2_density', cls='CTDBP_DensityTransformAlgorithm')
        tf_id = self.data_process_management.create_transform_function(tf)
        self.addCleanup(self.data_process_management.delete_transform_function, tf_id)
        return tf_id

    def create_salinity_transform_function(self):
        tf = TransformFunction(name='ctdbp_l2_salinity', module='ion.processes.data.transforms.ctdbp.ctdbp_L2_salinity', cls='CTDBP_SalinityTransformAlgorithm')
        tf_id = self.data_process_management.create_transform_function(tf)
        self.addCleanup(self.data_process_management.delete_transform_function, tf_id)
        return tf_id
    
    
    def bootstrap_params(self):
        parameter_helper = ParameterHelper(self.dataset_management, self.addCleanup)
        parameter_helper.create_parsed()

    def test_data_process_computation(self):
        self.bootstrap_params()
        instrument_data_product_id = self.sbe37_data_product()
        derived_data_product_id = self.make_data_product('parsed', 'ctd derived products')

        data_process_id = self.data_process_management.create_data_process(in_data_product_ids=[instrument_data_product_id], out_data_product_ids=[derived_data_product_id])
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)
    

        validated = Event()

        def validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)

            np.testing.assert_array_almost_equal(rdt['conductivity_L1'], np.array([42.914]))
            np.testing.assert_array_almost_equal(rdt['temp_L1'], np.array([20.]))
            np.testing.assert_array_almost_equal(rdt['pressure_L1'], np.array([3.068]))
            np.testing.assert_array_almost_equal(rdt['density'], np.array([1021.7144739593881], dtype='float32'))
            np.testing.assert_array_almost_equal(rdt['salinity'], np.array([30.935132729668283], dtype='float32'))

            validated.set()

        self.setup_subscriber(derived_data_product_id, callback=validation)
        self.publish_to_data_product(instrument_data_product_id)
        
        self.assertTrue(validated.wait(10))

    def test_data_process_via_rms(self):
        self.bootstrap_params()
        instrument_data_product_id = self.sbe37_data_product()
        derived_data_product_id = self.make_data_product('parsed', 'ctd derived products')

        data_process_id = self.data_process_management.create_data_process(in_data_product_ids=[instrument_data_product_id], out_data_product_ids=[derived_data_product_id])
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        res_get = self.rmsclient.get_resource(data_process_id, params=['subscriptions_count', 'subscriptions_active_count'])
        self.assertIn("subscriptions_count", res_get)
        self.assertIn("subscriptions_active_count", res_get)
        self.assertEquals(res_get["subscriptions_count"], 1)
        self.assertEquals(res_get["subscriptions_active_count"], 0)

        cmd = AgentCommand(command="activate", command_id="ID_1")
        cmd_res = self.rmsclient.execute_resource(data_process_id, cmd)
        self.assertEquals(cmd_res.command_id, "ID_1")
        self.assertEquals(cmd_res.command, "activate")
        self.assertEquals(cmd_res.status, 0)

        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)

        res_get = self.rmsclient.get_resource(data_process_id, params=['subscriptions_count', 'subscriptions_active_count'])
        self.assertEquals(res_get["subscriptions_count"], 1)
        self.assertEquals(res_get["subscriptions_active_count"], 1)

        subscription_ids, assocs = self.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasSubscription, id_only=True)
        found_one = False
        for subscription_id in subscription_ids:
            if self.pubsub_management.subscription_is_active(subscription_id):
                found_one = True
        if not found_one:
            self.fail("No active subscription found. Activate did not succeed")

        validated = Event()

        def validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)

            np.testing.assert_array_almost_equal(rdt['conductivity_L1'], np.array([42.914]))
            np.testing.assert_array_almost_equal(rdt['temp_L1'], np.array([20.]))
            np.testing.assert_array_almost_equal(rdt['pressure_L1'], np.array([3.068]))
            np.testing.assert_array_almost_equal(rdt['density'], np.array([1021.7144739593881], dtype='float32'))
            np.testing.assert_array_almost_equal(rdt['salinity'], np.array([30.935132729668283], dtype='float32'))

            validated.set()

        self.setup_subscriber(derived_data_product_id, callback=validation)
        self.publish_to_data_product(instrument_data_product_id)

        self.assertTrue(validated.wait(10))

        cmd = AgentCommand(command="deactivate", command_id="ID_2")
        cmd_res = self.rmsclient.execute_resource(data_process_id, cmd)
        self.assertEquals(cmd_res.command_id, "ID_2")
        self.assertEquals(cmd_res.command, "deactivate")
        self.assertEquals(cmd_res.status, 0)

        res_get = self.rmsclient.get_resource(data_process_id, params=['subscriptions_count', 'subscriptions_active_count'])
        self.assertEquals(res_get["subscriptions_count"], 1)
        self.assertEquals(res_get["subscriptions_active_count"], 0)

        subscription_ids, assocs = self.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasSubscription, id_only=True)
        for subscription_id in subscription_ids:
            if self.pubsub_management.subscription_is_active(subscription_id):
                self.fail("Active subscription found. Deactivate did not succeed")


    def attach_qc_document(self, data_product_id, document_keys=None):
        if document_keys is None: document_keys = []
        producers, _ = self.resource_registry.find_objects(data_product_id, PRED.hasDataProducer, id_only=False)
        if producers:
            producer = producers[0]
        else:
            producer = DataProducer(name='producer')
            doc_id, rev = self.resource_registry.create(producer)
            self.addCleanup(self.resource_registry.delete, doc_id)
            producer._id = doc_id
            producer._rev = rev
            assoc, _ = self.resource_registry.create_association(subject=data_product_id, object=doc_id,
                    predicate=PRED.hasDataProducer)
            self.addCleanup(self.resource_registry.delete_association, assoc)
        producer.producer_context = DataProcessProducerContext()
        producer.producer_context.configuration['qc_keys'] = document_keys
        self.resource_registry.update(producer)



    def test_static_lookup_values(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        ph.create_lookups()
        instrument_data_product_id = self.ctd_plain_input_data_product()
        offset_data_product_id = self.make_data_product('lookups', 'lookup_values')


        self.attach_qc_document(instrument_data_product_id, ['calibrated_ctd'])

        
        data_process_id = self.data_process_management.create_data_process(in_data_product_ids=[instrument_data_product_id], out_data_product_ids=[offset_data_product_id], configuration={'process':{'lookup_docs':['calibrated_ctd']}})
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)

        stored_value_manager = StoredValueManager(self.container)
        lookup_table = { 'offset_a': 2.781 }
        stored_value_manager.stored_value_cas('calibrated_ctd', lookup_table)
        self.addCleanup(stored_value_manager.delete_stored_value, 'calibrated_ctd')

        stored_value_manager.stored_value_cas('coefficient_document', {'offset_b': 10.})

        validated=Event()
        def validation(msg, route, stream_id):
            if validated.is_set():
                return
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['temp'] , np.array([20.]))
            np.testing.assert_array_almost_equal(rdt['calibrated'], np.array([22.781]))
            np.testing.assert_array_equal(rdt['calibrated_b'], np.array([32.781], dtype='float32'))
            validated.set()

        self.setup_subscriber(offset_data_product_id, callback=validation)
        self.publish_to_plain_data_product(instrument_data_product_id)

        self.assertTrue(validated.wait(10))

        stored_value_manager.stored_value_cas('new_coefficients', {'offset_a': 3.0})
        ep = EventPublisher(event_type=OT.ExternalReferencesUpdatedEvent)
        ep.publish_event(origin=instrument_data_product_id, reference_keys=['new_coefficients'])
        gevent.sleep(2)
        validated2 = Event()

        def validation2(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['temp'] , np.array([20.]))
            np.testing.assert_array_almost_equal(rdt['calibrated'], np.array([23.0]))
            np.testing.assert_array_equal(rdt['calibrated_b'], np.array([33.0], dtype='float32'))
            validated2.set()
        self.setup_subscriber(offset_data_product_id, callback=validation2)
        self.publish_to_plain_data_product(instrument_data_product_id)

        self.assertTrue(validated2.wait(10))
        

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_data_process_lookup_values(self):
        self.lc_preload()
        instrument_data_product_id = self.ctd_instrument_data_product()
        offset_data_product_id = self.make_data_product('lookup_value_test', 'lookup_values')


        data_producer = DataProducer(name='producer')
        data_producer.producer_context = DataProcessProducerContext()
        data_producer.producer_context.configuration['qc_keys'] = ['calibrated_ctd']
        data_producer_id, _ = self.resource_registry.create(data_producer)
        self.addCleanup(self.resource_registry.delete, data_producer_id)
        assoc,_ = self.resource_registry.create_association(subject=instrument_data_product_id, object=data_producer_id, predicate=PRED.hasDataProducer)
        self.addCleanup(self.resource_registry.delete_association, assoc)

        
        data_process_id = self.data_process_management.create_data_process(in_data_product_ids=[instrument_data_product_id],
            out_data_product_ids=[offset_data_product_id],
            configuration={'process':{'lookup_docs':['calibrated_ctd']}})

        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)

        stored_value_manager = StoredValueManager(self.container)
        lookup_table = {
                'coeffA': 2.781
                }
        stored_value_manager.stored_value_cas('calibrated_ctd', lookup_table)

        validated=Event()
        def validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['temp'] , np.array([20.]))
            np.testing.assert_array_almost_equal(rdt['calibrated_temperature'], np.array([22.781]))
            validated.set()

        self.setup_subscriber(offset_data_product_id, callback=validation)
        self.publish_to_plain_data_product(instrument_data_product_id)

        self.assertTrue(validated.wait(10))

        stored_value_manager.stored_value_cas('lctest', {'lookup_val':10.})

        validated2 = Event()
        def validation2(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['temp'] , np.array([20.]))
            np.testing.assert_array_almost_equal(rdt['calibrated_temperature'], np.array([22.781]))
            np.testing.assert_array_almost_equal(rdt['extended_calibrated_temperature'], np.array([12.781]))
            validated2.set()
        self.setup_subscriber(offset_data_product_id, callback=validation2)
        self.publish_to_plain_data_product(instrument_data_product_id)

        self.assertTrue(validated2.wait(10))



   
    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_data_process_prime(self):
        self.lc_preload()
        instrument_data_product_id = self.ctd_instrument_data_product()
        derived_data_product_id = self.ctd_derived_data_product()

        data_process_id = self.data_process_management.create_data_process(in_data_product_ids=[instrument_data_product_id], out_data_product_ids=[derived_data_product_id])
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)
    

        validated = Event()

        def validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)

            np.testing.assert_array_almost_equal(rdt['conductivity_L1'], np.array([42.914]))
            np.testing.assert_array_almost_equal(rdt['temp_L1'], np.array([20.]))
            np.testing.assert_array_almost_equal(rdt['pressure_L1'], np.array([3.068]))
            np.testing.assert_array_almost_equal(rdt['density'], np.array([1021.7144739593881], dtype='float32'))
            np.testing.assert_array_almost_equal(rdt['salinity'], np.array([30.935132729668283], dtype='float32'))

            validated.set()

        self.setup_subscriber(derived_data_product_id, callback=validation)
        self.publish_to_data_product(instrument_data_product_id)
        
        self.assertTrue(validated.wait(10))
        
    def test_older_transform(self):
        input_data_product_id = self.ctd_plain_input_data_product()

        conductivity_data_product_id = self.make_data_product('ctd_parsed_param_dict', 'conductivity_product', ['time', 'conductivity'])
        conductivity_stream_def_id = self.get_named_stream_def('conductivity_product stream_def')
        temperature_data_product_id = self.make_data_product('ctd_parsed_param_dict', 'temperature_product', ['time', 'temp'])
        temperature_stream_def_id = self.get_named_stream_def('temperature_product stream_def')
        pressure_data_product_id = self.make_data_product('ctd_parsed_param_dict', 'pressure_product', ['time', 'pressure'])
        pressure_stream_def_id = self.get_named_stream_def('pressure_product stream_def')

        dpd = DataProcessDefinition(name='ctdL0')
        dpd.data_process_type = DataProcessTypeEnum.TRANSFORM
        dpd.module = 'ion.processes.data.transforms.ctd.ctd_L0_all'
        dpd.class_name = 'ctd_L0_all'

        data_process_definition_id = self.data_process_management.create_data_process_definition(dpd)
        self.addCleanup(self.data_process_management.delete_data_process_definition, data_process_definition_id)

        self.data_process_management.assign_stream_definition_to_data_process_definition(conductivity_stream_def_id, data_process_definition_id, binding='conductivity')
        self.data_process_management.assign_stream_definition_to_data_process_definition(temperature_stream_def_id, data_process_definition_id, binding='temperature')
        self.data_process_management.assign_stream_definition_to_data_process_definition(pressure_stream_def_id, data_process_definition_id, binding='pressure')

        data_process_id = self.data_process_management.create_data_process(data_process_definition_id=data_process_definition_id, in_data_product_ids=[input_data_product_id], out_data_product_ids=[conductivity_data_product_id, temperature_data_product_id, pressure_data_product_id], timeout=30)
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)

        conductivity_validated = Event()
        def validate_conductivity(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['conductivity'], np.array([4.2914]))
            conductivity_validated.set()

        self.setup_subscriber(conductivity_data_product_id, callback=validate_conductivity)
        temperature_validated = Event()
        def validate_temperature(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['temp'], np.array([20.0]))
            temperature_validated.set()
        self.setup_subscriber(temperature_data_product_id, callback=validate_temperature)
        pressure_validated = Event()
        def validate_pressure(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['pressure'], np.array([3.068]))
            pressure_validated.set()
        self.setup_subscriber(pressure_data_product_id, callback=validate_pressure)
        self.publish_to_plain_data_product(input_data_product_id)
        self.assertTrue(conductivity_validated.wait(10))
        self.assertTrue(temperature_validated.wait(10))
        self.assertTrue(pressure_validated.wait(10))



    def get_named_stream_def(self, name):
        stream_def_ids, _ = self.resource_registry.find_resources(name=name, restype=RT.StreamDefinition, id_only=True)
        return stream_def_ids[0]

    def test_actors(self):
        input_data_product_id = self.ctd_plain_input_data_product()
        output_data_product_id = self.ctd_plain_density()
        actor = self.create_density_transform_function()
        route = {input_data_product_id: {output_data_product_id: actor}}
        config = DotDict()
        config.process.routes = route
        config.process.params.lat = 45.
        config.process.params.lon = -71.

        data_process_id = self.data_process_management.create_data_process(in_data_product_ids=[input_data_product_id], out_data_product_ids=[output_data_product_id], configuration=config)
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)

        validated = Event()
        def validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            # The value I use is a double, the value coming back is only a float32 so there's some data loss but it should be precise to the 4th digit
            np.testing.assert_array_almost_equal(rdt['density'], np.array([1021.6839775385847], dtype='float32'), decimal=4) 
            validated.set()

        self.setup_subscriber(output_data_product_id, callback=validation)

        self.publish_to_plain_data_product(input_data_product_id)
        self.assertTrue(validated.wait(10))

    def test_multi_in_out(self):
        input1 = self.ctd_plain_input_data_product()
        input2 = self.make_data_product('ctd_parsed_param_dict', 'input2')

        density_dp_id = self.ctd_plain_density()
        salinity_dp_id = self.ctd_plain_salinity()

        density_actor = self.create_density_transform_function()
        salinity_actor = self.create_salinity_transform_function()

        routes = {
            input1 : {
                density_dp_id : density_actor,
                salinity_dp_id : salinity_actor
                },
            input2 : {
                density_dp_id : density_actor
                }
            }

        config = DotDict()
        config.process.routes = routes
        config.process.params.lat = 45.
        config.process.params.lon = -71.


        data_process_id = self.data_process_management.create_data_process(in_data_product_ids=[input1, input2], out_data_product_ids=[density_dp_id, salinity_dp_id], configuration=config)
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)

        density_validated = Event()
        salinity_validated = Event()

        def density_validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['density'], np.array([1021.6839775385847], dtype='float32'), decimal=4) 
            density_validated.set()

        def salinity_validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['salinity'], np.array([30.93513240786831], dtype='float32'), decimal=4) 
            salinity_validated.set()

        self.setup_subscriber(density_dp_id, callback=density_validation)
        self.setup_subscriber(salinity_dp_id, callback=salinity_validation)
        
        self.publish_to_plain_data_product(input1)

        self.assertTrue(density_validated.wait(10))
        self.assertTrue(salinity_validated.wait(10))
        density_validated.clear()
        salinity_validated.clear()


        self.publish_to_plain_data_product(input2)
        self.assertTrue(density_validated.wait(10))
        self.assertFalse(salinity_validated.wait(0.75))
        density_validated.clear()
        salinity_validated.clear()



    def test_visual_transform(self):
        input_data_product_id = self.ctd_plain_input_data_product()
        output_data_product_id = self.google_dt_data_product()
        dpd = DataProcessDefinition(name='visual transform')
        dpd.data_process_type = DataProcessTypeEnum.TRANSFORM
        dpd.module = 'ion.processes.data.transforms.viz.google_dt'
        dpd.class_name = 'VizTransformGoogleDT'

        stream_definition_id = self.resource_registry.find_objects(output_data_product_id, PRED.hasStreamDefinition, id_only=True)[0][0]

        #--------------------------------------------------------------------------------
        # Walk before we base jump
        #--------------------------------------------------------------------------------

        data_process_definition_id = self.data_process_management.create_data_process_definition(dpd)
        self.addCleanup(self.data_process_management.delete_data_process_definition, data_process_definition_id)
        self.data_process_management.assign_stream_definition_to_data_process_definition(stream_definition_id, data_process_definition_id, binding='google_dt')
    
        data_process_id = self.data_process_management.create_data_process(data_process_definition_id=data_process_definition_id, in_data_product_ids=[input_data_product_id], out_data_product_ids=[output_data_product_id])
        self.addCleanup(self.data_process_management.delete_data_process,data_process_id)


        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)

        validated = Event()
        def validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            self.assertTrue(rdt['google_dt_components'] is not None)
            validated.set()

        self.setup_subscriber(output_data_product_id, callback=validation)

        self.publish_to_plain_data_product(input_data_product_id)
        self.assertTrue(validated.wait(10))

    def test_lookup_data_process(self):
        
        #--------------------------------------------------------------------------------
        # Input Data Product with lookup document
        #--------------------------------------------------------------------------------

        input_data_product_id = self.ctd_plain_input_data_product()
        data_producer = DataProducer(name='producer')
        data_producer.producer_context = DataProcessProducerContext()
        data_producer.producer_context.configuration['qc_keys'] = ['offset_document']
        data_producer_id, _ = self.resource_registry.create(data_producer)
        self.addCleanup(self.resource_registry.delete, data_producer_id)
        assoc,_ = self.resource_registry.create_association(subject=input_data_product_id, object=data_producer_id, predicate=PRED.hasDataProducer)
        self.addCleanup(self.resource_registry.delete_association, assoc)

        document_keys = self.data_acquisition_management.list_qc_references(input_data_product_id)
            
        self.assertEquals(document_keys, ['offset_document'])
        svm = StoredValueManager(self.container)
        svm.stored_value_cas('offset_document', {'offset_a':2.0})

        #-------------------------------------------------------------------------------- 
        # Output DataProduct with lookup values
        #-------------------------------------------------------------------------------- 
        
        ph = ParameterHelper(self.dataset_management, self.addCleanup)

        pdict_id = ph.create_lookups()

        stream_def_id = self.pubsub_management.create_stream_definition('lookup', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)

        data_product = DataProduct(name='lookup data product')
        tdom, sdom = time_series_domain()
        data_product.temporal_domain = tdom.dump()
        data_product.spatial_domain = sdom.dump()

        data_product_id = self.data_product_management.create_data_product(data_product, stream_definition_id=stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)

        data_process_id = self.data_process_management.create_data_process(in_data_product_ids=[input_data_product_id], out_data_product_ids=[data_product_id])
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)


        #--------------------------------------------------------------------------------
        # Make some data
        #--------------------------------------------------------------------------------
        validated = Event()
        def validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['calibrated'], np.array([22.0]))
            validated.set()
        self.setup_subscriber(data_product_id, callback=validation)
        self.publish_to_plain_data_product(input_data_product_id)
        self.assertTrue(validated.wait(10))

 
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
        calibrated = ParameterContext('calibrated', uom='', param_type=ParameterFunctionType(func, value_encoding='float32'), fill_value=-9999)
        calibrated_id = self.dataset_management.create_parameter_context(name='calibrated', parameter_context=calibrated.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, calibrated_id)
        contexts['calibrated'] = calibrated, calibrated_id

        return contexts

    def dpd_stored_value_transform(self):
        dpd = DataProcessDefinition(name='stored value transform')
        dpd.data_process_type = DataProcessTypeEnum.TRANSFORM
        dpd.module = 'ion.processes.data.transforms.stored_value_transform'
        dpd.class_name = 'StoredValueTransform'

        data_process_definition_id = self.data_process_management.create_data_process_definition(dpd)
        self.addCleanup(self.data_process_management.delete_data_process_definition, data_process_definition_id)
        return data_process_definition_id

    def get_rdt_for_data_product(self, data_product_id):
        stream_def_id = self.stream_def_for_data_product(data_product_id)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        return rdt

    def test_stored_value_transform(self):
        input_data_product_id = self.ctd_plain_input_data_product()
        data_process_definition_id = self.dpd_stored_value_transform()

        config = DotDict()
        config.process.document_key = 'example_document'
    
        data_process_id = self.data_process_management.create_data_process(data_process_definition_id=data_process_definition_id, in_data_product_ids=[input_data_product_id], out_data_product_ids=[],configuration=config)
        self.addCleanup(self.data_process_management.delete_data_process,data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)

        stream_def_ids, _ = self.resource_registry.find_objects(input_data_product_id, PRED.hasStreamDefinition, id_only=True)
        stream_def_id = stream_def_ids[0]
        svm = StoredValueManager(self.container)
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [0]
        rdt['lat'] = [40.38]
        rdt['lon'] = [-72.21]

        self.publish_to_data_product(input_data_product_id, rdt)

        def verifier():
            svm = StoredValueManager(self.container)
            try:
                doc = svm.read_value('example_document')
            except NotFound:
                return False
            np.testing.assert_almost_equal(doc['lat'], 40.38, 4)
            np.testing.assert_almost_equal(doc['lon'], -72.21, 4)
            return True
        success = poll(verifier)
        self.assertTrue(success)
        svm.delete_stored_value('example_document')


        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [0,1,2]
        rdt['lat'] = np.array([40.38, 40.36, 40.37])
        rdt['lon'] = np.array([-72.21, -72.22, -72.23])
        self.publish_to_data_product(input_data_product_id, rdt)

        def verifier2():
            svm = StoredValueManager(self.container)
            try:
                doc = svm.read_value('example_document')
            except NotFound:
                return False
            np.testing.assert_almost_equal(doc['lat'], 40.37, 4)
            np.testing.assert_almost_equal(doc['lon'], -72.23, 4)
            return True
        success = poll(verifier2)
        self.assertTrue(success)
        svm.delete_stored_value('example_document')

    def make_grt_parser(self):
        return self.data_acquisition_management.create_parser(Parser(name='grt', description='', module='ion.util.parsers.global_range_test', method='grt_parser', config=None))
    
    def attach_reference(self, data_product_id, parser_id, document):
        attachment = Attachment(name='qc ref', attachment_type=AttachmentType.REFERENCE,content=document, context=ReferenceAttachmentContext(parser_id=parser_id))
        att_id = self.resource_registry.create_attachment(data_product_id, attachment)
        self.addCleanup(self.resource_registry.delete_attachment, att_id)
        return att_id

            
    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_instrument_platform_integration(self):
        self.lc_preload()

        instrument_data_product_id = self.ctd_without_latlon_data_product()
        platform_data_product_id = self.ctd_platform_data_product()

        output_data_product_id = self.ctd_l2_data_product()

        parser_id = self.make_grt_parser()
        self.attach_reference(output_data_product_id, parser_id, global_range_test_document)


        stored_value_dpd_id = self.dpd_stored_value_transform()

        stored_value_transform_id = self.data_process_management.create_data_process(data_process_definition_id=stored_value_dpd_id, in_data_product_ids=[platform_data_product_id], out_data_product_ids=[],configuration={'process':{'document_key':'platform_persistence'}})
        self.data_process_management.activate_data_process(stored_value_transform_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, stored_value_transform_id)

        rdt = self.get_rdt_for_data_product(platform_data_product_id)
        rdt['time'] = [time.time() + 2208988800]
        rdt['lat'] = [45.]
        rdt['lon'] = [-71.]

        self.publish_to_data_product(platform_data_product_id, rdt)
        def verify_platform_doc():
            svm = StoredValueManager(self.container)
            try:
                doc = svm.read_value('platform_persistence')
            except NotFound:
                return False
            np.testing.assert_almost_equal(doc['lat'], 45., 4)
            np.testing.assert_almost_equal(doc['lon'], -71., 4)
            return True
        success = poll(verify_platform_doc)
        self.assertTrue(success)

        data_process_id = self.data_process_management.create_data_process(in_data_product_ids=[instrument_data_product_id], out_data_product_ids=[output_data_product_id], configuration={'process':{'lookup_docs':['platform_persistence']}})
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_process_id)

        rdt = self.get_rdt_for_data_product(instrument_data_product_id)
        ntp_now = time.time() + 2208988800
        rdt['internal_timestamp'] = [ntp_now]
        rdt['temp'] = [300000]
        rdt['preferred_timestamp'] = ['driver_timestamp']
        rdt['time'] = [ntp_now]
        rdt['port_timestamp'] = [ntp_now]
        rdt['quality_flag'] = [None]
        rdt['conductivity'] = [4341400]
        rdt['driver_timestamp'] = [ntp_now]
        rdt['pressure'] = [256.8]

        validated = Event()
        def validation(msg, route, stream_id):
            rdt = RecordDictionaryTool.load_from_granule(msg)
            np.testing.assert_array_almost_equal(rdt['salinity'], np.array([30.93513240786831], dtype='float32'))
            np.testing.assert_array_almost_equal(rdt['lookup_density'], np.array([1021.7144739593881], dtype='float32'))
            validated.set()
        self.setup_subscriber(output_data_product_id, callback=validation)
        self.publish_to_data_product(instrument_data_product_id, rdt)
        self.assertTrue(validated.wait(10))


    def test_instrument_platform_integration_full(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        ph.create_extended_and_platform()

        instrument_data_product_id = self.make_data_product('extended_parsed', 'Instrument')

        platform_data_product_id = self.make_data_product('platform_eng', 'Platform')

        self.data_product_management.activate_data_product_persistence(instrument_data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, instrument_data_product_id)
        instrument_dataset_id = self.dataset_for_data_product(instrument_data_product_id)

        dataset_monitor = DatasetMonitor(instrument_dataset_id)
        self.addCleanup(dataset_monitor.stop)
        rdt = self.get_rdt_for_data_product(instrument_data_product_id)
        ph.fill_parsed_rdt(rdt)

        self.publish_to_data_product(instrument_data_product_id, rdt)

        self.assertTrue(dataset_monitor.event.wait(10))

        def verifier(rdt):
            np.testing.assert_array_almost_equal(rdt['conductivity_L1'], np.array([42.914]))
            np.testing.assert_array_almost_equal(rdt['temp_L1'], np.array([20.]))
            np.testing.assert_array_almost_equal(rdt['pressure_L1'], np.array([3.068]))
            self.assertNotEquals(rdt['density_lookup'], [1021.7144739593881])
            return True
        self.assertTrue(self.verify_dataset(instrument_dataset_id, verifier))

        dpd_id = self.dpd_stored_value_transform()
        data_process_id = self.data_process_management.create_data_process(dpd_id, [platform_data_product_id], configuration={'process':{'document_key':'platform_key'}})
        self.addCleanup(self.data_process_management.delete_data_process, data_process_id)

        self.data_process_management.activate_data_process(data_process_id)
        self.addCleanup(self.data_process_management.deactivate_data_process,data_process_id)

        rdt = self.get_rdt_for_data_product(platform_data_product_id)
        rdt['time'] = [0]
        rdt['lat'] = [45]
        rdt['lon'] = [-71]
        self.publish_to_data_product(platform_data_product_id, rdt)

        def verify_lookup():
            svm = StoredValueManager(self.container)
            try:
                doc = svm.read_value('platform_key')
            except NotFound:
                return False
            np.testing.assert_almost_equal(doc['lat'], 45., 4)
            np.testing.assert_almost_equal(doc['lon'], -71., 4)
            return True
        success = poll(verify_lookup)
        self.assertTrue(success)
        
        ep = EventPublisher(event_type=OT.ExternalReferencesUpdatedEvent)
        ep.publish_event(origin=instrument_data_product_id, reference_keys=['platform_key'])
        gevent.sleep(2) # Yield

        rdt = self.get_rdt_for_data_product(instrument_data_product_id)
        ph.fill_parsed_rdt(rdt)

        dataset_monitor.event.clear()
        self.publish_to_data_product(instrument_data_product_id, rdt)
        self.assertTrue(dataset_monitor.event.wait(10))

        def verify_again(rdt):
            np.testing.assert_array_almost_equal(rdt['conductivity_L1'], np.array([42.914, 42.914]))
            np.testing.assert_array_almost_equal(rdt['temp_L1'], np.array([20., 20.]))
            np.testing.assert_array_almost_equal(rdt['pressure_L1'], np.array([3.068, 3.068]))
            np.testing.assert_array_almost_equal(rdt['density_lookup'][-1:], np.array([1021.7144739593881], dtype='float32'))
            return True
        self.assertTrue(self.verify_dataset(instrument_dataset_id, verify_again))




global_range_test_document = '''Array,Instrument Class,Reference Designator,Data Products,Units,Data Product Flagged,Minimum Range (lim(1)),Maximum Range (lim(2))
Array 1,SBE37,TEST,TEMPWAT,deg_C,TEMPWAT,10,28
Array 1,SBE37,TEST,CONDWAT,S m-1,CONDWAT,0,30
Array 1,SBE37,TEST,PRESWAT,dbar,PRESWAT,0,20'''

