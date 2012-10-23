#!/usr/bin/env python

'''
@file ion/services/sa/process/test/test_int_data_process_management_service.py
@author Maurice Manning
@test ion.services.sa.process.DataProcessManagementService integration test
'''

from nose.plugins.attrib import attr
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.utility.granule_utils import time_series_domain
from pyon.public import log, IonObject
from pyon.public import CFG, RT, PRED 
from pyon.core.exception import BadRequest
from pyon.util.context import LocalContextMixin
from pyon.util.int_test import IonIntegrationTestCase
from mock import patch
from coverage_model.coverage import GridDomain, GridShape, CRS
from coverage_model.basic_types import MutabilityEnum, AxisTypeEnum
from ion.agents.port.port_agent_process import PortAgentProcessType
import gevent

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
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.dataset_management = self.datasetclient



    def test_createDataProcess(self):

        #-------------------------------
        # Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L0_all',
                            description='transform ctd package into three separate L0 streams',
                            module='ion.processes.data.transforms.ctd.ctd_L0_all',
                            class_name='ctd_L0_all',
                            process_source='some_source_reference')
        try:
            dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)


        # test Data Process Definition creation in rr
        dprocdef_obj = self.dataprocessclient.read_data_process_definition(dprocdef_id)
        self.assertEquals(dprocdef_obj.name,'ctd_L0_all')

        # Create an input instrument
        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
        instrument_id, rev = self.rrclient.create(instrument_obj)

        # Register the instrument so that the data producer and stream object are created
        data_producer_id = self.damsclient.register_instrument(instrument_id)
        log.debug("TestIntDataProcessMgmtServiceMultiOut  data_producer_id %s", data_producer_id)

        # create a stream definition for the data from the ctd simulator
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(name='Simulated CTD data', parameter_dictionary_id=pdict_id)

        self.dataprocessclient.assign_input_stream_definition_to_data_process_definition(ctd_stream_def_id, dprocdef_id )


        #-------------------------------
        # Input Data Product
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create input data product")

        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()


        input_dp_obj = IonObject(   RT.DataProduct,
                                    name='InputDataProduct',
                                    description='some new dp',
                                    temporal_domain = tdom,
                                    spatial_domain = sdom)

        input_dp_id = self.dataproductclient.create_data_product(data_product=input_dp_obj, stream_definition_id=ctd_stream_def_id, exchange_point='test')

        self.damsclient.assign_data_product(instrument_id, input_dp_id)

        # Retrieve the stream via the DataProduct->Stream associations
        stream_ids, _ = self.rrclient.find_objects(input_dp_id, PRED.hasStream, None, True)

        log.debug("TestIntDataProcessMgmtServiceMultiOut: in stream_ids "   +  str(stream_ids))
        self.in_stream_id = stream_ids[0]
        log.debug("TestIntDataProcessMgmtServiceMultiOut: Input Stream: "   +  str( self.in_stream_id))

        #-------------------------------
        # Output Data Product
        #-------------------------------

        outgoing_stream_conductivity_id = self.pubsubclient.create_stream_definition(name='conductivity', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_conductivity_id, dprocdef_id,binding='conductivity' )

        outgoing_stream_pressure_id = self.pubsubclient.create_stream_definition(name='pressure', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_pressure_id, dprocdef_id, binding='pressure' )

        outgoing_stream_temperature_id = self.pubsubclient.create_stream_definition(name='temperature', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_temperature_id, dprocdef_id, binding='temperature' )


        self.output_products={}
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product conductivity")

        output_dp_obj = IonObject(RT.DataProduct,
            name='conductivity',
            description='transform output conductivity',
            temporal_domain = tdom,
            spatial_domain = sdom)

        output_dp_id_1 = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_conductivity_id)
        self.output_products['conductivity'] = output_dp_id_1


        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product pressure")

        output_dp_obj = IonObject(RT.DataProduct,
            name='pressure',
            description='transform output pressure',
            temporal_domain = tdom,
            spatial_domain = sdom)

        output_dp_id_2 = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_pressure_id)
        self.output_products['pressure'] = output_dp_id_2

        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product temperature")

        output_dp_obj = IonObject(RT.DataProduct,
            name='temperature',
            description='transform output ',
            temporal_domain = tdom,
            spatial_domain = sdom)

        output_dp_id_3 = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_temperature_id)
        self.output_products['temperature'] = output_dp_id_3


        #-------------------------------
        # Create the data process
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create_data_process start")
        try:
            dproc_id = self.dataprocessclient.create_data_process(dprocdef_id, [input_dp_id], self.output_products)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestIntDataProcessMgmtServiceMultiOut: create_data_process return")

        # these assigns happen inside create_data_process
        #self.damsclient.assign_data_product(input_resource_id=dproc_id, data_product_id=output_dp_id_1)
        #self.damsclient.assign_data_product(input_resource_id=dproc_id, data_product_id=output_dp_id_2)
        #self.damsclient.assign_data_product(input_resource_id=dproc_id, data_product_id=output_dp_id_3)



    @patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
    def test_createDataProcessUsingSim(self):
        #-------------------------------
        # Create InstrumentModel
        #-------------------------------
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel", model="SBE37IMModel" )
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        print 'test_createDataProcessUsingSim: new InstrumentModel id = ', instModel_id


        #-------------------------------
        # Create InstrumentAgent
        #-------------------------------
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_module="ion.agents.instrument.instrument_agent", driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        print 'test_createDataProcessUsingSim: new InstrumentAgent id = ', instAgent_id

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        #-------------------------------
        # Create InstrumentDevice
        #-------------------------------
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        print 'test_createDataProcessUsingSim: new InstrumentDevice id = ', instDevice_id

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

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance", svr_addr="localhost",
                                          driver_module="mi.instrument.seabird.sbe37smb.ooicore.driver", driver_class="SBE37Driver",
                                          comms_device_address=CFG.device.sbe37.host, comms_device_port=CFG.device.sbe37.port,
                                          port_agent_config = port_agent_config)
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)


        #-------------------------------
        # Create CTD Parsed as the first data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(name='SBE32_CDM', parameter_dictionary_id=pdict_id)

        print 'test_createDataProcessUsingSim: new Stream Definition id = ', instDevice_id

        print 'Creating new CDM data product with a stream definition'


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

        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)
        print 'test_createDataProcessUsingSim: Data product streams1 = ', stream_ids

        #-------------------------------
        # Create CTD Raw as the second data product
        #-------------------------------
        print 'test_createDataProcessUsingSim: Creating new RAW data product with a stream definition'
        raw_stream_def_id = self.pubsubclient.create_stream_definition(name='SBE37_RAW', parameter_dictionary_id=pdict_id)

        dp_obj.name = 'ctd_raw'
        ctd_raw_data_product = self.dataproductclient.create_data_product(dp_obj, raw_stream_def_id)
        print 'new ctd_raw_data_product_id = ', ctd_raw_data_product

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_raw_data_product)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_raw_data_product, PRED.hasStream, None, True)
        print 'Data product streams2 = ', stream_ids
        
        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------
        log.debug("test_createDataProcessUsingSim: create data process definition ctd_L0_all")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L0_all',
                            description='transform ctd package into three separate L0 streams',
                            module='ion.processes.data.transforms.ctd.ctd_L0_all',
                            class_name='ctd_L0_all',
                            process_source='some_source_reference')
        try:
            ctd_L0_all_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new ctd_L0_all data process definition: %s" %ex)
            
            
        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Output Data Products
        #-------------------------------

        outgoing_stream_l0_conductivity_id = self.pubsubclient.create_stream_definition(name='L0_Conductivity', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_conductivity_id, ctd_L0_all_dprocdef_id, binding='conductivity' )

        outgoing_stream_l0_pressure_id = self.pubsubclient.create_stream_definition(name='L0_Pressure', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_pressure_id, ctd_L0_all_dprocdef_id, binding='pressure' )

        outgoing_stream_l0_temperature_id = self.pubsubclient.create_stream_definition(name='L0_Temperature', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_temperature_id, ctd_L0_all_dprocdef_id, binding='temperature' )


        self.output_products={}
        log.debug("test_createDataProcessUsingSim: create output data product L0 conductivity")

        ctd_l0_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
                                                        name='L0_Conductivity',
                                                        description='transform output conductivity',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)


        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj,
                                                                                outgoing_stream_l0_conductivity_id)
        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id


        log.debug("test_createDataProcessUsingSim: create output data product L0 pressure")

        ctd_l0_pressure_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Pressure',
            description='transform output pressure',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj,
                                                                                    outgoing_stream_l0_pressure_id)
        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id

        log.debug("test_createDataProcessUsingSim: create output data product L0 temperature")

        ctd_l0_temperature_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Temperature',
            description='transform output temperature',
            temporal_domain = tdom,
            spatial_domain = sdom)


        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj,
                                                                                    outgoing_stream_l0_temperature_id)
        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id


        #-------------------------------
        # Create listener for data process events and verify that events are received.
        #-------------------------------

        # todo: add this validate for Req: L4-CI-SA-RQ-367  Data processing shall notify registered data product consumers about data processing workflow life cycle events

        
        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------
        log.debug("test_createDataProcessUsingSim: create data_process start")
        try:
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L0_all_dprocdef_id, [ctd_parsed_data_product], self.output_products)
            print 'CTD_L0 data process id: ', ctd_l0_all_data_process_id

        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createDataProcessUsingSim: data_process created: %s", str(ctd_l0_all_data_process_id))


        #-------------------------------
        # Retrieve a list of all data process defintions in RR and validate that the DPD is listed
        #-------------------------------

        # todo: add this validate for Req: L4-CI-SA-RQ-366  Data processing shall manage data topic definitions

        log.debug("test_createDataProcessUsingSim: activate_data_process ")
        self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)
        

        #todo: check that activate event is received L4-CI-SA-RQ-367


        # todo: monitor process to see if it is active (sa-rq-182)
        # todo: This has not yet been completed by CEI, will prbly surface thru a DPMS call
        log.debug("test_createDataProcessUsingSim: call CEI interface to monitor  ")

        log.debug("test_createDataProcessUsingSim: deactivate_data_process ")
        self.dataprocessclient.deactivate_data_process(ctd_l0_all_data_process_id)

        print 'Deleting data process ', ctd_l0_all_data_process_id
        self.dataprocessclient.delete_data_process(ctd_l0_all_data_process_id)

