#!/usr/bin/env python

'''
@file ion/services/sa/process/test/test_int_data_process_management_service.py
@author Maurice Manning
@test ion.services.sa.process.DataProcessManagementService integration test
'''

from nose.plugins.attrib import attr
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.public import CFG, RT, LCS, PRED, StreamPublisher, StreamSubscriber, StreamPublisherRegistrar, StreamSubscriberRegistrar
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.util.context import LocalContextMixin
import time
from pyon.util.int_test import IonIntegrationTestCase
from prototype.sci_data.stream_defs import ctd_stream_definition, L0_pressure_stream_definition, L0_temperature_stream_definition, L0_conductivity_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition
from interface.objects import StreamQuery, ExchangeQuery
import gevent
import unittest
from interface.objects import HdfStorage, CouchStorage
from prototype.sci_data.stream_parser import PointSupplementStreamParser
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand



class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
#@unittest.skip('not working')
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



#    #@unittest.skip('not working')
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
        log.debug("TestIntDataProcessMgmtServiceMultiOut  data_producer_id %s" % data_producer_id)

        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = ctd_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def, name='Simulated CTD data')

        self.dataprocessclient.assign_input_stream_definition_to_data_process_definition(ctd_stream_def_id, dprocdef_id )


        #-------------------------------
        # Input Data Product
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create input data product")
        input_dp_obj = IonObject(RT.DataProduct, name='InputDataProduct', description='some new dp')
        try:
            input_dp_id = self.dataproductclient.create_data_product(input_dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new input data product: %s" %ex)

        self.damsclient.assign_data_product(instrument_id, input_dp_id)

        # Retrieve the stream via the DataProduct->Stream associations
        stream_ids, _ = self.rrclient.find_objects(input_dp_id, PRED.hasStream, None, True)

        log.debug("TestIntDataProcessMgmtServiceMultiOut: in stream_ids "   +  str(stream_ids))
        self.in_stream_id = stream_ids[0]
        log.debug("TestIntDataProcessMgmtServiceMultiOut: Input Stream: "   +  str( self.in_stream_id))

        #-------------------------------
        # Output Data Product
        #-------------------------------

        outgoing_stream_conductivity = L0_conductivity_stream_definition()
        outgoing_stream_conductivity_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_conductivity, name='conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_conductivity_id, dprocdef_id )

        outgoing_stream_pressure = L0_pressure_stream_definition()
        outgoing_stream_pressure_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_pressure, name='pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_pressure_id, dprocdef_id )

        outgoing_stream_temperature = L0_temperature_stream_definition()
        outgoing_stream_temperature_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_temperature, name='temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_temperature_id, dprocdef_id )


        self.output_products={}
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product conductivity")
        output_dp_obj = IonObject(RT.DataProduct, name='conductivity',description='transform output conductivity')
        output_dp_id_1 = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_conductivity_id)
        self.output_products['conductivity'] = output_dp_id_1
        self.dataproductclient.activate_data_product_persistence(data_product_id=output_dp_id_1, persist_data=True, persist_metadata=True)


        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product pressure")
        output_dp_obj = IonObject(RT.DataProduct, name='pressure',description='transform output pressure')
        output_dp_id_2 = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_pressure_id)
        self.output_products['pressure'] = output_dp_id_2
        self.dataproductclient.activate_data_product_persistence(data_product_id=output_dp_id_2, persist_data=True, persist_metadata=True)

        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product temperature")
        output_dp_obj = IonObject(RT.DataProduct, name='temperature',description='transform output ')
        output_dp_id_3 = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_temperature_id)
        self.output_products['temperature'] = output_dp_id_3
        self.dataproductclient.activate_data_product_persistence(data_product_id=output_dp_id_3, persist_data=True, persist_metadata=True)


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




    #@unittest.skip('not working')
    def test_createDataProcessUsingSim(self):
        #-------------------------------
        # Create InstrumentModel
        #-------------------------------
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel", model_label="SBE37IMModel" )
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
        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance", svr_addr="localhost",
                                          driver_module="ion.agents.instrument.drivers.sbe37.sbe37_driver", driver_class="SBE37Driver",
                                          cmd_port=5556, evt_port=5557, comms_method="ethernet", comms_device_address=CFG.device.sbe37.host, comms_device_port=CFG.device.sbe37.port,
                                          comms_server_address="localhost", comms_server_port=8888)
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)

        #-------------------------------
        # Create CTD Parsed as the first data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def)

        print 'test_createDataProcessUsingSim: new Stream Definition id = ', instDevice_id

        print 'Creating new CDM data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='ctd_parsed',description='ctd stream test')
        try:
            ctd_parsed_data_product = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)
        print 'test_createDataProcessUsingSim: Data product streams1 = ', stream_ids

        #-------------------------------
        # Create CTD Raw as the second data product
        #-------------------------------
        print 'test_createDataProcessUsingSim: Creating new RAW data product with a stream definition'
        raw_stream_def = SBE37_RAW_stream_definition()
        raw_stream_def_id = self.pubsubclient.create_stream_definition(container=raw_stream_def)

        dp_obj = IonObject(RT.DataProduct,name='ctd_raw',description='raw stream test')
        try:
            ctd_raw_data_product = self.dataproductclient.create_data_product(dp_obj, raw_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new ctd_raw_data_product_id = ', ctd_raw_data_product

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_raw_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_raw_data_product, persist_data=True, persist_metadata=True)

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

        outgoing_stream_l0_conductivity = L0_conductivity_stream_definition()
        outgoing_stream_l0_conductivity_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l0_conductivity, name='L0_Conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_conductivity_id, ctd_L0_all_dprocdef_id )

        outgoing_stream_l0_pressure = L0_pressure_stream_definition()
        outgoing_stream_l0_pressure_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l0_pressure, name='L0_Pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_pressure_id, ctd_L0_all_dprocdef_id )

        outgoing_stream_l0_temperature = L0_temperature_stream_definition()
        outgoing_stream_l0_temperature_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l0_temperature, name='L0_Temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_temperature_id, ctd_L0_all_dprocdef_id )


        self.output_products={}
        log.debug("test_createDataProcessUsingSim: create output data product L0 conductivity")
        ctd_l0_conductivity_output_dp_obj = IonObject(RT.DataProduct, name='L0_Conductivity',description='transform output conductivity')
        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj, outgoing_stream_l0_conductivity_id)
        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_conductivity_output_dp_id, persist_data=True, persist_metadata=True)


        log.debug("test_createDataProcessUsingSim: create output data product L0 pressure")
        ctd_l0_pressure_output_dp_obj = IonObject(RT.DataProduct, name='L0_Pressure',description='transform output pressure')
        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj, outgoing_stream_l0_pressure_id)
        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_pressure_output_dp_id, persist_data=True, persist_metadata=True)

        log.debug("test_createDataProcessUsingSim: create output data product L0 temperature")
        ctd_l0_temperature_output_dp_obj = IonObject(RT.DataProduct, name='L0_Temperature',description='transform output temperature')
        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj, outgoing_stream_l0_temperature_id)
        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_temperature_output_dp_id, persist_data=True, persist_metadata=True)


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


        # todo: monitor process to se eif it is active (sa-rq-182)
        # todo: This has not yet been completed by CEI, will prbly surface thru a DPMS call
        log.debug("test_createDataProcessUsingSim: call CEI interface to monitor  ")



        log.debug("test_createDataProcessUsingSim: deactivate_data_process ")
        self.dataprocessclient.deactivate_data_process(ctd_l0_all_data_process_id)
