from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient

from prototype.sci_data.stream_defs import ctd_stream_definition, L0_pressure_stream_definition, L0_temperature_stream_definition, L0_conductivity_stream_definition
from prototype.sci_data.stream_defs import L1_pressure_stream_definition, L1_temperature_stream_definition, L1_conductivity_stream_definition, L2_practical_salinity_stream_definition, L2_density_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition

from pyon.public import log
from nose.plugins.attrib import attr

from pyon.public import StreamSubscriberRegistrar

from interface.objects import HdfStorage, CouchStorage

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, LCS, PRED
from pyon.core.exception import BadRequest, NotFound, Conflict

from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand

from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
import time


from pyon.util.context import LocalContextMixin


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('HARDWARE', group='sa')
@unittest.skip("run locally only")
class TestCTDTransformsIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        print 'started services'

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)

    def test_createTransformsThenActivateInstrument(self):

        # Set up the preconditions
        # ingestion configuration parameters
        self.exchange_point_id = 'science_data'
        self.number_of_workers = 2
        self.hdf_storage = HdfStorage(relative_path='ingest')
        self.couch_storage = CouchStorage(datastore_name='test_datastore')
        self.XP = 'science_data'
        self.exchange_name = 'ingestion_queue'

        #-------------------------------
        # Create ingestion configuration and activate it
        #-------------------------------
        ingestion_configuration_id =  self.ingestclient.create_ingestion_configuration(
            exchange_point_id=self.exchange_point_id,
            couch_storage=self.couch_storage,
            hdf_storage=self.hdf_storage,
            number_of_workers=self.number_of_workers
        )
        print 'test_createTransformsThenActivateInstrument: ingestion_configuration_id', ingestion_configuration_id

        # activate an ingestion configuration
        ret = self.ingestclient.activate_ingestion_configuration(ingestion_configuration_id)


        #-------------------------------
        # Create InstrumentModel
        #-------------------------------
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel", model_label="SBE37IMModel" )
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        print 'test_createTransformsThenActivateInstrument: new InstrumentModel id = ', instModel_id


        #-------------------------------
        # Create InstrumentAgent
        #-------------------------------
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_module="ion.services.mi.instrument_agent", driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        print 'test_createTransformsThenActivateInstrument: new InstrumentAgent id = ', instAgent_id

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

        print 'test_createTransformsThenActivateInstrument: new InstrumentDevice id = ', instDevice_id

        #-------------------------------
        # Create InstrumentAgentInstance to hold configuration information
        #-------------------------------
        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance", svr_addr="localhost",
                                          driver_module="ion.services.mi.drivers.sbe37_driver", driver_class="SBE37Driver",
                                          cmd_port=5556, evt_port=5557, comms_method="ethernet", comms_device_address=CFG.device.sbe37.host, comms_device_port=CFG.device.sbe37.port,
                                          comms_server_address="localhost", comms_server_port=8888)
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)


        #-------------------------------
        # Create CTD Parsed as the first data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def)

        print 'test_createTransformsThenActivateInstrument: new Stream Definition id = ', instDevice_id

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
        print 'test_createTransformsThenActivateInstrument: Data product streams1 = ', stream_ids

        #-------------------------------
        # Create CTD Raw as the second data product
        #-------------------------------
        print 'test_createTransformsThenActivateInstrument: Creating new RAW data product with a stream definition'
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
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition ctd_L0_all")
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
        # L1 Conductivity: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition CTDL1ConductivityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L1_conductivity',
                            description='create the L1 conductivity data product',
                            module='ion.processes.data.transforms.ctd.ctd_L1_conductivity',
                            class_name='CTDL1ConductivityTransform',
                            process_source='CTDL1ConductivityTransform source code here...')
        try:
            ctd_L1_conductivity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new CTDL1ConductivityTransform data process definition: %s" %ex)

        #-------------------------------
        # L1 Pressure: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition CTDL1PressureTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L1_pressure',
                            description='create the L1 pressure data product',
                            module='ion.processes.data.transforms.ctd.ctd_L1_pressure',
                            class_name='CTDL1PressureTransform',
                            process_source='CTDL1PressureTransform source code here...')
        try:
            ctd_L1_pressure_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new CTDL1PressureTransform data process definition: %s" %ex)


        #-------------------------------
        # L1 Temperature: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition CTDL1TemperatureTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L1_temperature',
                            description='create the L1 temperature data product',
                            module='ion.processes.data.transforms.ctd.ctd_L1_temperature',
                            class_name='CTDL1TemperatureTransform',
                            process_source='CTDL1TemperatureTransform source code here...')
        try:
            ctd_L1_temperature_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new CTDL1TemperatureTransform data process definition: %s" %ex)


        #-------------------------------
        # L2 Salinity: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition SalinityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L2_salinity',
                            description='create the L1 temperature data product',
                            module='ion.processes.data.transforms.ctd.ctd_L2_salinity',
                            class_name='SalinityTransform',
                            process_source='SalinityTransform source code here...')
        try:
            ctd_L2_salinity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new SalinityTransform data process definition: %s" %ex)


        #-------------------------------
        # L2 Density: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition DensityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L2_density',
                            description='create the L1 temperature data product',
                            module='ion.processes.data.transforms.ctd.ctd_L2_density',
                            class_name='DensityTransform',
                            process_source='DensityTransform source code here...')
        try:
            ctd_L2_density_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new DensityTransform data process definition: %s" %ex)





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
        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 conductivity")
        ctd_l0_conductivity_output_dp_obj = IonObject(RT.DataProduct, name='L0_Conductivity',description='transform output conductivity')
        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj, outgoing_stream_l0_conductivity_id)
        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_conductivity_output_dp_id, persist_data=True, persist_metadata=True)


        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 pressure")
        ctd_l0_pressure_output_dp_obj = IonObject(RT.DataProduct, name='L0_Pressure',description='transform output pressure')
        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj, outgoing_stream_l0_pressure_id)
        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_pressure_output_dp_id, persist_data=True, persist_metadata=True)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 temperature")
        ctd_l0_temperature_output_dp_obj = IonObject(RT.DataProduct, name='L0_Temperature',description='transform output temperature')
        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj, outgoing_stream_l0_temperature_id)
        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_temperature_output_dp_id, persist_data=True, persist_metadata=True)


        #-------------------------------
        # L1 Conductivity - Temperature - Pressure: Output Data Products
        #-------------------------------

        outgoing_stream_l1_conductivity = L1_conductivity_stream_definition()
        outgoing_stream_l1_conductivity_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l1_conductivity, name='L1_conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_conductivity_id, ctd_L1_conductivity_dprocdef_id )

        outgoing_stream_l1_pressure = L1_pressure_stream_definition()
        outgoing_stream_l1_pressure_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l1_pressure, name='L1_Pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_pressure_id, ctd_L1_pressure_dprocdef_id )

        outgoing_stream_l1_temperature = L1_temperature_stream_definition()
        outgoing_stream_l1_temperature_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l1_temperature, name='L1_Temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_temperature_id, ctd_L1_temperature_dprocdef_id )

        log.debug("test_createTransformsThenActivateInstrument: create output data product L1 conductivity")
        ctd_l1_conductivity_output_dp_obj = IonObject(RT.DataProduct, name='L1_Conductivity',description='transform output L1 conductivity')
        ctd_l1_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l1_conductivity_output_dp_obj, outgoing_stream_l1_conductivity_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l1_conductivity_output_dp_id, persist_data=True, persist_metadata=True)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L1 pressure")
        ctd_l1_pressure_output_dp_obj = IonObject(RT.DataProduct, name='L1_Pressure',description='transform output L1 pressure')
        ctd_l1_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l1_pressure_output_dp_obj, outgoing_stream_l1_pressure_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l1_pressure_output_dp_id, persist_data=True, persist_metadata=True)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L1 temperature")
        ctd_l1_temperature_output_dp_obj = IonObject(RT.DataProduct, name='L1_Temperature',description='transform output L1 temperature')
        ctd_l1_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l1_temperature_output_dp_obj, outgoing_stream_l1_temperature_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l1_temperature_output_dp_id, persist_data=True, persist_metadata=True)


        #-------------------------------
        # L2 Salinity - Density: Output Data Products
        #-------------------------------

        outgoing_stream_l2_salinity = L2_practical_salinity_stream_definition()
        outgoing_stream_l2_salinity_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l2_salinity, name='L2_salinity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_salinity_id, ctd_L2_salinity_dprocdef_id )

        outgoing_stream_l2_density = L2_density_stream_definition()
        outgoing_stream_l2_density_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l2_density, name='L2_Density')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_density_id, ctd_L2_density_dprocdef_id )

        log.debug("test_createTransformsThenActivateInstrument: create output data product L2 Salinity")
        ctd_l2_salinity_output_dp_obj = IonObject(RT.DataProduct, name='L2_Salinity',description='transform output L2 salinity')
        ctd_l2_salinity_output_dp_id = self.dataproductclient.create_data_product(ctd_l2_salinity_output_dp_obj, outgoing_stream_l2_salinity_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l2_salinity_output_dp_id, persist_data=True, persist_metadata=True)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L2 Density")
        ctd_l2_density_output_dp_obj = IonObject(RT.DataProduct, name='L2_Density',description='transform output pressure')
        ctd_l2_density_output_dp_id = self.dataproductclient.create_data_product(ctd_l2_density_output_dp_obj, outgoing_stream_l2_density_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l2_density_output_dp_id, persist_data=True, persist_metadata=True)
        
        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L0 all data_process start")
        try:
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L0_all_dprocdef_id, ctd_parsed_data_product, self.output_products)
            self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L0 all data_process return")


        #-------------------------------
        # L1 Conductivity: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L1 Conductivity data_process start")
        try:
            l1_conductivity_data_process_id = self.dataprocessclient.create_data_process(ctd_L1_conductivity_dprocdef_id, ctd_l0_conductivity_output_dp_id, {'output':ctd_l1_conductivity_output_dp_id})
            self.dataprocessclient.activate_data_process(l1_conductivity_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L1 Conductivity data_process return")


        #-------------------------------
        # L1 Pressure: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L1_Pressure data_process start")
        try:
            l1_pressure_data_process_id = self.dataprocessclient.create_data_process(ctd_L1_pressure_dprocdef_id, ctd_l0_pressure_output_dp_id, {'output':ctd_l1_pressure_output_dp_id})
            self.dataprocessclient.activate_data_process(l1_pressure_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L1_Pressure data_process return")



        #-------------------------------
        # L1 Temperature: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L1_Pressure data_process start")
        try:
            l1_temperature_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L1_temperature_dprocdef_id, ctd_l0_temperature_output_dp_id, {'output':ctd_l1_temperature_output_dp_id})
            self.dataprocessclient.activate_data_process(l1_temperature_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L1_Pressure data_process return")



        #-------------------------------
        # L2 Salinity: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L2_salinity data_process start")
        try:
            l2_salinity_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L2_salinity_dprocdef_id, ctd_parsed_data_product, {'output':ctd_l2_salinity_output_dp_id})
            self.dataprocessclient.activate_data_process(l2_salinity_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L2_salinity data_process return")

        #-------------------------------
        # L2 Density: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L2_Density data_process start")
        try:
            l2_density_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L2_density_dprocdef_id, ctd_parsed_data_product, {'output':ctd_l2_density_output_dp_id})
            self.dataprocessclient.activate_data_process(l2_density_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L2_Density data_process return")


        #-------------------------------
        # Launch InstrumentAgentInstance, connect to the resource agent client
        #-------------------------------
        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        inst_agent_instance_obj= self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        print 'test_createTransformsThenActivateInstrument: Instrument agent instance obj: = ', inst_agent_instance_obj

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient('iaclient', name=inst_agent_instance_obj.agent_process_id,  process=FakeProcess())
        print 'activate_instrument: got ia client %s', self._ia_client
        log.debug(" test_createTransformsThenActivateInstrument:: got ia client %s", str(self._ia_client))


#        #-------------------------------
#        # Streaming
#        #-------------------------------
#
#        cmd = AgentCommand(command='initialize')
#        retval = self._ia_client.execute_agent(cmd)
#        print retval
#        log.debug("test_createTransformsThenActivateInstrument:: initialize %s", str(retval))
#
#        time.sleep(2)
#
#        cmd = AgentCommand(command='go_active')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_createTransformsThenActivateInstrument:: go_active %s", str(reply))
#        time.sleep(2)
#
#        cmd = AgentCommand(command='run')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_createTransformsThenActivateInstrument:: run %s", str(reply))
#        time.sleep(2)
#
#        log.debug("test_activateInstrument: calling go_streaming ")
#        cmd = AgentCommand(command='go_streaming')
#        reply = self._ia_client.execute(cmd)
#        log.debug("test_createTransformsThenActivateInstrument:: go_streaming %s", str(reply))
#        time.sleep(30)
#
#        log.debug("test_activateInstrument: calling go_observatory")
#        cmd = AgentCommand(command='go_observatory')
#        reply = self._ia_client.execute(cmd)
#        log.debug("test_activateInstrument: return from go_observatory   %s", str(reply))
#        time.sleep(6)
#
#
#        log.debug("test_createTransformsThenActivateInstrument:: calling go_inactive ")
#        cmd = AgentCommand(command='go_inactive')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_createTransformsThenActivateInstrument:: return from go_inactive %s", str(reply))
#        time.sleep(2)
#
#        log.debug("test_createTransformsThenActivateInstrument:: calling reset ")
#        cmd = AgentCommand(command='reset')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_createTransformsThenActivateInstrument:: return from reset %s", str(reply))
#        time.sleep(2)

        #-------------------------------
        # Sampling
        #-------------------------------
        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        print retval
        log.debug("test_createTransformsThenActivateInstrument:: initialize %s", str(retval))
        time.sleep(2)

        cmd = AgentCommand(command='go_active')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: go_active %s", str(reply))
        time.sleep(2)

        cmd = AgentCommand(command='run')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: run %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling acquire_sample ")
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrument: return from acquire_sample %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling acquire_sample 2")
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrument: return from acquire_sample 2   %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling acquire_sample 3")
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrument: return from acquire_sample 3   %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling go_inactive ")
        cmd = AgentCommand(command='go_inactive')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: return from go_inactive %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling reset ")
        cmd = AgentCommand(command='reset')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: return from reset %s", str(reply))
        time.sleep(2)


        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)


        #get the dataset id of the ctd_parsed product from the dataproduct  ctd_parsed_data_product
        ctd_parsed_data_product_obj = self.dataproductclient.read_data_product(ctd_parsed_data_product)
        log.debug("test_createTransformsThenActivateInstrument:: ctd_parsed_data_product dataset id %s", str(ctd_parsed_data_product_obj.dataset_id))

        # ask for the dataset bounds from the datasetmgmtsvc
        bounds = self.datasetclient.get_dataset_bounds(ctd_parsed_data_product_obj.dataset_id)
        log.debug("test_createTransformsThenActivateInstrument:: ctd_parsed_data_product dataset bounds %s", str(bounds))
        print 'activate_instrument: got dataset bounds %s', str(bounds)

