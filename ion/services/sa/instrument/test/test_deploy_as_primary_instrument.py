from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.imarine_facility_management_service import MarineFacilityManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient

from prototype.sci_data.stream_defs import ctd_stream_definition, L0_pressure_stream_definition, L0_temperature_stream_definition, L0_conductivity_stream_definition
from prototype.sci_data.stream_defs import L1_pressure_stream_definition, L1_temperature_stream_definition, L1_conductivity_stream_definition, L2_practical_salinity_stream_definition, L2_density_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition

from pyon.public import log, LCS, LCE
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


@attr('INT', group='bvr')
#@unittest.skip("run locally only")
class TestIMSDeployAsPrimaryDevice(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        #self.container.start_rel_from_url('res/deploy/r2sa.yml')
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
        self.marinefacilityclient = MarineFacilityManagementServiceClient(node=self.container.node)

    def test_reassignPrimaryDevice(self):

        # Set up the preconditions
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
        print 'test_deployAsPrimaryDevice: new InstrumentModel id = ', instModel_id


        #-------------------------------
        # Create InstrumentAgent
        #-------------------------------
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_module="ion.services.mi.instrument_agent", driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        print 'test_deployAsPrimaryDevice: new InstrumentAgent id = ', instAgent_id

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)



        #-------------------------------
        # Create Logical Instrument
        #-------------------------------
        logicalInstrument_obj = IonObject(RT.LogicalInstrument, name='logicalInstrument1', description="SBE37IMLogicalInstrument" )
        try:
            logicalInstrument_id = self.marinefacilityclient.create_logical_instrument(logical_instrument=logicalInstrument_obj, parent_logical_platform_id='')
        except BadRequest as ex:
            self.fail("failed to create new LogicalInstrument: %s" %ex)
        print 'test_deployAsPrimaryDevice: new InstrumentAgent id = ', logicalInstrument_id

        self.marinefacilityclient.assign_instrument_model_to_logical_instrument(instModel_id, logicalInstrument_id)


        #-------------------------------
        # Create Old InstrumentDevice
        #-------------------------------
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDeviceYear1', description="SBE37IMDevice for the FIRST year of deployment", serial_number="12345" )
        try:
            oldInstDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, oldInstDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        print 'test_deployAsPrimaryDevice: new Year 1 InstrumentDevice id = ', oldInstDevice_id

        # deploy this device to the logical slot
        self.imsclient.deploy_instrument_device_to_logical_instrument(oldInstDevice_id, logicalInstrument_id)

        #self.imsclient.set_instrument_device_lifecycle(oldInstDevice_id, 'DEPLOYED_AVAILABLE')
        self.rrclient.execute_lifecycle_transition(oldInstDevice_id, LCE.DEVELOP)
        self.rrclient.execute_lifecycle_transition(oldInstDevice_id, LCE.DEPLOY)
        self.rrclient.set_lifecycle_state(oldInstDevice_id, LCS.DEPLOYED_AVAILABLE)

        # set this device as the current primary device
        self.imsclient.deploy_as_primary_instrument_device_to_logical_instrument(oldInstDevice_id, logicalInstrument_id)
        
        #-------------------------------
        # Create InstrumentAgentInstance for OldInstrumentDevice to hold configuration information
        #-------------------------------
        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstanceYear1', description="SBE37IMAgentInstance Year 1", svr_addr="localhost",
                                          driver_module="ion.services.mi.drivers.sbe37_driver", driver_class="SBE37Driver",
                                          cmd_port=5556, evt_port=5557, comms_method="ethernet", comms_device_address=CFG.device.sbe37.host, comms_device_port=CFG.device.sbe37.port,
                                          comms_server_address="localhost", comms_server_port=8888)
        oldInstAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, oldInstDevice_id)



        #-------------------------------
        # Create CTD Parsed as the Year 1 data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def)

        print 'test_deployAsPrimaryDevice: new Stream Definition id = ', ctd_stream_def_id

        print 'Creating new CDM data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='ctd_parsed_year1',description='ctd stream test year 1')
        try:
            ctd_parsed_data_product_year1 = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product_year1

        self.damsclient.assign_data_product(input_resource_id=oldInstDevice_id, data_product_id=ctd_parsed_data_product_year1)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product_year1, PRED.hasStream, None, True)
        print 'test_deployAsPrimaryDevice: Data product streams1 = ', stream_ids



        #-------------------------------
        # Create New InstrumentDevice
        #-------------------------------
        instDevice_obj_2 = IonObject(RT.InstrumentDevice, name='SBE37IMDeviceYear2', description="SBE37IMDevice for the SECOND year of deployment", serial_number="67890" )
        try:
            newInstDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj_2)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, newInstDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        print 'test_deployAsPrimaryDevice: new  Year 2 InstrumentDevice id = ', newInstDevice_id

        # deploy this device to the logical slot
        self.imsclient.deploy_instrument_device_to_logical_instrument(newInstDevice_id, logicalInstrument_id)
        #set the LCSTATE
        self.rrclient.execute_lifecycle_transition(newInstDevice_id, LCE.DEVELOP)
        self.rrclient.execute_lifecycle_transition(newInstDevice_id, LCE.DEPLOY)
        self.rrclient.set_lifecycle_state(newInstDevice_id, LCS.DEPLOYED_AVAILABLE)


        instDevice_obj_2 = self.rrclient.read(newInstDevice_id)
        log.debug("test_deployAsPrimaryDevice: Create New InstrumentDevice LCSTATE: %s ", str(instDevice_obj_2.lcstate))



        #-------------------------------
        # Create InstrumentAgentInstance for NewInstrumentDevice to hold configuration information
        #-------------------------------
        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstanceYear2', description="SBE37IMAgentInstance Year 2", svr_addr="localhost",
                                          driver_module="ion.services.mi.drivers.sbe37_driver", driver_class="SBE37Driver",
                                          cmd_port=5556, evt_port=5557, comms_method="ethernet", comms_device_address=CFG.device.sbe37.host, comms_device_port=CFG.device.sbe37.port,
                                          comms_server_address="localhost", comms_server_port=8888)
        newInstAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, oldInstDevice_id)


        #-------------------------------
        # Create CTD Parsed as the Year 2 data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def)

        print 'test_deployAsPrimaryDevice: new Stream Definition id = ', newInstDevice_id

        print 'Creating new CDM data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='ctd_parsed_year2',description='ctd stream test year 2')
        try:
            ctd_parsed_data_product_year2 = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product_year2

        self.damsclient.assign_data_product(input_resource_id=newInstDevice_id, data_product_id=ctd_parsed_data_product_year2)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product_year2, PRED.hasStream, None, True)
        print 'test_deployAsPrimaryDevice: Data product streams2 = ', stream_ids



        #-------------------------------
        # Logical Data Product: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition logical_transform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='logical_transform',
                            description='send the packet from the in stream to the out stream unchanged',
                            module='ion.processes.data.transforms.logical_transform',
                            class_name='logical_transform',
                            process_source='some_source_reference')
        try:
            logical_transform_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new ctd_L0_all data process definition: %s" %ex)


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
        # Logical Transform: Output Data Products
        #-------------------------------
        outgoing_logical_stream_def = SBE37_CDM_stream_definition()
        outgoing_logical_stream_def_id = self.pubsubclient.create_stream_definition(container=outgoing_logical_stream_def)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_logical_stream_def_id, logical_transform_dprocdef_id )

        log.debug("test_deployAsPrimaryDevice: create output parsed data product for Logical Instrument")
        ctd_logical_output_dp_obj = IonObject(RT.DataProduct, name='ctd_parsed_logical',description='ctd parsed from the logical instrument')
        logical_instrument_output_dp_id = self.dataproductclient.create_data_product(ctd_logical_output_dp_obj, outgoing_logical_stream_def_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=logical_instrument_output_dp_id, persist_data=True, persist_metadata=True)

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
        log.debug("test_deployAsPrimaryDevice: create output data product L0 conductivity")
        ctd_l0_conductivity_output_dp_obj = IonObject(RT.DataProduct, name='L0_Conductivity',description='transform output conductivity')
        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj, outgoing_stream_l0_conductivity_id)
        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_conductivity_output_dp_id, persist_data=True, persist_metadata=True)


        log.debug("test_deployAsPrimaryDevice: create output data product L0 pressure")
        ctd_l0_pressure_output_dp_obj = IonObject(RT.DataProduct, name='L0_Pressure',description='transform output pressure')
        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj, outgoing_stream_l0_pressure_id)
        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_pressure_output_dp_id, persist_data=True, persist_metadata=True)

        log.debug("test_deployAsPrimaryDevice: create output data product L0 temperature")
        ctd_l0_temperature_output_dp_obj = IonObject(RT.DataProduct, name='L0_Temperature',description='transform output temperature')
        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj, outgoing_stream_l0_temperature_id)
        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_temperature_output_dp_id, persist_data=True, persist_metadata=True)


        #-------------------------------
        # CTD Logical: Create the data process
        #-------------------------------
        log.debug("test_deployAsPrimaryDevice: create ctd_parsed logical  data_process start")
        try:
            ctd_parsed_logical_data_process_id = self.dataprocessclient.create_data_process(logical_transform_dprocdef_id, ctd_parsed_data_product_year1, {'output':logical_instrument_output_dp_id})
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)
        log.debug("test_deployAsPrimaryDevice: create L0 all data_process return")

        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process, listening to logical instrument output product!
        #-------------------------------
        log.debug("test_deployAsPrimaryDevice: create L0 all data_process start")
        try:
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L0_all_dprocdef_id, logical_instrument_output_dp_id, self.output_products)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)
        log.debug("test_deployAsPrimaryDevice: create L0 all data_process return")


        self.imsclient.deploy_as_primary_instrument_device_to_logical_instrument(newInstDevice_id, logicalInstrument_id)

        log.debug("test_deployAsPrimaryDevice: deploy_as_primary_instrument_device_to_logical_instrument return")
        # Make sure InstrumentDevice now has the primary assignment
        assoc = self.rrclient.get_association(newInstDevice_id, PRED.hasPrimaryDeployment, logicalInstrument_id)
        if not assoc:
            self.fail("Failed to reassign")
  
  