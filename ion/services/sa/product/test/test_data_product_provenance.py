from pyon.public import  log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.objects import  ContactInformation
from interface.objects import AttachmentType
from interface.objects import Device
from interface.objects import DataProduct
from interface.objects import DataProcess
from interface.objects import DataProducer

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest 
from pyon.public import RT, PRED, CFG
from nose.plugins.attrib import attr

from interface.objects import StreamConfiguration
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
DRV_URI_GOOD = CFG.device.sbe37.dvr_egg
from ion.services.dm.utility.granule_utils import time_series_domain
import base64
import unittest


class FakeProcess(LocalContextMixin):
    name = ''



@attr('INT', group='sa')
#@unittest.skip('not working')
class TestDataProductProvenance(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.dpmsclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.process_dispatcher   = ProcessDispatcherServiceClient()

        self.dataset_management = DatasetManagementServiceClient()


        # deactivate all data processes when tests are complete
        def killAllDataProcesses():
            for proc_id in self.rrclient.find_resources(RT.DataProcess, None, None, True)[0]:
                self.dataprocessclient.deactivate_data_process(proc_id)
                self.dataprocessclient.delete_data_process(proc_id)
        self.addCleanup(killAllDataProcesses)

    def test_get_data_product_provenance_report(self):
        #Create a test device
        device_obj = Device(name='Device1',
                                        description='test instrument site')
        device_id, _ = self.rrclient.create(device_obj)
        self.addCleanup(self.rrclient.delete, device_id)

        #Create a test DataProduct
        data_product1_obj = DataProduct(name='DataProduct1',
                                        description='test data product 1')
        data_product1_id, _ = self.rrclient.create(data_product1_obj)
        self.addCleanup(self.rrclient.delete, data_product1_id)

        #Create a test DataProcess
        data_process_obj = DataProcess(name='DataProcess',
                                       description='test data process')
        data_process_id, _ = self.rrclient.create(data_process_obj)
        self.addCleanup(self.rrclient.delete, data_process_id)

        #Create a second test DataProduct
        data_product2_obj = DataProduct(name='DataProduct2',
                                        description='test data product 2')
        data_product2_id, _ = self.rrclient.create(data_product2_obj)
        self.addCleanup(self.rrclient.delete, data_product2_id)

        #Create a test DataProducer
        data_producer_obj = DataProducer(name='DataProducer',
                                         description='test data producer')
        data_producer_id, rev = self.rrclient.create(data_producer_obj)

        #Link the DataProcess to the second DataProduct manually
        assoc_id, _ = self.rrclient.create_association(subject=data_process_id, predicate=PRED.hasInputProduct, object=data_product2_id)
        #self.addCleanup(self.rrclient.delete_association, assoc_id)

        # Register the instrument and process. This links the device and the data process
        # with their own producers
        self.damsclient.register_instrument(device_id)
        self.addCleanup(self.damsclient.unregister_instrument, device_id)
        self.damsclient.register_process(data_process_id)
        self.addCleanup(self.damsclient.unregister_process, data_process_id)

        #Manually link the first DataProduct with the test DataProducer
        assoc_id, _ = self.rrclient.create_association(subject=data_product1_id, predicate=PRED.hasDataProducer, object=data_producer_id)

        #Get the DataProducer linked to the DataProcess (created in register_process above)
        #Associate that with with DataProduct1's DataProducer
        data_process_producer_ids, _ = self.rrclient.find_objects(subject=data_process_id, predicate=PRED.hasDataProducer, object_type=RT.DataProducer, id_only=True)
        assoc_id, _ = self.rrclient.create_association(subject=data_process_producer_ids[0], predicate=PRED.hasParent, object=data_producer_id)
        #self.addCleanup(self.rrclient.delete_association, assoc_id)

        #Get the DataProducer linked to the Device (created in register_instrument
        #Associate that with the DataProcess's DataProducer
        device_producer_ids, _ = self.rrclient.find_objects(subject=device_id, predicate=PRED.hasDataProducer, object_type=RT.DataProducer, id_only=True)
        assoc_id, _ = self.rrclient.create_association(subject=data_producer_id, predicate=PRED.hasParent, object=device_producer_ids[0])

        #Create the links between the Device, DataProducts, DataProcess, and all DataProducers
        self.damsclient.assign_data_product(input_resource_id=device_id, data_product_id=data_product1_id)
        self.addCleanup(self.damsclient.unassign_data_product, device_id, data_product1_id)
        self.damsclient.assign_data_product(input_resource_id=data_process_id, data_product_id=data_product2_id)
        self.addCleanup(self.damsclient.unassign_data_product, data_process_id, data_product2_id)

        #Traverse through the relationships to get the links between objects
        res = self.dpmsclient.get_data_product_provenance_report(data_product2_id)

        #Make sure there are four keys
        self.assertEqual(len(res.keys()), 4)

        parent_count = 0
        config_count = 0
        for v in res.itervalues():
            if 'parent' in v:
                parent_count += 1
            if 'config' in v:
                config_count += 1

        #Make sure there are three parents and four configs
        self.assertEqual(parent_count, 3)
        self.assertEqual(config_count, 4)


    @unittest.skip('This test is obsolete with new framework')
    def test_get_provenance(self):

        #create a deployment with metadata and an initial site and device
        instrument_site_obj = IonObject(RT.InstrumentSite,
                                        name='InstrumentSite1',
                                        description='test instrument site')
        instrument_site_id = self.omsclient.create_instrument_site(instrument_site_obj, "")
        log.debug( 'test_get_provenance: new instrument_site_id id = %s ', str(instrument_site_id))


        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel" )

        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        log.debug( 'test_get_provenance: new InstrumentModel id = %s ', str(instModel_id))

        self.omsclient.assign_instrument_model_to_instrument_site(instModel_id, instrument_site_id)


        # Create InstrumentAgent
        parsed_config = StreamConfiguration(stream_name='parsed', parameter_dictionary_name='ctd_parsed_param_dict' )
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                name='agent007',
                                description="SBE37IMAgent",
                                driver_uri=DRV_URI_GOOD,
                                stream_configurations = [parsed_config] )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        log.debug( 'test_get_provenance:new InstrumentAgent id = %s', instAgent_id)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        log.debug('test_get_provenance: Create instrument resource to represent the SBE37 (SA Req: L4-CI-SA-RQ-241) ')
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        log.debug("test_get_provenance: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) ", instDevice_id)


        #-------------------------------
        # Create CTD Parsed  data product
        #-------------------------------
        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()


        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        parsed_stream_def_id = self.pubsubclient.create_stream_definition(name='parsed', parameter_dictionary_id=pdict_id)

        log.debug( 'test_get_provenance:Creating new CDM data product with a stream definition')


        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product = self.dpmsclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id)
        log.debug( 'new dp_id = %s', ctd_parsed_data_product)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)
        self.dpmsclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product)

        #-------------------------------
        # create a data product for the site to pass the OMS check.... we need to remove this check
        #-------------------------------
        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        log_data_product_id = self.dpmsclient.create_data_product(dp_obj, parsed_stream_def_id)


        #-------------------------------
        # Deploy instrument device to instrument site
        #-------------------------------
        deployment_obj = IonObject(RT.Deployment,
                                        name='TestDeployment',
                                        description='some new deployment')
        deployment_id = self.omsclient.create_deployment(deployment_obj)
        self.omsclient.deploy_instrument_site(instrument_site_id, deployment_id)
        self.imsclient.deploy_instrument_device(instDevice_id, deployment_id)

        log.debug("test_create_deployment: created deployment id: %s ", str(deployment_id) )

        self.omsclient.activate_deployment(deployment_id)
        inst_device_objs, _ = self.rrclient.find_objects(subject=instrument_site_id, predicate=PRED.hasDevice, object_type=RT.InstrumetDevice, id_only=False)
        log.debug("test_create_deployment: deployed device: %s ", str(inst_device_objs[0]) )

        #-------------------------------
        # Create the agent instance
        #-------------------------------

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


        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance',
            description="SBE37IMAgentInstance",
            port_agent_config = port_agent_config)

        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)


        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------
        log.debug("TestDataProductProvenance: create data process definition ctd_L0_all")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L0_all',
                            description='transform ctd package into three separate L0 streams',
                            module='ion.processes.data.transforms.ctd.ctd_L0_all',
                            class_name='ctd_L0_all')
        try:
            ctd_L0_all_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new ctd_L0_all data process definition: %s" %ex)


        #-------------------------------
        # L1 Conductivity: Data Process Definition
        #-------------------------------
        log.debug("TestDataProductProvenance: create data process definition CTDL1ConductivityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L1_conductivity',
                            description='create the L1 conductivity data product',
                            module='ion.processes.data.transforms.ctd.ctd_L1_conductivity',
                            class_name='CTDL1ConductivityTransform')
        try:
            ctd_L1_conductivity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new CTDL1ConductivityTransform data process definition: %s" %ex)

        #-------------------------------
        # L1 Pressure: Data Process Definition
        #-------------------------------
        log.debug("TestDataProductProvenance: create data process definition CTDL1PressureTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L1_pressure',
                            description='create the L1 pressure data product',
                            module='ion.processes.data.transforms.ctd.ctd_L1_pressure',
                            class_name='CTDL1PressureTransform')
        try:
            ctd_L1_pressure_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new CTDL1PressureTransform data process definition: %s" %ex)


        #-------------------------------
        # L1 Temperature: Data Process Definition
        #-------------------------------
        log.debug("TestDataProductProvenance: create data process definition CTDL1TemperatureTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L1_temperature',
                            description='create the L1 temperature data product',
                            module='ion.processes.data.transforms.ctd.ctd_L1_temperature',
                            class_name='CTDL1TemperatureTransform')
        try:
            ctd_L1_temperature_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new CTDL1TemperatureTransform data process definition: %s" %ex)


        #-------------------------------
        # L2 Salinity: Data Process Definition
        #-------------------------------
        log.debug("TestDataProductProvenance: create data process definition SalinityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L2_salinity',
                            description='create the L1 temperature data product',
                            module='ion.processes.data.transforms.ctd.ctd_L2_salinity',
                            class_name='SalinityTransform')
        try:
            ctd_L2_salinity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new SalinityTransform data process definition: %s" %ex)


        #-------------------------------
        # L2 Density: Data Process Definition
        #-------------------------------
        log.debug("TestDataProductProvenance: create data process definition DensityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L2_density',
                            description='create the L1 temperature data product',
                            module='ion.processes.data.transforms.ctd.ctd_L2_density',
                            class_name='DensityTransform')
        try:
            ctd_L2_density_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new DensityTransform data process definition: %s" %ex)



        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Output Data Products
        #-------------------------------

        outgoing_stream_l0_conductivity_id = self.pubsubclient.create_stream_definition(name='L0_Conductivity', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_conductivity_id, ctd_L0_all_dprocdef_id, binding='conductivity' )

        outgoing_stream_l0_pressure_id = self.pubsubclient.create_stream_definition(name='L0_Pressure', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_pressure_id, ctd_L0_all_dprocdef_id, binding='pressure' )

        outgoing_stream_l0_temperature_id = self.pubsubclient.create_stream_definition(name='L0_Temperature', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_temperature_id, ctd_L0_all_dprocdef_id, binding='temperature' )


        log.debug("TestDataProductProvenance: create output data product L0 conductivity")

        ctd_l0_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
                                                        name='L0_Conductivity',
                                                        description='transform output conductivity',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l0_conductivity_output_dp_id = self.dpmsclient.create_data_product(ctd_l0_conductivity_output_dp_obj,
                                                                                outgoing_stream_l0_conductivity_id)


        log.debug("TestDataProductProvenance: create output data product L0 pressure")

        ctd_l0_pressure_output_dp_obj = IonObject(  RT.DataProduct,
                                                    name='L0_Pressure',
                                                    description='transform output pressure',
                                                    temporal_domain = tdom,
                                                    spatial_domain = sdom)

        ctd_l0_pressure_output_dp_id = self.dpmsclient.create_data_product(ctd_l0_pressure_output_dp_obj,
                                                                            outgoing_stream_l0_pressure_id)
        log.debug("TestDataProductProvenance: create output data product L0 temperature")

        ctd_l0_temperature_output_dp_obj = IonObject(   RT.DataProduct,
                                                        name='L0_Temperature',
                                                        description='transform output temperature',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l0_temperature_output_dp_id = self.dpmsclient.create_data_product(ctd_l0_temperature_output_dp_obj,
                                                                                outgoing_stream_l0_temperature_id)

        #-------------------------------
        # L1 Conductivity - Temperature - Pressure: Output Data Products
        #-------------------------------

        outgoing_stream_l1_conductivity_id = self.pubsubclient.create_stream_definition(name='L1_conductivity', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_conductivity_id, ctd_L1_conductivity_dprocdef_id, binding='conductivity' )

        outgoing_stream_l1_pressure_id = self.pubsubclient.create_stream_definition(name='L1_Pressure', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_pressure_id, ctd_L1_pressure_dprocdef_id, binding='pressure' )

        outgoing_stream_l1_temperature_id = self.pubsubclient.create_stream_definition(name='L1_Temperature', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_temperature_id, ctd_L1_temperature_dprocdef_id, binding='temperature' )

        log.debug("TestDataProductProvenance: create output data product L1 conductivity")

        ctd_l1_conductivity_output_dp_obj = IonObject(RT.DataProduct,
            name='L1_Conductivity',
            description='transform output L1 conductivity',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l1_conductivity_output_dp_id = self.dpmsclient.create_data_product(ctd_l1_conductivity_output_dp_obj,
                                                                                outgoing_stream_l1_conductivity_id)


        log.debug("TestDataProductProvenance: create output data product L1 pressure")

        ctd_l1_pressure_output_dp_obj = IonObject(  RT.DataProduct,
                                                    name='L1_Pressure',
                                                    description='transform output L1 pressure',
                                                    temporal_domain = tdom,
                                                    spatial_domain = sdom)

        ctd_l1_pressure_output_dp_id = self.dpmsclient.create_data_product(ctd_l1_pressure_output_dp_obj,
                                                                            outgoing_stream_l1_pressure_id)


        log.debug("TestDataProductProvenance: create output data product L1 temperature")

        ctd_l1_temperature_output_dp_obj = IonObject(   RT.DataProduct,
                                                        name='L1_Temperature',
                                                        description='transform output L1 temperature',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l1_temperature_output_dp_id = self.dpmsclient.create_data_product(ctd_l1_temperature_output_dp_obj,
                                                                                outgoing_stream_l1_temperature_id)

        #-------------------------------
        # L2 Salinity - Density: Output Data Products
        #-------------------------------

        outgoing_stream_l2_salinity_id = self.pubsubclient.create_stream_definition(name='L2_salinity', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_salinity_id, ctd_L2_salinity_dprocdef_id, binding='salinity' )

        outgoing_stream_l2_density_id = self.pubsubclient.create_stream_definition(name='L2_Density', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_density_id, ctd_L2_density_dprocdef_id, binding='density' )

        log.debug("TestDataProductProvenance: create output data product L2 Salinity")

        ctd_l2_salinity_output_dp_obj = IonObject( RT.DataProduct,
                                                    name='L2_Salinity',
                                                    description='transform output L2 salinity',
                                                    temporal_domain = tdom,
                                                    spatial_domain = sdom)


        ctd_l2_salinity_output_dp_id = self.dpmsclient.create_data_product(ctd_l2_salinity_output_dp_obj,
                                                                            outgoing_stream_l2_salinity_id)


        log.debug("TestDataProductProvenance: create output data product L2 Density")

#        ctd_l2_density_output_dp_obj = IonObject(   RT.DataProduct,
#                                                    name='L2_Density',
#                                                    description='transform output pressure',
#                                                    temporal_domain = tdom,
#                                                    spatial_domain = sdom)
#
#        ctd_l2_density_output_dp_id = self.dpmsclient.create_data_product(ctd_l2_density_output_dp_obj,
#                                                                            outgoing_stream_l2_density_id,
#                                                                            parameter_dictionary)

        contactInfo = ContactInformation()
        contactInfo.individual_names_given = "Bill"
        contactInfo.individual_name_family = "Smith"
        contactInfo.street_address = "111 First St"
        contactInfo.city = "San Diego"
        contactInfo.email = "bill@yahoo.com"
        contactInfo.phones = ["858-555-6666"]
        contactInfo.country = "USA"
        contactInfo.postal_code = "92123"

        ctd_l2_density_output_dp_obj = IonObject(   RT.DataProduct,
                                                    name='L2_Density',
                                                    description='transform output pressure',
                                                    contacts = [contactInfo],
                                                    iso_topic_category = "my_iso_topic_category_here",
                                                    quality_control_level = "1",
                                                    temporal_domain = tdom,
                                                    spatial_domain = sdom)

        ctd_l2_density_output_dp_id = self.dpmsclient.create_data_product(ctd_l2_density_output_dp_obj,
                                                                            outgoing_stream_l2_density_id)

        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L0 all data_process start")
        try:
            input_data_products = [ctd_parsed_data_product]
            output_data_products = [ctd_l0_conductivity_output_dp_id, ctd_l0_pressure_output_dp_id, ctd_l0_temperature_output_dp_id]

            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(
                data_process_definition_id = ctd_L0_all_dprocdef_id,
                in_data_product_ids = input_data_products,
                out_data_product_ids = output_data_products
            )
            #activate only this data process just for coverage
            self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        contents = "this is the lookup table  contents, replace with a file..."
        att = IonObject(RT.Attachment, name='deviceLookupTable', content=base64.encodestring(contents), keywords=['DataProcessInput'], attachment_type=AttachmentType.ASCII)
        deviceAttachment = self.rrclient.create_attachment(ctd_l0_all_data_process_id, att)
        log.info( 'test_createTransformsThenActivateInstrument: InstrumentDevice attachment id = %s', deviceAttachment)

        log.debug("TestDataProductProvenance: create L0 all data_process return")


        #-------------------------------
        # L1 Conductivity: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L1 Conductivity data_process start")
        try:
            l1_conductivity_data_process_id = self.dataprocessclient.create_data_process(
                data_process_definition_id = ctd_L1_conductivity_dprocdef_id,
                in_data_product_ids = [ctd_l0_conductivity_output_dp_id],
                out_data_product_ids = [ctd_l1_conductivity_output_dp_id])
            
            self.dataprocessclient.activate_data_process(l1_conductivity_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)




        #-------------------------------
        # L1 Pressure: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L1_Pressure data_process start")
        try:
            l1_pressure_data_process_id = self.dataprocessclient.create_data_process(
                data_process_definition_id = ctd_L1_pressure_dprocdef_id,
                in_data_product_ids = [ctd_l0_pressure_output_dp_id],
                out_data_product_ids = [ctd_l1_pressure_output_dp_id])

            self.dataprocessclient.activate_data_process(l1_pressure_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)


        #-------------------------------
        # L1 Temperature: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L1_Pressure data_process start")
        try:
            l1_temperature_all_data_process_id = self.dataprocessclient.create_data_process(
                data_process_definition_id = ctd_L1_temperature_dprocdef_id,
                in_data_product_ids = [ctd_l0_temperature_output_dp_id],
                out_data_product_ids = [ctd_l1_temperature_output_dp_id])

            self.dataprocessclient.activate_data_process(l1_temperature_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        #-------------------------------
        # L2 Salinity: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L2_salinity data_process start")
        try:
            l2_salinity_all_data_process_id = self.dataprocessclient.create_data_process(
                data_process_definition_id = ctd_L2_salinity_dprocdef_id,
                in_data_product_ids = [ctd_l1_conductivity_output_dp_id, ctd_l1_pressure_output_dp_id, ctd_l1_temperature_output_dp_id],
                out_data_product_ids = [ctd_l2_salinity_output_dp_id])

            self.dataprocessclient.activate_data_process(l2_salinity_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)


        #-------------------------------
        # L2 Density: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L2_Density data_process start")
        try:
            in_dp_ids = [ctd_l1_conductivity_output_dp_id, ctd_l1_pressure_output_dp_id, ctd_l1_temperature_output_dp_id]
            out_dp_ids = [ctd_l2_density_output_dp_id]

            l2_density_all_data_process_id = self.dataprocessclient.create_data_process(
                data_process_definition_id = ctd_L2_density_dprocdef_id,
                in_data_product_ids = in_dp_ids,
                out_data_product_ids = out_dp_ids)

            self.dataprocessclient.activate_data_process(l2_density_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)



        #-------------------------------
        # Launch InstrumentAgentInstance, connect to the resource agent client
        #-------------------------------
        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        inst_agent_instance_obj= self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        print 'TestDataProductProvenance: Instrument agent instance obj: = ', inst_agent_instance_obj

        # Start a resource agent client to talk with the instrument agent.
#        self._ia_client = ResourceAgentClient('iaclient', name=ResourceAgentClient._get_agent_process_id(instDevice_id,  process=FakeProcess())
#        print 'activate_instrument: got ia client %s', self._ia_client
#        log.debug(" test_createTransformsThenActivateInstrument:: got ia client %s", str(self._ia_client))


        #-------------------------------
        # Deactivate InstrumentAgentInstance
        #-------------------------------
        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        self.dataprocessclient.deactivate_data_process(l2_density_all_data_process_id)
        self.dataprocessclient.deactivate_data_process(l2_salinity_all_data_process_id)
        self.dataprocessclient.deactivate_data_process(l1_temperature_all_data_process_id)
        self.dataprocessclient.deactivate_data_process(l1_pressure_data_process_id)
        self.dataprocessclient.deactivate_data_process(l1_conductivity_data_process_id)
        self.dataprocessclient.deactivate_data_process(ctd_l0_all_data_process_id)

        #-------------------------------
        # Retrieve the provenance info for the ctd density data product
        #-------------------------------
        provenance_dict = self.dpmsclient.get_data_product_provenance(ctd_l2_density_output_dp_id)
        log.debug("TestDataProductProvenance: provenance_dict  %s", str(provenance_dict))

        #validate that products are represented
        self.assertTrue (provenance_dict[str(ctd_l1_conductivity_output_dp_id)])
        self.assertTrue (provenance_dict[str(ctd_l0_conductivity_output_dp_id)])
        self.assertTrue (provenance_dict[str(ctd_l2_density_output_dp_id)])
        self.assertTrue (provenance_dict[str(ctd_l1_temperature_output_dp_id)])
        self.assertTrue (provenance_dict[str(ctd_l0_temperature_output_dp_id)])

        density_dict = (provenance_dict[str(ctd_l2_density_output_dp_id)])
        self.assertEquals(density_dict['producer'], [l2_density_all_data_process_id])


        #-------------------------------
        # Retrieve the extended resource for this data product
        #-------------------------------
        extended_product = self.dpmsclient.get_data_product_extension(ctd_l2_density_output_dp_id)
        self.assertEqual(1, len(extended_product.data_processes) )
        self.assertEqual(3, len(extended_product.process_input_data_products) )
#        log.debug("TestDataProductProvenance: DataProduct provenance_product_list  %s", str(extended_product.provenance_product_list))
#        log.debug("TestDataProductProvenance: DataProduct data_processes  %s", str(extended_product.data_processes))
#        log.debug("TestDataProductProvenance: DataProduct process_input_data_products  %s", str(extended_product.process_input_data_products))
#        log.debug("TestDataProductProvenance: provenance  %s", str(extended_product.computed.provenance.value))

        #-------------------------------
        # Retrieve the extended resource for this data process
        #-------------------------------
        extended_process_def = self.dataprocessclient.get_data_process_definition_extension(ctd_L0_all_dprocdef_id)

#        log.debug("TestDataProductProvenance: DataProcess extended_process_def  %s", str(extended_process_def))
#        log.debug("TestDataProductProvenance: DataProcess data_processes  %s", str(extended_process_def.data_processes))
#        log.debug("TestDataProductProvenance: DataProcess data_products  %s", str(extended_process_def.data_products))
        self.assertEqual(1, len(extended_process_def.data_processes) )
        self.assertEqual(3, len(extended_process_def.output_stream_definitions) )
        self.assertEqual(3, len(extended_process_def.data_products) ) #one list because of one data process

        #-------------------------------
        # Request the xml report
        #-------------------------------
        results = self.dpmsclient.get_data_product_provenance_report(ctd_l2_density_output_dp_id)
        print results

        #-------------------------------
        # Cleanup
        #-------------------------------

        self.dpmsclient.delete_data_product(ctd_parsed_data_product)
        self.dpmsclient.delete_data_product(log_data_product_id)
        self.dpmsclient.delete_data_product(ctd_l0_conductivity_output_dp_id)
        self.dpmsclient.delete_data_product(ctd_l0_pressure_output_dp_id)
        self.dpmsclient.delete_data_product(ctd_l0_temperature_output_dp_id)
        self.dpmsclient.delete_data_product(ctd_l1_conductivity_output_dp_id)
        self.dpmsclient.delete_data_product(ctd_l1_pressure_output_dp_id)
        self.dpmsclient.delete_data_product(ctd_l1_temperature_output_dp_id)
        self.dpmsclient.delete_data_product(ctd_l2_salinity_output_dp_id)
        self.dpmsclient.delete_data_product(ctd_l2_density_output_dp_id)

