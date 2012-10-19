
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
from interface.objects import  ContactInformation

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest 
from pyon.public import RT, PRED
from nose.plugins.attrib import attr
import unittest

from ion.util.parameter_yaml_IO import get_param_dict
from ion.services.dm.utility.granule_utils import time_series_domain

from coverage_model.coverage import GridDomain, GridShape, CRS
from coverage_model.basic_types import MutabilityEnum, AxisTypeEnum


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


    #@unittest.skip('not ready')
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
                                  description="SBE37IMModel",
                                  model="SBE37IMModel",
                                  stream_configuration= {'parsed': 'ctd_parsed_param_dict' } )

        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        log.debug( 'test_get_provenance: new InstrumentModel id = %s ', str(instModel_id))

        self.omsclient.assign_instrument_model_to_instrument_site(instModel_id, instrument_site_id)


        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_module="ion.agents.instrument.instrument_agent", driver_class="InstrumentAgent" )
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


        parsed_parameter_dictionary = get_param_dict('simple_data_particle_parsed_param_dict')
        parsed_stream_def_id = self.pubsubclient.create_stream_definition(name='parsed', parameter_dictionary=parsed_parameter_dictionary.dump())

        log.debug( 'test_get_provenance:Creating new CDM data product with a stream definition')

        parameter_dictionary = get_param_dict('simple_data_particle_parsed_param_dict')
        parameter_dictionary = parameter_dictionary.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product = self.dpmsclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id, parameter_dictionary=parsed_parameter_dictionary.dump())
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

        log_data_product_id = self.dpmsclient.create_data_product(dp_obj, parsed_stream_def_id, parsed_parameter_dictionary.dump())
        self.omsclient.create_site_data_product(instrument_site_id, log_data_product_id)


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

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance",
                                          driver_module='mi.instrument.seabird.sbe37smb.ooicore.driver', driver_class='SBE37Driver',
                                          comms_device_address='sbe37-simulator.oceanobservatories.org',   comms_device_port=4001,  port_agent_work_dir='/tmp/', port_agent_delimeter=['<<','>>'] )
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)


        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------
        log.debug("TestDataProductProvenance: create data process definition ctd_L0_all")
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
        log.debug("TestDataProductProvenance: create data process definition CTDL1ConductivityTransform")
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
        log.debug("TestDataProductProvenance: create data process definition CTDL1PressureTransform")
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
        log.debug("TestDataProductProvenance: create data process definition CTDL1TemperatureTransform")
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
        log.debug("TestDataProductProvenance: create data process definition SalinityTransform")
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
        log.debug("TestDataProductProvenance: create data process definition DensityTransform")
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

        outgoing_stream_l0_conductivity_id = self.pubsubclient.create_stream_definition(name='L0_Conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_conductivity_id, ctd_L0_all_dprocdef_id, binding='conductivity' )

        outgoing_stream_l0_pressure_id = self.pubsubclient.create_stream_definition(name='L0_Pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_pressure_id, ctd_L0_all_dprocdef_id, binding='pressure' )

        outgoing_stream_l0_temperature_id = self.pubsubclient.create_stream_definition(name='L0_Temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_temperature_id, ctd_L0_all_dprocdef_id, binding='temperature' )


        self.output_products={}
        log.debug("TestDataProductProvenance: create output data product L0 conductivity")

        ctd_l0_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
                                                        name='L0_Conductivity',
                                                        description='transform output conductivity',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l0_conductivity_output_dp_id = self.dpmsclient.create_data_product(ctd_l0_conductivity_output_dp_obj,
                                                                                outgoing_stream_l0_conductivity_id,
                                                                                parameter_dictionary)
        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id


        log.debug("TestDataProductProvenance: create output data product L0 pressure")

        ctd_l0_pressure_output_dp_obj = IonObject(  RT.DataProduct,
                                                    name='L0_Pressure',
                                                    description='transform output pressure',
                                                    temporal_domain = tdom,
                                                    spatial_domain = sdom)

        ctd_l0_pressure_output_dp_id = self.dpmsclient.create_data_product(ctd_l0_pressure_output_dp_obj,
                                                                            outgoing_stream_l0_pressure_id,
                                                                            parameter_dictionary)
        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id

        log.debug("TestDataProductProvenance: create output data product L0 temperature")

        ctd_l0_temperature_output_dp_obj = IonObject(   RT.DataProduct,
                                                        name='L0_Temperature',
                                                        description='transform output temperature',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l0_temperature_output_dp_id = self.dpmsclient.create_data_product(ctd_l0_temperature_output_dp_obj,
                                                                                outgoing_stream_l0_temperature_id,
                                                                                parameter_dictionary)
        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id


        #-------------------------------
        # L1 Conductivity - Temperature - Pressure: Output Data Products
        #-------------------------------

        outgoing_stream_l1_conductivity_id = self.pubsubclient.create_stream_definition(name='L1_conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_conductivity_id, ctd_L1_conductivity_dprocdef_id, binding='conductivity' )

        outgoing_stream_l1_pressure_id = self.pubsubclient.create_stream_definition(name='L1_Pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_pressure_id, ctd_L1_pressure_dprocdef_id, binding='pressure' )

        outgoing_stream_l1_temperature_id = self.pubsubclient.create_stream_definition(name='L1_Temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_temperature_id, ctd_L1_temperature_dprocdef_id, binding='temperature' )

        log.debug("TestDataProductProvenance: create output data product L1 conductivity")

        ctd_l1_conductivity_output_dp_obj = IonObject(RT.DataProduct,
            name='L1_Conductivity',
            description='transform output L1 conductivity',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l1_conductivity_output_dp_id = self.dpmsclient.create_data_product(ctd_l1_conductivity_output_dp_obj,
                                                                                outgoing_stream_l1_conductivity_id,
                                                                                parameter_dictionary)


        log.debug("TestDataProductProvenance: create output data product L1 pressure")

        ctd_l1_pressure_output_dp_obj = IonObject(  RT.DataProduct,
                                                    name='L1_Pressure',
                                                    description='transform output L1 pressure',
                                                    temporal_domain = tdom,
                                                    spatial_domain = sdom)

        ctd_l1_pressure_output_dp_id = self.dpmsclient.create_data_product(ctd_l1_pressure_output_dp_obj,
                                                                            outgoing_stream_l1_pressure_id,
                                                                            parameter_dictionary)


        log.debug("TestDataProductProvenance: create output data product L1 temperature")

        ctd_l1_temperature_output_dp_obj = IonObject(   RT.DataProduct,
                                                        name='L1_Temperature',
                                                        description='transform output L1 temperature',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l1_temperature_output_dp_id = self.dpmsclient.create_data_product(ctd_l1_temperature_output_dp_obj,
                                                                                outgoing_stream_l1_temperature_id,
                                                                                parameter_dictionary)

        #-------------------------------
        # L2 Salinity - Density: Output Data Products
        #-------------------------------

        outgoing_stream_l2_salinity_id = self.pubsubclient.create_stream_definition(name='L2_salinity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_salinity_id, ctd_L2_salinity_dprocdef_id, binding='salinity' )

        outgoing_stream_l2_density_id = self.pubsubclient.create_stream_definition(name='L2_Density')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_density_id, ctd_L2_density_dprocdef_id, binding='density' )

        log.debug("TestDataProductProvenance: create output data product L2 Salinity")

        ctd_l2_salinity_output_dp_obj = IonObject( RT.DataProduct,
                                                    name='L2_Salinity',
                                                    description='transform output L2 salinity',
                                                    temporal_domain = tdom,
                                                    spatial_domain = sdom)


        ctd_l2_salinity_output_dp_id = self.dpmsclient.create_data_product(ctd_l2_salinity_output_dp_obj,
                                                                            outgoing_stream_l2_salinity_id,
                                                                            parameter_dictionary)


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
                                                                            outgoing_stream_l2_density_id,
                                                                            parameter_dictionary)

        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L0 all data_process start")
        try:
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L0_all_dprocdef_id, [ctd_parsed_data_product], self.output_products)
            #activate only this data process just for coverage
            self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestDataProductProvenance: create L0 all data_process return")


        #-------------------------------
        # L1 Conductivity: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L1 Conductivity data_process start")
        try:
            l1_conductivity_data_process_id = self.dataprocessclient.create_data_process(ctd_L1_conductivity_dprocdef_id, [ctd_l0_conductivity_output_dp_id], {'conductivity':ctd_l1_conductivity_output_dp_id})
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestDataProductProvenance: create L1 Conductivity data_process return")


        #-------------------------------
        # L1 Pressure: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L1_Pressure data_process start")
        try:
            l1_pressure_data_process_id = self.dataprocessclient.create_data_process(ctd_L1_pressure_dprocdef_id, [ctd_l0_pressure_output_dp_id], {'pressure':ctd_l1_pressure_output_dp_id})
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestDataProductProvenance: create L1_Pressure data_process return")



        #-------------------------------
        # L1 Temperature: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L1_Pressure data_process start")
        try:
            l1_temperature_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L1_temperature_dprocdef_id, [ctd_l0_temperature_output_dp_id], {'temperature':ctd_l1_temperature_output_dp_id})
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestDataProductProvenance: create L1_Pressure data_process return")



        #-------------------------------
        # L2 Salinity: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L2_salinity data_process start")
        try:
            l2_salinity_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L2_salinity_dprocdef_id, [ctd_l1_conductivity_output_dp_id, ctd_l1_pressure_output_dp_id, ctd_l1_temperature_output_dp_id], {'salinity':ctd_l2_salinity_output_dp_id})
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestDataProductProvenance: create L2_salinity data_process return")

        #-------------------------------
        # L2 Density: Create the data process
        #-------------------------------
        log.debug("TestDataProductProvenance: create L2_Density data_process start")
        try:
            l2_density_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L2_density_dprocdef_id, [ctd_l1_conductivity_output_dp_id, ctd_l1_pressure_output_dp_id, ctd_l1_temperature_output_dp_id], {'density':ctd_l2_density_output_dp_id})
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestDataProductProvenance: create L2_Density data_process return")


        #-------------------------------
        # Launch InstrumentAgentInstance, connect to the resource agent client
        #-------------------------------
        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        inst_agent_instance_obj= self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        print 'TestDataProductProvenance: Instrument agent instance obj: = ', inst_agent_instance_obj

        # Start a resource agent client to talk with the instrument agent.
#        self._ia_client = ResourceAgentClient('iaclient', name=inst_agent_instance_obj.agent_process_id,  process=FakeProcess())
#        print 'activate_instrument: got ia client %s', self._ia_client
#        log.debug(" test_createTransformsThenActivateInstrument:: got ia client %s", str(self._ia_client))



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
        # Deactivate InstrumentAgentInstance
        #-------------------------------
        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)



        results = self.dpmsclient.get_data_product_provenance_report(ctd_l2_density_output_dp_id)

