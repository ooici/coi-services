#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
import unittest
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from ion.agents.port.port_agent_process import PortAgentProcessType

from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.public import IonObject
from interface.objects import PortTypeEnum
from pyon.util.containers import DotDict
#from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.containers import create_unique_identifier
from pyon.ion.resource import LCS

from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

#from pyon.core.exception import NotFound, Inconsistent, Unauthorized #, Conflict
from pyon.public import RT, LCE, PRED, CFG, log
from pyon.ion.resource import OT
from nose.plugins.attrib import attr

from ion.services.sa.test.helpers import any_old, add_keyworded_attachment, GenericIntHelperTestCase

from ion.services.sa.instrument.flag import KeywordFlag
from ion.services.dm.utility.granule_utils import time_series_domain

from ion.agents.port.port_agent_process import PortAgentType
DRV_URI_GOOD = CFG.device.sbe37.dvr_egg
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient


@attr('INT', group='sa')
class TestAssembly(GenericIntHelperTestCase):
    """
    assembly integration tests at the service level
    """

    def setUp(self):
        # Start container by calling parent's setUp
        super(TestAssembly, self).setUp()

        # Now create client to DataProductManagementService
        self.client = DotDict()
        self.client.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.client.DPMS = DataProductManagementServiceClient(node=self.container.node)
        self.client.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.client.OMS  = ObservatoryManagementServiceClient(node=self.container.node)
        self.client.PSMS = PubsubManagementServiceClient(node=self.container.node)
        self.client.DPRS = DataProcessManagementServiceClient(node=self.container.node)

        self.client.RR   = ResourceRegistryServiceClient(node=self.container.node)
        self.RR2 = EnhancedResourceRegistryClient(self.client.RR)
        self.dataset_management = DatasetManagementServiceClient()


        # deactivate all data processes when tests are complete
        def killAllDataProcesses():
            for proc_id in self.client.RR.find_resources(RT.DataProcess, None, None, True)[0]:
                self.client.DPRS.deactivate_data_process(proc_id)
                self.client.DPRS.delete_data_process(proc_id)
        self.addCleanup(killAllDataProcesses)


    #@unittest.skip('refactoring')
    def test_observatory_structure(self):
        """

        """

        c = self.client

        c2 = DotDict()
        c2.resource_registry = self.client.RR


        #generate a function that finds direct associations, using the more complex one in the service
        def gen_find_oms_association(output_type):
            def freeze():
                def finder_fun(obj_id):
                    log.debug("Finding related %s frames", output_type)
                    ret = c.OMS.find_related_frames_of_reference(obj_id, [output_type])
                    return ret[output_type]
                return finder_fun
            
            return freeze()



        ###############################################
        #
        # Assumptions or Order of Events for R2 Preloaded resources
        #
        # - orgs
        # - sites
        # - models
        # - agents
        # - devices
        # - instances
        # - attachments
        #
        ###############################################


        ###############################################
        #
        # orgs
        #
        ###############################################

        org_id = self.client.OMS.create_marine_facility(any_old(RT.Org))

        def add_to_org_fn(generic_resource_id):
            log.info("Associating with Org")
            self.client.OMS.assign_resource_to_observatory_org(generic_resource_id, org_id)



    ###############################################
        #
        # sites
        #
        ###############################################

        log.info("Create an observatory")
        observatory_id = self.perform_fcruf_script(RT.Observatory, 
                                          "observatory", 
                                          self.client.OMS, 
                                          actual_obj=None,
                                          extra_fn=add_to_org_fn)

        log.info("Create a subsite")
        subsite_id = self.perform_fcruf_script(RT.Subsite,
                                            "subsite",
                                            self.client.OMS,
                                            actual_obj=None,
                                            extra_fn=add_to_org_fn)

        log.info("Create a platform site")
        platform_site_id = self.perform_fcruf_script(RT.PlatformSite,
                                                     "platform_site",
                                                     self.client.OMS,
                                                     actual_obj=None,
                                                     extra_fn=add_to_org_fn)
        
        log.info("Create instrument site")
        instSite_obj = IonObject(RT.InstrumentSite,
                                 name="instrument_site",
                                 reference_designator="GA01SUMO-FI003-01-CTDMO0999")
        instrument_site_id = self.perform_fcruf_script(RT.InstrumentSite,
                                                       "instrument_site",
                                                       self.client.OMS,
                                                       actual_obj=instSite_obj,
                                                       extra_fn=add_to_org_fn)
        
        ###############################################
        #
        # models
        #
        ###############################################

        log.info("Create a platform model")
        platform_model_id = self.perform_fcruf_script(RT.PlatformModel, 
                                                     "platform_model", 
                                                     self.client.IMS)

        log.info("Create instrument model")
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel",
                                  custom_attributes= {'streams':{'raw': 'ctd_raw_param_dict' ,
                                                                 'parsed': 'ctd_parsed_param_dict' }})
        instrument_model_id = self.perform_fcruf_script(RT.InstrumentModel,
                                                        "instrument_model", 
                                                        self.client.IMS,
                                                        actual_obj=instModel_obj)

        log.info("Create sensor model")
        sensor_model_id = self.perform_fcruf_script(RT.SensorModel, 
                                                        "sensor_model", 
                                                        self.client.IMS)


        ###############################################
        #
        # agents
        #
        ###############################################

        log.info("Create platform agent")
        platform_agent_id = self.perform_fcruf_script(RT.PlatformAgent, 
                                                      "platform_agent", 
                                                      self.client.IMS)
        
        log.info("Create instrument agent")
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_uri=DRV_URI_GOOD)
        instrument_agent_id = self.perform_fcruf_script(RT.InstrumentAgent,
                                                        "instrument_agent", 
                                                        self.client.IMS,
                                                        actual_obj=instAgent_obj)


        ###############################################
        #
        # devices
        #
        ###############################################

        log.info("Create a platform device")
        platform_device_id = self.perform_fcruf_script(RT.PlatformDevice, 
                                                    "platform_device", 
                                                    self.client.IMS,
                                                    actual_obj=None,
                                                    extra_fn=add_to_org_fn)
        log.info("Create an instrument device")
        instrument_device_id = self.perform_fcruf_script(RT.InstrumentDevice,
                                                         "instrument_device", 
                                                         self.client.IMS,
                                                         actual_obj=None,
                                                         extra_fn=add_to_org_fn)

        log.info("Create a sensor device")
        sensor_device_id = self.perform_fcruf_script(RT.SensorDevice, 
                                                         "sensor_device", 
                                                         self.client.IMS,
                                                         actual_obj=None,
                                                         extra_fn=add_to_org_fn)




        ###############################################
        #
        # instances
        #
        ###############################################

        # we create instrument agent instance below, to verify some lcs checks


        ###############################################
        #
        #
        # attachments and LCS stuff
        #
        #
        ###############################################
        
        #----------------------------------------------
        #
        # orgs
        #
        #----------------------------------------------
        
        #----------------------------------------------
        #
        # sites
        #
        #----------------------------------------------

        log.info("Associate subsite with observatory")
        self.perform_association_script(c.OMS.assign_site_to_site,
                                        gen_find_oms_association(RT.Observatory),
                                        gen_find_oms_association(RT.Subsite),
                                        observatory_id,
                                        subsite_id)

        log.info("Associate platform site with subsite")
        self.perform_association_script(c.OMS.assign_site_to_site,
                                        gen_find_oms_association(RT.Subsite),
                                        gen_find_oms_association(RT.PlatformSite),
                                        subsite_id,
                                        platform_site_id)

        log.info("Associate instrument site with platform site")
        self.perform_association_script(c.OMS.assign_site_to_site,
                                        gen_find_oms_association(RT.PlatformSite),
                                        gen_find_oms_association(RT.InstrumentSite),
                                        platform_site_id,
                                        instrument_site_id)

        
        
        #----------------------------------------------
        #
        # models
        #
        #----------------------------------------------
        
        log.info("Associate platform model with platform site")
        self.perform_association_script(c.OMS.assign_platform_model_to_platform_site,
                                        self.RR2.find_platform_sites_by_platform_model_using_has_model,
                                        self.RR2.find_platform_models_of_platform_site_using_has_model,
                                        platform_site_id,
                                        platform_model_id)

        log.info("Associate instrument model with instrument site")
        self.perform_association_script(c.OMS.assign_instrument_model_to_instrument_site,
                                        self.RR2.find_instrument_sites_by_instrument_model_using_has_model,
                                        self.RR2.find_instrument_models_of_instrument_site_using_has_model,
                                        instrument_site_id,
                                        instrument_model_id)


        #----------------------------------------------
        #
        # agents
        #
        # - model required for DEVELOP
        # - egg required for INTEGRATE
        # - certification required for DEPLOY 
        #----------------------------------------------
        
        self.assert_lcs_pass(self.client.IMS, "platform_agent", platform_agent_id, LCE.PLAN, LCS.PLANNED)
        self.assert_lcs_fail(self.client.IMS, "platform_agent", platform_agent_id, LCE.DEVELOP)
        log.info("Associate platform model with platform agent")
        self.perform_association_script(c.IMS.assign_platform_model_to_platform_agent,
                                        self.RR2.find_platform_agents_by_platform_model_using_has_model,
                                        self.RR2.find_platform_models_of_platform_agent_using_has_model,
                                        platform_agent_id,
                                        platform_model_id)
        self.assert_lcs_pass(self.client.IMS, "platform_agent", platform_agent_id, LCE.DEVELOP, LCS.DEVELOPED)
        self.assert_lcs_fail(self.client.IMS, "platform_agent", platform_agent_id, LCE.INTEGRATE)
        add_keyworded_attachment(self.client.RR, platform_agent_id, [KeywordFlag.EGG_URL])
        self.assert_lcs_pass(self.client.IMS, "platform_agent", platform_agent_id, LCE.INTEGRATE, LCS.INTEGRATED)
        self.assert_lcs_fail(self.client.IMS, "platform_agent", platform_agent_id, LCE.DEPLOY)
        add_keyworded_attachment(self.client.RR, platform_agent_id, [KeywordFlag.CERTIFICATION, "platform attachment"])
        self.assert_lcs_pass(self.client.IMS, "platform_agent", platform_agent_id, LCE.DEPLOY, LCS.DEPLOYED)


        self.assert_lcs_pass(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.PLAN, LCS.PLANNED)
        self.assert_lcs_fail(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.DEVELOP)
        log.info("Associate instrument model with instrument agent")
        self.perform_association_script(c.IMS.assign_instrument_model_to_instrument_agent,
                                        c.IMS.find_instrument_agent_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_agent,
                                        instrument_agent_id,
                                        instrument_model_id)
        self.assert_lcs_pass(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.DEVELOP, LCS.DEVELOPED)

        self.assert_lcs_fail(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.INTEGRATE)
        add_keyworded_attachment(self.client.RR, instrument_agent_id, [KeywordFlag.EGG_URL])
        self.assert_lcs_pass(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.INTEGRATE, LCS.INTEGRATED)
        self.assert_lcs_fail(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.DEPLOY)
        add_keyworded_attachment(self.client.RR, instrument_agent_id, [KeywordFlag.CERTIFICATION])
        self.assert_lcs_pass(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.DEPLOY, LCS.DEPLOYED)


        #----------------------------------------------
        #
        # devices
        #
        #----------------------------------------------

        log.info("LCS plan")
        self.assert_lcs_pass(self.client.IMS, "platform_device", platform_device_id, LCE.PLAN, LCS.PLANNED)

        log.info("LCS develop")
        self.assert_lcs_fail(self.client.IMS, "platform_device", platform_device_id, LCE.DEVELOP)
        x = self.client.IMS.read_platform_device(platform_device_id)
        x.serial_number = "12345"
        self.client.IMS.update_platform_device(x)
        self.assert_lcs_fail(self.client.IMS, "platform_device", platform_device_id, LCE.DEVELOP)

        log.info("Associate platform model with platform device")
        self.assert_lcs_fail(self.client.IMS, "platform_device", platform_device_id, LCE.DEVELOP)
        self.perform_association_script(c.IMS.assign_platform_model_to_platform_device,
                                        c.IMS.find_platform_device_by_platform_model,
                                        c.IMS.find_platform_model_by_platform_device,
                                        platform_device_id,
                                        platform_model_id)
        self.assert_lcs_fail(self.client.IMS, "platform_device", platform_device_id, LCE.DEVELOP)
        add_keyworded_attachment(self.client.RR, platform_device_id, [KeywordFlag.VENDOR_TEST_RESULTS])
        self.assert_lcs_pass(self.client.IMS, "platform_device", platform_device_id, LCE.DEVELOP, LCS.DEVELOPED)

        log.info("LCS integrate")
        self.assert_lcs_fail(self.client.IMS, "platform_device", platform_device_id, LCE.INTEGRATE)
        add_keyworded_attachment(self.client.RR, platform_device_id, [KeywordFlag.VENDOR_TEST_RESULTS])
        self.assert_lcs_fail(self.client.IMS, "platform_device", platform_device_id, LCE.INTEGRATE)
        platform_agent_instance_id = self.create_plat_agent_instance(platform_agent_id, platform_device_id)
        self.assert_lcs_pass(self.client.IMS, "platform_device", platform_device_id, LCE.INTEGRATE, LCS.INTEGRATED)


        log.info("LCS deploy")
        self.assert_lcs_fail(self.client.IMS, "platform_device", platform_device_id, LCE.DEPLOY)




        log.info("LCS plan")
        self.assert_lcs_pass(self.client.IMS, "instrument_device", instrument_device_id, LCE.PLAN, LCS.PLANNED)

        log.info("LCS develop")
        self.assert_lcs_fail(self.client.IMS, "instrument_device", instrument_device_id, LCE.DEVELOP)
        x = self.client.IMS.read_instrument_device(instrument_device_id)
        x.serial_number = "12345"
        self.client.IMS.update_instrument_device(x)
        self.assert_lcs_fail(self.client.IMS, "instrument_device", instrument_device_id, LCE.DEVELOP)

        log.info("Associate instrument model with instrument device")
        self.perform_association_script(c.IMS.assign_instrument_model_to_instrument_device,
                                        c.IMS.find_instrument_device_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_device,
                                        instrument_device_id,
                                        instrument_model_id)
        self.assert_lcs_fail(self.client.IMS, "instrument_device", instrument_device_id, LCE.DEVELOP)
        add_keyworded_attachment(self.client.RR, instrument_device_id, [KeywordFlag.VENDOR_TEST_RESULTS])
        self.assert_lcs_pass(self.client.IMS, "instrument_device", instrument_device_id, LCE.DEVELOP, LCS.DEVELOPED)

        log.info("LCS integrate")
        self.assert_lcs_fail(self.client.IMS, "instrument_device", instrument_device_id, LCE.INTEGRATE)
        log.info("Associate instrument device with platform device")
        self.perform_association_script(c.IMS.assign_instrument_device_to_platform_device,
                                        c.IMS.find_platform_device_by_instrument_device,
                                        c.IMS.find_instrument_device_by_platform_device,
                                        platform_device_id,
                                        instrument_device_id)
        self.assert_lcs_fail(self.client.IMS, "instrument_device", instrument_device_id, LCE.INTEGRATE)
        log.info("Create instrument agent instance")
        instrument_agent_instance_id = self.create_inst_agent_instance(instrument_agent_id, instrument_device_id)
        self.assert_lcs_pass(self.client.IMS, "instrument_device", instrument_device_id, LCE.INTEGRATE, LCS.INTEGRATED)

        log.info("LCS deploy")
        self.assert_lcs_fail(self.client.IMS, "instrument_device", instrument_device_id, LCE.DEPLOY)





        log.info("Associate sensor model with sensor device")
        self.perform_association_script(c.IMS.assign_sensor_model_to_sensor_device,
                                        self.RR2.find_sensor_devices_by_sensor_model_using_has_model,
                                        self.RR2.find_sensor_models_of_sensor_device_using_has_model,
                                        sensor_device_id,
                                        sensor_model_id)



        log.info("Associate sensor device with instrument device")
        self.perform_association_script(c.IMS.assign_sensor_device_to_instrument_device,
                                        self.RR2.find_instrument_devices_by_sensor_device_using_has_device,
                                        self.RR2.find_sensor_devices_of_instrument_device_using_has_device,
                                        instrument_device_id,
                                        sensor_device_id)


        #----------------------------------------------
        #
        # instances
        #
        #----------------------------------------------






        #----------------------------------------------
        #
        # data production chain and swapping
        #
        #----------------------------------------------

        #------------------------------------------------------------------------------------------------
        # create a stream definition for the data from the ctd simulator
        #------------------------------------------------------------------------------------------------
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.client.PSMS.create_stream_definition(name='Simulated CTD data', parameter_dictionary_id=pdict_id)
        log.debug("Created stream def id %s", ctd_stream_def_id)


        #create data products for instrument data

        dp_obj = self.create_data_product_obj()
        #log.debug("Created an IonObject for a data product: %s", dp_obj)

        #------------------------------------------------------------------------------------------------
        # Create a set of ParameterContext objects to define the parameters in the coverage, add each to the ParameterDictionary
        #------------------------------------------------------------------------------------------------

        dp_obj.name = 'Data Product'
        inst_data_product_id = c.DPMS.create_data_product(dp_obj, ctd_stream_def_id)


        #assign data products appropriately
        c.DAMS.assign_data_product(input_resource_id=instrument_device_id,
                                   data_product_id=inst_data_product_id)

        port_assignments={}
        pp_obj = IonObject(OT.PlatformPort, reference_designator='GA01SUMO-FI003-01-CTDMO0999', port_type= PortTypeEnum.PAYLOAD, ip_address='1' )
        port_assignments[instrument_device_id] = pp_obj

        deployment_obj = IonObject(RT.Deployment,
                                   name='deployment',
                                   port_assignments=port_assignments,
                                   context=IonObject(OT.CabledNodeDeploymentContext))
        deployment_id = self.perform_fcruf_script(RT.Deployment, "deployment", c.OMS, actual_obj=deployment_obj,
                                                  extra_fn=add_to_org_fn)

        c.OMS.assign_site_to_deployment(platform_site_id, deployment_id)
        self.RR2.find_deployment_id_of_platform_site_using_has_deployment(platform_site_id)
        c.OMS.assign_device_to_deployment(platform_device_id, deployment_id)
        self.RR2.find_deployment_of_platform_device_using_has_deployment(platform_device_id)


        c.OMS.activate_deployment(deployment_id, True)
        self.assertLess(0, len(self.RR2.find_instrument_sites_by_instrument_device_using_has_device(instrument_device_id)))
        self.assertLess(0, len(self.RR2.find_instrument_devices_of_instrument_site_using_has_device(instrument_site_id)))
        self.assertLess(0, len(self.RR2.find_platform_sites_by_platform_device_using_has_device(platform_device_id)))
        self.assertLess(0, len(self.RR2.find_platform_devices_of_platform_site_using_has_device(platform_site_id)))

        self.assert_lcs_pass(self.client.IMS, "platform_device", platform_device_id, LCE.DEPLOY, LCS.DEPLOYED)
        self.assert_lcs_pass(self.client.IMS, "instrument_device", instrument_device_id, LCE.DEPLOY, LCS.DEPLOYED)


        idev_lcs = self.client.RR.read(instrument_device_id).lcstate

        log.info("L4-CI-SA-RQ-334 DEPLOY: Proposed change - Instrument activation shall support transition to " +
                 "the active state for instruments - state is %s" % idev_lcs)


        #now along comes a new device
        log.info("Create instrument device 2")
        instrument_device_id2 = self.perform_fcruf_script(RT.InstrumentDevice,
                                                         "instrument_device",
                                                         self.client.IMS,
                                                         actual_obj=None,
                                                         extra_fn=add_to_org_fn)

        log.info("Associate instrument model with instrument device 2")
        self.perform_association_script(c.IMS.assign_instrument_model_to_instrument_device,
                                        c.IMS.find_instrument_device_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_device,
                                        instrument_device_id2,
                                        instrument_model_id)
        log.info("Associate instrument device with platform device 2")
        self.perform_association_script(c.IMS.assign_instrument_device_to_platform_device,
                                    c.IMS.find_platform_device_by_instrument_device,
                                    c.IMS.find_instrument_device_by_platform_device,
                                    platform_device_id,
                                    instrument_device_id2)
        dp_obj.name = 'Instrument Data Product 2'
        inst_data_product_id2 = c.DPMS.create_data_product(dp_obj, ctd_stream_def_id)
        c.DAMS.assign_data_product(input_resource_id=instrument_device_id2,
                                   data_product_id=inst_data_product_id2)

        # create a new deployment for the new device
        deployment_obj = any_old(RT.Deployment, {"context": IonObject(OT.CabledNodeDeploymentContext)})
        deployment_id2 = self.perform_fcruf_script(RT.Deployment, "deployment", c.OMS, actual_obj=deployment_obj,
                                                   extra_fn=add_to_org_fn)
        log.debug("Associating instrument site with new deployment")
        c.OMS.assign_site_to_deployment(instrument_site_id, deployment_id2)
        log.debug("Associating instrument device with new deployment")
        c.OMS.assign_device_to_deployment(instrument_device_id2, deployment_id2)

        # activate the new deployment -- changing the primary device -- but don't switch subscription
        log.debug("Activating new deployment")
        c.OMS.activate_deployment(deployment_id2, False)
        #todo: assert site hasDevice instrument_device_id2
        assocs = self.client.RR.find_associations(instrument_site_id, PRED.hasDevice, instrument_device_id2, id_only=True)
        self.assertIsNotNone(assocs)


        #----------------------------------------------
        #
        # generic find ops
        #
        #----------------------------------------------



        log.info("Find an instrument site by observatory")

        entities = c.OMS.find_related_frames_of_reference(observatory_id, [RT.InstrumentSite])
        self.assertIn(RT.InstrumentSite, entities)
        inst_sites = entities[RT.InstrumentSite]
        self.assertEqual(1, len(inst_sites))
        self.assertEqual(instrument_site_id, inst_sites[0]._id)

        c.IMS.delete_instrument_agent(instrument_agent_id)
        instr_agent_obj_read = self.client.RR.read(instrument_agent_id)
        self.assertEquals(instr_agent_obj_read.lcstate, LCS.DELETED)
        log.info("L4-CI-SA-RQ-382: Instrument activation shall manage the life cycle of Instrument Agents")

        c.IMS.delete_instrument_device(instrument_device_id)
        # Check whether the instrument device has been retired
        instrument_obj_read = self.client.RR.read(instrument_device_id)
        log.debug("The instruments lcs state has been set to %s after the delete operation" % instrument_obj_read.lcstate)
        self.assertEquals(instrument_obj_read.lcstate, LCS.DELETED)
        log.debug("L4-CI-SA-RQ-334 DELETED")
        log.debug("L4-CI-SA-RQ-335: Instrument activation shall support transition to the retired state of instruments")

        #----------------------------------------------
        #
        # force_deletes
        #
        #----------------------------------------------

        # need to "pluck" some resources out of associations
        self.RR2.pluck(instrument_model_id)
        self.RR2.pluck(platform_model_id)
        self.RR2.pluck(instrument_agent_id)
        self.RR2.pluck(platform_agent_id)
        self.RR2.pluck(deployment_id)
        self.RR2.pluck(deployment_id2)

        self.perform_fd_script(observatory_id, "observatory", c.OMS)
        self.perform_fd_script(subsite_id, "subsite", c.OMS)
        self.perform_fd_script(platform_site_id, "platform_site", c.OMS)
        self.perform_fd_script(instrument_site_id, "instrument_site", c.OMS)
        self.perform_fd_script(platform_model_id, "platform_model", c.IMS)
        self.perform_fd_script(instrument_model_id, "instrument_model", c.IMS)
        self.perform_fd_script(platform_agent_id, "platform_agent", c.IMS)
        self.perform_fd_script(instrument_agent_id, "instrument_agent", c.IMS)
        self.perform_fd_script(platform_device_id, "platform_device", c.IMS)
        self.perform_fd_script(instrument_device_id, "instrument_device", c.IMS)
        self.perform_fd_script(sensor_device_id, "sensor_device", c.IMS)
        self.perform_fd_script(sensor_model_id, "sensor_model", c.IMS)
        self.perform_fd_script(platform_agent_instance_id, "platform_agent_instance", c.IMS)
        self.perform_fd_script(instrument_agent_instance_id, "instrument_agent_instance", c.IMS)
        self.perform_fd_script(deployment_id, "deployment", c.OMS)
        self.perform_fd_script(deployment_id2, "deployment", c.OMS)

    def create_data_product_obj(self):

        # Construct temporal and spatial Coordinate Reference System objects

        # creates an IonObject of RT.DataProduct and adds custom fields specified by dict
        return any_old(RT.DataProduct)


    def create_inst_agent_instance(self, agent_id, device_id):

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


        instAgentInstance_id = self.client.IMS.create_instrument_agent_instance(instAgentInstance_obj,
                                                                                agent_id,
                                                                                device_id)

        return instAgentInstance_id

    def create_plat_agent_instance(self, agent_id, device_id):
        #todo : do this for real
        platAgentInstance_id, _ = self.client.RR.create(any_old(RT.PlatformAgentInstance))
        self.client.RR.create_association(device_id,
                                          PRED.hasAgentInstance,
                                          platAgentInstance_id)
        return platAgentInstance_id

    def template_tst_deployment_context(self, context=None):
        """
        Creates a minimal deployment: 1 instrument, 1 site.  deployment context must be provided
        """
        c = self.client

        c2 = DotDict()
        c2.resource_registry = self.client.RR

        log.info("Create a instrument model")
        instrument_model_id = self.perform_fcruf_script(RT.InstrumentModel,
                                                      "instrument_model",
                                                      self.client.IMS)

        log.info("Create an instrument device")
        instrument_device_id = self.perform_fcruf_script(RT.InstrumentDevice,
                                                         "instrument_device",
                                                         self.client.IMS)

        log.info("Create instrument site")
        instrument_site_id = self.perform_fcruf_script(RT.InstrumentSite,
                                                       "instrument_site",
                                                       self.client.OMS)

        log.info("Associate instrument model with instrument site")
        self.perform_association_script(c.OMS.assign_instrument_model_to_instrument_site,
                                        self.RR2.find_instrument_sites_by_instrument_model_using_has_model,
                                        self.RR2.find_instrument_models_of_instrument_site_using_has_model,
                                        instrument_site_id,
                                        instrument_model_id)


        log.info("Associate instrument model with instrument device")
        self.perform_association_script(c.IMS.assign_instrument_model_to_instrument_device,
                                        c.IMS.find_instrument_device_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_device,
                                        instrument_device_id,
                                        instrument_model_id)


        log.info("Create a stream definition for the data from the ctd simulator")
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.client.PSMS.create_stream_definition(name='Simulated CTD data', parameter_dictionary_id=pdict_id)

        log.info("Create an IonObject for a data products")
        dp_obj = self.create_data_product_obj()

        dp_obj.name = create_unique_identifier('Inst Data Product')
        inst_data_product_id = c.DPMS.create_data_product(dp_obj, ctd_stream_def_id)


        #assign data products appropriately
        c.DAMS.assign_data_product(input_resource_id=instrument_device_id,
                                   data_product_id=inst_data_product_id)


        deployment_obj = any_old(RT.Deployment, dict(context=context))
        deployment_id = c.OMS.create_deployment(deployment_obj)

        c.OMS.assign_site_to_deployment(instrument_site_id, deployment_id)
        c.OMS.assign_device_to_deployment(instrument_device_id, deployment_id)

        c.OMS.activate_deployment(deployment_id, True)

        # cleanup
        self.RR2.pluck(instrument_model_id)
        self.RR2.pluck(deployment_id)
        self.RR2.pluck(instrument_device_id)
        c.IMS.force_delete_instrument_model(instrument_model_id)
        c.IMS.force_delete_instrument_device(instrument_device_id)
        c.OMS.force_delete_instrument_site(instrument_site_id)
        c.OMS.force_delete_deployment(deployment_id)



    # test all 4 deployment contexts.  can fill in these context when their fields get defined
    def test_deployment_remoteplatform(self):
        context = IonObject(OT.RemotePlatformDeploymentContext)
        self.template_tst_deployment_context(context)

    def test_deployment_cablednode(self):
        context = IonObject(OT.CabledNodeDeploymentContext)
        self.template_tst_deployment_context(context)

    def test_deployment_cabledinstrument(self):
        context = IonObject(OT.CabledInstrumentDeploymentContext)
        self.template_tst_deployment_context(context)

    @unittest.skip("mobile deployments are unimplemented")
    def test_deployment_mobile(self):
        context = IonObject(OT.MobileAssetDeploymentContext)
        self.template_tst_deployment_context(context)

    @unittest.skip("cruise deployments are unimplemented")
    def test_deployment_cruise(self):
        context = IonObject(OT.CruiseDeploymentContext)
        self.template_tst_deployment_context(context)



