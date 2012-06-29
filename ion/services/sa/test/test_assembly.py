#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, IonObject
from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from pyon.core.exception import BadRequest, NotFound, Inconsistent #, Conflict
from pyon.public import RT, LCS, LCE
from pyon.ion.resource import get_maturity_visibility
from nose.plugins.attrib import attr
import unittest

from ion.services.sa.test.helpers import any_old, add_keyworded_attachment
from ion.services.sa.observatory.instrument_site_impl import InstrumentSiteImpl
from ion.services.sa.observatory.platform_site_impl import PlatformSiteImpl
from ion.services.sa.instrument.platform_agent_impl import PlatformAgentImpl
from ion.services.sa.instrument.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.instrument.sensor_device_impl import SensorDeviceImpl

from ion.services.sa.instrument.flag import KeywordFlag

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition

# some stuff for logging info to the console
# import sys
# log = DotDict()
# printout = sys.stderr.write
# printout = lambda x: None
#
# log.debug = lambda x: printout("DEBUG: %s\n" % x)
# log.info = lambda x: printout("INFO: %s\n" % x)
# log.warn = lambda x: printout("WARNING: %s\n" % x)



@attr('INT', group='sa')
class TestAssembly(IonIntegrationTestCase):
    """
    assembly integration tests at the service level
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.client = DotDict()
        self.client.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.client.DPMS = DataProductManagementServiceClient(node=self.container.node)
        self.client.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.client.OMS = ObservatoryManagementServiceClient(node=self.container.node)
        self.client.PSMS = PubsubManagementServiceClient(node=self.container.node)

        self.client.RR   = ResourceRegistryServiceClient(node=self.container.node)

    @unittest.skip('this test just for debugging setup')
    def test_just_the_setup(self):
        return
    

    def _low_level_init(self):
        resource_ids = {}

        #TODO: still relevant?
        ## some stream definitions
        #resource_ids[RT.StreamDefinition] = {}

        # # get module and function by name specified in strings
        # sc_module = "prototype.sci_data.stream_defs"
        # sc_method = "ctd_stream_definition"
        # module = __import__(sc_module, fromlist=[sc_method])
        # creator_func = getattr(module, sc_method)

        # for n in ["SeabirdSim_raw", "SeabirdSim_parsed"]:
        #     container = creator_func()
            
        #     response = self.client.PSMS.create_stream_definition(container=container,
        #                                                          name=n,
        #                                                          description="inserted by test_assembly.py")
        #     resource_ids[RT.StreamDefinition][n] = response



        return resource_ids



    #@unittest.skip('refactoring')
    def test_observatory_structure(self):
        """

        """
        c = self.client

        c2 = DotDict()
        c2.resource_registry = self.client.RR

        instrument_site_impl    = InstrumentSiteImpl(c2)
        platform_site_impl      = PlatformSiteImpl(c2)
        platform_agent_impl     = PlatformAgentImpl(c2)
        instrument_device_impl  = InstrumentDeviceImpl(c2)
        sensor_device_impl      = SensorDeviceImpl(c2)

        #generate a function that finds direct associations, using the more complex one in the service
        def gen_find_oms_association(output_type):
            def freeze():
                def finder_fun(obj_id):
                    ret = c.OMS.find_related_frames_of_reference(obj_id, [output_type])
                    return ret[output_type]
                return finder_fun
            
            return freeze()


        #resource_ids = self._low_level_init()


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


        ###############################################
        #
        # sites
        #
        ###############################################

        log.info("Create an observatory")
        observatory_id = self.generic_fcruf_script(RT.Observatory, 
                                          "observatory", 
                                          self.client.OMS, 
                                          True)

        log.info("Create a subsite")
        subsite_id = self.generic_fcruf_script(RT.Subsite,
                                            "subsite",
                                            self.client.OMS,
                                            True)

        log.info("Create a platform site")
        platform_site_id = self.generic_fcruf_script(RT.PlatformSite,
                                                     "platform_site",
                                                     self.client.OMS,
                                                     True)
        
        log.info("Create instrument site")
        instrument_site_id = self.generic_fcruf_script(RT.InstrumentSite,
                                                       "instrument_site",
                                                       self.client.OMS,
                                                       True)
        
        ###############################################
        #
        # models
        #
        ###############################################

        log.info("Create a platform model")
        platform_model_id = self.generic_fcruf_script(RT.PlatformModel, 
                                                     "platform_model", 
                                                     self.client.IMS, 
                                                     True)

        log.info("Create instrument model")
        instrument_model_id = self.generic_fcruf_script(RT.InstrumentModel, 
                                                        "instrument_model", 
                                                        self.client.IMS, 
                                                        True)

        log.info("Create sensor model")
        sensor_model_id = self.generic_fcruf_script(RT.SensorModel, 
                                                        "sensor_model", 
                                                        self.client.IMS, 
                                                        True)


        ###############################################
        #
        # agents
        #
        ###############################################

        log.info("Create platform agent")
        platform_agent_id = self.generic_fcruf_script(RT.PlatformAgent, 
                                                      "platform_agent", 
                                                      self.client.IMS, 
                                                      False)
        
        log.info("Create instrument agent")
        instrument_agent_id = self.generic_fcruf_script(RT.InstrumentAgent, 
                                                        "instrument_agent", 
                                                        self.client.IMS, 
                                                        False)


        ###############################################
        #
        # devices
        #
        ###############################################

        log.info("Create a platform device")
        platform_device_id = self.generic_fcruf_script(RT.PlatformDevice, 
                                                    "platform_device", 
                                                    self.client.IMS, 
                                                    False)
        log.info("Create an instrument device")
        instrument_device_id = self.generic_fcruf_script(RT.InstrumentDevice, 
                                                         "instrument_device", 
                                                         self.client.IMS, 
                                                         False)

        log.info("Create a sensor device")
        sensor_device_id = self.generic_fcruf_script(RT.SensorDevice, 
                                                         "sensor_device", 
                                                         self.client.IMS, 
                                                         False)




        ###############################################
        #
        # instances
        #
        ###############################################




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
        self.generic_association_script(c.OMS.assign_site_to_site,
                                        gen_find_oms_association(RT.Observatory),
                                        gen_find_oms_association(RT.Subsite),
                                        observatory_id,
                                        subsite_id)

        log.info("Associate platform site with subsite")
        self.generic_association_script(c.OMS.assign_site_to_site,
                                        gen_find_oms_association(RT.Subsite),
                                        gen_find_oms_association(RT.PlatformSite),
                                        subsite_id,
                                        platform_site_id)

        log.info("Associate instrument site with platform site")
        self.generic_association_script(c.OMS.assign_site_to_site,
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
        self.generic_association_script(c.OMS.assign_platform_model_to_platform_site,
                                        platform_site_impl.find_having_model,
                                        platform_site_impl.find_stemming_model,
                                        platform_site_id,
                                        platform_model_id)

        log.info("Associate instrument model with instrument site")
        self.generic_association_script(c.OMS.assign_instrument_model_to_instrument_site,
                                        instrument_site_impl.find_having_model,
                                        instrument_site_impl.find_stemming_model,
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
        
        self.generic_lcs_pass(self.client.IMS, "platform_agent", platform_agent_id, LCE.PLAN, LCS.PLANNED)
        self.generic_lcs_fail(self.client.IMS, "platform_agent", platform_agent_id, LCE.DEVELOP)
        log.info("Associate platform model with platform agent")
        self.generic_association_script(c.IMS.assign_platform_model_to_platform_agent,
                                        platform_agent_impl.find_having_model,
                                        platform_agent_impl.find_stemming_model,
                                        platform_agent_id,
                                        platform_model_id)
        self.generic_lcs_pass(self.client.IMS, "platform_agent", platform_agent_id, LCE.DEVELOP, LCS.DEVELOPED)
        self.generic_lcs_fail(self.client.IMS, "platform_agent", platform_agent_id, LCE.INTEGRATE)
        add_keyworded_attachment(self.client.RR, platform_agent_id, [KeywordFlag.EGG_URL])
        self.generic_lcs_pass(self.client.IMS, "platform_agent", platform_agent_id, LCE.INTEGRATE, LCS.INTEGRATED)
        self.generic_lcs_fail(self.client.IMS, "platform_agent", platform_agent_id, LCE.DEPLOY)
        add_keyworded_attachment(self.client.RR, platform_agent_id, [KeywordFlag.CERTIFICATION, "platform attachment"])
        self.generic_lcs_pass(self.client.IMS, "platform_agent", platform_agent_id, LCE.DEPLOY, LCS.DEPLOYED)


        self.generic_lcs_pass(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.PLAN, LCS.PLANNED)
        self.generic_lcs_fail(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.DEVELOP)
        log.info("Associate instrument model with instrument agent")
        self.generic_association_script(c.IMS.assign_instrument_model_to_instrument_agent,
                                        c.IMS.find_instrument_agent_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_agent,
                                        instrument_agent_id,
                                        instrument_model_id)
        self.generic_lcs_pass(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.DEVELOP, LCS.DEVELOPED)
        self.generic_lcs_fail(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.INTEGRATE)
        add_keyworded_attachment(self.client.RR, instrument_agent_id, [KeywordFlag.EGG_URL])
        self.generic_lcs_pass(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.INTEGRATE, LCS.INTEGRATED)
        self.generic_lcs_fail(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.DEPLOY)
        add_keyworded_attachment(self.client.RR, instrument_agent_id, [KeywordFlag.CERTIFICATION])
        self.generic_lcs_pass(self.client.IMS, "instrument_agent", instrument_agent_id, LCE.DEPLOY, LCS.DEPLOYED)


        #----------------------------------------------
        #
        # devices
        #
        #----------------------------------------------

        self.generic_lcs_pass(self.client.IMS, "platform_device", platform_device_id, LCE.PLAN, LCS.PLANNED)
        self.generic_lcs_fail(self.client.IMS, "platform_device", platform_device_id, LCE.INTEGRATE)
        log.info("Associate platform model with platform device")
        self.generic_association_script(c.IMS.assign_platform_model_to_platform_device,
                                        c.IMS.find_platform_device_by_platform_model,
                                        c.IMS.find_platform_model_by_platform_device,
                                        platform_device_id,
                                        platform_model_id)
        self.generic_lcs_fail(self.client.IMS, "platform_device", platform_device_id, LCE.INTEGRATE)


        log.info("Associate instrument model with instrument device")
        self.generic_association_script(c.IMS.assign_instrument_model_to_instrument_device,
                                        c.IMS.find_instrument_device_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_device,
                                        instrument_device_id,
                                        instrument_model_id)


        log.info("Associate instrument device with platform device")
        self.generic_association_script(c.IMS.assign_instrument_device_to_platform_device,
                                        c.IMS.find_platform_device_by_instrument_device,
                                        c.IMS.find_instrument_device_by_platform_device,
                                        platform_device_id,
                                        instrument_device_id)


        log.info("Associate sensor model with sensor device")
        self.generic_association_script(c.IMS.assign_sensor_model_to_sensor_device,
                                        sensor_device_impl.find_having_model,
                                        sensor_device_impl.find_stemming_model,
                                        sensor_device_id,
                                        sensor_model_id)



        log.info("Associate sensor device with instrument device")
        self.generic_association_script(c.IMS.assign_sensor_device_to_instrument_device,
                                        instrument_device_impl.find_having_device,
                                        instrument_device_impl.find_stemming_device,
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

        #set up stream (this would be preload)
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = c.PSMS.create_stream_definition(container=ctd_stream_def)

        #create data products for instrument data
        inst_data_product_id = c.DPMS.create_data_product(any_old(RT.DataProduct), ctd_stream_def_id)
        log_data_product_id = c.DPMS.create_data_product(any_old(RT.DataProduct), ctd_stream_def_id)

        #assign data products appropriately
        c.DAMS.assign_data_product(input_resource_id=instrument_device_id,
                                   data_product_id=inst_data_product_id,
                                   create_stream=True)
        c.OMS.create_site_data_product(instrument_site_id, log_data_product_id)

        deployment_id = self.generic_fcruf_script(RT.Deployment, "deployment", c.OMS, False)

        c.OMS.deploy_instrument_site(instrument_site_id, deployment_id)
        c.IMS.deploy_instrument_device(instrument_device_id, deployment_id)

        c.OMS.activate_deployment(deployment_id)


        #now along comes a new device
        log.info("Create instrument device 2")
        instrument_device_id2 = self.generic_fcruf_script(RT.InstrumentDevice,
                                                         "instrument_device",
                                                         self.client.IMS,
                                                         False)
        log.info("Associate instrument model with instrument device 2")
        self.generic_association_script(c.IMS.assign_instrument_model_to_instrument_device,
                                        c.IMS.find_instrument_device_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_device,
                                        instrument_device_id2,
                                        instrument_model_id)
        log.info("Associate instrument device with platform device 2")
        self.generic_association_script(c.IMS.assign_instrument_device_to_platform_device,
                                    c.IMS.find_platform_device_by_instrument_device,
                                    c.IMS.find_instrument_device_by_platform_device,
                                    platform_device_id,
                                    instrument_device_id2)
        inst_data_product_id2 = c.DPMS.create_data_product(any_old(RT.DataProduct), ctd_stream_def_id)
        c.DAMS.assign_data_product(input_resource_id=instrument_device_id2,
                                   data_product_id=inst_data_product_id2,
                                   create_stream=True)

        c.OMS.transfer_site_subscription(deployment_id, instrument_site_id, instrument_device_id2)

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












    #############################
    #
    # HELPER STUFF
    #
    #############################


    def generic_lcs_fail(self, 
                         owner_service, 
                         resource_label, 
                         resource_id, 
                         lc_event):
        """
        execute an lcs event and verify that it fails

        @param owner_service instance of service client that will handle the request
        @param resource_label string like "instrument_device"
        @param resource_id string
        @param lc_event string like LCE.INTEGRATE
        """

        lcsmethod = getattr(owner_service, "execute_%s_lifecycle" % resource_label)
        
        self.assertRaises(BadRequest, lcsmethod, resource_id, lc_event)
        
        
    def generic_lcs_pass(self, 
                         owner_service, 
                         resource_label, 
                         resource_id, 
                         lc_event, 
                         lc_state):
        """
        execute an lcs event and verify that it passes and affects state

        @param owner_service instance of service client that will handle the request
        @param resource_label string like "instrument_device"
        @param resource_id string
        @param lc_event string like LCE.INTEGRATE
        @param lc_state string like LCS.INTEGRATED (where the state should end up
        """

        lcsmethod  = getattr(owner_service, "execute_%s_lifecycle" % resource_label)
        readmethod = getattr(owner_service, "read_%s" % resource_label)
        
        lcsmethod(resource_id, lc_event)
        resource_obj = readmethod(resource_id)
        
        parts = get_maturity_visibility(resource_obj.lcstate)

        self.assertEqual(lc_state, parts[0])
                      



    def generic_association_script(self,
                                   assign_obj_to_subj_fn,
                                   find_subj_fn,
                                   find_obj_fn,
                                   subj_id,
                                   obj_id):
        """
        create an association and test that it went properly

        @param assign_obj_to_subj_fn the service method that takes (obj, subj) and associates them
        @param find_subj_fn the service method that returns a list of subjects given an object
        @param find_obj_fn the service method that returns a list of objects given a subject
        @param subj_id the subject id to associate
        @param obj_id the object id to associate
        """
        initial_subj_count = len(find_subj_fn(obj_id))
        initial_obj_count  = len(find_obj_fn(subj_id))
        
        log.debug("Creating association")
        if not ("str" == type(subj_id).__name__ == type(obj_id).__name__):
            raise NotImplementedError("%s='%s' to %s='%s'" % 
                                      (type(subj_id), str(subj_id), type(obj_id), str(obj_id)))
        if not (subj_id and obj_id):
            raise NotImplementedError("%s='%s' to %s='%s'" % 
                                      (type(subj_id), str(subj_id), type(obj_id), str(obj_id)))
        assign_obj_to_subj_fn(obj_id, subj_id)

        log.debug("Verifying find-subj-by-obj")
        subjects = find_subj_fn(obj_id)
        self.assertEqual(initial_subj_count + 1, len(subjects))
        subject_ids = []
        for x in subjects:
            if not "_id" in x:
                raise Inconsistent("'_id' field not found in resource! got: %s" % str(x))
            subject_ids.append(x._id)
        self.assertIn(subj_id, subject_ids)

        log.debug("Verifying find-obj-by-subj")
        objects = find_obj_fn(subj_id)
        self.assertEqual(initial_obj_count + 1, len(objects))
        object_ids = []
        for x in objects:
            if not "_id" in x:
                raise Inconsistent("'_id' field not found in resource! got: %s" % str(x))
            object_ids.append(x._id)
        self.assertIn(obj_id, object_ids)



    def generic_d_script(self, resource_id, resource_label, owner_service):
        """
        delete a resource and check that it was properly deleted

        @param resource_id id to be deleted
        @param resource_label something like platform_model
        @param owner_service service client instance
        """

        del_op = getattr(owner_service, "delete_%s" % resource_label)
        
        del_op(resource_id)

        # try again to make sure that we get NotFound
        self.assertRaises(NotFound, del_op, resource_id)


    def generic_fcruf_script(self, resource_iontype, resource_label, owner_service, is_simple):
        """
        run through find, create, read, update, and find ops on a basic resource

        NO DELETE in here.

        @param resource_iontype something like RT.BlahBlar
        @param resource_label something like platform_model
        @param owner_service a service client instance
        @param is_simple whether to check for AVAILABLE LCS on create
        """

        # this section is just to make the LCA integration script easier to write.
        #
        # each resource type gets put through (essentially) the same steps.
        #
        # so, we just set up a generic service-esque object.
        # (basically just a nice package of shortcuts):
        #  create a fake service object and populate it with the methods we need

        some_service = DotDict()

        def fill(svc, method):
            """
            make a "shortcut service" for testing crud ops.  
            @param svc a dotdict 
            @param method the method name to add
            """

            realmethod = "%s_widget" % method
                
            setattr(svc, realmethod,  
                    getattr(owner_service, "%s_%s" % (method, resource_label)))


        
        fill(some_service, "create")
        fill(some_service, "read")
        fill(some_service, "update")
        fill(some_service, "delete")

        def find_widgets():
            ret, _ = self.client.RR.find_resources(resource_iontype, None, None, False)
            return ret

        #UX team: generic script for LCA resource operations begins here.
        # some_service will be replaced with whatever service you're calling
        # widget will be replaced with whatever resource you're working with
        # resource_label will be data_product or logical_instrument



        log.info("Finding %s objects" % resource_label)
        num_objs = len(find_widgets())
        log.info("I found %d %s objects" % (num_objs, resource_label))

        log.info("Creating a %s" % resource_label)
        generic_obj = any_old(resource_iontype)
        generic_id = some_service.create_widget(generic_obj)
        self.assertIsNotNone(generic_id, "%s failed its creation" % resource_iontype)

        log.info("Reading %s #%s" % (resource_label, generic_id))
        generic_ret = some_service.read_widget(generic_id)

        log.info("Verifying equality of stored and retrieved object")
        self.assertEqual(generic_obj.name, generic_ret.name)
        self.assertEqual(generic_obj.description, generic_ret.description)


        #"simple" resources go available immediately upon creation, so check:
        if is_simple:
            log.info("Verifying that resource went DEPLOYED_AVAILABLE on creation")
            self.assertEqual(generic_ret.lcstate, LCS.DEPLOYED_AVAILABLE)

        log.info("Updating %s #%s" % (resource_label, generic_id))
        generic_newname = "%s updated" % generic_ret.name
        generic_ret.name = generic_newname
        some_service.update_widget(generic_ret)

        log.info("Reading platform model #%s to verify update" % generic_id)
        generic_ret = some_service.read_widget(generic_id)

        self.assertEqual(generic_newname, generic_ret.name)
        self.assertEqual(generic_obj.description, generic_ret.description)

        log.info("Finding platform models... checking that there's a new one")
        num_objs2 = len(find_widgets())

        self.assertTrue(num_objs2 > num_objs)

        return generic_id
