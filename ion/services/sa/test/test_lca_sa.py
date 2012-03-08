#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, IonObject
#from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.imarine_facility_management_service import MarineFacilityManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS # , PRED
from nose.plugins.attrib import attr
import unittest

from ion.services.sa.test.helpers import any_old
from ion.services.sa.resource_impl.instrument_model_impl import InstrumentModelImpl

# some stuff for logging info to the console
import sys
log = DotDict()
printout = sys.stderr.write
printout = lambda x: None

log.debug = lambda x: printout("DEBUG: %s\n" % x)
log.info = lambda x: printout("INFO: %s\n" % x)
log.warn = lambda x: printout("WARNING: %s\n" % x)



@attr('INT', group='sa')
class TestLCASA(IonIntegrationTestCase):
    """
    LCA integration tests at the service level
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2sa.yml')

        # Now create client to DataProductManagementService
        self.client = DotDict()
        self.client.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.client.DPMS = DataProductManagementServiceClient(node=self.container.node)
        self.client.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.client.MFMS = MarineFacilityManagementServiceClient(node=self.container.node)
        self.client.PSMS = PubsubManagementServiceClient(node=self.container.node)

        self.client.RR   = ResourceRegistryServiceClient(node=self.container.node)

    #@unittest.skip('temporarily')
    def test_just_the_setup(self):
        return


    def _low_level_init(self):
        resource_ids = {}

        # some stream definitions
        resource_ids[RT.StreamDefinition] = {}

        # get module and function by name specified in strings
        sc_module = "prototype.sci_data.stream_defs"
        sc_method = "ctd_stream_definition"
        module = __import__(sc_module, fromlist=[sc_method])
        creator_func = getattr(module, sc_method)

        for n in ["SeabirdSim_raw", "SeabirdSim_parsed"]:
            container = creator_func()
            
            response = self.client.PSMS.create_stream_definition(container=container,
                                                                 name=n,
                                                                 description="inserted by test_lca_sa.py")
            resource_ids[RT.StreamDefinition][n] = response



        return resource_ids



    #@unittest.skip('temporarily')
    #@unittest.skip('Fixing data product creation')
    def test_lca_step_1_to_6(self):
        c = self.client

        c2 = DotDict()
        c2.resource_registry = self.client.RR
        inst_model_impl = InstrumentModelImpl(c2)
        
        def find_instrument_model_by_stream_definition(stream_definition_id):
            return inst_model_impl.find_having_stream_definition(stream_definition_id)

        def find_stream_definition_by_instrument_model(instrument_model_id):
            return inst_model_impl.find_stemming_stream_definition(instrument_model_id)

        
        resource_ids = self._low_level_init()

        log.info("LCA steps 1.3, 1.4, 1.5, 1.6, 1.7: FCRUF marine facility")
        marine_facility_id = self.generic_fcruf_script(RT.MarineFacility, 
                                          "marine_facility", 
                                          self.client.MFMS, 
                                          True)

        log.info("LCA steps 3.1, 3.2, 3.3, 3.4: FCRF site")
        site_id = self.generic_fcruf_script(RT.Site, 
                                            "site", 
                                            self.client.MFMS, 
                                            True)

        log.info("LCA <missing step>: associate site with marine facility")
        self.generic_association_script(c.MFMS.assign_site_to_marine_facility,
                                        c.MFMS.find_marine_facility_by_site,
                                        c.MFMS.find_site_by_marine_facility,
                                        marine_facility_id,
                                        site_id)

        

        log.info("LCA step 4.1, 4.2: FCU platform model")
        platform_model_id = self.generic_fcruf_script(RT.PlatformModel, 
                                                     "platform_model", 
                                                     self.client.IMS, 
                                                     True)

        log.info("LCA step 4.3, 4.4: CF logical platform")
        logical_platform_id = self.generic_fcruf_script(RT.LogicalPlatform, 
                                                    "logical_platform", 
                                                    self.client.MFMS, 
                                                    True)
        
        log.info("LCA step 4.5: C platform device")
        platform_device_id = self.generic_fcruf_script(RT.PlatformDevice, 
                                                    "platform_device", 
                                                    self.client.IMS, 
                                                    False)

        log.info("LCA step 4.6: Assign logical platform to site")
        self.generic_association_script(c.MFMS.assign_logical_platform_to_site,
                                        c.MFMS.find_site_by_logical_platform,
                                        c.MFMS.find_logical_platform_by_site,
                                        site_id,
                                        logical_platform_id)

        log.info("LCA <missing step>: assign_platform_model_to_platform_device")
        self.generic_association_script(c.IMS.assign_platform_model_to_platform_device,
                                        c.IMS.find_platform_device_by_platform_model,
                                        c.IMS.find_platform_model_by_platform_device,
                                        platform_device_id,
                                        platform_model_id)


        log.info("LCA <missing step>: assign_logical_platform_to_platform_device")
        self.generic_association_script(c.IMS.assign_logical_platform_to_platform_device,
                                        c.IMS.find_platform_device_by_logical_platform,
                                        c.IMS.find_logical_platform_by_platform_device,
                                        platform_device_id,
                                        logical_platform_id)


        log.info("LCA step 5.1, 5.2: FCU instrument model")
        instrument_model_id = self.generic_fcruf_script(RT.InstrumentModel, 
                                                       "instrument_model", 
                                                       self.client.IMS, 
                                                       True)

        log.info("LCA <missing step>: assign stream definitions to instrument model")
        for name, stream_definition_id in resource_ids[RT.StreamDefinition].iteritems():
            self.generic_association_script(c.IMS.assign_stream_definition_to_instrument_model,
                                            find_instrument_model_by_stream_definition,
                                            find_stream_definition_by_instrument_model,
                                            instrument_model_id,
                                            stream_definition_id)

        log.info("LCA step 5.3: CU logical instrument")
        logical_instrument_id = self.generic_fcruf_script(RT.LogicalInstrument, 
                                                    "logical_instrument", 
                                                    self.client.MFMS, 
                                                    True)


        log.info("Create a data product to be the 'logical' one")
        #TODO: do this automatically as part of logical instrument association with model?
        log_data_product_id = self.generic_fcruf_script(RT.DataProduct,
                                                        "data_product",
                                                        self.client.DPMS,
                                                        False)
        
        #### this is probably not how we'll end up establishing logical instruments
        # log.info("add data product to a logical instrument")
        # log.info("LCA <possible step>: find data products by logical instrument")
        # self.generic_association_script(c.MFMS.assign_data_product_to_logical_instrument,
        #                                 c.MFMS.find_logical_instrument_by_data_product,
        #                                 c.MFMS.find_data_product_by_logical_instrument,
        #                                 logical_instrument_id,
        #                                 log_data_product_id)



        log.info("Assigning logical instrument to logical platform")
        log.info("LCA step 5.4: list logical instrument by platform")
        self.generic_association_script(c.MFMS.assign_logical_instrument_to_logical_platform,
                                        c.MFMS.find_logical_platform_by_logical_instrument,
                                        c.MFMS.find_logical_instrument_by_logical_platform,
                                        logical_platform_id,
                                        logical_instrument_id)



        #THIS STEP IS IN THE WRONG PLACE...
        log.info("LCA step 5.5: list instruments by observatory")
        insts = c.MFMS.find_instrument_device_by_marine_facility(marine_facility_id)
        self.assertEqual(0, len(insts))
        #self.assertIn(instrument_device_id, insts)

        log.info("LCA step 5.6, 5.7, 5.9: CRU instrument_device")
        instrument_device_id = self.generic_fcruf_script(RT.InstrumentDevice, 
                                                    "instrument_device", 
                                                    self.client.IMS, 
                                                    False)


        log.info("LCA <missing step>: assign instrument model to instrument device")
        log.info("LCA <missing step>: create data products for instrument")
        self.generic_association_script(c.IMS.assign_instrument_model_to_instrument_device,
                                        c.IMS.find_instrument_device_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_device,
                                        instrument_device_id,
                                        instrument_model_id)



        log.info("LCA <missing step>: assign instrument device to platform device")
        self.generic_association_script(c.IMS.assign_instrument_device_to_platform_device,
                                        c.IMS.find_platform_device_by_instrument_device,
                                        c.IMS.find_instrument_device_by_platform_device,
                                        platform_device_id,
                                        instrument_device_id)

        #STEP 6.6 should really go here, otherwise there is no way to find instruments
        #         by a marine facility; only logical platforms are linked to sites
        log.info("LCA <6.6>: assign logical instrument to instrument device")

        # NOTE TO REVIEWERS
        #
        # We are not using the low-level association script right now.  
        #
        #self.generic_association_script(c.IMS.assign_logical_instrument_to_instrument_device,
        #                                c.IMS.find_instrument_device_by_logical_instrument,
        #                                c.IMS.find_logical_instrument_by_instrument_device,
        #                                instrument_device_id,
        #                                logical_instrument_id)
        #
        # Instead, we are using a more complete call that handles the data products
        # in addition to the instrument assignment.  Deciding what instrument data products
        # map to what logical instrument data products is currently a manual step.  If 
        # it ever becomes automatic, the following reassign_... function will become the
        # low-level portion of this concept.

        #first, we need the data product of the instrument
        inst_data_product_id = self.client.IMS.find_data_product_by_instrument_device(instrument_device_id)[0]

        #now GO!  2nd and 5th arguments are blank, because there is no prior instrument 
        c.IMS.reassign_logical_instrument_to_instrument_device(logical_instrument_id,
                                                               "",
                                                               instrument_device_id,
                                                               [log_data_product_id],
                                                               [],
                                                               [inst_data_product_id])
                                                               


        #THIS IS WHERE STEP 5.5 SHOULD BE
        log.info("LCA step 5.5: list instruments by observatory")
        insts = c.MFMS.find_instrument_device_by_marine_facility(marine_facility_id)
        self.assertIn(instrument_device_id, insts)


        log.info("LCA step 5.8: instrument device policy?")
        #TODO

        #todo: there is no default product created, need to remove asserts or add products based on instrument model first

        log.info("LCA step 5.10a: find data products by instrument device")
        products = self.client.IMS.find_data_product_by_instrument_device(instrument_device_id)
        #self.assertNotEqual(0, len(products))
        #data_product_id = products[0]

        log.info("LCA step 5.10b: find data products by platform")
        products = self.client.IMS.find_data_product_by_platform_device(platform_device_id)
        #self.assertIn(data_product_id, products)

        log.info("LCA step 5.10c: find data products by logical platform")
        products = self.client.MFMS.find_data_product_by_logical_platform(logical_platform_id)
        #self.assertIn(data_product_id, products)

        log.info("LCA step 5.10d: find data products by site")
        products = self.client.MFMS.find_data_product_by_site(site_id)
        #self.assertIn(data_product_id, products)

        log.info("LCA step 5.10e: find data products by marine facility")
        products = self.client.MFMS.find_data_product_by_marine_facility(marine_facility_id)
        #self.assertIn(data_product_id, products)



        log.info("LCA step 6.1, 6.2: FCU instrument agent")
        instrument_agent_id = self.generic_fcruf_script(RT.InstrumentAgent, 
                                                       "instrument_agent", 
                                                       self.client.IMS, 
                                                       False)
        
        log.info("LCA step <6.3, out of order>: associate instrument model to instrument agent")
        log.info("LCA step <6.4, out of order>: find instrument model by instrument agent")
        self.generic_association_script(c.IMS.assign_instrument_model_to_instrument_agent,
                                        c.IMS.find_instrument_agent_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_agent,
                                        instrument_agent_id,
                                        instrument_model_id)




        #  .-.  .               .    .  
        # (   )_|_            .'|  .'|  
        #  `-.  |  .-. .,-.     |    |  
        # (   ) | (.-' |   )    |    |  
        #  `-'  `-'`--'|`-'   '---''---'
        #              |                
        #  step 11

        #first, create an entire new instrument on this platform

        instrument_device_id2 = self.generic_fcruf_script(RT.InstrumentDevice, 
                                                          "instrument_device", 
                                                          self.client.IMS, 
                                                          False)
        self.generic_association_script(c.IMS.assign_instrument_model_to_instrument_device,
                                        c.IMS.find_instrument_device_by_instrument_model,
                                        c.IMS.find_instrument_model_by_instrument_device,
                                        instrument_device_id2,
                                        instrument_model_id)

        self.generic_association_script(c.IMS.assign_instrument_device_to_platform_device,
                                        c.IMS.find_platform_device_by_instrument_device,
                                        c.IMS.find_instrument_device_by_platform_device,
                                        platform_device_id,
                                        instrument_device_id2)
        
        #get the data product of the new instrument
        inst_data_product_id2 = self.client.IMS.find_data_product_by_instrument_device(instrument_device_id2)[0]

        #now GO!  2nd and 5th arguments are filled in with the old instrument
        c.IMS.reassign_logical_instrument_to_instrument_device(logical_instrument_id,
                                                               instrument_device_id,
                                                               instrument_device_id2,
                                                               [log_data_product_id],
                                                               [inst_data_product_id],
                                                               [inst_data_product_id2])







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
        assign_obj_to_subj_fn(obj_id, subj_id)

        log.debug("Verifying find-subj-by-obj")
        subjects = find_subj_fn(obj_id)
        self.assertEqual(initial_subj_count + 1, len(subjects))
        self.assertIn(subj_id, subjects)

        log.debug("Verifying find-obj-by-subj")
        objects = find_obj_fn(subj_id)
        self.assertEqual(initial_obj_count + 1, len(objects))
        self.assertIn(obj_id, objects)



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

        def make_plural(noun):
            if "y" == noun[-1]:
                return noun[:-1] + "ies"
            else:
                return noun + "s"
            
        
        def fill(svc, method, plural=False):
            """
            make a "shortcut service" for testing crud ops.  
            @param svc a dotdict 
            @param method the method name to add
            @param plural whether to make the resource label plural
            """

            reallabel = resource_label
            realmethod = "%s_widget" % method
            if plural:
                reallabel = make_plural(reallabel)
                realmethod = realmethod + "s"
                
            setattr(svc, realmethod,  
                    getattr(owner_service, "%s_%s" % (method, reallabel)))


        
        fill(some_service, "create")
        fill(some_service, "read")
        fill(some_service, "update")
        fill(some_service, "delete")
        fill(some_service, "find", True)



        #UX team: generic script for LCA resource operations begins here.
        # some_service will be replaced with whatever service you're calling
        # widget will be replaced with whatever resource you're working with
        # resource_label will be data_product or logical_instrument


        resource_labels = make_plural(resource_label)

        log.info("Finding %s" % resource_labels)
        num_objs = len(some_service.find_widgets())
        log.info("I found %d %s" % (num_objs, resource_labels))

        log.info("Creating a %s" % resource_label)
        generic_obj = any_old(resource_iontype)
        generic_id = some_service.create_widget(generic_obj)

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
        num_objs2 = len(some_service.find_widgets())

        self.assertTrue(num_objs2 > num_objs)

        return generic_id
