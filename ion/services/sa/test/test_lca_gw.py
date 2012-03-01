#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import  CFG
#from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from pyon.core.exception import BadRequest #, NotFound, Conflict
from pyon.public import RT, LCS # , PRED
from nose.plugins.attrib import attr
import unittest


from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.imarine_facility_management_service import MarineFacilityManagementServiceClient

import requests, json

from ion.services.sa.preload.preload_csv import PreloadCSV

# some stuff for logging info to the console
import sys
log = DotDict()
printout = sys.stderr.write
printout = lambda x: None

log.debug = lambda x: printout("DEBUG: %s\n" % x)
log.info = lambda x: printout("INFO: %s\n" % x)
log.warn = lambda x: printout("WARNING: %s\n" % x)


_obj_count = {}




@attr('INT', group='sa')
class TestLCAServiceGateway(IonIntegrationTestCase):
    """
    LCA integration tests at the service gateway level
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2sa.yml')

        # Now create client to DataProductManagementService
        self.client = DotDict()
        #self.client.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        #self.client.DPMS = DataProductManagementServiceClient(node=self.container.node)
        self.client.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.client.MFMS = MarineFacilityManagementServiceClient(node=self.container.node)

    #@unittest.skip('temporarily')
    def test_just_the_setup(self):
        return

    #@unittest.skip('temporarily')
    def test_csv_loader_all(self):
        loader = PreloadCSV(CFG.web_server.hostname, CFG.web_server.port)

        loader.preload(["ion/services/sa/preload/LogicalInstrument.csv",
                        "ion/services/sa/preload/InstrumentDevice.csv",
                        "ion/services/sa/preload/associations.csv"])

        log_inst_ids = self.client.MFMS.find_logical_instruments()
        self.assertEqual(2, len(log_inst_ids))

        inst_ids = self.client.IMS.find_instrument_devices()
        self.assertEqual(2, len(inst_ids))

        associated_ids = self.client.IMS.find_logical_instrument_by_instrument_device(inst_ids[0])
        self.assertEqual(1, len(associated_ids))


    #@unittest.skip('temporarily')
    def test_csv_loader_tagged(self):
        loader = PreloadCSV(CFG.web_server.hostname, CFG.web_server.port)

        loader.preload(["ion/services/sa/preload/LogicalInstrument.csv",
                        "ion/services/sa/preload/InstrumentDevice.csv",
                        "ion/services/sa/preload/associations.csv"],
                       "LCA")

        log_inst_ids = self.client.MFMS.find_logical_instruments()
        self.assertEqual(1, len(log_inst_ids))
        log_inst = self.client.MFMS.read_logical_instrument(logical_instrument_id=log_inst_ids[0])
        self.assertEqual(log_inst.name, "Logical Instrument 1")

        
        inst_ids = self.client.IMS.find_instrument_devices()
        self.assertEqual(1, len(inst_ids))
        inst = self.client.IMS.read_instrument_device(instrument_device_id=inst_ids[0])
        self.assertEqual(inst.name, "Instrument Device 1")

        associated_ids = self.client.IMS.find_logical_instrument_by_instrument_device(inst_ids[0])
        self.assertEqual(1, len(associated_ids))












    #@unittest.skip('temporarily')
    def test_lca_step_1_to_6(self):
        c = self.client

        log.info("LCA steps 1.3, 1.4, 1.5, 1.6, 1.7: FCRUF marine facility")
        marine_facility_id = self.generic_fcruf_script(RT.MarineFacility, 
                                          "marine_facility", 
                                          "marine_facility_management", 
                                          True)

        log.info("LCA steps 3.1, 3.2, 3.3, 3.4: FCRF site")
        site_id = self.generic_fcruf_script(RT.Site, 
                                            "site", 
                                            "marine_facility_management", 
                                            True)

        log.info("LCA <missing step>: associate site with marine facility")
        self.generic_association_script("marine_facility_management",
                                        "assign_site_to_marine_facility",
                                        "find_marine_facility_by_site",
                                        "find_site_by_marine_facility",
                                        "marine_facility_id",
                                        "site_id",
                                        marine_facility_id,
                                        site_id)

        

        log.info("LCA step 4.1, 4.2: FCU platform model")
        platform_model_id = self.generic_fcruf_script(RT.PlatformModel, 
                                                     "platform_model", 
                                                     "instrument_management", 
                                                     True)

        log.info("LCA step 4.3, 4.4: CF logical platform")
        logical_platform_id = self.generic_fcruf_script(RT.LogicalPlatform, 
                                                    "logical_platform", 
                                                    "marine_facility_management", 
                                                    True)
        
        log.info("LCA step 4.5: C platform device")
        platform_device_id = self.generic_fcruf_script(RT.PlatformDevice, 
                                                    "platform_device", 
                                                    "instrument_management", 
                                                    False)

        log.info("LCA step 4.6: Assign logical platform to site")
        self.generic_association_script("marine_facility_management",
                                        "assign_logical_platform_to_site",
                                        "find_site_by_logical_platform",
                                        "find_logical_platform_by_site",
                                        "site_id",
                                        "logical_platform_id",
                                        site_id,
                                        logical_platform_id)

        log.info("LCA <missing step>: assign_platform_model_to_platform_device")
        self.generic_association_script("instrument_management",
                                        "assign_platform_model_to_platform_device",
                                        "find_platform_device_by_platform_model",
                                        "find_platform_model_by_platform_device",
                                        "platform_device_id",
                                        "platform_model_id",
                                        platform_device_id,
                                        platform_model_id)


        log.info("LCA <missing step>: assign_logical_platform_to_platform_device")
        self.generic_association_script("instrument_management",
                                        "assign_logical_platform_to_platform_device",
                                        "find_platform_device_by_logical_platform",
                                        "find_logical_platform_by_platform_device",
                                        "platform_device_id",
                                        "logical_platform_id",
                                        platform_device_id,
                                        logical_platform_id)


        log.info("LCA step 5.1, 5.2: FCU instrument model")
        instrument_model_id = self.generic_fcruf_script(RT.InstrumentModel, 
                                                       "instrument_model", 
                                                       "instrument_management", 
                                                       True)

        log.info("LCA step 5.3: CU logical instrument")
        logical_instrument_id = self.generic_fcruf_script(RT.LogicalInstrument, 
                                                    "logical_instrument", 
                                                    "marine_facility_management", 
                                                    True)


        log.info("Create a data product for the logical instrument")
        #TODO: do this automatically as part of logical instrument association with model?
        log_data_product_id = self.generic_fcruf_script(RT.DataProduct,
                                                        "data_product",
                                                        "data_product_management",
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
        self.generic_association_script("marine_facility_management",
                                        "assign_logical_instrument_to_logical_platform",
                                        "find_logical_platform_by_logical_instrument",
                                        "find_logical_instrument_by_logical_platform",
                                        "logical_platform_id",
                                        "logical_instrument_id",
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
                                                    "instrument_management", 
                                                    False)

        log.info("LCA <missing step>: assign instrument device to platform device")
        self.generic_association_script("instrument_management",
                                        "assign_instrument_device_to_platform_device",
                                        "find_platform_device_by_instrument_device",
                                        "find_instrument_device_by_platform_device",
                                        "platform_device_id",
                                        "instrument_device_id",
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
                                                       "instrument_management", 
                                                       False)
        
        log.info("LCA step 6.3: associate instrument model to instrument agent")
        log.info("LCA step 6.4: find instrument model by instrument agent")
        self.generic_association_script("instrument_management",
                                        "assign_instrument_model_to_instrument_agent",
                                        "find_instrument_agent_by_instrument_model",
                                        "find_instrument_model_by_instrument_agent",
                                        "instrument_agent_id",
                                        "instrument_model_id",
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
                                                          "instrument_management", 
                                                          False)
        self.generic_association_script("instrument_management",
                                        "assign_instrument_device_to_platform_device",
                                        "find_platform_device_by_instrument_device",
                                        "find_instrument_device_by_platform_device",
                                        "platform_device_id",
                                        "instrument_device_id",
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
                                   owner_service,
                                   assign_obj_to_subj_method,
                                   find_subj_method,
                                   find_obj_method,
                                   subj_arg,
                                   obj_arg,
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
        assign_obj_to_subj_fn = self.funkify(owner_service, assign_obj_to_subj_method)
        find_subj_fn = self.funkify(owner_service, find_subj_method)
        find_obj_fn = self.funkify(owner_service, find_obj_method)

        kwargs = {obj_arg: obj_id}
        initial_subj_count = len(find_subj_fn(**kwargs))
        kwargs = {subj_arg: subj_id}
        initial_obj_count  = len(find_obj_fn(**kwargs))
        
        log.debug("Creating association")
        kwargs = {subj_arg: subj_id, obj_arg: obj_id}
        assign_obj_to_subj_fn(**kwargs)

        log.debug("Verifying find-subj-by-obj")
        kwargs = {obj_arg: obj_id}
        subjects = find_subj_fn(**kwargs)
        self.assertEqual(initial_subj_count + 1, len(subjects))
        self.assertIn(subj_id, subjects)

        log.debug("Verifying find-obj-by-subj")
        kwargs = {subj_arg: subj_id}
        objects = find_obj_fn(**kwargs)
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
                    self.funkify(owner_service, ("%s_%s" % (method, reallabel))))



        
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
        generic_obj = self.any_old(resource_iontype)
        kwargs = {resource_label: generic_obj}
        generic_id = some_service.create_widget(**kwargs)

        log.info("Reading %s #%s" % (resource_label, generic_id))
        kwargs = {("%s_id" % resource_label) : generic_id}
        generic_ret = some_service.read_widget(**kwargs)

        log.info("Verifying equality of stored and retrieved object")
        self.assertEqual(generic_obj[1]["name"], generic_ret["name"])
        self.assertEqual(generic_obj[1]["description"], generic_ret["description"])


        #"simple" resources go available immediately upon creation, so check:
        #if is_simple:
        #    log.info("Verifying that resource went DEPLOYED_AVAILABLE on creation")
        #    self.assertEqual(generic_ret.lcstate, LCS.DEPLOYED_AVAILABLE)

        log.info("Updating %s #%s" % (resource_label, generic_id))
        generic_newname = "%s updated" % generic_ret["name"]
        generic_ret["name"] = generic_newname
        kwargs = {resource_label: [resource_iontype, generic_ret]}
        some_service.update_widget(**kwargs)

        log.info("Reading platform model #%s to verify update" % generic_id)
        kwargs = {("%s_id" % resource_label) : generic_id}
        generic_ret = some_service.read_widget(**kwargs)

        self.assertEqual(generic_newname, generic_ret["name"])
        self.assertEqual(generic_obj[1]["description"], generic_ret["description"])

        log.info("Finding platform models... checking that there's a new one")
        num_objs2 = len(some_service.find_widgets())

        self.assertTrue(num_objs2 > num_objs)

        return generic_id


    # turn a gateway service call into a no-arg function
    def funkify(self, service, method):
        def wrap(**kwargs):
            return self._do_service_call(service, method, kwargs)

        return wrap


    # actually execute a service call with the given dict of params
    def _do_service_call(self, service, method, params):

        post_data = self._service_request_template()
        post_data['serviceRequest']['serviceName'] = service
        post_data['serviceRequest']['serviceOp'] = method
        for k, v in params.iteritems():
            post_data['serviceRequest']['params'][k] = v

        url = "http://%s:%s/ion-service/%s/%s" % (CFG.web_server.hostname, 
                                                  CFG.web_server.port, 
                                                  service, 
                                                  method)

        try:
            json.dumps(post_data)
        except:
            raise NotImplementedError("%s.%s: %s" % (service, method, str(params)))
        
        service_gateway_call = requests.post(
            url,
            data={'payload': json.dumps(post_data)}
            )

        if service_gateway_call.status_code != 200:
            raise BadRequest("The service gateway returned the following error: %d" % service_gateway_call.status_code)

        # debug lines
        #sys.stderr.write(str(url) + "\n")
        #sys.stderr.write(str(post_data['serviceRequest']) + "\n")

        resp = json.loads(service_gateway_call.content)

        #sys.stderr.write(str(resp) + "\n")

        if "GatewayResponse" not in resp["data"]:
            if "GatewayError" in resp["data"]:
                #raise BadRequest("%s: %s" % (resp["data"]["GatewayError"]["Exception"], 
                #                             resp["data"]["GatewayError"]["Message"]))
                raise BadRequest("%s: %s\nFROM\n%s.%s(%s)" % (resp["data"]["GatewayError"]["Exception"], 
                                                              resp["data"]["GatewayError"]["Message"],
                                                              service,
                                                              method,
                                                              str(params)))
            else:
                raise BadRequest("Unknown error object: %s" % str(resp))

        ret = resp["data"]["GatewayResponse"]

        return ret

    def _service_request_template(self):
        return {
            'serviceRequest': {
                'serviceName': '', 
                'serviceOp': '',
                'params': {
                    #'object': [] # Ex. [BankObject, {'name': '...'}] 
                    }
                }
            }


    
    def any_old(self, resource_type, extra_fields={}):
        """
        Create any old resource... a generic and unique dict of a given type
        @param resource_type the resource type
        @param extra_fields dict of any extra fields to set
        """
        if resource_type not in _obj_count:
            _obj_count[resource_type] = 0

        _obj_count[resource_type] = _obj_count[resource_type] + 1

        name = "%s %d" % (resource_type, _obj_count[resource_type])
        desc = "My %s #%d" % (resource_type, _obj_count[resource_type])
        log.debug("Creating any old %s object (#%d)" % (resource_type, _obj_count[resource_type]))

        ret = extra_fields
        ret["name"] = name
        ret["description"] = desc

        return [resource_type, ret]


