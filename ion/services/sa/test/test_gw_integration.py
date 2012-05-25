#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
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
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient

import requests, json

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
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.client = DotDict()
        #self.client.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        #self.client.DPMS = DataProductManagementServiceClient(node=self.container.node)
        self.client.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.client.OMS  = ObservatoryManagementServiceClient(node=self.container.node)

    @unittest.skip('service GW gives AttributeError(hostname)')
    def test_just_the_setup(self):
        return

    @unittest.skip('example test')
    def test_gw_example(self):
        gw_ims_create_instrument_fn = self.funkify(self.client.IMS, "create_instrument_device")
        
        gw_ims_create_instrument_fn(self.any_old(RT.InstrumentDevice))




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
        # resource_label will be data_product or instrument_site


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
        """
        service is an instance of a service, method is a string
        """
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

        url = "http://%s:%s/ion-service/%s/%s" % (CFG.container.service_gateway.web_server.hostname, 
                                                  CFG.container.service_gateway.web_server.port, 
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

        name = "%s_%d" % (resource_type, _obj_count[resource_type])
        desc = "My %s #%d" % (resource_type, _obj_count[resource_type])
        log.debug("Creating any old %s object (#%d)" % (resource_type, _obj_count[resource_type]))

        ret = extra_fields
        ret["name"] = name
        ret["description"] = desc

        return [resource_type, ret]


