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

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS # , PRED
from nose.plugins.attrib import attr

from ion.services.sa.test.helpers import any_old

class FakeProcess(LocalContextMixin):
    name = ''


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

        # number of ion objects per type
        self.ionobj_count = {}


    def test_just_the_setup(self):
        return

    def test_jg_slide1(self):
        self.generic_crud_script(RT.MarineFacility, "marine_facility", self.client.MFMS, True)


    def test_jg_slide3(self):
        self.generic_crud_script(RT.Site, "site", self.client.MFMS, True)



    def test_jg_slide4(self):
        c = self.client

        site_id = self.generic_crud_script(RT.Site, "site", self.client.MFMS, True)

        platform_model_id = self.generic_crud_script(RT.PlatformModel, 
                                                     "platform_model", 
                                                     self.client.IMS, 
                                                     True)

        logical_platform_id = self.generic_crud_script(RT.LogicalPlatform, 
                                                    "logical_platform", 
                                                    self.client.MFMS, 
                                                    True)

        log.info("Assigning logical platform to site")
        c.MFMS.assign_logical_platform_to_site(logical_platform_id, site_id)

        platform_device_id = self.generic_crud_script(RT.PlatformDevice, 
                                                    "platform_device", 
                                                    self.client.IMS, 
                                                    False)

        ("suppresss pyflakes errors:",
         site_id, 
         platform_model_id, 
         logical_platform_id, 
         platform_device_id,
         0)

    def test_jg_slide5ab(self):
        c = self.client

        site_id = self.generic_crud_script(RT.Site, "site", self.client.MFMS, True)

        logical_platform_id = self.generic_crud_script(RT.LogicalPlatform, 
                                                    "logical_platform", 
                                                    self.client.MFMS, 
                                                    True)

        instrument_model_id = self.generic_crud_script(RT.InstrumentModel, 
                                                       "instrument_model", 
                                                       self.client.IMS, 
                                                       True)

        logical_instrument_id = self.generic_crud_script(RT.LogicalInstrument, 
                                                    "logical_instrument", 
                                                    self.client.MFMS, 
                                                    True)

        log.info("Assigning logical instrument to logical platform")
        c.MFMS.assign_logical_instrument_to_logical_platform(logical_instrument_id, logical_platform_id)


        log.info("Part B")

        instrument_device_id = self.generic_crud_script(RT.InstrumentDevice, 
                                                    "instrument_device", 
                                                    self.client.IMS, 
                                                    False)

        #fixme: policy
        
        #fixme: find data products

        ("suppresss pyflakes errors:",
         site_id, 
         instrument_model_id, 
         logical_platform_id, 
         logical_instrument_id, 
         instrument_device_id,
         0)


    def test_jg_slide6(self):
        instrument_agent_id = self.generic_crud_script(RT.InstrumentAgent, 
                                                       "instrument_agent", 
                                                       self.client.IMS, 
                                                       True)        
        
        ("suppresss pyflakes errors:",
         instrument_agent_id, 
         0)


    def generic_crud_script(self, resource_iontype, resource_label, owner_service, is_simple):
        """
        run through crud ops on a basic resource

        @param resource_iontype something like RT.BlahBlar
        @param resource_label something like platform_model
        @param owner_service a service client instance
        @param is_simple whether to check for AVAILABLE LCS on create
        """

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
            @param plural whether to maek the resource label plural
            """

            reallabel = resource_label
            if plural:
                reallabel = make_plural(reallabel)
                
            setattr(svc, method,  
                    getattr(owner_service, "%s_%s" % (method, reallabel)))


        resource_labels = make_plural(resource_label)
        
        log.info("Finding %ss" % resource_labels)

        #create a fake service object and populate it with the methods we need
        # basically just a nice package of shortcuts
        svc = DotDict()

        fill(svc, "create")
        fill(svc, "read")
        fill(svc, "update")
        fill(svc, "delete")
        fill(svc, "find", True)


        num_objs = len(svc.find()["%s_list" % resource_label])
        log.info("I found %d %s" % (num_objs, resource_labels))

        log.info("Creating a %s" % resource_label)
        generic_obj = any_old(resource_iontype)
        generic_id = svc.create(generic_obj)["%s_id" % resource_label]

        log.info("Reading %s #%s" % (resource_label, generic_id))
        generic_ret = svc.read(generic_id)[resource_label]

        log.info("Verifying equality of stored and retrieved object")
        self.assertEqual(generic_obj.name, generic_ret.name)
        self.assertEqual(generic_obj.description, generic_ret.description)

        if is_simple:
            log.info("Verifying that resource went AVAILABLE on creation")
            self.assertEqual(generic_ret.lcstate, LCS.AVAILABLE)

        log.info("Updating %s #%s" % (resource_label, generic_id))
        generic_newname = "%s updated" % generic_ret.name
        generic_ret.name = generic_newname
        svc.update(generic_ret)

        log.info("Reading platform model #%s to verify update" % generic_id)
        generic_ret = svc.read(generic_id)[resource_label]

        self.assertEqual(generic_newname, generic_ret.name)
        self.assertEqual(generic_obj.description, generic_ret.description)

        log.info("Finding platform models... checking that there's a new one")
        num_objs2 = len(svc.find()["%s_list" % resource_label])

        self.assertTrue(num_objs2 > num_objs)

        return generic_id
