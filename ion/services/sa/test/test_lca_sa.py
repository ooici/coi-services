#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, IonObject
from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.imarine_facility_management_service import MarineFacilityManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED, LCS
from nose.plugins.attrib import attr


class FakeProcess(LocalContextMixin):
    name = ''


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

    def any_old(self, resource_type):
        """
        Create a generic and unique object of a given type
        @param resource_type the resource type
        """
        if resource_type not in self.ionobj_count:
            self.ionobj_count[resource_type] = 0

        self.ionobj_count[resource_type] = self.ionobj_count[resource_type] + 1

        name = "%s %d" % (resource_type, self.ionobj_count[resource_type])
        desc = "My %s #%d" % (resource_type, self.ionobj_count[resource_type])
        log.debug("Creating any old %s IonObject (#%d" % (resource_type, self.ionobj_count[resource_type]))

        return IonObject(resource_type, name=name, description=desc)
    
        
        
    def test_just_the_setup(self):
        return

    def test_jg_slides(self):
        self.jg_slide1()
        self.jg_slide3()

    def jg_slide1(self):
        c = self.client

        log.info("Finding marine facilities")
        num_facilities = len(c.MFMS.find_marine_facilities()["marine_facility_list"])
        log.info("I found %d marine facilities" % num_facilities)

        log.info("Creating a facility")
        mf_obj = self.any_old(RT.MarineFacility)
        mf_id = c.MFMS.create_marine_facility(mf_obj)["marine_facility_id"]

        log.info("Reading facility #%s" % mf_id)
        mf_ret = c.MFMS.read_marine_facility(mf_id)["marine_facility"]
        
        self.assertEqual(mf_obj.name, mf_ret.name)
        self.assertEqual(mf_obj.description, mf_ret.description)

        log.info("Updating facility #%s" % mf_id)
        mf_newname = "%s updated" % mf_ret.name
        mf_ret.name = mf_newname
        c.MFMS.update_marine_facility(mf_ret)

        log.info("Reading facility #%s to verify update" % mf_id)
        mf_ret = c.MFMS.read_marine_facility(mf_id)["marine_facility"]
        
        self.assertEqual(mf_newname, mf_ret.name)
        self.assertEqual(mf_obj.description, mf_ret.description)

        log.info("Finding marine facilities... checking that there's a new one")
        num_facilities2 = len(c.MFMS.find_marine_facilities()["marine_facility_list"])

        self.assertTrue(num_facilities2 > num_facilities)
        
        
    def jg_slide3(self):
        c = self.client

        log.info("Creating a site")
        site_obj = self.any_old(RT.Site)
        site_id = c.MFMS.create_site(site_obj)["site_id"]
        site_id #fixme, remove this 

