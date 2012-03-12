#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.marine_facility.marine_facility_management_service import MarineFacilityManagementService
from interface.services.sa.imarine_facility_management_service import IMarineFacilityManagementService, MarineFacilityManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict, Inconsistent
from pyon.public import RT, PRED
#from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log

from ion.services.sa.test.helpers import any_old



class FakeProcess(LocalContextMixin):
    name = ''

 
@attr('INT', group='sa')
class TestMarineFacilityManagementServiceIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2sa.yml')
        self.RR = ResourceRegistryServiceClient(node=self.container.node)
        self.client = MarineFacilityManagementServiceClient(node=self.container.node)
        #print 'TestMarineFacilityManagementServiceIntegration: started services'

    @unittest.skip('temporarily')
    def test_just_the_setup(self):
        return


    def test_resources_associations(self):
        """
        create one of each resource and association used by MFMS
        to guard against problems in ion-definitions
        """

        #raise unittest.SkipTest("https://jira.oceanobservatories.org/tasks/browse/CISWCORE-41")
        
        #stuff we control
        logical_instrument_id, _ = self.RR.create(any_old(RT.LogicalInstrument))
        logical_platform_id, _   = self.RR.create(any_old(RT.LogicalPlatform))
        logical_platform2_id, _  = self.RR.create(any_old(RT.LogicalPlatform))
        marine_facility_id, _    = self.RR.create(any_old(RT.MarineFacility))
        site_id, _               = self.RR.create(any_old(RT.Site))
        site2_id, _               = self.RR.create(any_old(RT.Site))

        #stuff we associate to
        instrument_agent_id, _ =           self.RR.create(any_old(RT.InstrumentAgent))
        platform_agent_id, _ =             self.RR.create(any_old(RT.PlatformAgent))


        #logical_instrument
        self.RR.create_association(logical_instrument_id, PRED.hasAgent, instrument_agent_id)

        #logical_platform
        self.RR.create_association(logical_platform_id, PRED.hasPlatform, logical_platform2_id)
        self.RR.create_association(logical_platform_id, PRED.hasInstrument, logical_instrument_id)
        self.RR.create_association(logical_platform_id, PRED.hasAgent, platform_agent_id)

        #marine_facility
        self.RR.create_association(marine_facility_id, PRED.hasSite, site_id)
        self.RR.create_association(marine_facility_id, PRED.hasPlatform, logical_platform_id)

        #site
        self.RR.create_association(site_id, PRED.hasSite, site2_id)
        
    def test_create_marine_facility(self):
        marine_facility_obj = IonObject(RT.MarineFacility,
                                        name='TestFacility',
                                        description='some new mf')
        self.client.create_marine_facility(marine_facility_obj)

    def test_find_marine_facility_org(self):
        marine_facility_obj = IonObject(RT.MarineFacility,
                                        name='TestFacility',
                                        description='some new mf')
        marine_facility_id = self.client.create_marine_facility(marine_facility_obj)
        org_id = self.client.find_marine_facility_org(marine_facility_id)
        print("org_id=<" + org_id + ">")

        #create a site with parent MF
        site_obj =  IonObject(RT.Site,
                                name= 'TestSite',
                                description = 'sample site')
        site_id = self.client.create_site(site_obj, marine_facility_id)
        self.assertIsNotNone(site_id, "Site not created.")


        # verify that Site is linked to MF
        mf_site_assoc = self.RR.get_association(marine_facility_id, PRED.hasSite, site_id)
        self.assertIsNotNone(mf_site_assoc, "Site not connected to MarineFacility.")


        # add the Site as a resource of this MF
        self.client.assign_resource_to_marine_facility(resource_id=site_id, marine_facility_id=marine_facility_id)
        # verify that Site is linked to Org
        org_site_assoc = self.RR.get_association(org_id, PRED.hasResource, site_id)
        self.assertIsNotNone(org_site_assoc, "Site not connected as resource to Org.")



        #create a logical platform with parent Site
        logical_platform_obj =  IonObject(RT.LogicalPlatform,
                                name= 'TestLogicalPlatform',
                                description = 'sample logical platform')
        logical_platform_id = self.client.create_logical_platform(logical_platform_obj, site_id)
        self.assertIsNotNone(logical_platform_id, "LogicalPlatform not created.")


        # verify that LogicalPlatform is linked to Site
        site_lp_assoc = self.RR.get_association(site_id, PRED.hasPlatform, logical_platform_id)
        self.assertIsNotNone(site_lp_assoc, "LogicalPlatform not connected to Site.")


        # add the LogicalPlatform as a resource of this MF
        self.client.assign_resource_to_marine_facility(resource_id=logical_platform_id, marine_facility_id=marine_facility_id)
        # verify that LogicalPlatform is linked to Org
        org_lp_assoc = self.RR.get_association(org_id, PRED.hasResource, logical_platform_id)
        self.assertIsNotNone(org_lp_assoc, "LogicalPlatform not connected as resource to Org.")



        #create a logical instrument with parent logical platform
        logical_instrument_obj =  IonObject(RT.LogicalInstrument,
                                name= 'TestLogicalInstrument',
                                description = 'sample logical instrument')
        logical_instrument_id = self.client.create_logical_instrument(logical_instrument_obj, logical_platform_id)
        self.assertIsNotNone(logical_instrument_id, "LogicalInstrument not created.")


        # verify that LogicalInstrument is linked to LogicalPlatform
        li_lp_assoc = self.RR.get_association(logical_platform_id, PRED.hasInstrument, logical_instrument_id)
        self.assertIsNotNone(li_lp_assoc, "LogicalInstrument not connected to LogicalPlatform.")


        # add the LogicalInstrument as a resource of this MF
        self.client.assign_resource_to_marine_facility(resource_id=logical_instrument_id, marine_facility_id=marine_facility_id)
        # verify that LogicalInstrument is linked to Org
        org_li_assoc = self.RR.get_association(org_id, PRED.hasResource, logical_instrument_id)
        self.assertIsNotNone(org_li_assoc, "LogicalInstrument not connected as resource to Org.")


        # remove the LogicalInstrument as a resource of this MF
        self.client.unassign_resource_from_marine_facility(logical_instrument_id, marine_facility_id)
        # verify that LogicalInstrument is linked to Org
        assocs,_ = self.RR.find_objects(org_id, PRED.hasResource, RT.LogicalInstrument, id_only=True )
        self.assertEqual(len(assocs), 0)

        # remove the LogicalInstrument
        self.client.delete_logical_instrument(logical_instrument_id)
        assocs, _ = self.RR.find_objects(logical_platform_id, PRED.hasInstrument, RT.LogicalInstrument, id_only=True )
        self.assertEqual(len(assocs), 0)


        # remove the LogicalPlatform as a resource of this MF
        self.client.unassign_resource_from_marine_facility(logical_platform_id, marine_facility_id)
        # verify that LogicalPlatform is linked to Org
        assocs,_ = self.RR.find_objects(org_id, PRED.hasResource, RT.LogicalPlatform, id_only=True )
        self.assertEqual(len(assocs), 0)

        # remove the LogicalPlatform
        self.client.delete_logical_platform(logical_platform_id)
        assocs, _ = self.RR.find_objects(site_id, PRED.hasPlatform, RT.LogicalPlatform, id_only=True )
        self.assertEqual(len(assocs), 0)



        # remove the Site as a resource of this MF
        self.client.unassign_resource_from_marine_facility(site_id, marine_facility_id)
        # verify that Site is linked to Org
        assocs,_ = self.RR.find_objects(org_id, PRED.hasResource, RT.Site, id_only=True )
        self.assertEqual(len(assocs), 0)

        # remove the Site
        self.client.delete_site(site_id)
        assocs, _ = self.RR.find_objects(marine_facility_id, PRED.hasSite, RT.Site, id_only=True )
        self.assertEqual(len(assocs), 0)
