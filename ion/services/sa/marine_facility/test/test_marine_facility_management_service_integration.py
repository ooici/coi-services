#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.marine_facility.marine_facility_management_service import MarineFacilityManagementService
from interface.services.sa.imarine_facility_management_service import IMarineFacilityManagementService, MarineFacilityManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
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


