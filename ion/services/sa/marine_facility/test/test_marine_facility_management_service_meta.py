#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
#from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.marine_facility.marine_facility_management_service import MarineFacilityManagementService
#from interface.services.sa.imarine_facility_management_service import IMarineFacilityManagementService, MarineFacilityManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED, LCS
#from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log

from ion.services.sa.resource_impl.resource_impl_metatest_integration import ResourceImplMetatestIntegration

from ion.services.sa.resource_impl.logical_instrument_impl import LogicalInstrumentImpl
from ion.services.sa.resource_impl.logical_platform_impl import LogicalPlatformImpl
from ion.services.sa.resource_impl.marine_facility_impl import MarineFacilityImpl
from ion.services.sa.resource_impl.site_impl import SiteImpl




class FakeProcess(LocalContextMixin):
    name = ''


@attr('META', group='sa')
class TestMarineFacilityManagementServiceMeta(IonIntegrationTestCase):

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
        
        print 'started services'

    
    @unittest.skip('this test just for debugging setup')
    def test_just_the_setup(self):
        return




rimi = ResourceImplMetatestIntegration(TestMarineFacilityManagementServiceMeta, MarineFacilityManagementService, log)

rimi.add_resource_impl_inttests(LogicalInstrumentImpl, {})
rimi.add_resource_impl_inttests(LogicalPlatformImpl, {})
rimi.add_resource_impl_inttests(MarineFacilityImpl, {})
rimi.add_resource_impl_inttests(SiteImpl, {})


