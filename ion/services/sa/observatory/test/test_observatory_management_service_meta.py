#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
#from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.observatory.observatory_management_service import ObservatoryManagementService
#from interface.services.sa.iobservatory_management_service import IObservatoryManagementService, ObservatoryManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED, LCS
#from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log

from ion.services.sa.resource_impl.resource_impl_metatest_integration import ResourceImplMetatestIntegration

from ion.services.sa.observatory.instrument_site_impl import InstrumentSiteImpl
from ion.services.sa.observatory.platform_site_impl import PlatformSiteImpl
from ion.services.sa.observatory.observatory_impl import ObservatoryImpl
from ion.services.sa.observatory.subsite_impl import SubsiteImpl




class FakeProcess(LocalContextMixin):
    name = ''


@attr('META', group='sa')
class TestObservatoryManagementServiceMeta(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.RR = ResourceRegistryServiceClient(node=self.container.node)
        
        print 'started services'

    
    @unittest.skip('this test just for debugging setup')
    def test_just_the_setup(self):
        return




rimi = ResourceImplMetatestIntegration(TestObservatoryManagementServiceMeta, ObservatoryManagementService, log)

rimi.add_resource_impl_inttests(ObservatoryImpl, {})
rimi.add_resource_impl_inttests(SubsiteImpl, {})
rimi.add_resource_impl_inttests(PlatformSiteImpl, {})
rimi.add_resource_impl_inttests(InstrumentSiteImpl, {})


