from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.sa.product.data_product_management_service import DataProductManagementService



from pyon.util.context import LocalContextMixin
from nose.plugins.attrib import attr
import unittest

from ion.services.sa.resource_impl.data_product_impl import DataProductImpl

from ion.services.sa.resource_impl.resource_impl_metatest_integration import ResourceImplMetatestIntegration



class FakeProcess(LocalContextMixin):
    name = ''


@attr('META', group='sa')
#@unittest.skip('not working')
class TestDataProductManagementServiceMeta(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2sa.yml')

        print 'started services'


 
#dynamically add tests to the test classes. THIS MUST HAPPEN OUTSIDE THE CLASS


rimi = ResourceImplMetatestIntegration(TestDataProductManagementServiceMeta, DataProductManagementService, log)
rimi.add_resource_impl_inttests(DataProductImpl)
