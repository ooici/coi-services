#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.observatory.observatory_management_service import ObservatoryManagementService
from interface.services.sa.iobservatory_management_service import IObservatoryManagementService, ObservatoryManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict, Inconsistent
from pyon.public import RT, PRED
#from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from ooi.logging import log

from ion.services.sa.test.helpers import any_old



class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
@unittest.skip('capabilities not yet available')
class TestPlatformManagement(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)



    @unittest.skip("TBD")
    def test_platform_resource_policies(self):

        # placeholder for demonstration of  L4-CI-SA-RQ-210 and L4-CI-SA-RQ-348

        #create a deployment with metadata and an initial site and device

        # create a use policy for both the platform and an instrument on the platform

        # request for use of the platform and instrument outside of the use policy and verify that access is denied


        pass
