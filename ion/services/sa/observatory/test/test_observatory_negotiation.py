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
from pyon.util.log import log

from ion.services.sa.test.helpers import any_old



class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
@unittest.skip('capabilities not yet available')
class TestObservatoryNegotiation(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)


    @unittest.skip("TDB")
    def test_request_resource(self):

        # L4-CI-SA-RQ-348 : Marine facility shall provide capabilities to define instrument use policies

        # L4-CI-SA-RQ-115 : Marine facility shall present resource requests to the marine infrastructure


        # create an observatory with resources including platforms with instruments

        # create an instrument use policy for one of the defined instruments

        # request access to the instrument that aligns with defined policy, verify that access is granted


        # request access to the instrument that is in conflict with defined policy, verify that access is NOT granted
        
        pass



    @unittest.skip("TBD")
    def test_request_config_change(self):

        # L4-CI-SA-RQ-342 : Marine facility shall present platform configuration change requests to the marine infrastructure

        # create an observatory with resources including platforms with instruments

        # request a configuration change to the platform t, verify that the request is submitted to the
        # Observatory operator and that then access is granted when that operator approves

        

        pass

  