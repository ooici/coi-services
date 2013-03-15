from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.icontainer_agent import ContainerAgentClient

#from pyon.ion.endpoint import ProcessRPCClient
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.services.sa.instrument.agent_configuration_builder import PlatformAgentConfigurationBuilder, InstrumentAgentConfigurationBuilder
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.core.exception import BadRequest

from pyon.datastore.datastore import DataStore
from pyon.public import Container, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from ion.util.parameter_yaml_IO import get_param_dict
from ion.services.dm.utility.granule_utils import time_series_domain


from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.objects import ComputedValueAvailability, ProcessDefinition, ProcessStateEnum, StatusType, StreamConfiguration
from interface.objects import ComputedIntValue, ComputedFloatValue, ComputedStringValue

from pyon.public import RT, PRED, CFG
from nose.plugins.attrib import attr
from ooi.logging import log
import unittest

import gevent

from ion.services.sa.test.helpers import any_old


@attr('FAKE_UX', group='sa')
class TestFakeUXLaunch(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2demo.yml')
        self.RR   = ResourceRegistryServiceClient(node=self.container.node)
        self.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.IDS  = IdentityManagementServiceClient(node=self.container.node)
        self.PSC  = PubsubManagementServiceClient(node=self.container.node)
        self.DP   = DataProductManagementServiceClient(node=self.container.node)
        self.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.DSC  = DatasetManagementServiceClient(node=self.container.node)
        self.PDC  = ProcessDispatcherServiceClient(node=self.container.node)

        self.RR2 = EnhancedResourceRegistryClient(self.RR)

        print 'started services'

#    @unittest.skip('this test just for debugging setup')
#    def test_just_the_setup(self):
#        return

    def find_object_by_name(self, name, resource_type):
        objects,_ = self.RR.find_resources(resource_type, id_only=False)
        self.assertGreaterEqual(len(objects), 1)

        filtered_objs = [obj for obj in objects if obj.name == name]
        if not len(filtered_objs):
            print "Got these %s objects:" % resource_type
            for obj in objects:
                print "\t'%s' (%s)" % (obj.name, obj._id)
        self.assertEquals(1, len(filtered_objs))

        return filtered_objs[0]

    def test_platform_agent_launch(self):
        """
        Verify that agent configurations are being built properly
        """

        platform_device = self.find_object_by_name("Beta Demonstration Platform Device",
                                                              RT.PlatformDevice)

        platform_agent_instance_id = self.RR2.find_platform_agent_instance_id_of_platform_device(platform_device._id)

        self.IMS.start_platform_agent_instance(platform_agent_instance_id)

        #self.fail(parent_config)
        #plauncher.prepare(will_launch=False)
