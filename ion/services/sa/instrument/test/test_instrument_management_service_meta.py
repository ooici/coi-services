from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.instrument.instrument_management_service import InstrumentManagementService
from interface.services.sa.iinstrument_management_service import IInstrumentManagementService, InstrumentManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED, LCS
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log

from ion.services.sa.resource_impl.resource_impl_metatest_integration import ResourceImplMetatestIntegration

from ion.services.sa.resource_impl.instrument_agent_instance_impl import InstrumentAgentInstanceImpl
from ion.services.sa.resource_impl.instrument_agent_impl import InstrumentAgentImpl
from ion.services.sa.resource_impl.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.resource_impl.instrument_model_impl import InstrumentModelImpl
from ion.services.sa.resource_impl.platform_agent_instance_impl import PlatformAgentInstanceImpl
from ion.services.sa.resource_impl.platform_agent_impl import PlatformAgentImpl
from ion.services.sa.resource_impl.platform_device_impl import PlatformDeviceImpl
from ion.services.sa.resource_impl.platform_model_impl import PlatformModelImpl
from ion.services.sa.resource_impl.sensor_device_impl import SensorDeviceImpl
from ion.services.sa.resource_impl.sensor_model_impl import SensorModelImpl


class FakeProcess(LocalContextMixin):
    name = ''


@attr('META', group='sa')
class TestInstrumentManagementServiceMeta(IonIntegrationTestCase):

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

    def test_just_the_setup(self):
        return

 
rimi = ResourceImplMetatestIntegration(TestInstrumentManagementServiceMeta, InstrumentManagementService, log)

rimi.add_resource_impl_inttests(InstrumentAgentInstanceImpl, {"exchange_name": "rhubarb"})
rimi.add_resource_impl_inttests(InstrumentAgentImpl, {"agent_version": "3", "time_source": "the universe"})
rimi.add_resource_impl_inttests(InstrumentDeviceImpl, {"serialnumber": "123", "firmwareversion": "x"})
rimi.add_resource_impl_inttests(InstrumentModelImpl, {"model": "redundant?", "weight": 20000})
rimi.add_resource_impl_inttests(PlatformAgentInstanceImpl, {"exchange_name": "sausage"})
rimi.add_resource_impl_inttests(PlatformAgentImpl, {"description": "the big donut"})
rimi.add_resource_impl_inttests(PlatformDeviceImpl, {"serial_number": "2345"})
rimi.add_resource_impl_inttests(PlatformModelImpl, {"description": "tammy breathed deeply"})
rimi.add_resource_impl_inttests(SensorDeviceImpl, {"serialnumber": "123"})
rimi.add_resource_impl_inttests(SensorModelImpl, {"model": "redundant field?", "weight": 2})

