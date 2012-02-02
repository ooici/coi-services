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

from ion.services.sa.resource_impl_metatest_integration import ResourceImplMetatestIntegration

from ion.services.sa.test.helpers import any_old

from ion.services.sa.instrument.instrument_agent_instance_impl import InstrumentAgentInstanceImpl
from ion.services.sa.instrument.instrument_agent_impl import InstrumentAgentImpl
from ion.services.sa.instrument.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.instrument.instrument_model_impl import InstrumentModelImpl
from ion.services.sa.instrument.platform_agent_instance_impl import PlatformAgentInstanceImpl
from ion.services.sa.instrument.platform_agent_impl import PlatformAgentImpl
from ion.services.sa.instrument.platform_device_impl import PlatformDeviceImpl
from ion.services.sa.instrument.platform_model_impl import PlatformModelImpl
from ion.services.sa.instrument.sensor_device_impl import SensorDeviceImpl
from ion.services.sa.instrument.sensor_model_impl import SensorModelImpl


class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
class TestInstrumentManagementServiceIntegration(IonIntegrationTestCase):

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

    def test_resources_associations(self):
        """
        create one of each resource and association used by IMS
        to guard against problems in ion-definitions
        """
        
        #stuff we control
        instrument_agent_instance_id, _ =  self.RR.create(any_old(RT.InstrumentAgentInstance))
        instrument_agent_id, _ =           self.RR.create(any_old(RT.InstrumentAgent))
        instrument_device_id, _ =          self.RR.create(any_old(RT.InstrumentDevice))
        instrument_model_id, _ =           self.RR.create(any_old(RT.InstrumentModel))
        platform_agent_instance_id, _ =    self.RR.create(any_old(RT.PlatformAgentInstance))
        platform_agent_id, _ =             self.RR.create(any_old(RT.PlatformAgent))
        platform_device_id, _ =            self.RR.create(any_old(RT.PlatformDevice))
        platform_model_id, _ =             self.RR.create(any_old(RT.PlatformModel))
        #sensor_device_id, _ =              self.RR.create(any_old(RT.SensorDevice))
        #sensor_model_id, _ =               self.RR.create(any_old(RT.SensorModel))

        #stuff we associate to
        logical_platform_id, _   = self.RR.create(any_old(RT.LogicalPlatform))
        logical_instrument_id, _ = self.RR.create(any_old(RT.LogicalInstrument))
        data_producer_id, _      = self.RR.create(any_old(RT.DataProducer))

        instrument_agent_instance_id #is only a target
        
        #instrument_agent
        self.RR.create_association(instrument_agent_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_agent_id, PRED.hasInstance, instrument_agent_instance_id)

        #instrument_device
        self.RR.create_association(instrument_device_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_device_id, PRED.hasAssignment, logical_instrument_id)
        self.RR.create_association(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)
        #self.RR.create_association(instrument_device_id, PRED.hasSensor, sensor_device_id)
        self.RR.create_association(instrument_device_id, PRED.hasDataProducer, data_producer_id)

        instrument_model_id #is only a target

        platform_agent_instance_id #is only a target
        
        #platform_agent
        self.RR.create_association(platform_agent_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_agent_id, PRED.hasInstance, platform_agent_instance_id)

        #platform_device
        self.RR.create_association(platform_device_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_device_id, PRED.hasAssignment, logical_platform_id)
        self.RR.create_association(platform_device_id, PRED.hasAgentInstance, platform_agent_instance_id)
        self.RR.create_association(platform_device_id, PRED.hasInstrument, instrument_device_id)

        platform_model_id #is only a target

        #sensor_device
        #self.RR.create_association(sensor_device_id, PRED.hasModel, sensor_model_id)

        #sensor_model_id #is only a target

 
rimi = ResourceImplMetatestIntegration(TestInstrumentManagementServiceIntegration, InstrumentManagementService, log)

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

