from interface.services.icontainer_agent import ContainerAgentClient

#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.instrument.instrument_management_service import InstrumentManagementService
from interface.services.sa.iinstrument_management_service import IInstrumentManagementService, InstrumentManagementServiceClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED, LCS
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
#from pyon.util.log import log

from ion.services.sa.test.helpers import any_old


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

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.RR = ResourceRegistryServiceClient(node=self.container.node)
        self.IMS = InstrumentManagementServiceClient(node=self.container.node)
        self.IDS = IdentityManagementServiceClient(node=self.container.node)
        
        print 'started services'

#    @unittest.skip('this test just for debugging setup')
#    def test_just_the_setup(self):
#        return

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
        sensor_device_id, _ =              self.RR.create(any_old(RT.SensorDevice))
        sensor_model_id, _ =               self.RR.create(any_old(RT.SensorModel))

        #stuff we associate to
        data_producer_id, _      = self.RR.create(any_old(RT.DataProducer))

        instrument_agent_instance_id #is only a target
        
        #instrument_agent
        self.RR.create_association(instrument_agent_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_agent_id, PRED.hasInstance, instrument_agent_instance_id)

        #instrument_device
        self.RR.create_association(instrument_device_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)
        self.RR.create_association(instrument_device_id, PRED.hasDataProducer, data_producer_id)

        instrument_model_id #is only a target

        platform_agent_instance_id #is only a target
        
        #platform_agent
        self.RR.create_association(platform_agent_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_agent_id, PRED.hasInstance, platform_agent_instance_id)

        #platform_device
        self.RR.create_association(platform_device_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_device_id, PRED.hasAgentInstance, platform_agent_instance_id)
        self.RR.create_association(platform_device_id, PRED.hasInstrument, instrument_device_id)

        platform_model_id #is only a target

        #sensor_device
        self.RR.create_association(sensor_device_id, PRED.hasModel, sensor_model_id)

        sensor_model_id #is only a target


    def test_get_extended_instrument_device(self):

        #stuff we control
        instrument_agent_instance_id, _ =  self.RR.create(any_old(RT.InstrumentAgentInstance))
        instrument_agent_id, _ =           self.RR.create(any_old(RT.InstrumentAgent))
        instrument_device_id, _ =          self.RR.create(any_old(RT.InstrumentDevice))
        instrument_model_id, _ =           self.RR.create(any_old(RT.InstrumentModel))
        platform_agent_instance_id, _ =    self.RR.create(any_old(RT.PlatformAgentInstance))
        platform_agent_id, _ =             self.RR.create(any_old(RT.PlatformAgent))
        platform_device_id, _ =            self.RR.create(any_old(RT.PlatformDevice))
        platform_model_id, _ =             self.RR.create(any_old(RT.PlatformModel))
        sensor_device_id, _ =              self.RR.create(any_old(RT.SensorDevice))
        sensor_model_id, _ =               self.RR.create(any_old(RT.SensorModel))

        #stuff we associate to
        data_producer_id, _      = self.RR.create(any_old(RT.DataProducer))

        instrument_agent_instance_id #is only a target

        #instrument_agent
        self.RR.create_association(instrument_agent_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_agent_id, PRED.hasInstance, instrument_agent_instance_id)

        #instrument_device
        self.RR.create_association(instrument_device_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)
        self.RR.create_association(instrument_device_id, PRED.hasDataProducer, data_producer_id)

        instrument_model_id #is only a target

        platform_agent_instance_id #is only a target

        #platform_agent
        self.RR.create_association(platform_agent_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_agent_id, PRED.hasInstance, platform_agent_instance_id)

        #platform_device
        self.RR.create_association(platform_device_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_device_id, PRED.hasAgentInstance, platform_agent_instance_id)
        self.RR.create_association(platform_device_id, PRED.hasInstrument, instrument_device_id)


        #Testing multiple instrument owners
        subject1 = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254"

        actor_identity_obj1 = IonObject("ActorIdentity", {"name": subject1})
        user_id1 = self.IDS.create_actor_identity(actor_identity_obj1)

        user_info_obj1 = IonObject("UserInfo", {"name": "Foo"})
        user_info_id1 = self.IDS.create_user_info(user_id1, user_info_obj1)

        self.RR.create_association(instrument_device_id, PRED.hasOwner, user_id1)

        subject2 = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Bob Cumbers A256"

        actor_identity_obj2 = IonObject("ActorIdentity", {"name": subject2})
        user_id2 = self.IDS.create_actor_identity(actor_identity_obj2)

        user_info_obj2 = IonObject("UserInfo", {"name": "Foo2"})
        user_info_id2 = self.IDS.create_user_info(user_id2, user_info_obj2)

        self.RR.create_association(instrument_device_id, PRED.hasOwner, user_id2)

        extended_instrument = self.IMS.get_instrument_device_extension(instrument_device_id)
        self.assertEqual(instrument_device_id,extended_instrument._id)
        self.assertEqual(len(extended_instrument.owners),2)
        self.assertEqual(extended_instrument.instrument_model._id, instrument_model_id)
        self.assertEqual(extended_instrument.computed.data_producer_count,1)
