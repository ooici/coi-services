from interface.services.icontainer_agent import ContainerAgentClient

#from pyon.ion.endpoint import ProcessRPCClient
from ion.services.sa.resource_impl.resource_impl import ResourceImpl
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from ion.util.parameter_yaml_IO import get_param_dict
from ion.services.dm.utility.granule_utils import time_series_domain

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.instrument.instrument_management_service import InstrumentManagementService
from interface.services.sa.iinstrument_management_service import IInstrumentManagementService, InstrumentManagementServiceClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.objects import ComputedValueAvailability

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED, OT, LCS
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
#from ooi.logging import log

from ion.services.sa.test.helpers import any_old


@attr('INT', group='sax')
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
        self.PSC =  PubsubManagementServiceClient(node=self.container.node)
        self.DP = DataProductManagementServiceClient(node=self.container.node)
        self.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        
        print 'started services'

#    @unittest.skip('this test just for debugging setup')
#    def test_just_the_setup(self):
#        return

    @attr('EXT')
    def test_resources_associations_extensions(self):
        """
        create one of each resource and association used by IMS
        to guard against problems in ion-definitions
        """
        
        #stuff we control
#        instrument_agent_instance_id, _ =  self.RR.create(any_old(RT.InstrumentAgentInstance))
        instrument_agent_id, _ =           self.RR.create(any_old(RT.InstrumentAgent))
        instrument_model_id, _ =           self.RR.create(any_old(RT.InstrumentModel))
        instrument_device_id, _ =          self.RR.create(any_old(RT.InstrumentDevice))
        platform_agent_instance_id, _ =    self.RR.create(any_old(RT.PlatformAgentInstance))
        platform_agent_id, _ =             self.RR.create(any_old(RT.PlatformAgent))
        platform_device_id, _ =            self.RR.create(any_old(RT.PlatformDevice))
        platform_model_id, _ =             self.RR.create(any_old(RT.PlatformModel))
        sensor_device_id, _ =              self.RR.create(any_old(RT.SensorDevice))
        sensor_model_id, _ =               self.RR.create(any_old(RT.SensorModel))

        #stuff we associate to
        data_producer_id, _      = self.RR.create(any_old(RT.DataProducer))
        org_id, _ =                self.RR.create(any_old(RT.Org))

        #instrument_agent_instance_id #is only a target
        
        #instrument_agent
        self.RR.create_association(instrument_agent_id, PRED.hasModel, instrument_model_id)
#        self.RR.create_association(instrument_agent_instance_id, PRED.hasAgentDefinition, instrument_agent_id)

        #instrument_device
        self.RR.create_association(instrument_device_id, PRED.hasModel, instrument_model_id)
#        self.RR.create_association(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)
        self.RR.create_association(instrument_device_id, PRED.hasDataProducer, data_producer_id)
        self.RR.create_association(instrument_device_id, PRED.hasDevice, sensor_device_id)
        self.RR.create_association(org_id, PRED.hasResource, instrument_device_id)


        instrument_model_id #is only a target

        platform_agent_instance_id #is only a target
        
        #platform_agent
        self.RR.create_association(platform_agent_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_agent_instance_id, PRED.hasAgentDefinition, platform_agent_id)

        #platform_device
        self.RR.create_association(platform_device_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_device_id, PRED.hasAgentInstance, platform_agent_instance_id)
        self.RR.create_association(platform_device_id, PRED.hasDevice, instrument_device_id)

        platform_model_id #is only a target

        #sensor_device
        self.RR.create_association(sensor_device_id, PRED.hasModel, sensor_model_id)
        self.RR.create_association(sensor_device_id, PRED.hasDevice, instrument_device_id)

        sensor_model_id #is only a target

        #create a parsed product for this instrument output
        tdom, sdom = time_series_domain()
        tdom = tdom.dump()
        sdom = sdom.dump()
        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            processing_level_code='Parsed_Canonical',
            temporal_domain = tdom,
            spatial_domain = sdom)
        parsed_parameter_dictionary = get_param_dict('simple_data_particle_parsed_param_dict')
        parsed_stream_def_id = self.PSC.create_stream_definition(name='parsed', parameter_dictionary=parsed_parameter_dictionary.dump())
        data_product_id1 = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id, parameter_dictionary=parsed_parameter_dictionary.dump())
        log.debug( 'new dp_id = %s', data_product_id1)

        self.DAMS.assign_data_product(input_resource_id=instrument_device_id, data_product_id=data_product_id1)


        def addInstOwner(inst_id, subject):

            actor_identity_obj = any_old(RT.ActorIdentity, {"name": subject})
            user_id = self.IDS.create_actor_identity(actor_identity_obj)
            user_info_obj = any_old(RT.UserInfo)
            user_info_id = self.IDS.create_user_info(user_id, user_info_obj)

            self.RR.create_association(inst_id, PRED.hasOwner, user_id)


        #Testing multiple instrument owners
        addInstOwner(instrument_device_id, "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254")
        addInstOwner(instrument_device_id, "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Bob Cumbers A256")

        extended_instrument = self.IMS.get_instrument_device_extension(instrument_device_id)

        self.assertEqual(instrument_device_id, extended_instrument._id)
        self.assertEqual(len(extended_instrument.owners), 2)
        self.assertEqual(extended_instrument.instrument_model._id, instrument_model_id)

        log.debug( 'get_instrument_device_extension data_product_set = %s', str(extended_instrument.computed.data_product_set))
        log.debug( 'get_instrument_device_extension data_product_set.value = %s', str(extended_instrument.computed.data_product_set.value) )
        #check data products
        #self.assertEqual(1, len(extended_instrument.data_products))

        #check model
        inst_model_obj = self.RR.read(instrument_model_id)
        self.assertEqual(inst_model_obj.name, extended_instrument.instrument_model.name)

        #check agent
        inst_agent_obj = self.RR.read(instrument_agent_id)
        self.assertEqual(inst_agent_obj.name, extended_instrument.instrument_agent.name)

        #check platform device
        plat_device_obj = self.RR.read(platform_device_id)
        self.assertEqual(plat_device_obj.name, extended_instrument.platform_device.name)

        #check sensor devices
        self.assertEqual(1, len(extended_instrument.sensor_devices))

        #none of these will work because there is no agent
        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
                         extended_instrument.computed.firmware_version.status)
        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
                         extended_instrument.computed.operational_state.status)
        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
                         extended_instrument.computed.power_status_roll_up.status)
        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
                         extended_instrument.computed.communications_status_roll_up.status)
        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
                         extended_instrument.computed.data_status_roll_up.status)
        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
                         extended_instrument.computed.location_status_roll_up.status)

#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.recent_events.status)
#        self.assertEqual([], extended_instrument.computed.recent_events.value)


        # cleanup
        c = DotDict()
        c.resource_registry = self.RR
        resource_impl = ResourceImpl(c)
        resource_impl.pluck(instrument_agent_id)
        resource_impl.pluck(instrument_model_id)
        resource_impl.pluck(instrument_device_id)
        resource_impl.pluck(platform_agent_id)
        self.IMS.force_delete_instrument_agent(instrument_agent_id)
        self.IMS.force_delete_instrument_model(instrument_model_id)
        self.IMS.force_delete_instrument_device(instrument_device_id)
        self.IMS.force_delete_platform_agent_instance(platform_agent_instance_id)
        self.IMS.force_delete_platform_agent(platform_agent_id)
        self.IMS.force_delete_platform_device(platform_device_id)
        self.IMS.force_delete_platform_model(platform_model_id)
        self.IMS.force_delete_sensor_device(sensor_device_id)
        self.IMS.force_delete_sensor_model(sensor_model_id)

        #stuff we associate to
        self.RR.delete(data_producer_id)
        self.RR.delete(org_id)



    def test_custom_attributes(self):
        """
        Test assignment of custom attributes
        """

        instrument_model_id, _ =           self.RR.create(any_old(RT.InstrumentModel,
                {"custom_attributes":
                         {"favorite_color": "attr desc goes here"}
            }))
        instrument_device_id, _ =          self.RR.create(any_old(RT.InstrumentDevice,
                {"custom_attributes":
                         {"favorite_color": "red",
                          "bogus_attr": "should raise warning"
                     }
            }))

        self.IMS.assign_instrument_model_to_instrument_device(instrument_model_id, instrument_device_id)

        # cleanup
        self.IMS.force_delete_instrument_device(instrument_device_id)
        self.IMS.force_delete_instrument_model(instrument_model_id)