#!/usr/bin/env python

from nose.plugins.attrib import attr
import unittest
import os

from ooi.logging import log

from pyon.datastore.datastore import DataStore
from pyon.public import RT, PRED, OT, LCE, IonObject, CFG
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.sa.test.helpers import any_old
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient


from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.objects import ComputedValueAvailability
from interface.objects import ComputedIntValue, ComputedFloatValue, ComputedStringValue


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
        unittest # suppress an pycharm inspector error if all unittest.skip references are commented out

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.RR   = ResourceRegistryServiceClient(node=self.container.node)
        self.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.IDS  = IdentityManagementServiceClient(node=self.container.node)
        self.PSC  = PubsubManagementServiceClient(node=self.container.node)
        self.DP   = DataProductManagementServiceClient(node=self.container.node)
        self.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.DSC  = DatasetManagementServiceClient(node=self.container.node)
        self.PDC  = ProcessDispatcherServiceClient(node=self.container.node)
        self.OMS = ObservatoryManagementServiceClient(node=self.container.node)
        self.RR2 = EnhancedResourceRegistryClient(self.RR)

#    @unittest.skip('this test just for debugging setup')
#    def test_just_the_setup(self):
#        return

    @attr('EXT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode as it depends on modifying CFG on service side')
    def test_resources_associations_extensions(self):
        """
        create one of each resource and association used by IMS
        to guard against problems in ion-definitions
        """
        self.patch_cfg(CFG["container"], {"extended_resources": {"strip_results": False}})

        #stuff we control
        instrument_agent_instance_id, _ =  self.RR.create(any_old(RT.InstrumentAgentInstance))
        instrument_agent_id, _ =           self.RR.create(any_old(RT.InstrumentAgent))
        instrument_model_id, _ =           self.RR.create(any_old(RT.InstrumentModel))
        instrument_device_id, _ =          self.RR.create(any_old(RT.InstrumentDevice))
        instrument_site_id, _ =            self.RR.create(any_old(RT.InstrumentSite))
        platform_agent_instance_id, _ =    self.RR.create(any_old(RT.PlatformAgentInstance))
        platform_agent_id, _ =             self.RR.create(any_old(RT.PlatformAgent))
        platform_site_id, _ =              self.RR.create(any_old(RT.PlatformSite))
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
        self.RR.create_association(instrument_agent_instance_id, PRED.hasAgentDefinition, instrument_agent_id)

        #instrument_device
        self.RR.create_association(instrument_device_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)
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

        self.RR.create_association(instrument_site_id, PRED.hasDevice, instrument_device_id)
        self.RR.create_association(platform_site_id, PRED.hasDevice, platform_device_id)
        self.RR.create_association(platform_site_id, PRED.hasSite, instrument_site_id)

        platform_model_id #is only a target

        #sensor_device
        self.RR.create_association(sensor_device_id, PRED.hasModel, sensor_model_id)
        self.RR.create_association(sensor_device_id, PRED.hasDevice, instrument_device_id)

        sensor_model_id #is only a target

        #create a parsed product for this instrument output
        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            processing_level_code='Parsed_Canonical')
        pdict_id = self.DSC.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        parsed_stream_def_id = self.PSC.create_stream_definition(name='parsed', parameter_dictionary_id=pdict_id)
        data_product_id1 = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id)
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

        # Lifecycle
        self.assertEquals(len(extended_instrument.lcstate_transitions), 6)
        self.assertEquals(set(extended_instrument.lcstate_transitions.keys()), set(['develop', 'deploy', 'retire', 'plan', 'integrate', 'delete']))
        self.assertEquals(len(extended_instrument.availability_transitions), 2)
        self.assertEquals(set(extended_instrument.availability_transitions.keys()), set(['enable', 'announce']))

        # Verify that computed attributes exist for the extended instrument
        self.assertIsInstance(extended_instrument.computed.last_data_received_datetime, ComputedFloatValue)
        self.assertIsInstance(extended_instrument.computed.uptime, ComputedStringValue)

        self.assertIsInstance(extended_instrument.computed.power_status_roll_up, ComputedIntValue)
        self.assertIsInstance(extended_instrument.computed.communications_status_roll_up, ComputedIntValue)
        self.assertIsInstance(extended_instrument.computed.data_status_roll_up, ComputedIntValue)
        self.assertIsInstance(extended_instrument.computed.location_status_roll_up, ComputedIntValue)

        log.debug("extended_instrument.computed: %s", extended_instrument.computed)

        #check model
        inst_model_obj = self.RR.read(instrument_model_id)
        self.assertEqual(inst_model_obj.name, extended_instrument.instrument_model.name)

        #check agent instance
        inst_agent_instance_obj = self.RR.read(instrument_agent_instance_id)
        self.assertEqual(inst_agent_instance_obj.name, extended_instrument.agent_instance.name)

        #check agent
        inst_agent_obj = self.RR.read(instrument_agent_id)
        #compound assoc return list of lists so check the first element
        self.assertEqual(inst_agent_obj.name, extended_instrument.instrument_agent.name)

        #check platform device
        plat_device_obj = self.RR.read(platform_device_id)
        self.assertEqual(plat_device_obj.name, extended_instrument.platform_device.name)

        extended_platform = self.IMS.get_platform_device_extension(platform_device_id)

        self.assertEqual(1, len(extended_platform.portals))
        self.assertEqual(1, len(extended_platform.portal_instruments))
        #self.assertEqual(1, len(extended_platform.computed.portal_status.value)) # no agent started so NO statuses reported
        self.assertEqual(1, len(extended_platform.instrument_devices))
        self.assertEqual(instrument_device_id, extended_platform.instrument_devices[0]._id)
        self.assertEqual(1, len(extended_platform.instrument_models))
        self.assertEqual(instrument_model_id, extended_platform.instrument_models[0]._id)
        self.assertEquals(extended_platform.platform_agent._id, platform_agent_id)

        self.assertEquals(len(extended_platform.lcstate_transitions), 6)
        self.assertEquals(set(extended_platform.lcstate_transitions.keys()), set(['develop', 'deploy', 'retire', 'plan', 'integrate', 'delete']))
        self.assertEquals(len(extended_platform.availability_transitions), 2)
        self.assertEquals(set(extended_platform.availability_transitions.keys()), set(['enable', 'announce']))

        #check sensor devices
        self.assertEqual(1, len(extended_instrument.sensor_devices))

        ##check data_product_parameters_set
        # !!!  OOIION-1342 The UI does not use data_product_parameters_set and it is an expensive calc so the attribute calc was disabled
        # !!!   Remove check in this test
        #self.assertEqual(ComputedValueAvailability.PROVIDED,
        #                 extended_instrument.computed.data_product_parameters_set.status)
        #self.assertTrue( 'Parsed_Canonical' in extended_instrument.computed.data_product_parameters_set.value)
        ## the ctd parameters should include 'temp'
        #self.assertTrue( 'temp' in extended_instrument.computed.data_product_parameters_set.value['Parsed_Canonical'])

        #none of these will work because there is no agent
#        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
#                         extended_instrument.computed.firmware_version.status)
#        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
#                         extended_instrument.computed.operational_state.status)
#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.power_status_roll_up.status)
#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.communications_status_roll_up.status)
#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.data_status_roll_up.status)
#        self.assertEqual(DeviceStatusType.STATUS_OK,
#                        extended_instrument.computed.data_status_roll_up.value)
#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.location_status_roll_up.status)

#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.recent_events.status)
#        self.assertEqual([], extended_instrument.computed.recent_events.value)


        # cleanup
        c = DotDict()
        c.resource_registry = self.RR
        self.RR2.pluck(instrument_agent_id)
        self.RR2.pluck(instrument_model_id)
        self.RR2.pluck(instrument_device_id)
        self.RR2.pluck(platform_agent_id)
        self.RR2.pluck(sensor_device_id)
        self.IMS.force_delete_instrument_agent(instrument_agent_id)
        self.IMS.force_delete_instrument_model(instrument_model_id)
        self.IMS.force_delete_instrument_device(instrument_device_id)
        self.IMS.force_delete_platform_agent_instance(platform_agent_instance_id)
        self.IMS.force_delete_platform_agent(platform_agent_id)
        self.OMS.force_delete_instrument_site(instrument_site_id)
        self.OMS.force_delete_platform_site(platform_site_id)
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

        instModel_obj = IonObject(OT.CustomAttribute,
                                  name='SBE37IMModelAttr',
                                  description="model custom attr")

        instrument_model_id, _ =           self.RR.create(any_old(RT.InstrumentModel,
                {"custom_attributes":
                         [instModel_obj]
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






    def _get_datastore(self, dataset_id):
        dataset = self.DSC.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore



    def test_data_producer(self):
        idevice_id = self.IMS.create_instrument_device(any_old(RT.InstrumentDevice))
        self.assertEqual(1, len(self.RR2.find_data_producer_ids_of_instrument_device_using_has_data_producer(idevice_id)))

        pdevice_id = self.IMS.create_platform_device(any_old(RT.PlatformDevice))
        self.assertEqual(1, len(self.RR2.find_data_producer_ids_of_platform_device_using_has_data_producer(pdevice_id)))


    @attr('PREP')
    def test_prepare_resource_support(self):
        """
        create one of each resource and association used by IMS
        to guard against problems in ion-definitions
        """

        #stuff we control
        instrument_agent_instance_id, _ =  self.RR.create(any_old(RT.InstrumentAgentInstance))
        instrument_agent_id, _ =           self.RR.create(any_old(RT.InstrumentAgent))
        instrument_model_id, _ =           self.RR.create(any_old(RT.InstrumentModel))
        instrument_device_id, _ =          self.RR.create(any_old(RT.InstrumentDevice))
        platform_agent_instance_id, _ =    self.RR.create(any_old(RT.PlatformAgentInstance))
        platform_agent_id, _ =             self.RR.create(any_old(RT.PlatformAgent))
        platform_device_id, _ =            self.RR.create(any_old(RT.PlatformDevice))
        platform_model_id, _ =             self.RR.create(any_old(RT.PlatformModel))
        sensor_device_id, _ =              self.RR.create(any_old(RT.SensorDevice))
        sensor_model_id, _ =               self.RR.create(any_old(RT.SensorModel))

        instrument_device2_id, _ =          self.RR.create(any_old(RT.InstrumentDevice))
        instrument_device3_id, _ =          self.RR.create(any_old(RT.InstrumentDevice))

        platform_device2_id, _ =            self.RR.create(any_old(RT.PlatformDevice))
        sensor_device2_id, _ =              self.RR.create(any_old(RT.SensorDevice))

        #stuff we associate to
        data_producer_id, _      = self.RR.create(any_old(RT.DataProducer))
        org_id, _ =                self.RR.create(any_old(RT.Org))

        #instrument_agent_instance_id #is only a target

        #instrument_agent
        self.RR.create_association(instrument_agent_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_agent_instance_id, PRED.hasAgentDefinition, instrument_agent_id)

        #instrument_device
        self.RR.create_association(instrument_device_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_device_id, PRED.hasAgentInstance, instrument_agent_instance_id)
        self.RR.create_association(instrument_device_id, PRED.hasDataProducer, data_producer_id)
        self.RR.create_association(instrument_device_id, PRED.hasDevice, sensor_device_id)
        self.RR.create_association(org_id, PRED.hasResource, instrument_device_id)

        self.RR.create_association(instrument_device2_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(org_id, PRED.hasResource, instrument_device2_id)


        instrument_model_id #is only a target

        platform_agent_instance_id #is only a target

        #platform_agent
        self.RR.create_association(platform_agent_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_agent_instance_id, PRED.hasAgentDefinition, platform_agent_id)

        #platform_device
        self.RR.create_association(platform_device_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_device_id, PRED.hasAgentInstance, platform_agent_instance_id)
        self.RR.create_association(platform_device_id, PRED.hasDevice, instrument_device_id)

        self.RR.create_association(platform_device2_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_device2_id, PRED.hasDevice, instrument_device2_id)

        platform_model_id #is only a target

        #sensor_device
        self.RR.create_association(sensor_device_id, PRED.hasModel, sensor_model_id)
        self.RR.create_association(sensor_device_id, PRED.hasDevice, instrument_device_id)

        self.RR.create_association(sensor_device2_id, PRED.hasModel, sensor_model_id)
        self.RR.create_association(sensor_device2_id, PRED.hasDevice, instrument_device2_id)

        sensor_model_id #is only a target

        #set lcstate - used for testing prepare - not setting all to DEVELOP, only some
        self.RR.execute_lifecycle_transition(instrument_agent_id, LCE.DEVELOP)
        self.RR.execute_lifecycle_transition(instrument_device_id, LCE.DEVELOP)
        self.RR.execute_lifecycle_transition(instrument_device2_id, LCE.DEVELOP)
        self.RR.execute_lifecycle_transition(platform_device_id, LCE.DEVELOP)
        self.RR.execute_lifecycle_transition(platform_device2_id, LCE.DEVELOP)
        self.RR.execute_lifecycle_transition(platform_agent_id, LCE.DEVELOP)



        #create a parsed product for this instrument output
        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            processing_level_code='Parsed_Canonical')
        pdict_id = self.DSC.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        parsed_stream_def_id = self.PSC.create_stream_definition(name='parsed', parameter_dictionary_id=pdict_id)
        data_product_id1 = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id)
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

        def ion_object_encoder(obj):
            return obj.__dict__


        #First call to create
        instrument_data = self.IMS.prepare_instrument_device_support()

        #print simplejson.dumps(instrument_data, default=ion_object_encoder, indent=2)


        self.assertEqual(instrument_data._id, '')
        self.assertEqual(instrument_data.type_, OT.InstrumentDevicePrepareSupport)
        self.assertEqual(len(instrument_data.associations['InstrumentModel'].resources), 1)
        self.assertEqual(instrument_data.associations['InstrumentModel'].resources[0]._id, instrument_model_id)
        self.assertEqual(len(instrument_data.associations['InstrumentAgentInstance'].resources), 1)
        self.assertEqual(instrument_data.associations['InstrumentAgentInstance'].resources[0]._id, instrument_agent_instance_id)
        self.assertEqual(len(instrument_data.associations['InstrumentModel'].associated_resources), 0)
        self.assertEqual(len(instrument_data.associations['InstrumentAgentInstance'].associated_resources), 0)
        self.assertEqual(len(instrument_data.associations['SensorDevice'].resources), 0)



        #Next call to update
        instrument_data = self.IMS.prepare_instrument_device_support(instrument_device_id)

        #print 'Update results'
        #print simplejson.dumps(instrument_data, default=ion_object_encoder, indent=2)

        self.assertEqual(instrument_data._id, instrument_device_id)
        self.assertEqual(instrument_data.type_, OT.InstrumentDevicePrepareSupport)
        self.assertEqual(len(instrument_data.associations['InstrumentModel'].resources), 1)
        self.assertEqual(instrument_data.associations['InstrumentModel'].resources[0]._id, instrument_model_id)
        self.assertEqual(len(instrument_data.associations['InstrumentAgentInstance'].resources), 1)
        self.assertEqual(instrument_data.associations['InstrumentAgentInstance'].resources[0]._id, instrument_agent_instance_id)
        self.assertEqual(len(instrument_data.associations['InstrumentModel'].associated_resources), 1)
        self.assertEqual(instrument_data.associations['InstrumentModel'].associated_resources[0].s, instrument_device_id)
        self.assertEqual(instrument_data.associations['InstrumentModel'].associated_resources[0].o, instrument_model_id)
        self.assertEqual(len(instrument_data.associations['InstrumentAgentInstance'].associated_resources), 1)
        self.assertEqual(instrument_data.associations['InstrumentAgentInstance'].associated_resources[0].o, instrument_agent_instance_id)
        self.assertEqual(instrument_data.associations['InstrumentAgentInstance'].associated_resources[0].s, instrument_device_id)
        self.assertEqual(len(instrument_data.associations['SensorDevice'].resources), 1)
        self.assertEqual(instrument_data.associations['SensorDevice'].resources[0]._id, sensor_device_id)
        self.assertEqual(len(instrument_data.associations['SensorDevice'].associated_resources), 1)
        self.assertEqual(instrument_data.associations['SensorDevice'].associated_resources[0].o, instrument_device_id)
        self.assertEqual(instrument_data.associations['SensorDevice'].associated_resources[0].s, sensor_device_id)
        self.assertEqual(instrument_data.associations['InstrumentModel'].assign_request.request_parameters['instrument_device_id'], instrument_device_id)


        #test prepare for create of instrument agent instance
        instrument_agent_data = self.IMS.prepare_instrument_agent_instance_support()

        #print 'Update results'
        #print simplejson.dumps(instrument_agent_data, default=ion_object_encoder, indent=2)

        self.assertEqual(instrument_agent_data._id, '')
        self.assertEqual(instrument_agent_data.type_, OT.InstrumentAgentInstancePrepareSupport)
        self.assertEqual(len(instrument_agent_data.associations['InstrumentDevice'].resources), 2)
        self.assertEqual(len(instrument_agent_data.associations['InstrumentAgent'].resources), 1)
        self.assertEqual(instrument_agent_data.associations['InstrumentAgent'].resources[0]._id, instrument_agent_id)
        self.assertEqual(len(instrument_agent_data.associations['InstrumentDevice'].associated_resources), 0)
        self.assertEqual(len(instrument_agent_data.associations['InstrumentAgent'].associated_resources), 0)


        #test prepare for update of instrument agent instance to see if it is associated with the instrument that was created
        instrument_agent_data = self.IMS.prepare_instrument_agent_instance_support(instrument_agent_instance_id=instrument_agent_instance_id)

        #print 'Update results'
        #print simplejson.dumps(instrument_agent_data, default=ion_object_encoder, indent=2)

        self.assertEqual(instrument_agent_data._id, instrument_agent_instance_id)
        self.assertEqual(instrument_agent_data.type_, OT.InstrumentAgentInstancePrepareSupport)
        self.assertEqual(len(instrument_agent_data.associations['InstrumentDevice'].resources), 3)
        self.assertEqual(len(instrument_agent_data.associations['InstrumentAgent'].resources), 1)
        self.assertEqual(instrument_agent_data.associations['InstrumentAgent'].resources[0]._id, instrument_agent_id)
        self.assertEqual(len(instrument_agent_data.associations['InstrumentDevice'].associated_resources), 1)
        self.assertEqual(instrument_agent_data.associations['InstrumentDevice'].associated_resources[0].s, instrument_device_id)
        self.assertEqual(instrument_agent_data.associations['InstrumentDevice'].associated_resources[0].o, instrument_agent_instance_id)
        self.assertEqual(len(instrument_agent_data.associations['InstrumentAgent'].associated_resources), 1)
        self.assertEqual(instrument_agent_data.associations['InstrumentAgent'].associated_resources[0].o, instrument_agent_id)
        self.assertEqual(instrument_agent_data.associations['InstrumentAgent'].associated_resources[0].s, instrument_agent_instance_id)
        self.assertEqual(instrument_agent_data.associations['InstrumentAgent'].assign_request.request_parameters['instrument_agent_instance_id'], instrument_agent_instance_id)


        #test prepare for update of data product to see if it is associated with the instrument that was created
        data_product_data = self.DP.prepare_data_product_support(data_product_id1)

        #print simplejson.dumps(data_product_data, default=ion_object_encoder, indent=2)

        self.assertEqual(data_product_data._id, data_product_id1)
        self.assertEqual(data_product_data.type_, OT.DataProductPrepareSupport)
        self.assertEqual(len(data_product_data.associations['StreamDefinition'].resources), 1)

        self.assertEqual(len(data_product_data.associations['Dataset'].resources), 0)

        self.assertEqual(len(data_product_data.associations['StreamDefinition'].associated_resources), 1)
        self.assertEqual(data_product_data.associations['StreamDefinition'].associated_resources[0].s, data_product_id1)

        self.assertEqual(len(data_product_data.associations['Dataset'].associated_resources), 0)

        self.assertEqual(len(data_product_data.associations['InstrumentDeviceHasOutputProduct'].resources), 3)

        self.assertEqual(len(data_product_data.associations['InstrumentDeviceHasOutputProduct'].associated_resources), 1)
        self.assertEqual(data_product_data.associations['InstrumentDeviceHasOutputProduct'].associated_resources[0].s, instrument_device_id)
        self.assertEqual(data_product_data.associations['InstrumentDeviceHasOutputProduct'].associated_resources[0].o, data_product_id1)

        self.assertEqual(len(data_product_data.associations['PlatformDevice'].resources), 2)


        platform_data = self.IMS.prepare_platform_device_support()

        #print simplejson.dumps(platform_data, default=ion_object_encoder, indent=2)

        self.assertEqual(platform_data._id, '')
        self.assertEqual(platform_data.type_, OT.PlatformDevicePrepareSupport)
        self.assertEqual(len(platform_data.associations['PlatformModel'].resources), 1)
        self.assertEqual(platform_data.associations['PlatformModel'].resources[0]._id, platform_model_id)
        self.assertEqual(len(platform_data.associations['PlatformAgentInstance'].resources), 1)
        self.assertEqual(platform_data.associations['PlatformAgentInstance'].resources[0]._id, platform_agent_instance_id)
        self.assertEqual(len(platform_data.associations['PlatformModel'].associated_resources), 0)
        self.assertEqual(len(platform_data.associations['PlatformAgentInstance'].associated_resources), 0)
        self.assertEqual(len(platform_data.associations['InstrumentDevice'].resources), 1)

        platform_data = self.IMS.prepare_platform_device_support(platform_device_id)

        #print simplejson.dumps(platform_data, default=ion_object_encoder, indent=2)

        self.assertEqual(platform_data._id, platform_device_id)
        self.assertEqual(platform_data.type_, OT.PlatformDevicePrepareSupport)
        self.assertEqual(len(platform_data.associations['PlatformModel'].resources), 1)
        self.assertEqual(platform_data.associations['PlatformModel'].resources[0]._id, platform_model_id)
        self.assertEqual(len(platform_data.associations['PlatformAgentInstance'].resources), 1)
        self.assertEqual(platform_data.associations['PlatformAgentInstance'].resources[0]._id, platform_agent_instance_id)
        self.assertEqual(len(platform_data.associations['PlatformModel'].associated_resources), 1)
        self.assertEqual(platform_data.associations['PlatformModel'].associated_resources[0].s, platform_device_id)
        self.assertEqual(platform_data.associations['PlatformModel'].associated_resources[0].o, platform_model_id)
        self.assertEqual(len(platform_data.associations['PlatformAgentInstance'].associated_resources), 1)
        self.assertEqual(platform_data.associations['PlatformAgentInstance'].associated_resources[0].o, platform_agent_instance_id)
        self.assertEqual(platform_data.associations['PlatformAgentInstance'].associated_resources[0].s, platform_device_id)
        self.assertEqual(len(platform_data.associations['InstrumentDevice'].resources), 2)
        #self.assertEqual(len(platform_data.associations['InstrumentDevice'].associated_resources), 1)
        #self.assertEqual(platform_data.associations['InstrumentDevice'].associated_resources[0].s, platform_device_id)
        #self.assertEqual(platform_data.associations['InstrumentDevice'].associated_resources[0].o, instrument_device_id)
        self.assertEqual(platform_data.associations['PlatformModel'].assign_request.request_parameters['platform_device_id'], platform_device_id)


        # cleanup
        c = DotDict()
        c.resource_registry = self.RR
        self.RR2.pluck(instrument_agent_id)
        self.RR2.pluck(instrument_model_id)
        self.RR2.pluck(instrument_device_id)
        self.RR2.pluck(platform_agent_id)
        self.RR2.pluck(sensor_device_id)
        self.RR2.pluck(sensor_device2_id)
        self.IMS.force_delete_instrument_agent(instrument_agent_id)
        self.IMS.force_delete_instrument_model(instrument_model_id)
        self.IMS.force_delete_instrument_device(instrument_device_id)
        self.IMS.force_delete_instrument_device(instrument_device2_id)
        self.IMS.force_delete_platform_agent_instance(platform_agent_instance_id)
        self.IMS.force_delete_platform_agent(platform_agent_id)
        self.IMS.force_delete_platform_device(platform_device_id)
        self.IMS.force_delete_platform_device(platform_device2_id)
        self.IMS.force_delete_platform_model(platform_model_id)
        self.IMS.force_delete_sensor_device(sensor_device_id)
        self.IMS.force_delete_sensor_device(sensor_device2_id)
        self.IMS.force_delete_sensor_model(sensor_model_id)

        #stuff we associate to
        self.RR.delete(data_producer_id)
        self.RR.delete(org_id)
