from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.icontainer_agent import ContainerAgentClient

#from pyon.ion.endpoint import ProcessRPCClient
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.services.sa.resource_impl.resource_impl import ResourceImpl
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

from pyon.public import RT, PRED, CFG
from nose.plugins.attrib import attr
from ooi.logging import log
import unittest

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
        self.RR   = ResourceRegistryServiceClient(node=self.container.node)
        self.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.IDS  = IdentityManagementServiceClient(node=self.container.node)
        self.PSC  = PubsubManagementServiceClient(node=self.container.node)
        self.DP   = DataProductManagementServiceClient(node=self.container.node)
        self.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.DSC  = DatasetManagementServiceClient(node=self.container.node)
        self.PDC  = ProcessDispatcherServiceClient(node=self.container.node)

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


        #check model
        inst_model_obj = self.RR.read(instrument_model_id)
        self.assertEqual(inst_model_obj.name, extended_instrument.instrument_model.name)

        #check agent instance
        inst_agent_instance_obj = self.RR.read(instrument_agent_instance_id)
        self.assertEqual(inst_agent_instance_obj.name, extended_instrument.agent_instance.name)

        #check agent
        inst_agent_obj = self.RR.read(instrument_agent_id)
        #compound assoc return list of lists so check the first element
        self.assertEqual(inst_agent_obj.name, extended_instrument.instrument_agent[0].name)

        #check platform device
        plat_device_obj = self.RR.read(platform_device_id)
        self.assertEqual(plat_device_obj.name, extended_instrument.platform_device.name)

        #check sensor devices
        self.assertEqual(1, len(extended_instrument.sensor_devices))

        #check data_product_parameters_set
        self.assertEqual(ComputedValueAvailability.PROVIDED,
                         extended_instrument.computed.data_product_parameters_set.status)
        self.assertTrue( 'Parsed_Canonical' in extended_instrument.computed.data_product_parameters_set.value)
        # the ctd parameters should include 'temp'
        self.assertTrue( 'temp' in extended_instrument.computed.data_product_parameters_set.value['Parsed_Canonical'])

        #none of these will work because there is no agent
        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
                         extended_instrument.computed.firmware_version.status)
#        self.assertEqual(ComputedValueAvailability.NOTAVAILABLE,
#                         extended_instrument.computed.operational_state.status)
#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.power_status_roll_up.status)
#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.communications_status_roll_up.status)
#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.data_status_roll_up.status)
#        self.assertEqual(StatusType.STATUS_OK,
#                        extended_instrument.computed.data_status_roll_up.value)
#        self.assertEqual(ComputedValueAvailability.PROVIDED,
#                         extended_instrument.computed.location_status_roll_up.status)

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






    def _get_datastore(self, dataset_id):
        dataset = self.DSC.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore




    def test_resource_state_save_restore(self):

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel")
        instModel_id = self.IMS.create_instrument_model(instModel_obj)
        log.debug( 'new InstrumentModel id = %s ', instModel_id)

        # Create InstrumentAgent
        raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='ctd_raw_param_dict', records_per_granule=2, granule_publish_rate=5 )
        parsed_config = StreamConfiguration(stream_name='parsed', parameter_dictionary_name='ctd_parsed_param_dict', records_per_granule=2, granule_publish_rate=5 )
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_module="mi.instrument.seabird.sbe37smb.ooicore.driver",
                                  driver_class="SBE37Driver",
                                    stream_configurations = [raw_config, parsed_config] )
        instAgent_id = self.IMS.create_instrument_agent(instAgent_obj)
        log.debug( 'new InstrumentAgent id = %s', instAgent_id)

        self.IMS.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        log.debug('test_activateInstrumentSample: Create instrument resource to represent the SBE37 '
        + '(SA Req: L4-CI-SA-RQ-241) ')
        instDevice_obj = IonObject(RT.InstrumentDevice,
                                   name='SBE37IMDevice',
                                   description="SBE37IMDevice",
                                   serial_number="12345" )
        instDevice_id = self.IMS.create_instrument_device(instrument_device=instDevice_obj)
        self.IMS.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)

        log.debug("test_activateInstrumentSample: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) ",
                  instDevice_id)

        port_agent_config = {
            'device_addr':  CFG.device.sbe37.host,
            'device_port':  CFG.device.sbe37.port,
            'process_type': PortAgentProcessType.UNIX,
            'binary_path': "port_agent",
            'port_agent_addr': 'localhost',
            'command_port': CFG.device.sbe37.port_agent_cmd_port,
            'data_port': CFG.device.sbe37.port_agent_data_port,
            'log_level': 5,
            'type': PortAgentType.ETHERNET
        }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance',
                                          description="SBE37IMAgentInstance",
                                          comms_device_address='sbe37-simulator.oceanobservatories.org',
                                          comms_device_port=4001,
                                          port_agent_config = port_agent_config)


        instAgentInstance_id = self.IMS.create_instrument_agent_instance(instAgentInstance_obj,
                                                                               instAgent_id,
                                                                               instDevice_id)

        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()


        spdict_id = self.DSC.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        parsed_stream_def_id = self.PSC.create_stream_definition(name='parsed', parameter_dictionary=spdict_id)

        rpdict_id = self.DSC.read_parameter_dictionary_by_name('ctd_raw_param_dict')
        raw_stream_def_id = self.PSC.create_stream_definition(name='raw', parameter_dictionary=rpdict_id)


        #-------------------------------
        # Create Raw and Parsed Data Products for the device
        #-------------------------------

        dp_obj = IonObject(RT.DataProduct,
                           name='the parsed data',
                           description='ctd stream test',
                           temporal_domain = tdom,
                           spatial_domain = sdom)

        data_product_id1 = self.DP.create_data_product(data_product=dp_obj,
                                                       stream_definition_id=parsed_stream_def_id,
                                                       parameter_dictionary=spdict_id)
        log.debug( 'new dp_id = %s', data_product_id1)

        self.DAMS.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id1)



        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'Data product streams1 = %s', stream_ids)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasDataset, RT.Dataset, True)
        log.debug( 'Data set for data_product_id1 = %s', dataset_ids[0])
        self.parsed_dataset = dataset_ids[0]
        #create the datastore at the beginning of each int test that persists data
        self._get_datastore(self.parsed_dataset)

        self.DP.activate_data_product_persistence(data_product_id=data_product_id1)


        dp_obj = IonObject(RT.DataProduct,
                           name='the raw data',
                           description='raw stream test',
                           temporal_domain = tdom,
                           spatial_domain = sdom)

        data_product_id2 = self.DP.create_data_product(data_product=dp_obj,
                                                       stream_definition_id=raw_stream_def_id,
                                                       parameter_dictionary=rpdict_id)
        log.debug( 'new dp_id = %s', str(data_product_id2))

        self.DAMS.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.DP.activate_data_product_persistence(data_product_id=data_product_id2)

        # spin up agent
        self.IMS.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)


        self.addCleanup(self.IMS.stop_instrument_agent_instance,
                        instrument_agent_instance_id=instAgentInstance_id)

        #wait for start
        instance_obj = self.IMS.read_instrument_agent_instance(instAgentInstance_id)
        gate = ProcessStateGate(self.PDC.read_process,
                                instance_obj.agent_process_id,
                                ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        instance_obj.agent_process_id)


        # take snapshot of config
        snap_id = self.IMS.save_resource_state(instDevice_id, "xyzzy snapshot")
        snap_obj = self.RR.read_attachment(snap_id, include_content=True)
        print "Saved config:"
        print snap_obj.content

        #modify config
        instance_obj.driver_config["comms_config"] = "BAD_DATA"
        self.RR.update(instance_obj)

        #restore config
        self.IMS.restore_resource_state(instDevice_id, snap_id)
        instance_obj = self.RR.read(instAgentInstance_id)
        self.assertNotEqual("BAD_DATA", instance_obj.driver_config["comms_config"])


