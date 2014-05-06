from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

#from pyon.ion.endpoint import ProcessRPCClient
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.services.sa.instrument.agent_configuration_builder import PlatformAgentConfigurationBuilder, InstrumentAgentConfigurationBuilder
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.agent.agent import ResourceAgentClient
from pyon.core.exception import NotFound

from pyon.public import IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.dm.utility.granule_utils import time_series_domain


from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.objects import ProcessStateEnum, StreamConfiguration, PortTypeEnum


from pyon.public import RT, OT, PRED, CFG
from nose.plugins.attrib import attr
from ooi.logging import log
import unittest
import time
import calendar

DRV_URI_GOOD = CFG.device.sbe37.dvr_egg


from ion.services.sa.test.helpers import any_old, AgentProcessStateGate


@attr('INT', group='sa')
class TestAgentLaunchOps(IonIntegrationTestCase):

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


    def test_get_agent_client_noprocess(self):
        inst_device_id = self.RR2.create(any_old(RT.InstrumentDevice))

        iap = ResourceAgentClient._get_agent_process_id(inst_device_id)

        # should be no running agent
        self.assertIsNone(iap)

        # should raise NotFound
        self.assertRaises(NotFound, ResourceAgentClient, inst_device_id)



    def test_resource_state_save_restore(self):

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel")
        instModel_id = self.IMS.create_instrument_model(instModel_obj)
        log.debug( 'new InstrumentModel id = %s ', instModel_id)

        # Create InstrumentAgent
        raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='ctd_raw_param_dict' )
        parsed_config = StreamConfiguration(stream_name='parsed', parameter_dictionary_name='ctd_parsed_param_dict')
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_uri=DRV_URI_GOOD,
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
                                          port_agent_config = port_agent_config)


        instAgentInstance_id = self.IMS.create_instrument_agent_instance(instAgentInstance_obj,
                                                                               instAgent_id,
                                                                               instDevice_id)


        spdict_id = self.DSC.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        parsed_stream_def_id = self.PSC.create_stream_definition(name='parsed', parameter_dictionary_id=spdict_id)

        rpdict_id = self.DSC.read_parameter_dictionary_by_name('ctd_raw_param_dict', id_only=True)
        raw_stream_def_id = self.PSC.create_stream_definition(name='raw', parameter_dictionary_id=rpdict_id)


        #-------------------------------
        # Create Raw and Parsed Data Products for the device
        #-------------------------------

        dp_obj = IonObject(RT.DataProduct,
                           name='the parsed data',
                           description='ctd stream test')

        data_product_id1 = self.DP.create_data_product(data_product=dp_obj,
                                                       stream_definition_id=parsed_stream_def_id)
                                                       
        log.debug( 'new dp_id = %s', data_product_id1)

        self.DAMS.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id1)
        self.DP.activate_data_product_persistence(data_product_id=data_product_id1)
        self.addCleanup(self.DP.suspend_data_product_persistence, data_product_id1)



        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'Data product streams1 = %s', stream_ids)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.RR.find_objects(data_product_id1, PRED.hasDataset, RT.Dataset, True)
        log.debug( 'Data set for data_product_id1 = %s', dataset_ids[0])
        self.parsed_dataset = dataset_ids[0]
        #create the datastore at the beginning of each int test that persists data



        dp_obj = IonObject(RT.DataProduct,
                           name='the raw data',
                           description='raw stream test')

        data_product_id2 = self.DP.create_data_product(data_product=dp_obj,
                                                       stream_definition_id=raw_stream_def_id)
        log.debug( 'new dp_id = %s', str(data_product_id2))

        self.DAMS.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.DP.activate_data_product_persistence(data_product_id=data_product_id2)
        self.addCleanup(self.DP.suspend_data_product_persistence, data_product_id2)

        # spin up agent
        self.IMS.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)


        self.addCleanup(self.IMS.stop_instrument_agent_instance,
                        instrument_agent_instance_id=instAgentInstance_id)

        #wait for start
        instance_obj = self.IMS.read_instrument_agent_instance(instAgentInstance_id)
        gate = AgentProcessStateGate(self.PDC.read_process,
                                     instDevice_id,
                                     ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        gate.process_id)


        # take snapshot of config
        snap_id = self.IMS.save_resource_state(instDevice_id, "xyzzy snapshot")
        snap_obj = self.RR.read_attachment(snap_id, include_content=True)


        #modify config
        instance_obj.driver_config["comms_config"] = "BAD_DATA"
        self.RR.update(instance_obj)

        #restore config
        self.IMS.restore_resource_state(instDevice_id, snap_id)
        instance_obj = self.RR.read(instAgentInstance_id)

        if "BAD_DATA" == instance_obj.driver_config["comms_config"]:
            print "Saved config:"
            print snap_obj.content
            self.fail("Saved config was not properly restored")

        self.assertNotEqual("BAD_DATA", instance_obj.driver_config["comms_config"])

        
        self.DP.delete_data_product(data_product_id1)
        self.DP.delete_data_product(data_product_id2)


    def test_agent_instance_config_hasDevice(self):
        def assign_fn(child_device_id, parent_device_id):
            self.RR2.create_association(parent_device_id, PRED.hasDevice, child_device_id)

        def find_fn(parent_device_id):
            ret, _ = self.RR.find_objects(subject=parent_device_id, predicate=PRED.hasDevice, id_only=True)
            return ret

        self.base_agent_instance_config(assign_fn, find_fn)
        log.info("END test_agent_instance_config_hasDevice")

    def test_agent_instance_config_hasNetworkParent(self):
        def assign_fn(child_device_id, parent_device_id):
            self.RR2.create_association(child_device_id, PRED.hasNetworkParent, parent_device_id)

        def find_fn(parent_device_id):
            ret, _ = self.RR.find_subjects(object=parent_device_id, predicate=PRED.hasNetworkParent, id_only=True)
            return ret

        self.base_agent_instance_config(assign_fn, find_fn)
        log.info("END test_agent_instance_config_hasNetworkParent")

    def base_agent_instance_config(self, 
                                   assign_child_platform_to_parent_platform_fn, 
                                   find_child_platform_ids_of_parent_platform_fn):
        """
        Verify that agent configurations are being built properly
        """
        clients = DotDict()
        clients.resource_registry  = self.RR
        clients.pubsub_management  = self.PSC
        clients.dataset_management = self.DSC
        config_builder = DotDict
        config_builder.i = None
        config_builder.p = None

        def refresh_pconfig_builder_hack(config_builder):
            """
            ugly hack to get around "idempotent" RR2 caching
            remove after https://github.com/ooici/coi-services/pull/1190
            """
            config_builder.p = PlatformAgentConfigurationBuilder(clients)

        def refresh_iconfig_builder_hack(config_builder):
            """
            ugly hack to get around "idempotent" RR2 caching
            remove after https://github.com/ooici/coi-services/pull/1190
            """
            config_builder.i = InstrumentAgentConfigurationBuilder(clients)



        org_obj = any_old(RT.Org)
        org_id = self.RR2.create(org_obj)

        inst_startup_config = {'startup': 'config'}

        generic_alerts_config = [ {'lvl2': 'lvl3val'} ]

        required_config_keys = [
            'org_governance_name',
            'device_type',
            'agent',
            'driver_config',
            'stream_config',
            'startup_config',
            'aparam_alerts_config',
            'children']



        def verify_instrument_config(config, device_id):
            for key in required_config_keys:
                self.assertIn(key, config)
            self.assertEqual(org_obj.org_governance_name, config['org_governance_name'])
            self.assertEqual(RT.InstrumentDevice, config['device_type'])
            self.assertIn('driver_config', config)
            driver_config = config['driver_config']
            expected_driver_fields = {'process_type': ('ZMQPyClassDriverLauncher',),
                                      }
            for k, v in expected_driver_fields.iteritems():
                self.assertIn(k, driver_config)
                self.assertEqual(v, driver_config[k])
            self.assertEqual

            self.assertEqual({'resource_id': device_id}, config['agent'])
            self.assertEqual(inst_startup_config, config['startup_config'])
            self.assertIn('aparam_alerts_config', config)
            self.assertEqual(generic_alerts_config, config['aparam_alerts_config'])
            self.assertIn('stream_config', config)
            for key in ['children']:
                self.assertEqual({}, config[key])


        # TODO(OOIION-1495) review the asserts below related with
        # requiring 'ports' to be present in the driver_config.
        # See recent adjustment in agent_configuration_builder.py,
        # which I did to avoid other tests to fail.
        # The asserts below would make the following tests fail:
        #  test_agent_instance_config_hasDevice
        #  test_agent_instance_config_hasNetworkParent

        def verify_child_config(config, device_id, inst_device_id=None):
            for key in required_config_keys:
                self.assertIn(key, config)
            self.assertEqual(org_obj.org_governance_name, config['org_governance_name'])
            self.assertEqual(RT.PlatformDevice, config['device_type'])
            self.assertEqual({'resource_id': device_id}, config['agent'])
            self.assertIn('aparam_alerts_config', config)
            self.assertEqual(generic_alerts_config, config['aparam_alerts_config'])
            self.assertIn('stream_config', config)
            self.assertIn('driver_config', config)
            self.assertIn('foo', config['driver_config'])
            # TODO(OOIION-1495) review "ports" aspect in driver config
            """
            self.assertIn('ports', config['driver_config'])
            """
            self.assertEqual('bar', config['driver_config']['foo'])
            self.assertIn('process_type', config['driver_config'])
            self.assertEqual(('ZMQPyClassDriverLauncher',), config['driver_config']['process_type'])

            if None is inst_device_id:
                for key in ['children', 'startup_config']:
                    self.assertEqual({}, config[key])
            else:
                for key in ['startup_config']:
                    self.assertEqual({}, config[key])

                self.assertIn(inst_device_id, config['children'])
                verify_instrument_config(config['children'][inst_device_id], inst_device_id)

            # TODO(OOIION-1495) review "ports" aspect in driver config
            """
            if config['driver_config']['ports']:
                self.assertTrue( isinstance(config['driver_config']['ports'], dict) )
            """

        def verify_parent_config(config, parent_device_id, child_device_id, inst_device_id=None):
            for key in required_config_keys:
                self.assertIn(key, config)
            self.assertEqual(org_obj.org_governance_name, config['org_governance_name'])
            self.assertEqual(RT.PlatformDevice, config['device_type'])
            self.assertIn('process_type', config['driver_config'])
            # TODO(OOIION-1495) review "ports" aspect in driver config
            """
            self.assertIn('ports', config['driver_config'])
            """
            self.assertEqual(('ZMQPyClassDriverLauncher',), config['driver_config']['process_type'])
            self.assertEqual({'resource_id': parent_device_id}, config['agent'])
            self.assertIn('aparam_alerts_config', config)
            self.assertEqual(generic_alerts_config, config['aparam_alerts_config'])
            self.assertIn('stream_config', config)
            for key in ['startup_config']:
                self.assertEqual({}, config[key])

            # TODO(OOIION-1495) review "ports" aspect in driver config
            """
            if config['driver_config']['ports']:
                self.assertTrue( isinstance(config['driver_config']['ports'], dict) )
            """
            self.assertIn(child_device_id, config['children'])
            verify_child_config(config['children'][child_device_id], child_device_id, inst_device_id)






        rpdict_id = self.DSC.read_parameter_dictionary_by_name('ctd_raw_param_dict', id_only=True)
        raw_stream_def_id = self.PSC.create_stream_definition(name='raw', parameter_dictionary_id=rpdict_id)
        #todo: create org and figure out which agent resource needs to get assigned to it


        def _make_platform_agent_structure(name='', agent_config=None):
            if None is agent_config: agent_config = {}

            # instance creation
            platform_agent_instance_obj = any_old(RT.PlatformAgentInstance, {'driver_config': {'foo': 'bar'},
                                                                             'alerts': generic_alerts_config})
            platform_agent_instance_obj.agent_config = agent_config
            platform_agent_instance_id = self.IMS.create_platform_agent_instance(platform_agent_instance_obj)

            # agent creation
            raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='ctd_raw_param_dict' )
            platform_agent_obj = any_old(RT.PlatformAgent, {"stream_configurations":[raw_config]})
            platform_agent_id = self.IMS.create_platform_agent(platform_agent_obj)

            # device creation
            platform_device_id = self.IMS.create_platform_device(any_old(RT.PlatformDevice))

            # data product creation
            dp_obj = any_old(RT.DataProduct)
            dp_id = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id)
            self.DAMS.assign_data_product(input_resource_id=platform_device_id, data_product_id=dp_id)
            self.DP.activate_data_product_persistence(data_product_id=dp_id)
            self.addCleanup(self.DP.suspend_data_product_persistence, dp_id)


            #deployment creation
            site_obj = IonObject(RT.PlatformSite, name='sitePlatform')
            site_id = self.OMS.create_platform_site(platform_site=site_obj)

            # find current deployment using time constraints
            current_time =  int( calendar.timegm(time.gmtime()) )
            # two years on either side of current time
            start = current_time - 63115200
            end = current_time + 63115200
            temporal_bounds = IonObject(OT.TemporalBounds, name='planned', start_datetime=str(start), end_datetime=str(end))
            platform_port_obj= IonObject(OT.PlatformPort, reference_designator = 'GA01SUMO-FI003-09-CTDMO0999',
                                                            port_type=PortTypeEnum.UPLINK,
                                                            ip_address=0)
            deployment_obj = IonObject(RT.Deployment,
                                       name='TestPlatformDeployment_' + name,
                                       description='some new deployment',
                                       context=IonObject(OT.CabledNodeDeploymentContext),
                                       constraint_list=[temporal_bounds],
                                       port_assignments={platform_device_id:platform_port_obj})

            deploy_id = self.OMS.create_deployment(deployment=deployment_obj, site_id=site_id, device_id=platform_device_id)

            # assignments
            self.RR2.assign_platform_agent_instance_to_platform_device_with_has_agent_instance(platform_agent_instance_id, platform_device_id)
            self.RR2.assign_platform_agent_to_platform_agent_instance_with_has_agent_definition(platform_agent_id, platform_agent_instance_id)
            self.RR2.assign_platform_device_to_org_with_has_resource(platform_agent_instance_id, org_id)

            return platform_agent_instance_id, platform_agent_id, platform_device_id


        def _make_instrument_agent_structure(agent_config=None):
            if None is agent_config: agent_config = {}

            # instance creation
            instrument_agent_instance_obj = any_old(RT.InstrumentAgentInstance, {"startup_config": inst_startup_config,
                                                                                 'alerts': generic_alerts_config})
            instrument_agent_instance_obj.agent_config = agent_config
            instrument_agent_instance_id = self.IMS.create_instrument_agent_instance(instrument_agent_instance_obj)

            # agent creation
            raw_config = StreamConfiguration(stream_name='raw',
                                             parameter_dictionary_name='ctd_raw_param_dict' )
            instrument_agent_obj = any_old(RT.InstrumentAgent, {"stream_configurations":[raw_config]})
            instrument_agent_id = self.IMS.create_instrument_agent(instrument_agent_obj)

            # device creation
            instrument_device_id = self.IMS.create_instrument_device(any_old(RT.InstrumentDevice))

            # data product creation
            dp_obj = any_old(RT.DataProduct)
            dp_id = self.DP.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id)
            self.DAMS.assign_data_product(input_resource_id=instrument_device_id, data_product_id=dp_id)
            self.DP.activate_data_product_persistence(data_product_id=dp_id)
            self.addCleanup(self.DP.suspend_data_product_persistence, dp_id)

            #deployment creation
            site_obj = IonObject(RT.InstrumentSite, name='siteInstrument')
            site_id = self.OMS.create_instrument_site(instrument_site =site_obj)

            # find current deployment using time constraints
            current_time =  int( calendar.timegm(time.gmtime()) )
            # two years on either side of current time
            start = current_time - 63115200
            end = current_time + 63115200
            temporal_bounds = IonObject(OT.TemporalBounds, name='planned', start_datetime=str(start), end_datetime=str(end))
            platform_port_obj= IonObject(OT.PlatformPort, reference_designator = 'GA01SUMO-FI003-08-CTDMO0888',
                                                            port_type=PortTypeEnum.PAYLOAD,
                                                            ip_address=0)
            deployment_obj = IonObject(RT.Deployment,
                                       name='TestDeployment for Cabled Instrument',
                                       description='some new deployment',
                                       context=IonObject(OT.CabledInstrumentDeploymentContext),
                                       constraint_list=[temporal_bounds],
                                       port_assignments={instrument_device_id:platform_port_obj})

            deploy_id = self.OMS.create_deployment(deployment=deployment_obj, site_id=site_id, device_id=instrument_device_id)


            # assignments
            self.RR2.assign_instrument_agent_instance_to_instrument_device_with_has_agent_instance(instrument_agent_instance_id, instrument_device_id)
            self.RR2.assign_instrument_agent_to_instrument_agent_instance_with_has_agent_definition(instrument_agent_id, instrument_agent_instance_id)
            self.RR2.assign_instrument_device_to_org_with_has_resource(instrument_agent_instance_id, org_id)

            return instrument_agent_instance_id, instrument_agent_id, instrument_device_id



        # can't do anything without an agent instance obj
        log.debug("Testing that preparing a launcher without agent instance raises an error")
        refresh_pconfig_builder_hack(config_builder) # associations have changed since builder was instantiated
        self.assertRaises(AssertionError, config_builder.p.prepare, will_launch=False)

        log.debug("Making the structure for a platform agent, which will be the child")
        platform_agent_instance_child_id, _, platform_device_child_id  = _make_platform_agent_structure(name='child')
        platform_agent_instance_child_obj = self.RR2.read(platform_agent_instance_child_id)

        log.debug("Preparing a valid agent instance launch, for config only")
        refresh_pconfig_builder_hack(config_builder) # associations have changed since builder was instantiated
        config_builder.p.set_agent_instance_object(platform_agent_instance_child_obj)
        child_config = config_builder.p.prepare(will_launch=False)
        verify_child_config(child_config, platform_device_child_id)


        log.debug("Making the structure for a platform agent, which will be the parent")
        platform_agent_instance_parent_id, _, platform_device_parent_id  = _make_platform_agent_structure(name='parent')
        platform_agent_instance_parent_obj = self.RR2.read(platform_agent_instance_parent_id)

        log.debug("Testing child-less parent as a child config")
        refresh_pconfig_builder_hack(config_builder) # associations have changed since builder was instantiated
        config_builder.p.set_agent_instance_object(platform_agent_instance_parent_obj)
        parent_config = config_builder.p.prepare(will_launch=False)
        verify_child_config(parent_config, platform_device_parent_id)

        log.debug("assigning child platform to parent")
        assign_child_platform_to_parent_platform_fn(platform_device_child_id, platform_device_parent_id)

        child_device_ids = find_child_platform_ids_of_parent_platform_fn(platform_device_parent_id)
        self.assertNotEqual(0, len(child_device_ids))

        log.debug("Testing parent + child as parent config")
        refresh_pconfig_builder_hack(config_builder) # associations have changed since builder was instantiated
        config_builder.p.set_agent_instance_object(platform_agent_instance_parent_obj)
        parent_config = config_builder.p.prepare(will_launch=False)
        verify_parent_config(parent_config, platform_device_parent_id, platform_device_child_id)


        log.debug("making the structure for an instrument agent")
        instrument_agent_instance_id, _, instrument_device_id = _make_instrument_agent_structure()
        instrument_agent_instance_obj = self.RR2.read(instrument_agent_instance_id)

        log.debug("Testing instrument config")
        refresh_iconfig_builder_hack(config_builder) # associations have changed since builder was instantiated
        config_builder.i.set_agent_instance_object(instrument_agent_instance_obj)
        instrument_config = config_builder.i.prepare(will_launch=False)
        verify_instrument_config(instrument_config, instrument_device_id)

        log.debug("assigning instrument to platform")
        self.RR2.assign_instrument_device_to_platform_device_with_has_device(instrument_device_id, platform_device_child_id)

        child_device_ids = self.RR2.find_instrument_device_ids_of_platform_device_using_has_device(platform_device_child_id)
        self.assertNotEqual(0, len(child_device_ids))

        log.debug("Testing entire config")
        refresh_pconfig_builder_hack(config_builder) # associations have changed since builder was instantiated
        config_builder.p.set_agent_instance_object(platform_agent_instance_parent_obj)
        full_config = config_builder.p.prepare(will_launch=False)
        verify_parent_config(full_config, platform_device_parent_id, platform_device_child_id, instrument_device_id)

        #self.fail(parent_config)
        #plauncher.prepare(will_launch=False)
        log.info("END base_agent_instance_config")
