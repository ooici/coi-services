#!/usr/bin/env python
'''
@author Luke Campbell
@date Thu May  2 13:19:39 EDT 2013
@file ion/services/dm/test/test_instrument_integration.py
@description Integration Test that verifies instrument streaming through DM
'''

from ion.services.dm.test.dm_test_case import DMTestCase, FakeProcess
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from pyon.public import RT, IonObject, CFG
from interface.objects import StreamConfiguration, ProcessStateEnum, AgentCommand
from nose.plugins.attrib import attr
from pyon.agent.agent import ResourceAgentClient, ResourceAgentState, ResourceAgentEvent
import ion.agents.instrument.test.test_instrument_agent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent

@attr('INT', group='dm')
class TestInstrumentIntegration(DMTestCase):

    def create_instrument_model(self):
        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel")
        instrument_model_id = self.instrument_management.create_instrument_model(instModel_obj)
        self.addCleanup(self.instrument_management.delete_instrument_model, instrument_model_id)
        return instrument_model_id

    def create_instrument_agent(self, instrument_model_id):
        raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='raw', records_per_granule=2, granule_publish_rate=5 )
        parsed_config = StreamConfiguration(stream_name='parsed', parameter_dictionary_name='ctd_parsed_param_dict', records_per_granule=2, granule_publish_rate=5)
        # Create InstrumentAgent
        instagent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_uri="http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.0.1a-py2.7.egg",
                                  stream_configurations = [raw_config, parsed_config])
        instrument_agent_id = self.instrument_management.create_instrument_agent(instagent_obj)
        self.addCleanup(self.instrument_management.delete_instrument_agent, instrument_agent_id)
        self.instrument_management.assign_instrument_model_to_instrument_agent(instrument_model_id, instrument_agent_id)
        return instrument_agent_id

    def create_instrument_device(self, instrument_model_id):
        instDevice_obj = IonObject(RT.InstrumentDevice,
                                   name='SBE37IMDevice',
                                   description="SBE37IMDevice",
                                   serial_number="12345" )
        instrument_device_id = self.instrument_management.create_instrument_device(instrument_device=instDevice_obj)
        self.addCleanup(self.instrument_management.delete_instrument_device, instrument_device_id)
        self.instrument_management.assign_instrument_model_to_instrument_device(instrument_model_id, instrument_device_id)
        return instrument_device_id
        
    def create_instrument_agent_instance(self, instrument_agent_id, instrument_device_id):
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
                                          port_agent_config = port_agent_config,
                                            alerts= [])


        instrument_agent_instance_id = self.instrument_management.create_instrument_agent_instance(instAgentInstance_obj,
                                                                               instrument_agent_id,
                                                                               instrument_device_id)
        self.addCleanup(self.instrument_management.delete_instrument_agent_instance, instrument_agent_instance_id)
        return instrument_agent_instance_id

    def start_instrument_agent_instance(self, instrument_agent_instance_id):
        self.instrument_management.start_instrument_agent_instance(instrument_agent_instance_id)
        self.addCleanup(self.instrument_management.stop_instrument_agent_instance, instrument_agent_instance_id)

    def create_instrument_data_products(self, instrument_device_id):
        raw_dp_id = self.create_data_product('raw', param_dict_name='raw') 
        self.data_product_management.activate_data_product_persistence(raw_dp_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, raw_dp_id)
        self.data_acquisition_management.assign_data_product(input_resource_id=instrument_device_id, data_product_id=raw_dp_id)

        parsed_dp_id = self.create_data_product('parsed', param_dict_name='ctd_parsed_param_dict')
        self.data_product_management.activate_data_product_persistence(parsed_dp_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, parsed_dp_id)
        self.data_acquisition_management.assign_data_product(input_resource_id=instrument_device_id, data_product_id=parsed_dp_id)
        return raw_dp_id, parsed_dp_id

    def poll_instrument_agent_instance(self,instrument_agent_instance_id):
        inst_agent_instance_obj = self.instrument_management.read_instrument_agent_instance(instrument_agent_instance_id)
        gate = ProcessStateGate(self.process_dispatcher.read_process,
                                inst_agent_instance_obj.agent_process_id,
                                ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        inst_agent_instance_obj.agent_process_id)
        return inst_agent_instance_obj.agent_process_id

    def agent_state_transition(self, agent_client, agent_event, expected_state):
        cmd = AgentCommand(command=agent_event)
        retval = agent_client.execute_agent(cmd)
        state = agent_client.get_agent_state()
        self.assertEqual(expected_state,state)
        return retval

    def test_example(self):
        instrument_model_id = self.create_instrument_model()
        instrument_agent_id = self.create_instrument_agent(instrument_model_id)
        instrument_device_id = self.create_instrument_device(instrument_model_id)
        instrument_agent_instance_id = self.create_instrument_agent_instance(instrument_agent_id, instrument_device_id)

        raw_dp_id, parsed_dp_id = self.create_instrument_data_products(instrument_device_id)

        self.start_instrument_agent_instance(instrument_agent_instance_id)

        agent_process_id = self.poll_instrument_agent_instance(instrument_agent_instance_id)

        agent_client = ResourceAgentClient(instrument_device_id,
                                              to_name=agent_process_id,
                                              process=FakeProcess())

        self.agent_state_transition(agent_client, ResourceAgentEvent.INITIALIZE, ResourceAgentState.INACTIVE)
        self.agent_state_transition(agent_client, ResourceAgentEvent.GO_ACTIVE, ResourceAgentState.IDLE)
        self.agent_state_transition(agent_client, ResourceAgentEvent.RUN, ResourceAgentState.COMMAND)

        for i in xrange(10):
            agent_client.execute_resource(AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE))


