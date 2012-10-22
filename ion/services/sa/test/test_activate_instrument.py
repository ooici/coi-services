#!/usr/bin/env python


from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient

from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
from ion.services.dm.utility.granule_utils import time_series_domain

from interface.objects import AgentCommand
from interface.objects import ProcessDefinition
from interface.objects import ProcessStateEnum

from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.agents.port.port_agent_process import PortAgentProcessType

from pyon.public import RT, PRED
from pyon.public import log, IonObject
from pyon.datastore.datastore import DataStore

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin

from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentEvent


from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from interface.objects import Granule

from nose.plugins.attrib import attr

import gevent

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('SMOKE', group='sa')
class TestActivateInstrumentIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataretrieverclient = DataRetrieverServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()
        
        #setup listerner vars
        self._data_greenlets = []
        self._no_samples = None
        self._samples_received = []


    def create_logger(self, name, stream_id=''):

        # logger process
        producer_definition = ProcessDefinition(name=name+'_logger')
        producer_definition.executable = {
            'module':'ion.processes.data.stream_granule_logger',
            'class':'StreamGranuleLogger'
        }

        logger_procdef_id = self.processdispatchclient.create_process_definition(process_definition=producer_definition)
        configuration = {
            'process':{
                'stream_id':stream_id,
                }
        }
        pid = self.processdispatchclient.schedule_process(process_definition_id=logger_procdef_id,
                                                          configuration=configuration)

        return pid

    def get_datastore(self, dataset_id):
        dataset = self.datasetclient.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore


    #@unittest.skip("TBD")
    def test_activateInstrumentSample(self):

        self.loggerpids = []

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel",
                                  model="SBE37IMModel",
                                  stream_configuration= {'raw': 'ctd_raw_param_dict' , 'parsed': 'ctd_parsed_param_dict' })
        instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        log.debug( 'new InstrumentModel id = %s ', instModel_id)

        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_module="ion.agents.instrument.instrument_agent",
                                  driver_class="InstrumentAgent" )
        instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        log.debug( 'new InstrumentAgent id = %s', instAgent_id)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        log.debug('test_activateInstrumentSample: Create instrument resource to represent the SBE37 '
                + '(SA Req: L4-CI-SA-RQ-241) ')
        instDevice_obj = IonObject(RT.InstrumentDevice,
                                   name='SBE37IMDevice',
                                   description="SBE37IMDevice",
                                   serial_number="12345" )
        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)

        log.debug("test_activateInstrumentSample: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) ",
                  instDevice_id)

        port_agent_config = {
            'device_addr': 'sbe37-simulator.oceanobservatories.org',
            'device_port': 4001,
            'process_type': PortAgentProcessType.UNIX,
            'binary_path': "port_agent",
            'command_port': 4002,
            'data_port': 4003,
            'log_level': 5,
        }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance',
                                          description="SBE37IMAgentInstance",
                                          driver_module='mi.instrument.seabird.sbe37smb.ooicore.driver',
                                          driver_class='SBE37Driver',
                                          comms_device_address='sbe37-simulator.oceanobservatories.org',
                                          comms_device_port=4001,
                                          port_agent_config = port_agent_config)


        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
                                                                               instAgent_id,
                                                                               instDevice_id)

        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()


        parsed_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        parsed_stream_def_id = self.pubsubcli.create_stream_definition(name='parsed', parameter_dictionary_id=parsed_pdict_id)

        raw_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_raw_param_dict', id_only=True)
        raw_stream_def_id = self.pubsubcli.create_stream_definition(name='raw', parameter_dictionary_id=raw_pdict_id)


        #-------------------------------
        # Create Raw and Parsed Data Products for the device
        #-------------------------------

        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id1 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id)
        log.debug( 'new dp_id = %s', data_product_id1)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id1)



        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'Data product streams1 = %s', stream_ids)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasDataset, RT.Dataset, True)
        log.debug( 'Data set for data_product_id1 = %s', dataset_ids[0])
        self.parsed_dataset = dataset_ids[0]
        #create the datastore at the beginning of each int test that persists data
        self.get_datastore(self.parsed_dataset)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1)

        pid = self.create_logger('ctd_parsed', stream_ids[0] )
        self.loggerpids.append(pid)


        dp_obj = IonObject(RT.DataProduct,
            name='the raw data',
            description='raw stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id2 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id)
        log.debug( 'new dp_id = %s', str(data_product_id2))

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasStream, None, True)
        log.debug( 'Data product streams2 = %s', str(stream_ids))

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasDataset, RT.Dataset, True)
        log.debug( 'Data set for data_product_id2 = %s', dataset_ids[0])
        self.raw_dataset = dataset_ids[0]

#        #-------------------------------
#        # L0 Conductivity - Temperature - Pressure: Data Process Definition
#        #-------------------------------
#        log.debug("test_activateInstrumentSample: create data process definition ctd_L0_all")
#        dpd_obj = IonObject(RT.DataProcessDefinition,
#                            name='ctd_L0_all',
#                            description='transform ctd package into three separate L0 streams',
#                            module='ion.processes.data.transforms.ctd.ctd_L0_all',
#                            class_name='ctd_L0_all',
#                            process_source='some_source_reference')
#        ctd_L0_all_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
#
#
#
#        #-------------------------------
#        # L0 Conductivity - Temperature - Pressure: Output Data Products
#        #-------------------------------
#
#        outgoing_stream_l0_conductivity_id = self.pubsubcli.create_stream_definition(
#            name='L0_Conductivity')
#        self.dataprocessclient.assign_stream_definition_to_data_process_definition(
#            outgoing_stream_l0_conductivity_id,
#            ctd_L0_all_dprocdef_id )
#
#        outgoing_stream_l0_pressure_id = self.pubsubcli.create_stream_definition(
#            name='L0_Pressure')
#        self.dataprocessclient.assign_stream_definition_to_data_process_definition(
#            outgoing_stream_l0_pressure_id,
#            ctd_L0_all_dprocdef_id )
#
#        outgoing_stream_l0_temperature_id = self.pubsubcli.create_stream_definition(
#            name='L0_Temperature')
#        self.dataprocessclient.assign_stream_definition_to_data_process_definition(
#            outgoing_stream_l0_temperature_id,
#            ctd_L0_all_dprocdef_id )
#
#
#        self.output_products={}
#        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 conductivity")
#
#        ctd_l0_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
#                                                        name='L0_Conductivity',
#                                                        description='transform output conductivity',
#                                                        temporal_domain = tdom,
#                                                        spatial_domain = sdom)
#
#        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(
#            ctd_l0_conductivity_output_dp_obj,
#            outgoing_stream_l0_conductivity_id,
#            parameter_dictionary)
#        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id
#        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_conductivity_output_dp_id)
#
#        stream_ids, _ = self.rrclient.find_objects(ctd_l0_conductivity_output_dp_id, PRED.hasStream, None, True)
#        log.debug(" ctd_l0_conductivity stream id =  %s", str(stream_ids) )
#        pid = self.create_logger(' ctd_l1_conductivity', stream_ids[0] )
#        self.loggerpids.append(pid)
#
#        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 pressure")
#
#        ctd_l0_pressure_output_dp_obj = IonObject(  RT.DataProduct,
#                                                    name='L0_Pressure',
#                                                    description='transform output pressure',
#                                                    temporal_domain = tdom,
#                                                    spatial_domain = sdom)
#
#        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(  ctd_l0_pressure_output_dp_obj,
#                                                                                    outgoing_stream_l0_pressure_id,
#                                                                                    parameter_dictionary)
#        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id
#        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_pressure_output_dp_id)
#
#        stream_ids, _ = self.rrclient.find_objects(ctd_l0_pressure_output_dp_id, PRED.hasStream, None, True)
#        log.debug(" ctd_l0_pressure stream id =  %s", str(stream_ids) )
#        pid = self.create_logger(' ctd_l0_pressure', stream_ids[0] )
#        self.loggerpids.append(pid)
#
#        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 temperature")
#
#        ctd_l0_temperature_output_dp_obj = IonObject(   RT.DataProduct,
#                                                        name='L0_Temperature',
#                                                        description='transform output temperature',
#                                                        temporal_domain = tdom,
#                                                        spatial_domain = sdom)
#
#        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj,
#                                                                                    outgoing_stream_l0_temperature_id,
#                                                                                    parameter_dictionary)
#        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id
#        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_temperature_output_dp_id)
#
#        stream_ids, _ = self.rrclient.find_objects(ctd_l0_temperature_output_dp_id, PRED.hasStream, None, True)
#        log.debug(" ctd_l0_temperature stream id =  %s", str(stream_ids) )
#        pid = self.create_logger(' ctd_l0_temperature', stream_ids[0] )
#        self.loggerpids.append(pid)
#
#        #-------------------------------
#        # L0 Conductivity - Temperature - Pressure: Create the data process
#        #-------------------------------
#        log.debug("test_activateInstrumentSample: create L0 all data_process start")
#        ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(
#                ctd_L0_all_dprocdef_id,
#                [data_product_id1],
#                self.output_products)
#        self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)
#
#        log.debug("test_createTransformsThenActivateInstrument: create L0 all data_process return")

        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        gevent.sleep(2)

        #wait for start
        instance_obj = self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
                                instance_obj.agent_process_id,
                                ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        instance_obj.agent_process_id)

        inst_agent_instance_obj = self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        log.debug( 'Instrument agent instance obj: = %s', str(inst_agent_instance_obj))

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(instDevice_id,
                                              to_name=inst_agent_instance_obj.agent_process_id,
                                              process=FakeProcess())

        log.debug("test_activateInstrumentSample: got ia client %s", str(self._ia_client))

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: initialize %s", str(retval))


        log.debug("(L4-CI-SA-RQ-334): Sending go_active command ")
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: return value from go_active %s", str(reply))

        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("(L4-CI-SA-RQ-334): current state after sending go_active command %s", str(state))

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: run %s", str(reply))

        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        log.debug("test_activateInstrumentSample: return from sample %s", str(retval))
        retval = self._ia_client.execute_resource(cmd)
        log.debug("test_activateInstrumentSample: return from sample %s", str(retval))
        retval = self._ia_client.execute_resource(cmd)
        log.debug("test_activateInstrumentSample: return from sample %s", str(retval))


#        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
#        retval = self._ia_client.execute_resource(cmd)
#        log.debug("test_activateInstrumentSample: return from START_AUTOSAMPLE: %s", str(retval))
#
#        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
#        retval = self._ia_client.execute_resource(cmd)
#        log.debug("test_activateInstrumentSample: return from STOP_AUTOSAMPLE: %s", str(retval))

        log.debug("test_activateInstrumentSample: calling reset ")
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: return from reset %s", str(reply))

        #--------------------------------------------------------------------------------
        # Now get the data in one chunk using an RPC Call to start_retreive
        #--------------------------------------------------------------------------------

        replay_data = self.dataretrieverclient.retrieve(self.parsed_dataset)
        self.assertIsInstance(replay_data, Granule)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        log.info('RDT parsed: %s' % rdt.pretty_print())
        temp_vals = rdt['temp']
        self.assertTrue(len(temp_vals) == 3)


        replay_data = self.dataretrieverclient.retrieve(self.raw_dataset)
        self.assertIsInstance(replay_data, Granule)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        log.info('RDT raw: %s' % rdt.pretty_print())

        raw_vals = rdt['raw']
        self.assertTrue(len(raw_vals) == 3)

        #-------------------------------
        # Deactivate InstrumentAgentInstance
        #-------------------------------
        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)

