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

from pyon.public import RT, PRED, CFG
from pyon.public import IonObject, log
from pyon.datastore.datastore import DataStore

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin

from pyon.agent.agent import ResourceAgentClient, ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from interface.objects import Granule

from nose.plugins.attrib import attr
from mock import patch
import gevent

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('SMOKE', group='sa')
#@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
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
                                  stream_configuration= {'raw': 'ctd_raw_param_dict' , 'parsed': 'ctd_parsed_param_dict' })
        instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        print  'new InstrumentModel id = %s ' % instModel_id

        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_module="mi.instrument.seabird.sbe37smb.ooicore.driver",
                                  driver_class="SBE37Driver" )
        instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        print  'new InstrumentAgent id = %s' % instAgent_id

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        print 'test_activateInstrumentSample: Create instrument resource to represent the SBE37 ' +\
                 '(SA Req: L4-CI-SA-RQ-241) '
        instDevice_obj = IonObject(RT.InstrumentDevice,
                                   name='SBE37IMDevice',
                                   description="SBE37IMDevice",
                                   serial_number="12345" )
        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)

        print "test_activateInstrumentSample: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) " %\
                  instDevice_id

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
        print  'new dp_id = %s' % data_product_id1

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id1)



        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        print  'Data product streams1 = %s' % stream_ids

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasDataset, RT.Dataset, True)
        print  'Data set for data_product_id1 = %s' % dataset_ids[0]
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
        print  'new dp_id = %s' % str(data_product_id2)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasStream, None, True)
        print  'Data product streams2 = %s' % str(stream_ids)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasDataset, RT.Dataset, True)
        print  'Data set for data_product_id2 = %s' % dataset_ids[0]
        self.raw_dataset = dataset_ids[0]

        def start_instrument_agent():
            self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        gevent.joinall([gevent.spawn(start_instrument_agent)])

        self.addCleanup(self.imsclient.stop_instrument_agent_instance,
                        instrument_agent_instance_id=instAgentInstance_id)

        #wait for start
        instance_obj = self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
                                instance_obj.agent_process_id,
                                ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        instance_obj.agent_process_id)

        inst_agent_instance_obj = self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        print  'Instrument agent instance obj: = %s' % str(inst_agent_instance_obj)

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(instDevice_id,
                                              to_name=inst_agent_instance_obj.agent_process_id,
                                              process=FakeProcess())

        print "test_activateInstrumentSample: got ia client %s" % str(self._ia_client)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        print "test_activateInstrumentSample: initialize %s" % str(retval)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        print "(L4-CI-SA-RQ-334): Sending go_active command "
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client.execute_agent(cmd)
        print "test_activateInstrument: return value from go_active %s" % str(reply)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        print "(L4-CI-SA-RQ-334): current state after sending go_active command %s" % str(state)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client.execute_agent(cmd)
        print "test_activateInstrumentSample: run %s" % str(reply)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.PAUSE)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.STOPPED)

        cmd = AgentCommand(command=ResourceAgentEvent.RESUME)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=ResourceAgentEvent.CLEAR)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        retval = self._ia_client.execute_agent(cmd)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.COMMAND)

        cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
        retval = self._ia_client.execute_resource(cmd)
        print "test_activateInstrumentSample: return from sample %s" % str(retval)
        retval = self._ia_client.execute_resource(cmd)
        print "test_activateInstrumentSample: return from sample %s" % str(retval)
        retval = self._ia_client.execute_resource(cmd)
        print "test_activateInstrumentSample: return from sample %s" % str(retval)

        print "test_activateInstrumentSample: calling reset "
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client.execute_agent(cmd)
        print "test_activateInstrumentSample: return from reset %s" % str(reply)

        #--------------------------------------------------------------------------------
        # Now get the data in one chunk using an RPC Call to start_retreive
        #--------------------------------------------------------------------------------

        replay_data = self.dataretrieverclient.retrieve(self.parsed_dataset)
        self.assertIsInstance(replay_data, Granule)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        log.debug("RDT parsed: %s", str(rdt.pretty_print()) )
        temp_vals = rdt['temp']
        self.assertTrue(len(temp_vals) == 3)


        replay_data = self.dataretrieverclient.retrieve(self.raw_dataset)
        self.assertIsInstance(replay_data, Granule)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        log.debug("RDT raw: %s", str(rdt.pretty_print()) )

        raw_vals = rdt['raw']
        self.assertTrue(len(raw_vals) == 3)


#        #--------------------------------------------------------------------------------
#        # Get the extended data product to see if it contains the granules
#        #--------------------------------------------------------------------------------
#        extended_product = self.dpclient.get_data_product_extension(data_product_id1)
#        self.assertEqual(data_product_id1, extended_product._id)
#        log.debug( "test_activateInstrumentSample: extended_product.computed.last_granule.value %s", str(extended_product.computed.last_granule.value) )
#        log.debug( "test_activateInstrumentSample: extended_product.computed.recent_granules.value %s", str(extended_product.computed.recent_granules.value) )
#        log.debug("test_activateInstrumentSample: extended_product.computed.provenance_product_list.value %s", str(extended_product.computed.provenance_product_list.value) )



        #-------------------------------
        # Deactivate loggers
        #-------------------------------

        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)

