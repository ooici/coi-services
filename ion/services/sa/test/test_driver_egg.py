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

from ion.services.dm.utility.granule_utils import time_series_domain

from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from pyon.core.exception import ServerError

from pyon.public import RT, PRED, CFG
from pyon.public import IonObject, log
from pyon.datastore.datastore import DataStore
from pyon.event.event import EventPublisher

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.util.ion_time import IonTime
from pyon.util.containers import  get_ion_ts

from pyon.agent.agent import ResourceAgentClient, ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from interface.objects import Granule, DeviceStatusType, DeviceCommsType, StreamConfiguration
from interface.objects import AgentCommand, ProcessDefinition, ProcessStateEnum, ComputedStringValue

# Load current seabird egg.
from ion.agents.instrument.test.load_test_driver_egg import load_egg
load_egg()

# This import will dynamically load the driver egg.  It is needed for the MI includes below
import ion.agents.instrument.test.test_instrument_agent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
DRV_URI_GOOD = CFG.device.sbe37.dvr_egg
from ion.agents.instrument.test.agent_test_constants import DRV_URI_BAD, DRV_URI_404

import unittest

from nose.plugins.attrib import attr
from mock import patch
import gevent, time

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestDriverEgg(IonIntegrationTestCase):

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

        self.event_publisher = EventPublisher()



    def get_datastore(self, dataset_id):
        dataset = self.datasetclient.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore

    def get_streamConfigs(self):
        raw_config = StreamConfiguration(stream_name='raw',
                                         parameter_dictionary_name='ctd_raw_param_dict')

        parsed_config = StreamConfiguration(stream_name='parsed',
                                            parameter_dictionary_name='ctd_parsed_param_dict')

        return raw_config, parsed_config

    ##########################
    #
    #  The following tests generate different agent configs and pass them to a common base test script
    #
    ###########################

    @unittest.skip("this test can't be run from coi services. it is missing dependencies")
    def test_driverLaunchModuleNoURI(self):
        raw_config, parsed_config = self.get_streamConfigs()

        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_module="mi.instrument.seabird.sbe37smb.ooicore.driver",
                                  driver_class="SBE37Driver",
                                  stream_configurations = [raw_config, parsed_config])

        self.base_activateInstrumentSample(instAgent_obj)

    def test_driverLaunchModuleWithURI(self):
        raw_config, parsed_config = self.get_streamConfigs()

        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_module="mi.instrument.seabird.sbe37smb.ooicore.driver",
                                  driver_class="SBE37Driver",
                                  driver_uri=DRV_URI_GOOD,
                                  stream_configurations = [raw_config, parsed_config])

        self.base_activateInstrumentSample(instAgent_obj)

    def test_driverLaunchNoModuleOnlyURI(self):
        raw_config, parsed_config = self.get_streamConfigs()

        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  #driver_module="mi.instrument.seabird.sbe37smb.ooicore.driver",
                                  #driver_class="SBE37Driver",
                                  driver_uri=DRV_URI_GOOD,
                                  stream_configurations = [raw_config, parsed_config])

        self.base_activateInstrumentSample(instAgent_obj)

    def test_driverLaunchBogusModuleWithURI(self):
        raw_config, parsed_config = self.get_streamConfigs()

        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_module="bogus",
                                  driver_class="Bogus",
                                  driver_uri=DRV_URI_GOOD,
                                  stream_configurations = [raw_config, parsed_config])

        self.base_activateInstrumentSample(instAgent_obj)

    @unittest.skip("Launches an egg 'process' even though the egg download should produce error 404")
    def test_driverLaunchNoModule404URI(self):
        raw_config, parsed_config = self.get_streamConfigs()

        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  #driver_module="mi.instrument.seabird.sbe37smb.ooicore.driver",
                                  #driver_class="SBE37Driver",
                                  driver_uri=DRV_URI_404,
                                  stream_configurations = [raw_config, parsed_config])

        self.base_activateInstrumentSample(instAgent_obj, False)

    def test_driverLaunchNoModuleBadEggURI(self):
        raw_config, parsed_config = self.get_streamConfigs()
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  #driver_module="mi.instrument.seabird.sbe37smb.ooicore.driver",
                                  #driver_class="SBE37Driver",
                                  driver_uri=DRV_URI_BAD,
                                  stream_configurations = [raw_config, parsed_config])

        self.base_activateInstrumentSample(instAgent_obj, True, False)


    def base_activateInstrumentSample(self, instAgent_obj, expect_launch=True, expect_command=True):
        """
        This method runs a test of launching a driver with a given agent configuration
        """

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel")
        instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        print  'new InstrumentModel id = %s ' % instModel_id



        # Create InstrumentAgent

        instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        print  'new InstrumentAgent id = %s' % instAgent_id

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        instDevice_obj = IonObject(RT.InstrumentDevice,
                                   name='SBE37IMDevice',
                                   description="SBE37IMDevice",
                                   serial_number="12345" )
        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)


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


        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
                                                                               instAgent_id,
                                                                               instDevice_id)



        parsed_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',
                                                                                    id_only=True)
        raw_pdict_id    = self.dataset_management.read_parameter_dictionary_by_name('ctd_raw_param_dict',
                                                                                    id_only=True)

        parsed_stream_def_id = self.pubsubcli.create_stream_definition(name='parsed',
                                                                       parameter_dictionary_id=parsed_pdict_id)
        raw_stream_def_id    = self.pubsubcli.create_stream_definition(name='raw',
                                                                       parameter_dictionary_id=raw_pdict_id)


        #-------------------------------
        # Create Raw and Parsed Data Products for the device
        #-------------------------------

        dp_obj = IonObject(RT.DataProduct,
                           name='the parsed data',
                           description='ctd stream test')

        data_product_id1 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id)
        print  'new dp_id = %s' % data_product_id1
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1)

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


        dp_obj = IonObject(RT.DataProduct,
                           name='the raw data',
                           description='raw stream test')

        data_product_id2 = self.dpclient.create_data_product(data_product=dp_obj,
                                                             stream_definition_id=raw_stream_def_id)
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

        # add start/stop for instrument agent
        gevent.joinall([gevent.spawn(lambda:
            self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id))])
        self.addCleanup(self.imsclient.stop_instrument_agent_instance,
                        instrument_agent_instance_id=instAgentInstance_id)

        #wait for start
        inst_agent_instance_obj = self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        agent_process_id = ResourceAgentClient._get_agent_process_id(instDevice_id)

        print "Agent process id is '%s'" % str(agent_process_id)
        self.assertTrue(agent_process_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
                                agent_process_id,
                                ProcessStateEnum.RUNNING)

        if not expect_launch:
            self.assertFalse(gate.await(30), "The instance (%s) of bogus instrument agent spawned in 30 seconds ?!?" %
                                             agent_process_id)
            return

        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        agent_process_id)


        print "Instrument Agent Instance successfully triggered ProcessStateGate as RUNNING"

        #print  'Instrument agent instance obj: = %s' % str(inst_agent_instance_obj)

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(instDevice_id,
                                              to_name=agent_process_id,
                                              process=FakeProcess())

        print "ResourceAgentClient created: %s" % str(self._ia_client)

        print "Sending command=ResourceAgentEvent.INITIALIZE"
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)

        if not expect_command:
            self.assertRaises(ServerError, self._ia_client.execute_agent, cmd)
            return

        retval = self._ia_client.execute_agent(cmd)
        print "Result of INITIALIZE: %s" % str(retval)
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)

        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertTrue(state, 'DRIVER_STATE_COMMAND')

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)

        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)

        # This gevent sleep is there to test the autosample time, which will show something different from default
        # only if the instrument runs for over a minute
        gevent.sleep(90)

        extended_instrument = self.imsclient.get_instrument_device_extension(instrument_device_id=instDevice_id)

        self.assertIsInstance(extended_instrument.computed.uptime, ComputedStringValue)

        autosample_string = extended_instrument.computed.uptime.value
        autosampling_time = int(autosample_string.split()[4])

        self.assertTrue(autosampling_time > 0)

        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)

        print "Sending command=ResourceAgentEvent.RESET"
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client.execute_agent(cmd)
        print "Result of RESET: %s" % str(reply)

