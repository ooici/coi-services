#!/usr/bin/env python

import gevent
import sys

from pyon.util.containers import DotDict
from pyon.util.poller import poll
from pyon.core.bootstrap import get_sys_name
from gevent.event import AsyncResult

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient


from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.inventory.index_management_service import IndexManagementService

from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.services.sa.test.helpers import AgentProcessStateGate
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.agents.instrument.driver_process import ZMQEggDriverProcess, DriverProcessType

from pyon.public import RT, PRED
from pyon.core.bootstrap import CFG
from pyon.public import IonObject, log
from pyon.datastore.datastore import DataStore
from pyon.event.event import EventPublisher, EventSubscriber

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.util.containers import  get_ion_ts

from pyon.agent.agent import ResourceAgentClient, ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
import unittest, os
from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from interface.objects import Granule, DeviceStatusType, DeviceCommsType, StreamConfiguration
from interface.objects import AgentCommand, ProcessDefinition, ProcessStateEnum
# Alarm types and events.
from interface.objects import StreamAlertType,AggregateStatusType, DeviceStatusType

from nose.plugins.attrib import attr
from mock import patch


# A seabird driver.
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/nobska_mavs4_ooicore-0.0.7-py2.7.egg'
DRV_MOD = 'mi.instrument.nobska.mavs4.ooicore.driver'
DRV_CLS = 'mavs4InstrumentDriver'
WORK_DIR = '/tmp/'

# Driver config.
# DVR_CONFIG['comms_config']['port'] is set by the setup.
DVR_CONFIG = {
    'dvr_egg' : DRV_URI,
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : None
}

# Launch from egg or a local MI repo.
LAUNCH_FROM_EGG=True

if LAUNCH_FROM_EGG:
    # Dynamically load the egg into the test path
    launcher = ZMQEggDriverProcess(DVR_CONFIG)
    egg = launcher._get_egg(DRV_URI)
    from hashlib import sha1
    with open(egg,'r') as f:
        doc = f.read()
        sha = sha1(doc).hexdigest()

    if not egg in sys.path: sys.path.insert(0, egg)
    DVR_CONFIG['process_type'] = (DriverProcessType.EGG,)

else:
    mi_repo = os.getcwd() + os.sep + 'extern' + os.sep + 'mi_repo'
    if not mi_repo in sys.path: sys.path.insert(0, mi_repo)
    DVR_CONFIG['process_type'] = (DriverProcessType.PYTHON_MODULE,)
    DVR_CONFIG['mi_repo'] = mi_repo

# Load MI modules from the egg
#from mi.core.instrument.instrument_driver import DriverProtocolState
#from mi.core.instrument.instrument_driver import DriverConnectionState
#from mi.instrument.nobska.mavs4.ooicore.driver import ProtocolEvent

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('SMOKE', group='sa')
#@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestActivateRSNVel3DInstrument(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        super(TestActivateRSNVel3DInstrument, self).setUp()
        config = DotDict()

        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml', config)

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




    @attr('LOCOINT')
    @unittest.skip('under construction')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    @patch.dict(CFG, {'endpoint':{'receive':{'timeout': 180}}})
    def test_activate_rsn_vel3d(self):


        log.info("--------------------------------------------------------------------------------------------------------")
        # load_parameter_scenarios
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=dict(
            op="load",
            scenario="BETA",
            path="master",
            categories="ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition",
            clearcols="owner_id,org_ids",
            assets="res/preload/r2_ioc/ooi_assets",
            parseooi="True",
        ))

        self.loggerpids = []

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='Vel3DMModel',
                                  description="Vel3DMModel")
        instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        log.debug( 'test_activate_rsn_vel3d new InstrumentModel id = %s ', instModel_id)


        raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='raw' )
        vel3d_b_sample = StreamConfiguration(stream_name='vel3d_b_sample', parameter_dictionary_name='vel3d_b_sample')
        vel3d_b_engineering = StreamConfiguration(stream_name='vel3d_b_engineering', parameter_dictionary_name='vel3d_b_engineering')

        RSN_VEL3D_01 = {
                           'DEV_ADDR'  : "10.180.80.6",
                           'DEV_PORT'  : 2101,
                           'DATA_PORT' : 1026,
                           'CMD_PORT'  : 1025,
                           'PA_BINARY' : "port_agent"
                       }

        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='Vel3DAgent',
                                  description="Vel3DAgent",
                                  driver_uri="http://sddevrepo.oceanobservatories.org/releases/nobska_mavs4_ooicore-0.0.7-py2.7.egg",
                                  stream_configurations = [raw_config, vel3d_b_sample, vel3d_b_engineering])
        instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        log.debug('test_activate_rsn_vel3d new InstrumentAgent id = %s', instAgent_id)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        log.debug('test_activate_rsn_vel3d: Create instrument resource to represent the Vel3D ')
        instDevice_obj = IonObject(RT.InstrumentDevice,
                                   name='Vel3DDevice',
                                   description="Vel3DDevice",
                                   serial_number="12345" )
        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        log.debug("test_activate_rsn_vel3d: new InstrumentDevice id = %s  " , instDevice_id)


        port_agent_config = {
            'device_addr':  '10.180.80.6',
            'device_port':  2101,
            'process_type': PortAgentProcessType.UNIX,
            'binary_path': "port_agent",
            'port_agent_addr': 'localhost',
            'command_port': 1025,
            'data_port': 1026,
            'log_level': 5,
            'type': PortAgentType.ETHERNET
        }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='Vel3DAgentInstance',
                                          description="Vel3DAgentInstance",
                                          port_agent_config = port_agent_config,
                                            alerts= [])


        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
                                                                               instAgent_id,
                                                                               instDevice_id)




        parsed_sample_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('vel3d_b_sample', id_only=True)
        parsed_sample_stream_def_id = self.pubsubcli.create_stream_definition(name='vel3d_b_sample', parameter_dictionary_id=parsed_sample_pdict_id)

        parsed_eng_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('vel3d_b_engineering', id_only=True)
        parsed_eng_stream_def_id = self.pubsubcli.create_stream_definition(name='vel3d_b_engineering', parameter_dictionary_id=parsed_eng_pdict_id)

        raw_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('raw', id_only=True)
        raw_stream_def_id = self.pubsubcli.create_stream_definition(name='raw', parameter_dictionary_id=raw_pdict_id)


        #-------------------------------
        # Create Raw and Parsed Data Products for the device
        #-------------------------------

        dp_obj = IonObject(RT.DataProduct,
            name='vel3d_b_sample',
            description='vel3d_b_sample')

        sample_data_product_id = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_sample_stream_def_id)
        log.debug( 'new dp_id = %s' , sample_data_product_id)
        self.dpclient.activate_data_product_persistence(data_product_id=sample_data_product_id)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=sample_data_product_id)



        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(sample_data_product_id, PRED.hasStream, None, True)
        log.debug('sample_data_product streams1 = %s', stream_ids)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(sample_data_product_id, PRED.hasDataset, RT.Dataset, True)
        log.debug('Data set for sample_data_product = %s' , dataset_ids[0])
        self.parsed_dataset = dataset_ids[0]

        pid = self.create_logger('vel3d_b_sample', stream_ids[0] )
        self.loggerpids.append(pid)


        dp_obj = IonObject(RT.DataProduct,
            name='vel3d_b_engineering',
            description='vel3d_b_engineering')

        eng_data_product_id = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_eng_stream_def_id)
        log.debug( 'new dp_id = %s' , eng_data_product_id)
        self.dpclient.activate_data_product_persistence(data_product_id=eng_data_product_id)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=eng_data_product_id)



        dp_obj = IonObject(RT.DataProduct,
            name='the raw data',
            description='raw stream test')

        data_product_id2 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id)
        log.debug('new dp_id = %s', data_product_id2)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasStream, None, True)
        log.debug('test_activate_rsn_vel3d Data product streams2 = %s' , str(stream_ids))

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasDataset, RT.Dataset, True)
        log.debug('test_activate_rsn_vel3d Data set for data_product_id2 = %s' , dataset_ids[0])
        self.raw_dataset = dataset_ids[0]


        def start_instrument_agent():
            self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        gevent.joinall([gevent.spawn(start_instrument_agent)])


        #cleanup
        self.addCleanup(self.imsclient.stop_instrument_agent_instance,
            instrument_agent_instance_id=instAgentInstance_id)


        #wait for start
        inst_agent_instance_obj = self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        gate = AgentProcessStateGate(self.processdispatchclient.read_process,
            instDevice_id,
            ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        gate.process_id)

        #log.trace('Instrument agent instance obj: = %s' , str(inst_agent_instance_obj))

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(instDevice_id,
            to_name=gate.process_id,
            process=FakeProcess())


        def check_state(label, desired_state):
            actual_state = self._ia_client.get_agent_state()
            log.debug("%s instrument agent is in state '%s'", label, actual_state)
            self.assertEqual(desired_state, actual_state)

        log.debug("test_activate_rsn_vel3d: got ia client %s" , str(self._ia_client))

        check_state("just-spawned", ResourceAgentState.UNINITIALIZED)

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        log.debug("test_activate_rsn_vel3d: initialize %s" , str(retval))
        check_state("initialized", ResourceAgentState.INACTIVE)

        log.debug("test_activate_rsn_vel3d Sending go_active command ")
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activate_rsn_vel3d: return value from go_active %s" , str(reply))
        check_state("activated", ResourceAgentState.IDLE)


        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("current state after sending go_active command %s" , str(state))
#
        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activate_rsn_vel3d: run %s" , str(reply))
        check_state("commanded", ResourceAgentState.COMMAND)



        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("current state after sending run command %s" , str(state))


#        cmd = AgentCommand(command=ProtocolEvent.START_AUTOSAMPLE)
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_activate_rsn_vel3d: run %s" , str(reply))
#        state = self._ia_client.get_agent_state()
#        self.assertEqual(ResourceAgentState.COMMAND, state)
#
#        gevent.sleep(5)
#
#        cmd = AgentCommand(command=ProtocolEvent.STOP_AUTOSAMPLE)
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_activate_rsn_vel3d: run %s" , str(reply))
#        state = self._ia_client.get_agent_state()
#        self.assertEqual(ResourceAgentState.COMMAND, state)
#
#        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
#        retval = self._ia_client.execute_agent(cmd)
#        state = retval.result
#        log.debug("current state after sending STOP_AUTOSAMPLE command %s" , str(state))

#
#        cmd = AgentCommand(command=ResourceAgentEvent.PAUSE)
#        retval = self._ia_client.execute_agent(cmd)
#        state = self._ia_client.get_agent_state()
#        self.assertEqual(ResourceAgentState.STOPPED, state)
#
#        cmd = AgentCommand(command=ResourceAgentEvent.RESUME)
#        retval = self._ia_client.execute_agent(cmd)
#        state = self._ia_client.get_agent_state()
#        self.assertEqual(ResourceAgentState.COMMAND, state)
#
#        cmd = AgentCommand(command=ResourceAgentEvent.CLEAR)
#        retval = self._ia_client.execute_agent(cmd)
#        state = self._ia_client.get_agent_state()
#        self.assertEqual(ResourceAgentState.IDLE, state)
#
#        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
#        retval = self._ia_client.execute_agent(cmd)
#        state = self._ia_client.get_agent_state()
#        self.assertEqual(ResourceAgentState.COMMAND, state)

        log.debug( "test_activate_rsn_vel3d: calling reset ")
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activate_rsn_vel3d: return from reset %s" , str(reply))


        #--------------------------------------------------------------------------------
        # Now get the data in one chunk using an RPC Call to start_retreive
        #--------------------------------------------------------------------------------

        replay_data_raw = self.dataretrieverclient.retrieve(self.raw_dataset)
        self.assertIsInstance(replay_data_raw, Granule)
        rdt_raw = RecordDictionaryTool.load_from_granule(replay_data_raw)
        log.debug("RDT raw: %s", str(rdt_raw.pretty_print()) )

        self.assertIn('raw', rdt_raw)
        raw_vals = rdt_raw['raw']



        #--------------------------------------------------------------------------------
        # Deactivate loggers
        #--------------------------------------------------------------------------------

        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)

        self.dpclient.delete_data_product(sample_data_product_id)
        self.dpclient.delete_data_product(eng_data_product_id)
        self.dpclient.delete_data_product(data_product_id2)
