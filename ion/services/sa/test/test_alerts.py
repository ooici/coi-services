#!/usr/bin/env python

"""
@package ion.services.sa.test
@file ion/services/sa/test/test_alerts.py
@author Swarbhanu Chatterjee
@brief Test alerts

"""

from pyon.public import log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

# This import will dynamically load the driver egg.  It is needed for the MI includes below
import ion.agents.instrument.test.test_instrument_agent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter

from nose.plugins.attrib import attr
from ion.services.dm.utility.granule_utils import time_series_domain


from pyon.public import CFG, RT, PRED

from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.agent import ResourceAgentClient

from mock import patch
import gevent

from pyon.util.context import LocalContextMixin

from interface.objects import ProcessStateEnum, StreamConfiguration, AgentCommand, ProcessDefinition, ComputedStringValue
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType

# Used to validate param config retrieved from driver.
PARAMS = {
    SBE37Parameter.OUTPUTSAL : bool,
    SBE37Parameter.OUTPUTSV : bool,
    SBE37Parameter.NAVG : int,
    SBE37Parameter.SAMPLENUM : int,
    SBE37Parameter.INTERVAL : int,
    SBE37Parameter.STORETIME : bool,
    SBE37Parameter.TXREALTIME : bool,
    SBE37Parameter.SYNCMODE : bool,
    SBE37Parameter.SYNCWAIT : int,
    SBE37Parameter.TCALDATE : tuple,
    SBE37Parameter.TA0 : float,
    SBE37Parameter.TA1 : float,
    SBE37Parameter.TA2 : float,
    SBE37Parameter.TA3 : float,
    SBE37Parameter.CCALDATE : tuple,
    SBE37Parameter.CG : float,
    SBE37Parameter.CH : float,
    SBE37Parameter.CI : float,
    SBE37Parameter.CJ : float,
    SBE37Parameter.WBOTC : float,
    SBE37Parameter.CTCOR : float,
    SBE37Parameter.CPCOR : float,
    SBE37Parameter.PCALDATE : tuple,
    SBE37Parameter.PA0 : float,
    SBE37Parameter.PA1 : float,
    SBE37Parameter.PA2 : float,
    SBE37Parameter.PTCA0 : float,
    SBE37Parameter.PTCA1 : float,
    SBE37Parameter.PTCA2 : float,
    SBE37Parameter.PTCB0 : float,
    SBE37Parameter.PTCB1 : float,
    SBE37Parameter.PTCB2 : float,
    SBE37Parameter.POFFSET : float,
    SBE37Parameter.RCALDATE : tuple,
    SBE37Parameter.RTCA0 : float,
    SBE37Parameter.RTCA1 : float,
    SBE37Parameter.RTCA2 : float
}

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('HARDWARE', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestCTDTransformsIntegration(IonIntegrationTestCase):
    pdict_id = None

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        print 'started services'

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.dataset_management = self.datasetclient

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
        pid = self.processdispatchclient.schedule_process(process_definition_id= logger_procdef_id, configuration=configuration)

        return pid

    def _create_instrument_model(self):

        instModel_obj = IonObject(  RT.InstrumentModel,
            name='SBE37IMModel',
            description="SBE37IMModel"  )
        instModel_id = self.imsclient.create_instrument_model(instModel_obj)

        return instModel_id

    def _create_instrument_agent(self, instModel_id):

        raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='ctd_raw_param_dict', records_per_granule=2, granule_publish_rate=5 )
        parsed_config = StreamConfiguration(stream_name='parsed', parameter_dictionary_name='ctd_parsed_param_dict', records_per_granule=2, granule_publish_rate=5 )

        instAgent_obj = IonObject(RT.InstrumentAgent,
            name='agent007',
            description="SBE37IMAgent",
            driver_uri="http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.0.4-py2.7.egg",
            stream_configurations = [raw_config, parsed_config] )
        instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        return instAgent_id

    def _create_instrument_device(self, instModel_id):

        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )

        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)

        log.debug("test_activateInstrumentSample: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) ", instDevice_id)

        return instDevice_id

    def _create_instrument_agent_instance(self, instAgent_id,instDevice_id):


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

        return instAgentInstance_id


    def test_alerts(self):

        self.loggerpids = []

        #-------------------------------------------------------------------------------------
        # Create InstrumentModel
        #-------------------------------------------------------------------------------------

        instModel_id = self._create_instrument_model()

        #-------------------------------------------------------------------------------------
        # Create InstrumentAgent
        #-------------------------------------------------------------------------------------

        instAgent_id = self._create_instrument_agent(instModel_id)

        #-------------------------------------------------------------------------------------
        # Create InstrumentDevice
        #-------------------------------------------------------------------------------------

        instDevice_id = self._create_instrument_device(instModel_id)

        #-------------------------------------------------------------------------------------
        # Create Instrument Agent Instance
        #-------------------------------------------------------------------------------------

        instAgentInstance_id = self._create_instrument_agent_instance(instAgent_id,instDevice_id )

        #-------------------------------------------------------------------------------------
        # Launch InstrumentAgentInstance, connect to the resource agent client
        #-------------------------------------------------------------------------------------
        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)
        self.addCleanup(self.imsclient.stop_instrument_agent_instance,
            instrument_agent_instance_id=instAgentInstance_id)

        inst_agent_instance_obj= self.imsclient.read_instrument_agent_instance(instAgentInstance_id)

        # Wait for instrument agent to spawn
        gate = ProcessStateGate(self.processdispatchclient.read_process,
            inst_agent_instance_obj.agent_process_id, ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(15), "The instrument agent instance did not spawn in 15 seconds")

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(instDevice_id,
            to_name=inst_agent_instance_obj.agent_process_id,
            process=FakeProcess())

        #-------------------------------------------------------------------------------------
        # Streaming
        #-------------------------------------------------------------------------------------


        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)

        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("(L4-CI-SA-RQ-334): current state after sending go_active command %s", str(state))
        self.assertTrue(state, 'DRIVER_STATE_COMMAND')

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)

        #todo ResourceAgentClient no longer has method set_param
        #        # Make sure the sampling rate and transmission are sane.
        #        params = {
        #            SBE37Parameter.NAVG : 1,
        #            SBE37Parameter.INTERVAL : 5,
        #            SBE37Parameter.TXREALTIME : True
        #        }
        #        self._ia_client.set_param(params)


        #todo There is no ResourceAgentEvent attribute for go_streaming... so what should be the command for it?
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

        #todo There is no ResourceAgentEvent attribute for go_observatory... so what should be the command for it?
        #        log.debug("test_activateInstrumentStream: calling go_observatory")
        #        cmd = AgentCommand(command='go_observatory')
        #        reply = self._ia_client.execute_agent(cmd)
        #        cmd = AgentCommand(command='get_current_state')
        #        retval = self._ia_client.execute_agent(cmd)
        #        state = retval.result
        #        log.debug("test_activateInstrumentStream: return from go_observatory state  %s", str(state))

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)


        #-------------------------------------------------------------------------------------------------
        # Cleanup processes
        #-------------------------------------------------------------------------------------------------
        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)


        #--------------------------------------------------------------------------------
        # Cleanup data products
        #--------------------------------------------------------------------------------
        dp_ids, _ = self.rrclient.find_resources(restype=RT.DataProduct, id_only=True)

        for dp_id in dp_ids:
            self.dataproductclient.delete_data_product(dp_id)



