#!/usr/bin/env python

"""
@package ion.services.sa.test
@file ion/services/sa/test/test_alerts.py
@author Swarbhanu Chatterjee
@brief Test alerts

"""

from pyon.public import log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, PRED
from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.agent import ResourceAgentClient
from pyon.util.context import LocalContextMixin
from pyon.event.event import EventSubscriber

from interface.objects import StreamAlertType, AggregateStatusType
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

from interface.objects import ProcessStateEnum, StreamConfiguration, AgentCommand, ProcessDefinition, ComputedStringValue, DataProduct
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.services.dm.utility.granule_utils import time_series_domain

# This import will dynamically load the driver egg.  It is needed for the MI includes below
import ion.agents.instrument.test.test_instrument_agent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter

from mock import patch
import gevent
from gevent import event
from nose.plugins.attrib import attr

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
class TestInstrumentAlerts(IonIntegrationTestCase):
    pdict_id = None

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)

        self.catch_alert = gevent.event.AsyncResult()

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

    def _create_instrument_stream_alarms(self, instDevice_id):
        #Create stream alarms
            """
            test_two_sided_interval
            Test interval alarm and alarm event publishing for a closed
            inteval.
            """

            temp_alert_def = {
                'name' : 'temperature_warning_interval',
                'stream_name' : 'parsed',
                'message' : 'Temperature is below the normal range of 50.0 and above.',
                'alert_type' : StreamAlertType.WARNING,
                'aggregate_type' : AggregateStatusType.AGGREGATE_DATA,
                'value_id' : 'temp',
                'resource_id' : instDevice_id,
                'origin_type' : 'device',
                'lower_bound' : 50.0,
                'lower_rel_op' : '<',
                'alert_class' : 'IntervalAlert'
            }

            late_data_alert_def = {
                'name' : 'late_data_warning',
                'stream_name' : 'parsed',
                'message' : 'Expected data has not arrived.',
                'alert_type' : StreamAlertType.WARNING,
                'aggregate_type' : AggregateStatusType.AGGREGATE_COMMS,
                'value_id' : None,
                'resource_id' : instDevice_id,
                'origin_type' : 'device',
                'time_delta' : 2,
                'alert_class' : 'LateDataAlert'
            }
            return temp_alert_def, late_data_alert_def


    def _create_instrument_agent_instance(self, instAgent_id, instDevice_id):


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

        temp_alert, late_data_alert = self._create_instrument_stream_alarms(instDevice_id)

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance',
            description="SBE37IMAgentInstance",
            port_agent_config = port_agent_config,
            alerts= [temp_alert, late_data_alert]
            )

        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
            instAgent_id,
            instDevice_id)

        return instAgentInstance_id

    def test_alerts(self):

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

        # It is necessary for the instrument device to be associated with atleast one output data product
        tdom, sdom = time_series_domain()
        parsed_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        parsed_stream_def_id = self.pubsubclient.create_stream_definition(name='parsed', parameter_dictionary_id=parsed_pdict_id)

        dp_obj = IonObject(RT.DataProduct,
            name='output_data_prod',
            description='Output data product for instrument',
            temporal_domain = tdom.dump(),
            spatial_domain = sdom.dump())

        out_data_product_id = self.dataproductclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=out_data_product_id)

        log.debug("assigning instdevice id: %s to data product: %s", instDevice_id, out_data_product_id)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=out_data_product_id)

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
        # Set up the subscriber to catch the alert event
        #-------------------------------------------------------------------------------------

        def callback_for_alert(*args, **kwargs):
            self.catch_alert.set()

        self.event_subscriber = EventSubscriber(event_type='StreamAlertEvent',
            origin=instDevice_id,
            callback=callback_for_alert)

        self.event_subscriber.start()
        self.addCleanup(self.event_subscriber.stop)

        #-------------------------------------------------------------------------------------
        # Running the instrument....
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

        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)

        # This gevent sleep is there to test the autosample time, which will show something different from default
        # only if the instrument runs for over a minute
        gevent.sleep(45)

        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)


        #-------------------------------------------------------------------------------------------------
        # Cleanup processes
        #-------------------------------------------------------------------------------------------------


