#!/usr/bin/env python

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

# This import will dynamically load the driver egg.  It is needed for the MI includes below
import ion.agents.instrument.test.test_instrument_agent
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent

from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.inventory.index_management_service import IndexManagementService

from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType

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
from interface.objects import Granule, DeviceStatusType, DeviceCommsType, StatusType, StreamConfiguration
from interface.objects import AgentCommand, ProcessDefinition, ProcessStateEnum
from interface.objects import UserInfo, NotificationRequest
from interface.objects import ComputedIntValue, ComputedFloatValue, ComputedStringValue, ComputedDictValue, ComputedListValue, ComputedEventListValue
from interface.objects import StreamAlarmType

from ion.processes.bootstrap.index_bootstrap import STD_INDEXES
from nose.plugins.attrib import attr
import gevent
import elasticpy as ep
from mock import patch

import time

use_es = CFG.get_safe('system.elasticsearch',False)

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
        super(TestActivateInstrumentIntegration, self).setUp()
        config = DotDict()
        config.bootstrap.use_es = True

        self._start_container()
        self.addCleanup(TestActivateInstrumentIntegration.es_cleanup)

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
        self.usernotificationclient = UserNotificationServiceClient()

        #setup listerner vars
        self._data_greenlets = []
        self._no_samples = None
        self._samples_received = []

        self.event_publisher = EventPublisher()

    @staticmethod
    def es_cleanup():
        es_host = CFG.get_safe('server.elasticsearch.host', 'localhost')
        es_port = CFG.get_safe('server.elasticsearch.port', '9200')
        es = ep.ElasticSearch(
            host=es_host,
            port=es_port,
            timeout=10
        )
        indexes = STD_INDEXES.keys()
        indexes.append('%s_resources_index' % get_sys_name().lower())
        indexes.append('%s_events_index' % get_sys_name().lower())

        for index in indexes:
            IndexManagementService._es_call(es.river_couchdb_delete,index)
            IndexManagementService._es_call(es.index_delete,index)

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

    def _create_notification(self, user_name = '', instrument_id='', product_id=''):
        #--------------------------------------------------------------------------------------
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name= 'notification_1',
            origin=instrument_id,
            origin_type="instrument",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(   name='notification_2',
            origin=product_id,
            origin_type="data product",
            event_type='DetectionEvent')

        #--------------------------------------------------------------------------------------
        # Create a user and get the user_id
        #--------------------------------------------------------------------------------------

        user = UserInfo()
        user.name = user_name
        user.contact.email = '%s@yahoo.com' % user_name

        user_id, _ = self.rrclient.create(user)

        #--------------------------------------------------------------------------------------
        # Create notification
        #--------------------------------------------------------------------------------------

        self.usernotificationclient.create_notification(notification=notification_request_1, user_id=user_id)
        self.usernotificationclient.create_notification(notification=notification_request_2, user_id=user_id)
        log.debug( "test_activateInstrumentSample: create_user_notifications user_id %s", str(user_id) )

        return user_id

    def get_datastore(self, dataset_id):
        dataset = self.datasetclient.read_dataset(dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)
        return datastore

    def _check_computed_attributes_of_extended_instrument(self, expected_instrument_device_id = '',extended_instrument = None):

        # Verify that computed attributes exist for the extended instrument
        self.assertIsInstance(extended_instrument.computed.firmware_version, ComputedFloatValue)
        self.assertIsInstance(extended_instrument.computed.last_data_received_datetime, ComputedFloatValue)
        self.assertIsInstance(extended_instrument.computed.last_calibration_datetime, ComputedFloatValue)
        self.assertIsInstance(extended_instrument.computed.uptime, ComputedStringValue)

        self.assertIsInstance(extended_instrument.computed.power_status_roll_up, ComputedIntValue)
        self.assertIsInstance(extended_instrument.computed.communications_status_roll_up, ComputedIntValue)
        self.assertIsInstance(extended_instrument.computed.data_status_roll_up, ComputedIntValue)
        self.assertIsInstance(extended_instrument.computed.location_status_roll_up, ComputedIntValue)

        # the following assert will not work without elasticsearch.
        #self.assertEqual( 1, len(extended_instrument.computed.user_notification_requests.value) )
        self.assertEqual(extended_instrument.computed.communications_status_roll_up.value, StatusType.STATUS_WARNING)
        self.assertEqual(extended_instrument.computed.data_status_roll_up.value, StatusType.STATUS_OK)
        self.assertEqual(extended_instrument.computed.power_status_roll_up.value, StatusType.STATUS_WARNING)

        # Verify the computed attribute for user notification requests
        self.assertEqual( 1, len(extended_instrument.computed.user_notification_requests.value) )
        notifications = extended_instrument.computed.user_notification_requests.value
        notification = notifications[0]
        self.assertEqual(notification.origin, expected_instrument_device_id)
        self.assertEqual(notification.origin_type, "instrument")
        self.assertEqual(notification.event_type, 'ResourceLifecycleEvent')


    def _check_computed_attributes_of_extended_product(self, expected_data_product_id = '', extended_data_product = None):

        self.assertEqual(expected_data_product_id, extended_data_product._id)
        log.debug("extended_data_product.computed: %s", extended_data_product.computed)

        # Verify that computed attributes exist for the extended instrument
        self.assertIsInstance(extended_data_product.computed.product_download_size_estimated, ComputedIntValue)
        self.assertIsInstance(extended_data_product.computed.number_active_subscriptions, ComputedIntValue)
        self.assertIsInstance(extended_data_product.computed.data_url, ComputedStringValue)
        self.assertIsInstance(extended_data_product.computed.stored_data_size, ComputedIntValue)
        self.assertIsInstance(extended_data_product.computed.recent_granules, ComputedDictValue)
        self.assertIsInstance(extended_data_product.computed.parameters, ComputedListValue)
        self.assertIsInstance(extended_data_product.computed.recent_events, ComputedEventListValue)

        self.assertIsInstance(extended_data_product.computed.provenance, ComputedDictValue)
        self.assertIsInstance(extended_data_product.computed.user_notification_requests, ComputedListValue)
        self.assertIsInstance(extended_data_product.computed.active_user_subscriptions, ComputedListValue)
        self.assertIsInstance(extended_data_product.computed.past_user_subscriptions, ComputedListValue)
        self.assertIsInstance(extended_data_product.computed.last_granule, ComputedDictValue)
        self.assertIsInstance(extended_data_product.computed.is_persisted, ComputedIntValue)
        self.assertIsInstance(extended_data_product.computed.data_contents_updated, ComputedStringValue)
        self.assertIsInstance(extended_data_product.computed.data_datetime, ComputedListValue)

        # exact text here keeps changing to fit UI capabilities.  keep assertion general...
        self.assertTrue( 'ok' in extended_data_product.computed.last_granule.value['quality_flag'] )
        self.assertEqual( 2, len(extended_data_product.computed.data_datetime.value) )

        notifications = extended_data_product.computed.user_notification_requests.value

        notification = notifications[0]
        self.assertEqual(notification.origin, expected_data_product_id)
        self.assertEqual(notification.origin_type, "data product")
        self.assertEqual(notification.event_type, 'DetectionEvent')


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    @patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
    def test_activateInstrumentSample(self):

        self.loggerpids = []

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel")
        instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        log.debug( 'new InstrumentModel id = %s ', instModel_id)



        #Create stream alarms
        """
        test_two_sided_interval
        Test interval alarm and alarm event publishing for a closed
        inteval.
        """

        #        kwargs = {
        #            'name' : 'test_sim_warning',
        #            'stream_name' : 'parsed',
        #            'value_id' : 'temp',
        #            'message' : 'Temperature is above test range of 5.0.',
        #            'type' : StreamAlarmType.WARNING,
        #            'upper_bound' : 5.0,
        #            'upper_rel_op' : '<'
        #        }


        kwargs = {
            'name' : 'temperature_warning_interval',
            'stream_name' : 'parsed',
            'value_id' : 'temp',
            'message' : 'Temperature is below the normal range of 50.0 and above.',
            'type' : StreamAlarmType.WARNING,
            'lower_bound' : 50.0,
            'lower_rel_op' : '<'
        }

        # Create alarm object.
        alarm = {}
        alarm['type'] = 'IntervalAlarmDef'
        alarm['kwargs'] = kwargs

        raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='ctd_raw_param_dict', records_per_granule=2, granule_publish_rate=5 )
        parsed_config = StreamConfiguration(stream_name='parsed', parameter_dictionary_name='ctd_parsed_param_dict', records_per_granule=2, granule_publish_rate=5, alarms=[alarm] )


        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_uri="http://sddevrepo.oceanobservatories.org/releases/seabird_sbe37smb_ooicore-0.0.1a-py2.7.egg",
                                  stream_configurations = [raw_config, parsed_config])
        instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        log.debug('new InstrumentAgent id = %s', instAgent_id)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        log.debug('test_activateInstrumentSample: Create instrument resource to represent the SBE37 (SA Req: L4-CI-SA-RQ-241) ')
        instDevice_obj = IonObject(RT.InstrumentDevice,
                                   name='SBE37IMDevice',
                                   description="SBE37IMDevice",
                                   serial_number="12345" )
        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)

        log.debug("test_activateInstrumentSample: new InstrumentDevice id = %s (SA Req: L4-CI-SA-RQ-241) " , instDevice_id)


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
        log.debug( 'new dp_id = %s' , data_product_id1)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id1)



        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug('Data product streams1 = %s', stream_ids)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasDataset, RT.Dataset, True)
        log.debug('Data set for data_product_id1 = %s' , dataset_ids[0])
        self.parsed_dataset = dataset_ids[0]


        pid = self.create_logger('ctd_parsed', stream_ids[0] )
        self.loggerpids.append(pid)


        dp_obj = IonObject(RT.DataProduct,
            name='the raw data',
            description='raw stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id2 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id)
        log.debug('new dp_id = %s', data_product_id2)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2)

        # setup notifications for the device and parsed data product
        user_id_1 = self._create_notification( user_name='user_1', instrument_id=instDevice_id, product_id=data_product_id1)
        #---------- Create notifications for another user and verify that we see different computed subscriptions for the two users ---------
        user_id_2 = self._create_notification( user_name='user_2', instrument_id=instDevice_id, product_id=data_product_id2)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasStream, None, True)
        log.debug('Data product streams2 = %s' , str(stream_ids))

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasDataset, RT.Dataset, True)
        log.debug('Data set for data_product_id2 = %s' , dataset_ids[0])
        self.raw_dataset = dataset_ids[0]

        #elastic search debug
        es_indexes, _ = self.container.resource_registry.find_resources(restype='ElasticSearchIndex')
        log.debug('ElasticSearch indexes: %s', [i.name for i in es_indexes])
        log.debug('Bootstrap %s', CFG.bootstrap.use_es)


        def start_instrument_agent():
            self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        gevent.joinall([gevent.spawn(start_instrument_agent)])


        #setup a subscriber to alarm events from the device
        self._events_received= []
        self._event_count = 0
        self._samples_out_of_range = 0
        self._samples_complete = False
        self._async_sample_result = AsyncResult()

        def consume_event(*args, **kwargs):
            log.debug('TestActivateInstrument recieved ION event: args=%s, kwargs=%s, event=%s.',
                str(args), str(kwargs), str(args[0]))
            self._events_received.append(args[0])
            self._event_count = len(self._events_received)
            self._async_sample_result.set()

        self._event_subscriber = EventSubscriber(
            event_type= 'StreamWarningAlarmEvent',   #'StreamWarningAlarmEvent', #  StreamAlarmEvent
            callback=consume_event,
            origin=instDevice_id)
        self._event_subscriber.start()


        #cleanup
        self.addCleanup(self.imsclient.stop_instrument_agent_instance,
                        instrument_agent_instance_id=instAgentInstance_id)

        def stop_subscriber():
            self._event_subscriber.stop()
            self._event_subscriber = None

        self.addCleanup(stop_subscriber)


        #wait for start
        inst_agent_instance_obj = self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
                                inst_agent_instance_obj.agent_process_id,
                                ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        inst_agent_instance_obj.agent_process_id)

        log.debug('Instrument agent instance obj: = %s' , str(inst_agent_instance_obj))

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(instDevice_id,
                                              to_name=inst_agent_instance_obj.agent_process_id,
                                              process=FakeProcess())

        log.debug("test_activateInstrumentSample: got ia client %s" , str(self._ia_client))

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: initialize %s" , str(retval))
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        log.debug("(L4-CI-SA-RQ-334): Sending go_active command ")
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: return value from go_active %s" , str(reply))
        state = self._ia_client.get_agent_state()
        self.assertEqual(state, ResourceAgentState.IDLE)

        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("(L4-CI-SA-RQ-334): current state after sending go_active command %s" , str(state))

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: run %s" , str(reply))
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
        for i in xrange(10):
            retval = self._ia_client.execute_resource(cmd)
            log.debug("test_activateInstrumentSample: return from sample %s" , str(retval))

        log.debug( "test_activateInstrumentSample: calling reset ")
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: return from reset %s" , str(reply))

        self._samples_complete = True

        #--------------------------------------------------------------------------------
        # Now get the data in one chunk using an RPC Call to start_retreive
        #--------------------------------------------------------------------------------

        replay_data = self.dataretrieverclient.retrieve(self.parsed_dataset)
        self.assertIsInstance(replay_data, Granule)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        log.debug("test_activateInstrumentSample: RDT parsed: %s", str(rdt.pretty_print()) )
        temp_vals = rdt['temp']
        self.assertEquals(len(temp_vals) , 10)
        log.debug("test_activateInstrumentSample: all temp_vals: %s", temp_vals )

        #out_of_range_temp_vals = [i for i in temp_vals if i > 5]
        out_of_range_temp_vals = [i for i in temp_vals if i < 50.0]
        log.debug("test_activateInstrumentSample: Out_of_range_temp_vals: %s", out_of_range_temp_vals )
        self._samples_out_of_range = len(out_of_range_temp_vals)

        # if no bad values were produced, then do not wait for an event
        if self._samples_out_of_range == 0:
            self._async_sample_result.set()


        log.debug("test_activateInstrumentSample: _events_received: %s", self._events_received )
        log.debug("test_activateInstrumentSample: _event_count: %s", self._event_count )

        self._async_sample_result.get(timeout=CFG.endpoint.receive.timeout)

        replay_data = self.dataretrieverclient.retrieve(self.raw_dataset)
        self.assertIsInstance(replay_data, Granule)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        log.debug("RDT raw: %s", str(rdt.pretty_print()) )

        raw_vals = rdt['raw']
        self.assertEquals(len(raw_vals) , 10)


        log.debug("l4-ci-sa-rq-138")
        """
        Physical resource control shall be subject to policy

        Instrument management control capabilities shall be subject to policy

        The actor accessing the control capabilities must be authorized to send commands.

        note from maurice 2012-05-18: Talk to tim M to verify that this is policy.  If it is then talk with Stephen to
                                      get an example of a policy test and use that to create a test stub that will be
                                      completed when we have instrument policies.

        Tim M: The "actor", aka observatory operator, will access the instrument through ION.

        """


        #--------------------------------------------------------------------------------
        # Get the extended data product to see if it contains the granules
        #--------------------------------------------------------------------------------
        extended_product = self.dpclient.get_data_product_extension(data_product_id=data_product_id1, user_id=user_id_1)
        def poller(extended_product):
            return len(extended_product.computed.user_notification_requests.value) == 1

        poll(poller, extended_product, timeout=30)

        self._check_computed_attributes_of_extended_product( expected_data_product_id = data_product_id1, extended_data_product = extended_product)

        #--------------------------------------------------------------------------------
        #put some events into the eventsdb to test - this should set the comms and data status to WARNING
        #--------------------------------------------------------------------------------

        t = get_ion_ts()
        self.event_publisher.publish_event(  ts_created= t,  event_type = 'DeviceStatusEvent',
            origin = instDevice_id, state=DeviceStatusType.OUT_OF_RANGE, values = [200] )
        self.event_publisher.publish_event( ts_created= t,   event_type = 'DeviceCommsEvent',
            origin = instDevice_id, state=DeviceCommsType.DATA_DELIVERY_INTERRUPTION, lapse_interval_seconds = 20 )

        #--------------------------------------------------------------------------------
        # Get the extended instrument
        #--------------------------------------------------------------------------------

        extended_instrument = self.imsclient.get_instrument_device_extension(instrument_device_id=instDevice_id, user_id=user_id_1)
        self._check_computed_attributes_of_extended_instrument(expected_instrument_device_id = instDevice_id, extended_instrument = extended_instrument)

        #--------------------------------------------------------------------------------
        # For the second user, check the extended data product and the extended intrument
        #--------------------------------------------------------------------------------
        extended_product = self.dpclient.get_data_product_extension(data_product_id=data_product_id2, user_id=user_id_2)
        self._check_computed_attributes_of_extended_product(expected_data_product_id = data_product_id2, extended_data_product = extended_product)

        #---------- Put some events into the eventsdb to test - this should set the comms and data status to WARNING  ---------

        t = get_ion_ts()
        self.event_publisher.publish_event(  ts_created= t,  event_type = 'DeviceStatusEvent',
            origin = instDevice_id, state=DeviceStatusType.OUT_OF_RANGE, values = [200] )
        self.event_publisher.publish_event( ts_created= t,   event_type = 'DeviceCommsEvent',
            origin = instDevice_id, state=DeviceCommsType.DATA_DELIVERY_INTERRUPTION, lapse_interval_seconds = 20 )

        #--------------------------------------------------------------------------------
        # Get the extended instrument
        #--------------------------------------------------------------------------------

        extended_instrument = self.imsclient.get_instrument_device_extension(instrument_device_id=instDevice_id, user_id=user_id_2)
        self._check_computed_attributes_of_extended_instrument(expected_instrument_device_id = instDevice_id, extended_instrument = extended_instrument)

        #--------------------------------------------------------------------------------
        # Deactivate loggers
        #--------------------------------------------------------------------------------

        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)

        self.dpclient.delete_data_product(data_product_id1)
        self.dpclient.delete_data_product(data_product_id2)

