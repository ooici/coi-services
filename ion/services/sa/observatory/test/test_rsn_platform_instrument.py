#!/usr/bin/env python

"""
@package ion.services.sa.observatory.test.test_platform_instrument
@file    ion/services/sa/observatory/test/test_platform_instrument.py
@author  Carlos Rueda, Maurice Manning
@brief   Tests involving some more detailed platform-instrument interations
"""

__author__ = 'Carlos Rueda, Maurice Manning'
__license__ = 'Apache 2.0'

#
# Base preparations and construction of the platform topology are provided by
# the base class BaseTestPlatform.
#

# developer conveniences:
# bin/nosetests -sv ion/services/sa/observatory/test/test_platform_instrument.py:Test.test_platform_with_instrument_streaming

from pyon.public import log, OT, RT

from ion.agents.platform.test.base_test_platform_agent_with_rsn import BaseIntTestPlatform

from pyon.agent.agent import ResourceAgentClient
from ion.agents.platform.test.base_test_platform_agent_with_rsn import FakeProcess
from pyon.agent.agent import ResourceAgentState
from pyon.event.event import EventSubscriber

from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.sa.test.helpers import any_old, AgentProcessStateGate
from pyon.public import RT, PRED, IonObject

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from pyon.util.containers import DotDict

from interface.objects import AgentCommand, StreamConfiguration, ProcessStateEnum, PortTypeEnum
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType

from ion.agents.platform.platform_agent import PlatformAgentState, PlatformAgentEvent
from ion.agents.port.port_agent_process import PortAgentProcess

from ion.services.cei.process_dispatcher_service import ProcessStateGate

from ion.agents.platform.rsn.test.oms_test_mixin import OmsTestMixin

import unittest
import pprint
import time
import calendar
import gevent
from gevent.event import AsyncResult

from mock import patch
from pyon.public import CFG

# -------------------------------- MI ----------------------------
# the following adapted from test_instrument_agent to be able to import from
# the MI repo, using egg directly.

import sys
from ion.agents.instrument.driver_process import ZMQEggDriverProcess

# EVENT_TIMEOUT: timeout for reception of event
EVENT_TIMEOUT = 25

# initialization of the driver configuration.
OMS_URI = 'http://alice:1234@10.180.80.10:9021/'
PLTFRM_DVR_CONFIG = {
    'oms_uri': OMS_URI
}

PLTFRM_DVR_MOD = 'ion.agents.platform.rsn.rsn_platform_driver'
PLTFRM_DVR_CLS = 'RSNPlatformDriver'

# A VEL3D driver.
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/nobska_mavs4_ooicore-0.0.7-py2.7.egg'
DRV_MOD = 'mi.instrument.nobska.mavs4.ooicore.driver'
DRV_CLS = 'mavs4InstrumentDriver'

WORK_DIR = '/tmp/'

DVR_CONFIG = {
    'dvr_egg' : DRV_URI,
    'dvr_mod' : DRV_MOD,
    'dvr_cls' : DRV_CLS,
    'workdir' : WORK_DIR,
    'process_type' : None
}

RSN_VEL3D_01 = {
                    'DEV_ADDR'  : "10.180.80.6",
                    'DEV_PORT'  : 2101,
                    'DATA_PORT' : 1026,
                    'CMD_PORT'  : 1025,
                    'PA_BINARY' : "port_agent"
                },


# Dynamically load the egg into the test path
launcher = ZMQEggDriverProcess(DVR_CONFIG)
egg = launcher._get_egg(DRV_URI)
if not egg in sys.path:
    sys.path.insert(0, egg)


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 300}}})
class TestPlatformInstrument(BaseIntTestPlatform):

    def setUp(self):
        self._start_container()

        self._pp = pprint.PrettyPrinter()

        log.debug("oms_uri = %s", OMS_URI)
        self.oms = CIOMSClientFactory.create_instance(OMS_URI)

        #url = OmsTestMixin.start_http_server()
        #log.debug("TestPlatformInstrument:setup http url %s", url)
        #
        #result = self.oms.event.register_event_listener(url)
        #log.debug("TestPlatformInstrument:setup register_event_listener result %s", result)


        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()
        self.RR2  = EnhancedResourceRegistryClient(self.rrclient)

        self.org_id = self.RR2.create(any_old(RT.Org))
        log.debug("Org created: %s", self.org_id)

        # see _set_receive_timeout
        self._receive_timeout = 300

        self.instrument_device_id = ''
        self.platform_device_id = ''
        self.platform_site_id = ''
        self.platform_agent_instance_id = ''
        self._pa_client = ''

        def done():
            CIOMSClientFactory.destroy_instance(self.oms)
            event_notifications = OmsTestMixin.stop_http_server()
            log.info("event_notifications = %s" % str(event_notifications))

        self.addCleanup(done)


    @unittest.skip('Must be run locally...')
    def test_platform_with_instrument_streaming(self):
        #
        # The following is with just a single platform and the single
        # instrument "SBE37_SIM_08", which corresponds to the one on port 4008.
        #

        #load the paramaters and the param dicts necesssary for the VEL3D
        log.debug( "load params------------------------------------------------------------------------------")
        self._load_params()

        log.debug( " _register_oms_listener------------------------------------------------------------------------------")
        self._register_oms_listener()

        #create the instrument device/agent/mode
        log.debug( "---------- create_instrument_resources ----------" )
        self._create_instrument_resources()

        #create the platform device, agent and instance
        log.debug( "---------- create_platform_configuration ----------" )
        self._create_platform_configuration('LPJBox_CI_Ben_Hall')

        self.rrclient.create_association(subject=self.platform_device_id, predicate=PRED.hasDevice, object=self.instrument_device_id)

        log.debug( "---------- start_platform ----------" )
        self._start_platform()
        self.addCleanup(self._stop_platform)

        # get everything in command mode:
        self._ping_agent()
        log.debug( " ---------- initialize ----------" )
        self._initialize()


        _ia_client = ResourceAgentClient(self.instrument_device_id, process=FakeProcess())
        state = _ia_client.get_agent_state()
        log.info("TestPlatformInstrument get_agent_state %s", state)

        log.debug( " ---------- go_active ----------" )
        self._go_active()
        state = _ia_client.get_agent_state()
        log.info("TestPlatformInstrument get_agent_state %s", state)

        log.debug( "---------- run ----------" )
        self._run()

        gevent.sleep(2)


        log.debug( " ---------- _start_resource_monitoring ----------" )
        self._start_resource_monitoring()
        gevent.sleep(2)
#
#        # verify the instrument is command state:
#        state = ia_client.get_agent_state()
#        log.debug(" TestPlatformInstrument get_agent_state: %s", state)
#        self.assertEqual(state, ResourceAgentState.COMMAND)  _stop_resource_monitoring

        log.debug( " ---------- _stop_resource_monitoring ----------" )
        self._stop_resource_monitoring()
        gevent.sleep(2)


        log.debug( " ---------- go_inactive ----------" )
        self._go_inactive()
        state = _ia_client.get_agent_state()
        log.info("TestPlatformInstrument get_agent_state %s", state)



        self._reset()
        self._shutdown()


    def _get_platform_attributes(self):
        log.debug( " ----------get_platform_attributes ----------")
        attr_infos = self.oms.attr.get_platform_attributes('LPJBox_CI_Ben_Hall')
        log.debug('_get_platform_attributes: %s', self._pp.pformat(attr_infos))

        attrs = attr_infos['LPJBox_CI_Ben_Hall']
        for attrid, arrinfo in attrs.iteritems():
            arrinfo['attr_id'] = attrid

        log.debug('_get_platform_attributes: %s', self._pp.pformat(attrs))
        return attrs

    def _load_params(self):

        log.info(" ---------- load_params ----------")
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

    def _create_platform_configuration(self, platform_id, parent_platform_id=None):
        """
        This method is an adaptation of test_agent_instance_config in
        test_instrument_management_service_integration.py

        @param platform_id
        @param parent_platform_id
        @return a DotDict with various of the constructed elements associated
                to the platform.
        """


        param_dict_name = 'platform_eng_parsed'
        parsed_rpdict_id = self.dataset_management.read_parameter_dictionary_by_name(
            param_dict_name,
            id_only=True)
        self.parsed_stream_def_id = self.pubsubclient.create_stream_definition(
            name='parsed',
            parameter_dictionary_id=parsed_rpdict_id)


        driver_config = PLTFRM_DVR_CONFIG
        driver_config['attributes'] = self._get_platform_attributes()    #self._platform_attributes[platform_id]

        #OMS returning an error for port.get_platform_ports
        #driver_config['ports']      = self._platform_ports[platform_id]
        log.debug("driver_config: %s", driver_config)

        # instance creation
        platform_agent_instance_obj = any_old(RT.PlatformAgentInstance, {
            'driver_config': driver_config})

        platform_agent_instance_obj.agent_config = {
                'platform_config': { 'platform_id': 'LPJBox_CI_Ben_Hall', 'parent_platform_id':  None }
            }

        self.platform_agent_instance_id = self.imsclient.create_platform_agent_instance(platform_agent_instance_obj)

        # agent creation
        platform_agent_obj = any_old(RT.PlatformAgent, {
            "stream_configurations": self._get_platform_stream_configs(),
            'driver_module':         PLTFRM_DVR_MOD,
            'driver_class':          PLTFRM_DVR_CLS})
        platform_agent_id = self.imsclient.create_platform_agent(platform_agent_obj)

        # device creation
        self.platform_device_id = self.imsclient.create_platform_device(any_old(RT.PlatformDevice))

        # data product creation
        dp_obj = any_old(RT.DataProduct)
        dp_id = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=self.parsed_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=self.platform_device_id, data_product_id=dp_id)
        self.dpclient.activate_data_product_persistence(data_product_id=dp_id)
        self.addCleanup(self.dpclient.delete_data_product, dp_id)

        # assignments
        self.RR2.assign_platform_agent_instance_to_platform_device_with_has_agent_instance(self.platform_agent_instance_id, self.platform_device_id)
        self.RR2.assign_platform_agent_to_platform_agent_instance_with_has_agent_definition(platform_agent_id, self.platform_agent_instance_id)
        self.RR2.assign_platform_device_to_org_with_has_resource(self.platform_agent_instance_id, self.org_id)

        #######################################
        # dataset

        log.debug('data product = %s', dp_id)

        stream_ids, _ = self.rrclient.find_objects(dp_id, PRED.hasStream, None, True)
        log.debug('Data product stream_ids = %s', stream_ids)
        stream_id = stream_ids[0]

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(dp_id, PRED.hasDataset, RT.Dataset, True)
        log.debug('Data set for data_product_id1 = %s', dataset_ids[0])
        #######################################


        log.debug('_create_platform_site_and_deployment  platform_device_id: %s', self.platform_device_id)

        site_object = IonObject(RT.PlatformSite, name='PlatformSite1')
        self.platform_site_id = self.omsclient.create_platform_site(platform_site=site_object, parent_id='')
        log.debug('_create_platform_site_and_deployment  site id: %s', self.platform_site_id)

        #create supporting objects for the Deployment resource
        # 1. temporal constraint
        # find current deployment using time constraints
        current_time =  int( calendar.timegm(time.gmtime()) )
        # two years on either side of current time
        start = current_time - 63115200
        end = current_time + 63115200
        temporal_bounds = IonObject(OT.TemporalBounds, name='planned', start_datetime=str(start), end_datetime=str(end))
        # 2. PlatformPort object which defines device to port map
        platform_port_obj= IonObject(OT.PlatformPort, reference_designator = 'GA01SUMO-FI003-01-CTDMO0999',
                                                        port_type=PortTypeEnum.UPLINK,
                                                        ip_address='0')

        # now create the Deployment
        deployment_obj = IonObject(RT.Deployment,
                                   name='TestPlatformDeployment',
                                   description='some new deployment',
                                   context=IonObject(OT.CabledNodeDeploymentContext),
                                   constraint_list=[temporal_bounds],
                                   port_assignments={self.platform_device_id:platform_port_obj})

        platform_deployment_id = self.omsclient.create_deployment(deployment=deployment_obj, site_id=self.platform_site_id, device_id=self.platform_device_id)
        log.debug('_create_platform_site_and_deployment  deployment_id: %s', platform_deployment_id)

        deploy_obj2 = self.omsclient.read_deployment(platform_deployment_id)
        log.debug('_create_platform_site_and_deployment  deploy_obj2 : %s', deploy_obj2)
        return self.platform_site_id, platform_deployment_id


    def _create_instrument_resources(self):
        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
            name='VEL3D',
            description="VEL3D")
        instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        log.debug( 'new InstrumentModel id = %s ', instModel_id)


        raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='raw' )
        vel3d_b_sample = StreamConfiguration(stream_name='vel3d_b_sample', parameter_dictionary_name='vel3d_b_sample')
        vel3d_b_engineering = StreamConfiguration(stream_name='vel3d_b_engineering', parameter_dictionary_name='vel3d_b_engineering')


        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent,
            name='agent007',
            description="SBE37IMAgent",
            driver_uri="http://sddevrepo.oceanobservatories.org/releases/nobska_mavs4_ooicore-0.0.7-py2.7.egg",
            stream_configurations = [raw_config, vel3d_b_sample, vel3d_b_engineering])
        instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        log.debug('new InstrumentAgent id = %s', instAgent_id)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        instDevice_obj = IonObject(RT.InstrumentDevice,
            name='VEL3DDevice',
            description="VEL3DDevice",
            serial_number="12345" )
        self.instrument_device_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, self.instrument_device_id)

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

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='VEL3DAgentInstance',
            description="VEL3DAgentInstance",
            port_agent_config = port_agent_config,
            alerts= [])


        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
            instAgent_id,
            self.instrument_device_id)
        self._start_port_agent(self.imsclient.read_instrument_agent_instance(instAgentInstance_id))

        vel3d_b_sample_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('vel3d_b_sample', id_only=True)
        vel3d_b_sample_stream_def_id = self.pubsubclient.create_stream_definition(name='vel3d_b_sample', parameter_dictionary_id=vel3d_b_sample_pdict_id)

        vel3d_b_engineering_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('vel3d_b_engineering', id_only=True)
        vel3d_b_engineering_stream_def_id = self.pubsubclient.create_stream_definition(name='vel3d_b_engineering', parameter_dictionary_id=vel3d_b_engineering_pdict_id)

        raw_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('raw', id_only=True)
        raw_stream_def_id = self.pubsubclient.create_stream_definition(name='raw', parameter_dictionary_id=raw_pdict_id)


        #-------------------------------
        # Create Raw and Parsed Data Products for the device
        #-------------------------------
        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='vel3d_b_sample',
            description='vel3d_b_sample')

        data_product_id1 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=vel3d_b_sample_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=self.instrument_device_id, data_product_id=data_product_id1)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1)


        dp_obj = IonObject(RT.DataProduct,
            name='vel3d_b_engineering',
            description='vel3d_b_engineering')

        data_product_id2 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=vel3d_b_engineering_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=self.instrument_device_id, data_product_id=data_product_id2)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2)


        dp_obj = IonObject(RT.DataProduct,
            name='the raw data',
            description='raw stream test')

        data_product_id3 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=self.instrument_device_id, data_product_id=data_product_id3)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id3)

        #create instrument site and associated deployment
        site_object = IonObject(RT.InstrumentSite, name='InstrumentSite1')
        instrument_site_id = self.omsclient.create_instrument_site(instrument_site=site_object, parent_id=self.platform_site_id)
        log.debug('_create_instrument_site_and_deployment  site id: %s', instrument_site_id)


        #create supporting objects for the Deployment resource
        # 1. temporal constraint
        # find current deployment using time constraints
        current_time =  int( calendar.timegm(time.gmtime()) )
        # two years on either side of current time
        start = current_time - 63115200
        end = current_time + 63115200
        temporal_bounds = IonObject(OT.TemporalBounds, name='planned', start_datetime=str(start), end_datetime=str(end))
        # 2. PlatformPort object which defines device to port map
        platform_port_obj= IonObject(OT.PlatformPort, reference_designator = 'GA01SUMO-FI003-03-CTDMO0999',
                                                        port_type=PortTypeEnum.PAYLOAD,
                                                        ip_address='0')

        # now create the Deployment
        deployment_obj = IonObject(RT.Deployment,
                                   name='TestInstrumentDeployment',
                                   description='some new deployment',
                                   context=IonObject(OT.CabledInstrumentDeploymentContext),
                                   constraint_list=[temporal_bounds],
                                   port_assignments={self.instrument_device_id:platform_port_obj})

        instrument_deployment_id = self.omsclient.create_deployment(deployment=deployment_obj, site_id=instrument_site_id, device_id=self.instrument_device_id)
        log.debug('_create_instrument_site_and_deployment  deployment_id: %s', instrument_deployment_id)




    def _start_port_agent(self, instrument_agent_instance_obj=None):
        """
        Construct and start the port agent, ONLY NEEDED FOR INSTRUMENT AGENTS.
        """

        _port_agent_config = instrument_agent_instance_obj.port_agent_config

        # It blocks until the port agent starts up or a timeout
        _pagent = PortAgentProcess.launch_process(_port_agent_config,  test_mode = True)
        pid = _pagent.get_pid()
        port = _pagent.get_data_port()
        cmd_port = _pagent.get_command_port()
        log.info("IMS:_start_pagent returned from PortAgentProcess.launch_process pid: %s ", pid)

        # Hack to get ready for DEMO.  Further though needs to be put int
        # how we pass this config info around.
        host = 'localhost'

        driver_config = instrument_agent_instance_obj.driver_config
        comms_config = driver_config.get('comms_config')
        if comms_config:
            host = comms_config.get('addr')
        else:
            log.warn("No comms_config specified, using '%s'" % host)

        # Configure driver to use port agent port number.
        instrument_agent_instance_obj.driver_config['comms_config'] = {
            'addr' : host,
            'cmd_port' : cmd_port,
            'port' : port
        }
        instrument_agent_instance_obj.driver_config['pagent_pid'] = pid
        self.imsclient.update_instrument_agent_instance(instrument_agent_instance_obj)
        return self.imsclient.read_instrument_agent_instance(instrument_agent_instance_obj._id)


    def _start_platform(self):
        """
        Starts the given platform waiting for it to transition to the
        UNINITIALIZED state (note that the agent starts in the LAUNCHING state).

        More in concrete the sequence of steps here are:
        - prepares subscriber to receive the UNINITIALIZED state transition
        - launches the platform process
        - waits for the start of the process
        - waits for the transition to the UNINITIALIZED state
        """
        ##############################################################
        # prepare to receive the UNINITIALIZED state transition:
        async_res = AsyncResult()

        def consume_event(evt, *args, **kwargs):
            log.debug("Got ResourceAgentStateEvent %s from origin %r", evt.state, evt.origin)
            if evt.state == PlatformAgentState.UNINITIALIZED:
                async_res.set(evt)

        # start subscriber:
        sub = EventSubscriber(event_type="ResourceAgentStateEvent",
                              origin=self.platform_device_id,
                              callback=consume_event)
        sub.start()
        log.info("registered event subscriber to wait for state=%r from origin %r",
                 PlatformAgentState.UNINITIALIZED, self.platform_device_id)
        #self._event_subscribers.append(sub)
        sub._ready_event.wait(timeout=EVENT_TIMEOUT)

        ##############################################################
        # now start the platform:
        agent_instance_id = self.platform_agent_instance_id
        log.debug("about to call start_platform_agent_instance with id=%s", agent_instance_id)
        pid = self.imsclient.start_platform_agent_instance(platform_agent_instance_id=agent_instance_id)
        log.debug("start_platform_agent_instance returned pid=%s", pid)

        #wait for start
        agent_instance_obj = self.imsclient.read_platform_agent_instance(agent_instance_id)
        gate = AgentProcessStateGate(self.processdispatchclient.read_process,
                                     self.platform_device_id,
                                     ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(90), "The platform agent instance did not spawn in 90 seconds")

        # Start a resource agent client to talk with the agent.
        self._pa_client = ResourceAgentClient(self.platform_device_id,
                                              name=gate.process_id,
                                              process=FakeProcess())
        log.debug("got platform agent client %s", str(self._pa_client))

        ##############################################################
        # wait for the UNINITIALIZED event:
        async_res.get(timeout=self._receive_timeout)

    def _register_oms_listener(self):

        #load the paramaters and the param dicts necesssary for the VEL3D
        log.debug( "---------- connect_to_oms ---------- ")
        log.debug("oms_uri = %s", OMS_URI)
        self.oms = CIOMSClientFactory.create_instance(OMS_URI)

        #buddha url
        url = "http://10.22.88.168:5000/ion-service/oms_event"
        log.info("test_oms_events_receive:setup http url %s", url)

        result = self.oms.event.register_event_listener(url)
        log.debug("_register_oms_listener register_event_listener result %s", result)

        #-------------------------------------------------------------------------------------
        # Set up the subscriber to catch the alert event
        #-------------------------------------------------------------------------------------

        def callback_for_alert(event, *args, **kwargs):
            log.debug("caught an OMSDeviceStatusEvent: %s", event)
            self.catch_alert.put(event)

        self.event_subscriber = EventSubscriber(event_type='OMSDeviceStatusEvent',
            callback=callback_for_alert)

        self.event_subscriber.start()
        self.addCleanup(self.event_subscriber.stop)


        result = self.oms.event.generate_test_event({'platform_id': 'fake_platform_id', 'message': "fake event triggered from CI using OMS' generate_test_event", 'severity': '3', 'group ': 'power'})
        log.debug("_register_oms_listener generate_test_event result %s", result)


    def _stop_platform(self):
        try:
            self.IMS.stop_platform_agent_instance(self.platform_agent_instance_id)
        except Exception:
            log.warn(
                "platform_id=%r: Exception in IMS.stop_platform_agent_instance with "
                "platform_agent_instance_id = %r. Perhaps already dead.",
                self.platform_device_id, self.platform_agent_instance_id)
