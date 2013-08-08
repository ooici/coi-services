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

from pyon.public import log

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
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from pyon.util.containers import DotDict

from interface.objects import AgentCommand, StreamConfiguration, ProcessStateEnum
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType

from ion.agents.platform.platform_agent import PlatformAgentState, PlatformAgentEvent
from ion.agents.port.port_agent_process import PortAgentProcess

from ion.services.cei.process_dispatcher_service import ProcessStateGate

from ion.agents.platform.rsn.test.oms_test_mixin import OmsTestMixin

import unittest

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

# initialization of the driver configuration. See setUp for possible update
# of the 'oms_uri' entry related with the special value "launchsimulator".
PLTFRM_DVR_CONFIG = {
    'oms_uri': 'http://alice:1234@10.180.80.10:9021/'
}

PLTFRM_DVR_MOD = 'ion.agents.platform.rsn.rsn_platform_driver'
PLTFRM_DVR_CLS = 'RSNPlatformDriver'
OMS_URI = 'http://alice:1234@10.180.80.10:9021/'

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

# now we can import Mavs4 ProtocolEvent
#from mi.instrument.nobska.mavs4.ooicore.driver import ProtocolEvent

# ------------------------------------------------------------------------


@patch.dict(CFG, {'endpoint': {'receive': {'timeout': 180}}})
class TestPlatformInstrument(BaseIntTestPlatform):

    def setUp(self):
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()
        self.RR2  = EnhancedResourceRegistryClient(self.rrclient)

        self.org_id = self.RR2.create(any_old(RT.Org))
        log.debug("Org created: %s", self.org_id)

        # see _set_receive_timeout
        self._receive_timeout = 177

        self.instrument_device = ''
        self.platform_device = ''
        self.platform_agent_instance_id = ''
        self._pa_client = ''

        log.debug("oms_uri = %s", OMS_URI)
        self.oms = CIOMSClientFactory.create_instance(OMS_URI)
        url = OmsTestMixin.start_http_server()
        log.info("TestPlatformInstrument:setup http url %s", url)

        result = self.oms.event.register_event_listener(url)
        log.info("TestPlatformInstrument:setup register_event_listener result %s", result)


#        response = self.oms.port.get_platform_ports('LPJBox_CI_Ben_Hall')
#        log.info("TestPlatformInstrument:setup get_platform_ports %s", response)

        def done():
            CIOMSClientFactory.destroy_instance(self.oms)
            event_notifications = OmsTestMixin.stop_http_server()
            log.info("event_notifications = %s" % str(event_notifications))

        self.addCleanup(done)


    @unittest.skip('Still in construction...')
    def test_platform_with_instrument_streaming(self):
        #
        # The following is with just a single platform and the single
        # instrument "SBE37_SIM_08", which corresponds to the one on port 4008.
        #

        #load the paramaters and the param dicts necesssary for the VEL3D
        self._load_params()

        #create the instrument device/agent/mode
        self._create_instrument_resources()

        #create the platform device, agent and instance
        self._create_platform_configuration('LPJBox_CI_Ben_Hall')

        self.rrclient.create_association(subject=self.platform_device, predicate=PRED.hasDevice, object=self.instrument_device)


        self._start_platform()
#        self.addCleanup(self._stop_platform, p_root)

        # get everything in command mode:
        self._ping_agent()
        self._initialize()


        _ia_client = ResourceAgentClient(self.instrument_device, process=FakeProcess())
        state = _ia_client.get_agent_state()
        log.info("TestPlatformInstrument get_agent_state %s", state)


        self._go_active()
#        self._run()

        gevent.sleep(3)

        # note that this includes the instrument also getting to the command state

#        self._stream_instruments()

        # get client to the instrument:
        # the i_obj is a DotDict with various pieces captured during the
        # set-up of the instrument, in particular instrument_device_id
        #i_obj = self._get_instrument(instr_key)

#        log.debug("KK creating ResourceAgentClient")
#        ia_client = ResourceAgentClient(i_obj.instrument_device_id,
#                                        process=FakeProcess())
#        log.debug("KK got ResourceAgentClient: %s", ia_client)
#
#        # verify the instrument is command state:
#        state = ia_client.get_agent_state()
#        log.debug("KK instrument state: %s", state)
#        self.assertEqual(state, ResourceAgentState.COMMAND)



        self._reset()
        self._shutdown()




    def _load_params(self):

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

    def _create_platform_configuration(self, platform_id, parent_platform_id=None):
        """
        This method is an adaptation of test_agent_instance_config in
        test_instrument_management_service_integration.py

        @param platform_id
        @param parent_platform_id
        @return a DotDict with various of the constructed elements associated
                to the platform.
        """

        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()

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
        self.platform_device = self.imsclient.create_platform_device(any_old(RT.PlatformDevice))

        # data product creation
        dp_obj = any_old(RT.DataProduct, {"temporal_domain":tdom, "spatial_domain": sdom})
        dp_id = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=self.parsed_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=self.platform_device, data_product_id=dp_id)
        self.dpclient.activate_data_product_persistence(data_product_id=dp_id)
        self.addCleanup(self.dpclient.delete_data_product, dp_id)

        # assignments
        self.RR2.assign_platform_agent_instance_to_platform_device_with_has_agent_instance(self.platform_agent_instance_id, self.platform_device)
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


        return

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
        self.instrument_device = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, self.instrument_device)

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
            self.instrument_device)
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
            description='vel3d_b_sample',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id1 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=vel3d_b_sample_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=self.instrument_device, data_product_id=data_product_id1)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1)


        dp_obj = IonObject(RT.DataProduct,
            name='vel3d_b_engineering',
            description='vel3d_b_engineering',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id2 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=vel3d_b_engineering_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=self.instrument_device, data_product_id=data_product_id2)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2)


        dp_obj = IonObject(RT.DataProduct,
            name='the raw data',
            description='raw stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id3 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=raw_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=self.instrument_device, data_product_id=data_product_id3)
        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id3)





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









    def _get_platform_attributes(self):
        # Use the network definition provided by RSN OMS directly.
        rsn_oms = CIOMSClientFactory.create_instance('http://alice:1234@10.180.80.10:9021/')
        attr_infos = rsn_oms.attr.get_platform_attributes('LPJBox_CI_Ben_Hall')

        log.debug('_get_platform_attributes: %s', attr_infos)

#        ret_infos = attr_infos['LPJBox_CI_Ben_Hall']
#        for attrName, attr_defn in ret_infos.iteritems():
#            attr = AttrNode(attrName, attr_defn)
#            pnode.add_attribute(attr)
        return attr_infos


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
                              origin=self.platform_device,
                              callback=consume_event)
        sub.start()
        log.info("registered event subscriber to wait for state=%r from origin %r",
                 PlatformAgentState.UNINITIALIZED, self.platform_device)
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
                                     self.platform_device._id,
                                     ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(90), "The platform agent instance did not spawn in 90 seconds")

        # Start a resource agent client to talk with the agent.
        self._pa_client = ResourceAgentClient(self.platform_device,
                                              name=gate.process_id,
                                              process=FakeProcess())
        log.debug("got platform agent client %s", str(self._pa_client))

        ##############################################################
        # wait for the UNINITIALIZED event:
        async_res.get(timeout=self._receive_timeout)