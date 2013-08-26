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

# This import will dynamically load the driver egg.  It is needed for the MI includes below
import ion.agents.instrument.test.test_instrument_agent
from ion.agents.instrument.test.test_instrument_agent import DRV_URI_GOOD

from ion.services.dm.utility.granule_utils import time_series_domain

from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory

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

from interface.objects import Granule, DeviceStatusType, DeviceCommsType, StreamConfiguration
from interface.objects import AgentCommand, ProcessDefinition, ProcessStateEnum



from ion.processes.bootstrap.index_bootstrap import STD_INDEXES
from nose.plugins.attrib import attr
import gevent
import elasticpy as ep
from mock import patch

import time
import sys
from ion.agents.instrument.driver_process import ZMQEggDriverProcess

use_es = CFG.get_safe('system.elasticsearch',False)



# A VEL3D driver.
DRV_URI = 'http://sddevrepo.oceanobservatories.org/releases/nobska_mavs4_ooicore-0.0.8-py2.7.egg'
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


# Dynamically load the egg into the test path
launcher = ZMQEggDriverProcess(DVR_CONFIG)
egg = launcher._get_egg(DRV_URI)
if not egg in sys.path:
    sys.path.insert(0, egg)


START_AUTOSAMPLE = 'DRIVER_EVENT_START_AUTOSAMPLE'
STOP_AUTOSAMPLE = 'DRIVER_EVENT_STOP_AUTOSAMPLE'

OMS_URI = 'http://alice:1234@10.180.80.10:9021/'

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


class TestRSNIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        super(TestRSNIntegration, self).setUp()
        config = DotDict()
        #config.bootstrap.use_es = True

        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml', config)

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()

        self.catch_alert= gevent.queue.Queue()


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



    @unittest.skip('Still in construction...')
    def test_oms_events_receive(self):

        log.debug("oms_uri = %s", OMS_URI)
        self.oms = CIOMSClientFactory.create_instance(OMS_URI)

        #CI url
        #url = "http://128.54.26.120:5000/ion-service/oms_event"

        #buddha url
        url = "http://128.54.29.48:5000/ion-service/oms_event"

        log.info("test_oms_events_receive:setup http url %s", url)

        result = self.oms.event.register_event_listener(url)
        log.info("test_oms_events_receive:setup register_event_listener result %s", result)

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


        #test the listener
        result = self.oms.event.generate_test_event({'platform_id': 'fake_platform_id1', 'message': "fake event triggered from CI using OMS' generate_test_event", 'severity': '3', 'group ': 'power'})
        log.info("test_oms_events_receive:setup generate_test_event result %s", result)

        result = self.oms.event.generate_test_event({'platform_id': 'fake_platform_id2', 'message': "fake event triggered from CI using OMS' generate_test_event", 'severity': '3', 'group ': 'power'})
        log.info("test_oms_events_receive:setup generate_test_event result %s", result)

        result = self.oms.event.generate_test_event({'platform_id': 'fake_platform_id3', 'message': "fake event triggered from CI using OMS' generate_test_event", 'severity': '3', 'group ': 'power'})
        log.info("test_oms_events_receive:setup generate_test_event result %s", result)


        oms_event_counter = 0
        runtime = 0
        starttime = time.time()
        caught_events = []

        #catch several events to get some samples from OMS
        while oms_event_counter < 10 and runtime < 60 :
            a = self.catch_alert.get(timeout=60)
            caught_events.append(a)
            oms_event_counter += 1
            runtime = time.time() - starttime

        result = self.oms.event.unregister_event_listener(url)
        log.debug("unregister_event_listener result: %s", result)

        self.assertTrue(oms_event_counter > 0)