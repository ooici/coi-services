#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Tue Oct 16 09:14:37 EDT 2012
@file ion/services/dm/utility/test/test_granule.py
@brief Tests for granule
'''

from pyon.ion.stream import StandaloneStreamPublisher, StandaloneStreamSubscriber
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule import RecordDictionaryTool

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

from gevent.event import Event
from nose.plugins.attrib import attr

import numpy as np

@attr('INT',group='dm')
class RecordDictionaryIntegrationTest(IonIntegrationTestCase):
    xps = []
    xns = []
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management = DatasetManagementServiceClient()
        self.pubsub_management  = PubsubManagementServiceClient()

        self.rdt                      = None
        self.data_producer_id         = None
        self.provider_metadata_update = None
        self.event                    = Event()

    def tearDown(self):
        for xn in self.xns:
            xni = self.container.ex_manager.create_xn_queue(xn)
            xni.delete()
        for xp in self.xps:
            xpi = self.container.ex_manager.create_xp(xp)
            xpi.delete()

    def verify_incoming(self, m,r,s):
        rdt = RecordDictionaryTool.load_from_granule(m)
        self.assertEquals(rdt, self.rdt)
        self.assertEquals(m.data_producer_id, self.data_producer_id)
        self.assertEquals(m.provider_metadata_update, self.provider_metadata_update)
        self.event.set()


    def test_granule(self):
        
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        stream_def_id = self.pubsub_management.create_stream_definition('ctd', parameter_dictionary_id=pdict_id)
        pdict = DatasetManagementService.get_parameter_dictionary_by_name('ctd_parsed_param_dict')

        stream_id, route = self.pubsub_management.create_stream('ctd_stream', 'xp1', stream_definition_id=stream_def_id)
        self.xps.append('xp1')
        publisher = StandaloneStreamPublisher(stream_id, route)

        subscriber = StandaloneStreamSubscriber('sub', self.verify_incoming)
        subscriber.start()

        subscription_id = self.pubsub_management.create_subscription('sub', stream_ids=[stream_id])
        self.xns.append('sub')
        self.pubsub_management.activate_subscription(subscription_id)


        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(10)
        rdt['temp'] = np.random.randn(10) * 10 + 30
        rdt['pressure'] = [20] * 10

        self.assertEquals(set(pdict.keys()), set(rdt.fields))
        self.assertEquals(pdict.temporal_parameter_name, rdt.temporal_parameter)

        self.rdt = rdt
        self.data_producer_id = 'data_producer'
        self.provider_metadata_update = {1:1}

        publisher.publish(rdt.to_granule(data_producer_id='data_producer', provider_metadata_update={1:1}))

        self.assertTrue(self.event.wait(10))
        
        self.pubsub_management.deactivate_subscription(subscription_id)
        self.pubsub_management.delete_subscription(subscription_id)


