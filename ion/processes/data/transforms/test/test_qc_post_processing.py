#!/usr/bin/env python
'''
@author Luke Campbell <My email is around here somewhere>
@file ion/processes/data/transforms/test/test_qc_post_processing.py
@date Tue May  7 15:34:54 EDT 2013
'''

from ion.services.dm.test.dm_test_case import DMTestCase
from interface.objects import ProcessDefinition
from pyon.core.exception import BadRequest
from nose.plugins.attrib import attr
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.util.stored_values import StoredValueManager
from pyon.util.containers import DotDict
from gevent.event import Event
from pyon.ion.event import EventSubscriber
from pyon.public import OT
import time
import numpy as np

@attr('INT',group='dm')
class TestQCPostProcessing(DMTestCase):
    def setUp(self):
        DMTestCase.setUp(self)

        process_definition = ProcessDefinition(name='qc_post_processor',
                executable={'module':'ion.processes.data.transforms.qc_post_processing', 'class':'QCPostProcessing'})
        self.process_definition_id = self.process_dispatcher.create_process_definition(process_definition)
        self.addCleanup(self.process_dispatcher.delete_process_definition,self.process_definition_id)

        self.process_id = self.process_dispatcher.create_process(self.process_definition_id)


    def make_large_dataset(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_simple_qc_pdict()

        stream_def_id = self.pubsub_management.create_stream_definition('global range', parameter_dictionary_id=pdict_id, stream_configuration={'reference_designator':'QCTEST'})
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        svm = StoredValueManager(self.container)
        svm.stored_value_cas('grt_QCTEST_TEMPWAT', {'grt_min_value':-2., 'grt_max_value':40.})

        dp_id = self.create_data_product('qc data product', stream_def_id=stream_def_id)
        self.data_product_management.activate_data_product_persistence(dp_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, dp_id)
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(dp_id)

        # Make 24 hours of data
        for i in xrange(24):
            dataset_monitor = DatasetMonitor(dataset_id)
            rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
            rdt['time'] = np.arange(2208988800+(i*3600), 2208988800+((i+1)*3600))
            rdt['temp'] = [41] + [39] * 3599
            ph.publish_rdt_to_data_product(dp_id, rdt)
            dataset_monitor.event.wait(10)

            rdt = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve_last_data_points(dataset_id, 3600))
            np.testing.assert_array_almost_equal(rdt['tempwat_glblrng_qc'], [0] + [1]*3599)
            
        return dataset_id

    
    def test_qc_post_processing(self):
        with self.assertRaises(BadRequest):
            self.process_dispatcher.schedule_process(self.process_definition_id, process_id=self.process_id)
        dataset_id = self.make_large_dataset()
        config = DotDict()
        config.process.dataset_id = dataset_id
        config.process.start_time = 0
        config.process.end_time = 3600
        config.process.qc_parameters = ['glblrng_qc']


        flagged = Event()
        def cb(event, *args, **kwargs):
            times = event.temporal_values
            self.assertEquals(times,[2208988800])
            flagged.set()

        event_subscriber = EventSubscriber(event_type=OT.ParameterQCEvent, origin=dataset_id, callback=cb, auto_delete=True) 
        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)
        self.process_dispatcher.schedule_process(self.process_definition_id, process_id=self.process_id, configuration=config)

        self.assertTrue(flagged.wait(10))



