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
from pyon.ion.event import EventSubscriber, EventPublisher
from uuid import uuid4
from interface.objects import ProcessStateEnum
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from interface.services.cei.ischeduler_service import SchedulerServiceClient
from pyon.public import OT
import time
import numpy as np
from gevent.queue import Queue, Empty

@attr('INT',group='dm')
class TestQCPostProcessing(DMTestCase):
    '''
    ion/processes/data/transforms/test/test_qc_post_processing.py:TestQCPostProcessing
    '''
    def setUp(self):
        DMTestCase.setUp(self)

        process_definition = ProcessDefinition(name='qc_post_processor',
                executable={'module':'ion.processes.data.transforms.qc_post_processing', 'class':'QCPostProcessing'})
        self.process_definition_id = self.process_dispatcher.create_process_definition(process_definition)
        self.addCleanup(self.process_dispatcher.delete_process_definition,self.process_definition_id)

        self.process_id = self.process_dispatcher.create_process(self.process_definition_id)
        self.scheduler_service = SchedulerServiceClient()


    def populate_qc_tables(self):
        svm = StoredValueManager(self.container)
        svm.stored_value_cas('grt_QCTEST_TEMPWAT', {'grt_min_value':-2., 'grt_max_value':40.})
        svm.stored_value_cas('svt_QCTEST_TEMPWAT', {'svt_resolution':0.001, 'svt_n': 4})
        svm.stored_value_cas('spike_QCTEST_TEMPWAT', {'acc': 0.1, 'spike_n':5, 'spike_l':5})
    
    def sync_launch(self, config):
        self.process_dispatcher.schedule_process(self.process_definition_id, process_id=self.process_id, configuration=config)

        gate = ProcessStateGate(self.process_dispatcher.read_process,
                self.process_id,
                ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(30))
        self.addCleanup(self.process_dispatcher.cancel_process, self.process_id)

    def make_data_product(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_simple_qc_pdict()

        stream_def_id = self.create_stream_definition('global range', parameter_dictionary_id=pdict_id, stream_configuration={'reference_designator':'QCTEST'})

        self.populate_qc_tables()

        dp_id = self.create_data_product('qc data product', stream_def_id=stream_def_id)
        self.activate_data_product(dp_id)
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(dp_id)
        return dp_id, dataset_id, stream_def_id

    def make_large_dataset(self, temp_vector):

        monitor_queue = Queue()
        # Make 27 hours of data
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        data_product_id, dataset_id, stream_def_id = self.make_data_product()
        es = EventSubscriber(event_type=OT.DatasetModified, origin=dataset_id, auto_delete=True, callback = lambda *args, **kwargs : monitor_queue.put(1))
        es.start()
        self.addCleanup(es.stop)
        for rdt in self.populate_vectors(stream_def_id, 3, temp_vector):
            ph.publish_rdt_to_data_product(data_product_id, rdt)

        try:
            for i in xrange(3):
                monitor_queue.get(timeout=10)
        except Empty:
            raise AssertionError('Failed to populate dataset in time')

            
        return data_product_id

    def populate_vectors(self, stream_def_id, hours, temp_vector):
        now = time.time()
        ntp_now = now + 2208988800


        for i in xrange(hours):
            rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
            st = ntp_now - (3600 * (hours-i))
            et = ntp_now - (3600 * (hours - (i+1)))
            rdt['time'] = np.arange(st, et)
            rdt['temp'] = temp_vector(3600)
            yield rdt

    
    def process_execution(self, temp_vector, qc_params, bad_times):
        interval_key = uuid4().hex
        data_product_id = self.make_large_dataset(temp_vector)
        async_queue = Queue()

        def cb(event, *args, **kwargs):
            if '_'.join(event.qc_parameter.split('_')[1:]) not in qc_params:
                # I don't care about
                return
            times = event.temporal_values
            self.assertEquals(len(times), bad_times)
            async_queue.put(1)


        es = EventSubscriber(event_type=OT.ParameterQCEvent, origin=data_product_id, callback=cb, auto_delete=True)
        es.start()
        self.addCleanup(es.stop)
        config = DotDict()
        config.process.interval_key = interval_key
        config.process.qc_params = qc_params
        self.sync_launch(config)

        # So now the process is started, time to throw an event at it
        ep = EventPublisher(event_type='TimerEvent')
        ep.publish_event(origin=interval_key)

        try:
            async_queue.get(timeout=120)
        except Empty:
            raise AssertionError('QC was not flagged in time')

    def test_glblrng_qc_processing(self):
        def temp_vector(size):
            return [41] + [39]*(size-1)

        self.process_execution(temp_vector, ['glblrng_qc'], 1)

    def test_stuckvl_qc_processing(self):
        def temp_vector(size):
            assert size > 7
            return [20] * 6 + range(size-6)

        self.process_execution(temp_vector, ['stuckvl_qc'], 6)

    def test_spketst_qc_processing(self):
        def temp_vector(size):
            assert size > 8
            return [-1, 3, 40, -1, 1, -6, -6, 1] + [5] * (size-8)

        self.process_execution(temp_vector, ['spketst_qc'], 1)


    
    def test_qc_interval_integration(self):

        # 1 need to make a dataset that only has one discrete qc violation
        # 2 Launch the process
        # 3 Setup the scheduler to run it say three times
        # 4 Get the Events and verify the data
    
        #-------------------------------------------------------------------------------- 
        # Make a dataset that has only one discrete qc violation
        #-------------------------------------------------------------------------------- 

        dp_id, dataset_id, stream_def_id = self.make_data_product()
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        monitor = DatasetMonitor(dataset_id)
        self.addCleanup(monitor.stop)
        for rdt in self.populate_vectors(stream_def_id, 1, lambda x : [41] + [39] * (x-1)):
            ph.publish_rdt_to_data_product(dp_id, rdt)
        self.assertTrue(monitor.event.wait(10)) # Give it 10 seconds to populate


        #--------------------------------------------------------------------------------
        # Launch the process
        #--------------------------------------------------------------------------------

        interval_key = uuid4().hex
        config = DotDict()
        config.process.interval_key = interval_key
        config.process.qc_params = ['glblrng_qc'] # The others are tested in other tests for completeness
        self.sync_launch(config)

        async_queue = Queue()
        def callback(event, *args, **kwargs):
            times = event.temporal_values
            self.assertEquals(len(times), 1)
            async_queue.put(1)
        es = EventSubscriber(event_type=OT.ParameterQCEvent, origin=dp_id, callback=callback, auto_delete=True)
        es.start()
        self.addCleanup(es.stop)

        #--------------------------------------------------------------------------------
        # Setup the scheduler
        #--------------------------------------------------------------------------------


        timer_id = self.scheduler_service.create_interval_timer(start_time=time.time(),
                end_time=time.time()+13,
                interval=5,
                event_origin=interval_key)


        #--------------------------------------------------------------------------------
        # Get the events and verify them
        #--------------------------------------------------------------------------------

        try:
            for i in xrange(2):
                async_queue.get(timeout=10)
        except Empty:
            raise AssertionError('QC Events not raised')
            

