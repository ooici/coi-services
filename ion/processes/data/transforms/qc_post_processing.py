#!/usr/bin/env python
'''
@author Luke Campbell <My email is around here somewhere>
@file ion/processes/data/transforms/qc_post_processing.py
@date Tue May  7 15:34:54 EDT 2013
'''

from pyon.core.exception import BadRequest
from pyon.ion.process import ImmediateProcess, SimpleProcess
from interface.services.dm.idata_retriever_service import DataRetrieverServiceProcessClient
from ion.services.dm.utility.granule import RecordDictionaryTool
import time
from pyon.ion.event import EventPublisher
from pyon.public import OT, RT,PRED
from pyon.util.arg_check import validate_is_not_none
from pyon.ion.event import EventSubscriber
from pyon.util.log import log
from gevent.event import Event
import numpy as np
import threading

class QCPostProcessing(SimpleProcess):
    '''
    QC Post Processing Process

    This process provides the capability to ION clients and operators to evaluate the automated quality control flags on
    various data products. This process should be run periodically with overlapping spans of data to ensure complete
    dataset QC verification.

    This parameters that this process accepts as configurations are:
        - dataset_id: The dataset identifier, required.
        - start_time: Unix timestamp, defaults to 24 hours in the past
        - end_time: Unix timestamp, defaults to current time
        - qc_params: a list of qc functions to evaluate, currently supported functions are: ['glblrng_qc',
          'spketst_qc', 'stuckvl_qc'], defaults to all

    '''

    qc_suffixes = ['glblrng_qc', 'spketst_qc', 'stuckvl_qc']
    def on_start(self):
        SimpleProcess.on_start(self)
        self.data_retriever = DataRetrieverServiceProcessClient(process=self)
        self.interval_key = self.CFG.get_safe('process.interval_key',None)
        self.qc_params    = self.CFG.get_safe('process.qc_params',[])
        validate_is_not_none(self.interval_key, 'An interval key is necessary to paunch this process')
        self.event_subscriber = EventSubscriber(event_type=OT.TimerEvent, origin=self.interval_key, callback=self._event_callback, auto_delete=True)
        self.add_endpoint(self.event_subscriber)
        self.resource_registry = self.container.resource_registry
        self.run_interval = self.CFG.get_safe('service.qc_processing.run_interval', 24)
    
    def _event_callback(self, *args, **kwargs):
        log.info('QC Post Processing Triggered')
        dataset_ids, _ = self.resource_registry.find_resources(restype=RT.Dataset, id_only=True)
        for dataset_id in dataset_ids:
            log.info('QC Post Processing for dataset %s', dataset_id)
            try:
                self.process(dataset_id)
            except BadRequest as e:
                if 'Problems reading from the coverage' in e.message:
                    log.error('Failed to read from dataset')

    def process(self, dataset_id, start_time=0, end_time=0):
        if not dataset_id:
            raise BadRequest('No dataset id specified.')
        now = time.time()
        start_time = start_time or (now - (3600*(self.run_interval+1))) # Every N hours with 1 of overlap
        end_time   = end_time or now
        
        qc_params  = [i for i in self.qc_params if i in self.qc_suffixes] or self.qc_suffixes
        
        self.qc_publisher = EventPublisher(event_type=OT.ParameterQCEvent)
        log.debug('Iterating over the data blocks')

        for st,et in self.chop(int(start_time),int(end_time)):
            log.debug('Chopping %s:%s', st, et)
            log.debug("Retrieving data: data_retriever.retrieve('%s', query={'start_time':%s, 'end_time':%s')", dataset_id, st, et)
            try:
                granule = self.data_retriever.retrieve(dataset_id, query={'start_time':st, 'end_time':et})
            except BadRequest:
                data_products, _ = self.container.resource_registry.find_subjects(object=dataset_id, predicate=PRED.hasDataset, subject_type=RT.DataProduct)
                for data_product in data_products:
                    log.exception('Failed to perform QC Post Processing on %s', data_product.name)
                    log.error('Calculated Start Time: %s', st)
                    log.error('Calculated End Time:   %s', et)
                raise
            log.debug('Retrieved Data')
            rdt = RecordDictionaryTool.load_from_granule(granule)
            qc_fields = [i for i in rdt.fields if any([i.endswith(j) for j in qc_params])]
            log.debug('QC Fields: %s', qc_fields)
            for field in qc_fields:
                val = rdt[field]
                if val is None:
                    continue
                if not np.all(val):
                    log.debug('Found QC Alerts')
                    indexes = np.where(val==0)
                    timestamps = rdt[rdt.temporal_parameter][indexes[0]]
                    self.flag_qc_parameter(dataset_id, field, timestamps.tolist(),{})



    def flag_qc_parameter(self, dataset_id, parameter, temporal_values, configuration):
        log.info('Flagging QC for %s', parameter)
        data_product_ids, _ = self.resource_registry.find_subjects(object=dataset_id, subject_type=RT.DataProduct, predicate=PRED.hasDataset, id_only=True)
        for data_product_id in data_product_ids:
            self.qc_publisher.publish_event(origin=data_product_id, qc_parameter=parameter, temporal_values=temporal_values, configuration=configuration)

    @classmethod
    def chop(cls, start_time, end_time):
        while start_time < end_time:
            yield (start_time, min(start_time+3600, end_time))
            start_time = min(start_time+3600, end_time)
        return

class QCProcessor(SimpleProcess):
    def __init__(self):
        self.event = Event() # Synchronizes the thread
        self.timeout = 10

    def on_start(self):
        self._thread = self._process.thread_manager.spawn(self.event_loop)
        self.timeout = self.CFG.get_safe('endpoint.receive.timeout', 10)
        self.resource_registry = self.container.resource_registry
        self.qc_evals = [
            self.global_range,
            self.gradient_test,
            self.local_range_test,
            self.spike_test,
            self.stuck_value_test,
            self.trend_test ]

    def on_quit(self):
        self.suspend()

    def event_loop(self):
        threading.current_thread().name = '%s-qc-processor' % self.id
        while not self.event.wait(1):
            data_products, _ = self.container.resource_registry.find_resources(restype=RT.DataProduct, id_only=False)
            for data_product in data_products:
                log.error("Looking at %s", data_product.name)
                # Get the reference designator
                try:
                    rd = self._get_reference_designator(data_product._id)
                except BadRequest:
                    continue
                for p in self.get_parameters(data_product):
                    log.error(p.name)
                log.error("Data Product RD: %s", rd)
                for qc_eval in self.qc_evals:
                    qc_eval(data_product, rd)

                # Break early if we can
                if self.event.is_set(): 
                    break

    def suspend(self):
        self.event.set()
        self._thread.join(self.timeout)
        log.info("QC Thread Suspended")


    def _get_reference_designator(self, data_product_id=''):
        '''
        Returns the reference designator for a data product if it has one
        '''

        device_ids, _ = self.resource_registry.find_subjects(object=data_product_id, predicate=PRED.hasOutputProduct, subject_type=RT.InstrumentDevice, id_only=True)
        if not device_ids: 
            raise BadRequest("No instrument device associated with this data product")
        device_id = device_ids[0]

        sites, _ = self.resource_registry.find_subjects(object=device_id, predicate=PRED.hasDevice, subject_type=RT.InstrumentSite, id_only=False)
        if not sites:
            raise BadRequest("No site is associated with this data product")
        site = sites[0]
        rd = site.reference_designator
        return rd

    def global_range(self, data_product, rd):
        if data_product.qc_glblrng != 'applicable':
            log.error("Global Range not enabled")
            return
        log.error("Global Range enabled")

    def gradient_test(self, data_product, rd):
        if data_product.qc_gradtst != 'applicable':
            log.error("Gradient Test not enabled")
            return
        log.error("Gradient Test enabled")
            
    def local_range_test(self, data_product, rd):
        if data_product.qc_loclrng != 'applicable':
            log.error("Local Range Test not enabled")
            return
        log.error("Local Range Test enabled")
    
    def spike_test(self, data_product, rd):
        if data_product.qc_spketest != 'applicable':
            log.error("Spike Test not enabled")
            return
        log.error("Spike Test enabled")
    
    def stuck_value_test(self, data_product, rd):
        if data_product.qc_stuckvl != 'applicable':
            log.error("Stuck Value Test not enabled")
            return
        log.error("Stuck Value Test enabled")

    def trend_test(self, data_product, rd):
        if data_product.qc_trndtst != 'applicable':
            log.error("Trend Test not enabled")
            return
        log.error("Trend Test enabled")

    def fetch_lookup(self, data_product, rd, qc):
        # TODO: Object store stuff

        pass

    def get_parameters(self, data_product):

        # DataProduct -> StreamDefinition
        stream_defs, _ = self.resource_registry.find_objects(data_product._id, PRED.hasStreamDefinition, id_only=False)
        stream_def = stream_defs[0]

        # StreamDefinition -> ParameterDictionary
        pdict_ids, _ = self.resource_registry.find_objects(stream_def._id, PRED.hasParameterDictionary, id_only=True)
        pdict_id = pdict_ids[0]

        # ParameterDictionary -> ParameterContext
        pctxts, _ = self.resource_registry.find_objects(pdict_id, PRED.hasParameterContext, id_only=False)
        relevant = [ctx for ctx in pctxts if not stream_def.available_fields or (stream_def.available_fields and ctx.name in stream_def.available_fields)]
        return relevant

