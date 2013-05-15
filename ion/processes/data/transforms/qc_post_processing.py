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
import numpy as np

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
        self.data_retriever = DataRetrieverServiceProcessClient(process=self)
        self.interval_key = self.CFG.get_safe('process.interval_key',None)
        self.qc_params    = self.CFG.get_safe('process.qc_params',[])
        validate_is_not_none(self.interval_key, 'An interval key is necessary to paunch this process')
        self.event_subscriber = EventSubscriber(event_type=OT.TimerEvent, origin=self.interval_key, callback=self._event_callback, auto_delete=True)
        self.resource_registry = self.container.resource_registry
        self.event_subscriber.start()

    def on_quit(self):
        self.event_subscriber.stop()
    
    def _event_callback(self, *args, **kwargs):
        dataset_ids, _ = self.resource_registry.find_resources(restype=RT.Dataset, id_only=True)
        for dataset_id in dataset_ids:
            self.process(dataset_id)

    def process(self, dataset_id, start_time=0, end_time=0):
        if not dataset_id:
            raise BadRequest('No dataset id specified.')
        now = time.time()
        start_time = start_time or (now - (3600*25)) # Do 25 hours to overlap
        end_time   = end_time or now
        
        qc_params  = [i for i in self.qc_params if i in self.qc_suffixes] or self.qc_suffixes
        
        self.qc_publisher = EventPublisher(event_type=OT.ParameterQCEvent)

        for st,et in self.chop(int(start_time),int(end_time)):
            granule = self.data_retriever.retrieve(dataset_id, query={'start_time':st, 'end_time':et})
            rdt = RecordDictionaryTool.load_from_granule(granule)
            qc_fields = [i for i in rdt.fields if any([i.endswith(j) for j in qc_params])]
            for field in qc_fields:
                val = rdt[field]
                if val is None:
                    continue
                if not np.all(val):
                    indexes = np.where(val==0)
                    timestamps = rdt[rdt.temporal_parameter][indexes[0]]
                    self.flag_qc_parameter(dataset_id, field, timestamps.tolist(),{})



    def flag_qc_parameter(self, dataset_id, parameter, temporal_values, configuration):
        data_product_ids, _ = self.resource_registry.find_subjects(object=dataset_id, subject_type=RT.DataProduct, predicate=PRED.hasDataset, id_only=True)
        for data_product_id in data_product_ids:
            self.qc_publisher.publish_event(origin=data_product_id, qc_parameter=parameter, temporal_values=temporal_values, configuration=configuration)

    @classmethod
    def chop(cls, start_time, end_time):
        while start_time < end_time:
            yield (start_time, min(start_time+3600, end_time))
            start_time = min(start_time+3600, end_time)
        return


