#!/usr/bin/env python
'''
@author Luke Campbell <My email is around here somewhere>
@file ion/processes/data/transforms/qc_post_processing.py
@date Tue May  7 15:34:54 EDT 2013
'''

from pyon.core.exception import BadRequest
from pyon.ion.process import ImmediateProcess
from interface.services.dm.idata_retriever_service import DataRetrieverServiceProcessClient
from ion.services.dm.utility.granule import RecordDictionaryTool
import time
from pyon.ion.event import EventPublisher
from pyon.public import OT
import numpy as np

class QCPostProcessing(ImmediateProcess):
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
        dataset_id = self.CFG.get_safe('process.dataset_id', None)
        data_retriever = DataRetrieverServiceProcessClient(process=self)
        now = time.time()
        if not dataset_id:
            raise BadRequest('No dataset id specified.')
        start_time = self.CFG.get_safe('process.start_time', now - (3600*24))
        end_time   = self.CFG.get_safe('process.end_time', now)
        qc_params  = [i for i in self.CFG.get_safe('process.qc_parameters', self.qc_suffixes) if i in self.qc_suffixes]
        self.qc_publisher = EventPublisher(event_type=OT.ParameterQCEvent)

        for st,et in self.chop(int(start_time),int(end_time)):
            granule = data_retriever.retrieve(dataset_id, query={'start_time':st, 'end_time':et})
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
        self.qc_publisher.publish_event(origin=dataset_id, qc_parameter=parameter, temporal_values=temporal_values, configuration=configuration)

    @classmethod
    def chop(cls, start_time, end_time):
        while start_time < end_time:
            yield (start_time, min(start_time+3600, end_time))
            start_time = min(start_time+3600, end_time)
        return


