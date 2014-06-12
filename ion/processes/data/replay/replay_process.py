#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/replay/replay_process.py
@date 06/14/12 13:31
@description Implementation for a replay process.
'''

from pyon.core.exception import BadRequest
from pyon.core.object import IonObjectDeserializer
from pyon.core.bootstrap import get_obj_registry
from pyon.util.arg_check import validate_is_instance
from pyon.util.log import log

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.util.time_utils import TimeUtils

from coverage_model import utils
from coverage_model.parameter_functions import ParameterFunctionException

from interface.services.dm.idataset_management_service import DatasetManagementServiceProcessClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from interface.services.dm.ireplay_process import BaseReplayProcess
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

from pyon.core.exception import CorruptionError

from gevent.event import Event
from numbers import Number
import gevent
import numpy as np
from datetime import datetime
import calendar
from pyon.util.breakpoint import debug_wrapper

class ReplayProcess(BaseReplayProcess):

    '''
    ReplayProcess - A process spawned for the purpose of replaying data
    --------------------------------------------------------------------------------
    Configurations
    ==============
    process:
      dataset_id:      ""     # Dataset to be replayed
      delivery_format: {}     # Delivery format to be replayed back (unused for now)
      query:
        start_time: 0         # Start time (index value) to be replayed
        end_time:   0         # End time (index value) to be replayed
        parameters: []        # List of parameters to form in the granule
      

    '''
    process_type  = 'standalone'
    publish_limit = 10
    dataset_id    = None
    delivery_format = {}
    start_time      = None
    end_time        = None
    stride_time     = None
    parameters      = None
    stream_id       = ''
    stream_def_id   = ''


    def __init__(self, *args, **kwargs):
        super(ReplayProcess,self).__init__(*args,**kwargs)
        self.deserializer = IonObjectDeserializer(obj_registry=get_obj_registry())
        self.publishing   = Event()
        self.play         = Event()
        self.end          = Event()

    def on_start(self):
        '''
        Starts the process
        '''
        log.info('Replay Process Started')
        super(ReplayProcess,self).on_start()
        dsm_cli = DatasetManagementServiceProcessClient(process=self)
        pubsub  = PubsubManagementServiceProcessClient(process=self)

        self.dataset_id      = self.CFG.get_safe('process.dataset_id', None)
        self.delivery_format = self.CFG.get_safe('process.delivery_format',{})
        self.start_time      = self.CFG.get_safe('process.query.start_time', None)
        self.end_time        = self.CFG.get_safe('process.query.end_time', None)
        self.stride_time     = self.CFG.get_safe('process.query.stride_time', None)
        self.parameters      = self.CFG.get_safe('process.query.parameters',None)
        self.publish_limit   = self.CFG.get_safe('process.query.publish_limit', 10)
        self.tdoa            = self.CFG.get_safe('process.query.tdoa',None)
        self.stream_id       = self.CFG.get_safe('process.publish_streams.output', '')
        self.stream_def      = pubsub.read_stream_definition(stream_id=self.stream_id)
        self.stream_def_id   = self.stream_def._id
        self.replay_thread   = None

        self.publishing.clear()
        self.play.set()
        self.end.clear()

        if self.dataset_id is None:
            raise BadRequest('dataset_id not specified')

        self.dataset = dsm_cli.read_dataset(self.dataset_id)
        self.pubsub = PubsubManagementServiceProcessClient(process=self)


    @classmethod
    def get_time_idx(cls, coverage, timeval):
        corrected_time = cls.convert_time(coverage, timeval)

        idx = TimeUtils.get_relative_time(coverage, corrected_time)
        return idx

    @classmethod
    def convert_time(cls, coverage, timeval):
        tname = coverage.temporal_parameter_name
        uom = coverage.get_parameter_context(tname).uom

        corrected_time = TimeUtils.ts_to_units(uom, timeval)
        return corrected_time

    @classmethod
    def _data_dict_to_rdt(cls, data_dict, stream_def_id, coverage):
        if stream_def_id:
            rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        else:
            rdt = RecordDictionaryTool(param_dictionary=coverage.parameter_dictionary)
        if not data_dict:
            log.warning('Retrieve returning empty set')
            return rdt

        if 'time' in data_dict and data_dict['time'].shape[0] == 0:
            log.warning('Retrieve returning empty set')
            return rdt


        rdt[coverage.temporal_parameter_name] = data_dict[coverage.temporal_parameter_name]
        for field in rdt.fields:
            if field == coverage.temporal_parameter_name:
                continue
            # The values have already been inside a coverage so we know they're safe and they exist, so they can be inserted directly.
            if field in data_dict:
                rdt._rd[field] = data_dict[field]
            #rdt[k] = v

        return rdt

    @classmethod
    @debug_wrapper
    def _cov2granule(cls, coverage, start_time=None, end_time=None, stride_time=None, stream_def_id=None, parameters=None, tdoa=None):
        # Deal with the NTP
        if start_time:
            start_time += 2208988800
        if end_time:
            end_time += 2208988800

        if tdoa is None:
            if start_time is None and end_time is None:
                data_dict = coverage.get_parameter_values(param_names=parameters, stride_length=stride_time, fill_empty_params=True).get_data()
            else:
                data_dict = coverage.get_parameter_values(param_names=parameters, time_segment=(start_time, end_time), stride_length=stride_time, fill_empty_params=True).get_data()
        elif isinstance(tdoa, slice):
            log.warning("Using tdoa argument on large datasets can consume too much memory")
            data_dict = coverage.get_parameter_values(param_names=parameters, fill_empty_params=True).get_data()
            data_dict = data_dict[tdoa]
        else:
            raise TypeError("tdoa is incorrect type: %s" % type(tdoa))

        return cls._data_dict_to_rdt(data_dict, stream_def_id, coverage)
       


    def execute_retrieve(self):
        '''
        execute_retrieve Executes a retrieval and returns the result 
        as a value in lieu of publishing it on a stream
        '''
        try: 
            coverage = DatasetManagementService._get_coverage(self.dataset_id,mode='r')
            if coverage.is_empty():
                log.info('Reading from an empty coverage')
                rdt = RecordDictionaryTool(param_dictionary=coverage.parameter_dictionary)
            else: 
                rdt = ReplayProcess._cov2granule(coverage=coverage, 
                        start_time=self.start_time, 
                        end_time=self.end_time,
                        stride_time=self.stride_time, 
                        parameters=self.parameters, 
                        stream_def_id=self.delivery_format, 
                        tdoa=self.tdoa)
        except:
            log.exception('Problems reading from the coverage')
            raise BadRequest('Problems reading from the coverage')
        finally:
            coverage.close(timeout=5)
        return rdt.to_granule()

    @classmethod
    def get_last_values(cls, dataset_id, number_of_points=100, delivery_format=''):
        stream_def_id = delivery_format
        try:
            cov = DatasetManagementService._get_coverage(dataset_id, mode='r')
            if cov.is_empty():
                rdt = RecordDictionaryTool(param_dictionary=cov.parameter_dictionary)
            else:
                time_array = cov.get_parameter_values([cov.temporal_parameter_name], sort_parameter=cov.temporal_parameter_name).get_data()
                time_array = time_array[cov.temporal_parameter_name][-number_of_points:]

                t0 = np.asscalar(time_array[0])
                t1 = np.asscalar(time_array[-1])

                data_dict = cov.get_parameter_values(time_segment=(t0, t1), fill_empty_params=True).get_data()
                rdt = cls._data_dict_to_rdt(data_dict, stream_def_id, cov)
        except:
            log.exception('Problems reading from the coverage')
            raise BadRequest('Problems reading from the coverage')
        finally:
            if cov is not None:
                cov.close(timeout=5)
        return rdt


    def execute_replay(self):
        '''
        execute_replay Performs a replay and publishes the results on a stream. 
        '''
        if self.publishing.is_set():
            return False
        self.replay_thread = self._process.thread_manager.spawn(self.replay)
        return True

    def replay(self):
        self.publishing.set() # Minimal state, supposed to prevent two instances of the same process from replaying on the same stream
        for rdt in self._replay():
            if self.end.is_set():
                return
            self.play.wait()
            self.output.publish(rdt.to_granule())

        self.publishing.clear()
        return 

    def pause(self):
        self.play.clear()

    def resume(self):
        self.play.set()

    def stop(self):
        self.end.set()

    def _replay(self):
        coverage = DatasetManagementService._get_coverage(self.dataset_id,mode='r')
        rdt = self._cov2granule(coverage=coverage, start_time=self.start_time, end_time=self.end_time, stride_time=self.stride_time, parameters=self.parameters, stream_def_id=self.stream_def_id)
        elements = len(rdt)
        
        for i in xrange(elements / self.publish_limit):
            outgoing = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
            fields = self.parameters or outgoing.fields
            for field in fields:
                v = rdt[field]
                if v is not None:
                    outgoing[field] = v[(i*self.publish_limit) : ((i+1)*self.publish_limit)]
            yield outgoing
        coverage.close(timeout=5)
        return 

class RetrieveProcess:
    '''
    A class used by processing to get data from a coverage instance
    '''

    def __init__(self, dataset_id):
        self.dataset_id = dataset_id

    def retrieve(self, time1, time2):
        '''
        Returns all of the values between time1 and time2
        '''
        coverage = self.get_coverage()
        # Convert python datetimes to unix timestamps
        if isinstance(time1, datetime):
            time1 = calendar.timegm(time1.timetuple())
        if isinstance(time2, datetime):
            time2 = calendar.timegm(time2.timetuple())

        rdt = ReplayProcess._cov2granule(coverage, time1, time2)
        return rdt

    def get_coverage(self):
        return DatasetManagementService._get_coverage(self.dataset_id, mode='r')




