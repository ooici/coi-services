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

from coverage_model import utils

from interface.services.dm.idataset_management_service import DatasetManagementServiceProcessClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from interface.services.dm.ireplay_process import BaseReplayProcess

from gevent.event import Event
from numbers import Number
import datetime
import dateutil.parser
import gevent
import netCDF4
import numpy as np
import calendar
import time


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

        self.publishing.clear()
        self.play.set()
        self.end.clear()

        if self.dataset_id is None:
            raise BadRequest('dataset_id not specified')

        self.dataset = dsm_cli.read_dataset(self.dataset_id)
        self.pubsub = PubsubManagementServiceProcessClient(process=self)


    @classmethod
    def get_time_idx(cls, coverage, timeval):
        temporal_variable = coverage.temporal_parameter_name
        uom = coverage.get_parameter_context(temporal_variable).uom

        units = cls.ts_to_units(uom, timeval)

        idx = cls.get_relative_time(coverage, units)
        return idx


    @classmethod
    def _coverage_to_granule(cls, coverage, start_time=None, end_time=None, stride_time=None, fuzzy_stride=True, parameters=None, stream_def_id=None, tdoa=None):
        slice_ = slice(None) # Defaults to all values


        # Validations
        if start_time is not None:
            validate_is_instance(start_time, Number, 'start_time must be a number for striding.')
        if end_time is not None:
            validate_is_instance(end_time, Number, 'end_time must be a number for striding.')
        if stride_time is not None:
            validate_is_instance(stride_time, Number, 'stride_time must be a number for striding.')

        if tdoa is not None and isinstance(tdoa,slice):
            slice_ = tdoa
        
        elif stride_time is not None and not fuzzy_stride: # SLOW 
            ugly_range = np.arange(start_time, end_time, stride_time)
            idx_values = [cls.get_time_idx(coverage,i) for i in ugly_range]
            idx_values = list(set(idx_values)) # Removing duplicates - also mixes the order of the list!!!
            idx_values.sort()
            slice_ = [idx_values]


        elif not (start_time is None and end_time is None):
            if start_time is not None:
                start_time = cls.get_time_idx(coverage,start_time)
            if end_time is not None:
                end_time = cls.get_time_idx(coverage,end_time)

            slice_ = slice(start_time,end_time,stride_time)
            log.info('Slice: %s', slice_)

        if stream_def_id:
            rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        else:
            rdt = RecordDictionaryTool(param_dictionary=coverage.parameter_dictionary)
        if parameters is not None:
            # TODO: Improve efficiency here
            fields = list(set(parameters).intersection(rdt.fields))
        else:
            fields = rdt.fields

        for field in fields:
            log.info( 'Slice is %s' , slice_)
            n = coverage.get_parameter_values(field,tdoa=slice_)
            if n is None:
                rdt[field] = [n]
            elif isinstance(n,np.ndarray):
                if coverage.get_data_extents(field)[0] != coverage.num_timesteps:
                    log.error("Misformed coverage detected, padding with fill_value")
                    arr_len = utils.slice_shape(slice_, (coverage.num_timesteps,))[0]
                    fill_arr = np.empty(arr_len - n.shape[0] , dtype=n.dtype)
                    fill_arr.fill(coverage.get_parameter_context(field).fill_value)
                    n = np.append(n,fill_arr)
                rdt[field] = n
            else:
                rdt[field] = [n]
        return rdt

    def execute_retrieve(self):
        '''
        execute_retrieve Executes a retrieval and returns the result 
        as a value in lieu of publishing it on a stream
        '''
        try: 
            coverage = DatasetManagementService._get_coverage(self.dataset_id,mode='r')
            if coverage.num_timesteps == 0:
                log.info('Reading from an empty coverage')
                rdt = RecordDictionaryTool(param_dictionary=coverage.parameter_dictionary)
            else: 
                rdt = self._coverage_to_granule(coverage=coverage,start_time=self.start_time, end_time=self.end_time, stride_time=self.stride_time, parameters=self.parameters,tdoa=self.tdoa)
        except Exception as e:
            import traceback
            traceback.print_exc(e)
            raise BadRequest('Problems reading from the coverage')
        finally:
            coverage.close(timeout=5)
        return rdt.to_granule()



    def execute_replay(self):
        '''
        execute_replay Performs a replay and publishes the results on a stream. 
        '''
        if self.publishing.is_set():
            return False
        gevent.spawn(self.replay)
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




    @classmethod
    def get_last_values(cls, dataset_id, number_of_points):
        coverage = DatasetManagementService._get_coverage(dataset_id,mode='r')
        if coverage.num_timesteps < number_of_points:
            if coverage.num_timesteps == 0:
                rdt = RecordDictionaryTool(param_dictionary=coverage.parameter_dictionary)
                return rdt.to_granule()
            number_of_points = coverage.num_timesteps
        rdt = cls._coverage_to_granule(coverage,tdoa=slice(-number_of_points,None))
        coverage.close(timeout=5)
        
        return rdt.to_granule()

    def _replay(self):
        coverage = DatasetManagementService._get_coverage(self.dataset_id,mode='r')
        rdt = self._coverage_to_granule(coverage=coverage, start_time=self.start_time, end_time=self.end_time, stride_time=self.stride_time, parameters=self.parameters, stream_def_id=self.stream_def_id)
        elements = len(rdt)
        
        for i in xrange(elements / self.publish_limit):
            outgoing = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
            fields = self.parameters or outgoing.fields
            for field in fields:
                outgoing[field] = rdt[field][(i*self.publish_limit) : ((i+1)*self.publish_limit)]
            yield outgoing
        coverage.close(timeout=5)
        return 


    @classmethod
    def get_relative_time(cls, coverage, time):
        '''
        Determines the relative time in the coverage model based on a given time
        The time must match the coverage's time units
        '''
        time_name = coverage.temporal_parameter_name
        pc = coverage.get_parameter_context(time_name)
        units = pc.uom
        if 'iso' in units:
            return None # Not sure how to implement this....  How do you compare iso strings effectively?
        values = coverage.get_parameter_values(time_name)
        return cls.find_nearest(values,time)

    @classmethod
    def ts_to_units(cls,units, val):
        '''
        Converts a unix timestamp into various formats
        Example:
        ts = time.time()
        CoverageCraft.ts_to_units('days since 2000-01-01', ts)
        '''
        if 'iso' in units:
            return time.strftime('%Y-%d-%mT%H:%M:%S', time.gmtime(val))
        elif 'since' in units:
            t = netCDF4.netcdftime.utime(units)
            return t.date2num(datetime.datetime.utcfromtimestamp(val))
        else:
            return val


    @classmethod
    def units_to_ts(cls, units, val):
        '''
        Converts known time formats into a unix timestamp
        Example:
        ts = CoverageCraft.units_to_ts('days since 2000-01-01', 1200)
        '''
        if 'since' in units:
            t = netCDF4.netcdftime.utime(units)
            dtg = t.num2date(val)
            return calendar.timegm(dtg.timetuple())
        elif 'iso' in units:
            t = dateutil.parser.parse(val)
            return time.mktime(t.timetuple())
        else:
            return val

    @classmethod
    def find_nearest(cls, arr, val):
        '''
        The sexiest algorithm for finding the best matching value for a numpy array
        '''
        idx = np.abs(arr-val).argmin()
        return idx

