#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/utility/granule_utils.py
@date Wed Jul 18 09:00:22 EDT 2012
@description Utilities for crafting granules into a coverage
'''

from ion.services.dm.utility.granule import TaxyTool, RecordDictionaryTool, build_granule 
from pyon.util.arg_check import validate_equal
from coverage_model.coverage import GridDomain, CRS, AxisTypeEnum, MutabilityEnum, GridShape, SimplexCoverage
from coverage_model.parameter import ParameterContext, ParameterDictionary 
from coverage_model.parameter_types import QuantityType
from pyon.public import log
import dateutil.parser
import netCDF4
import time
import datetime
import numpy as np
'''
Assuming all values are np.float64 except data which is int8
'''
class CoverageCraft(object):
    '''
    AKA the BlackBox
    PFM courtesy of Tommy Vandesteene
    '''
    def __init__(self,coverage=None, granule=None):
        if coverage is None:
            self.coverage = self.create_coverage()
            self.rdt = RecordDictionaryTool(param_dictionary=self.coverage.parameter_dictionary)
        else:
            self.coverage = coverage
            if granule is not None:
                self.sync_with_granule(granule)
            else:
                self.sync_rdt_with_coverage()
        self.pdict = self.coverage.parameter_dictionary


    def sync_rdt_with_granule(self, granule):
        rdt = RecordDictionaryTool.load_from_granule(granule)
        self.rdt = rdt
        return rdt

    def sync_with_granule(self, granule=None):
        if granule is not None:
            self.sync_rdt_with_granule(granule)
        if self.rdt is None:
            log.error('Failed to add granule, no granule assigned.')
            return
        start_index = self.coverage.num_timesteps 
        elements = self.rdt._shp[0]
        if start_index == 1:
            start_index = 0
            self.coverage.insert_timesteps(elements - 1)
        else:
            self.coverage.insert_timesteps(elements)

        for k,v in self.rdt.iteritems():
            log.info("key: %s" , k)
            log.info("value: %s" , v)
            slice_ = slice(start_index,None)
            log.info("slice: %s",  slice_)
            self.coverage.set_parameter_values(param_name=k,tdoa=slice_, value=v)


    def sync_rdt_with_coverage(self, coverage=None, tdoa=None, start_time=None, end_time=None, parameters=None):
        '''
        Builds a granule based on the coverage
        '''
        if coverage is None:
            coverage = self.coverage

        slice_ = slice(None) # Defaults to all values
        if tdoa is not None and isinstance(tdoa,slice):
            slice_ = tdoa

        elif not (start_time is None and end_time is None):
            uom = coverage.get_parameter_context('time').uom
            if start_time is not None:
                start_units = self.ts_to_units(uom,start_time)
                log.info('Units: %s', start_units)
                start_idx = self.get_relative_time(coverage,start_units)
                log.info('Start Index: %s', start_idx)
                start_time = start_idx
            if end_time is not None:
                end_units   = self.ts_to_units(uom,end_time)
                log.info('End units: %s', end_units)
                end_idx   = self.get_relative_time(coverage,end_units)
                log.info('End index: %s',  end_idx)
                end_time = end_idx
            slice_ = slice(start_time,end_time)
            log.info('Slice: %s', slice_)

        if parameters is not None:
            pdict = ParameterDictionary()
            params = set(coverage.list_parameters()).intersection(parameters)
            for param in params:
                pdict.add_context(coverage.get_parameter_context(param))
            rdt = RecordDictionaryTool(param_dictionary=pdict)
            self.pdict = pdict
        else:
            rdt = RecordDictionaryTool(param_dictionary=coverage.parameter_dictionary)
        
        fields = coverage.list_parameters()
        if parameters is not None:
            fields = set(fields).intersection(parameters)

        for d in fields:
            rdt[d] = coverage.get_parameter_values(d,tdoa=slice_)
        self.rdt = rdt # Sync

    def to_granule(self):
        return build_granule('from coverage', param_dictionary=self.pdict, record_dictionary=self.rdt)

    @classmethod
    def create_coverage(cls):
        pdict = cls.create_parameters()
        sdom, tdom = cls.create_domains()
    
        scov = SimplexCoverage('sample grid coverage_model', pdict, sdom, tdom)

        return scov

    @classmethod
    def create_domains(cls):
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT, AxisTypeEnum.HEIGHT])

        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # Dimensionality is excluded for now
        return sdom, tdom

    @classmethod
    def create_parameters(cls):
        pdict = ParameterDictionary()
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0
        pdict.add_context(lon_ctxt)

        depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=np.float32))
        depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        depth_ctxt.uom = 'meters'
        depth_ctxt.fill_value = 0e0
        pdict.add_context(depth_ctxt)

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
        temp_ctxt.uom = 'degree_Celsius'
        temp_ctxt.fill_value = 0e0
        pdict.add_context(temp_ctxt)

        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.float32))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0
        pdict.add_context(cond_ctxt)

        data_ctxt = ParameterContext('data', param_type=QuantityType(value_encoding=np.int8))
        data_ctxt.uom = 'byte'
        data_ctxt.fill_value = 0x0
        pdict.add_context(data_ctxt)

        return pdict
        
    @classmethod
    def get_relative_time(cls, coverage, time):
        '''
        Determines the relative time in the coverage model based on a given time
        The time must match the coverage's time units
        '''
        pc = coverage.get_parameter_context('time')
        units = pc.uom
        if 'iso' in units:
            return None # Not sure how to implement this....  How do you compare iso strings effectively?
        values = coverage.get_parameter_values('time')
        return cls.find_nearest(values,time)
       
    @classmethod
    def find_nearest(cls, arr, val):
        '''
        The sexiest algorithm for finding the best matching value for a numpy array
        '''
        idx = np.abs(arr-val).argmin()
        return idx



    @staticmethod
    def ts_to_units(units, val):
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


    @staticmethod
    def units_to_ts(units, val):
        '''
        Converts known time formats into a unix timestamp
        Example:
        ts = CoverageCraft.units_to_ts('days since 2000-01-01', 1200)
        '''
        if 'since' in units:
            t = netCDF4.netcdftime.utime(units)
            dtg = t.num2date(val)
            return time.mktime(dtg.timetuple())
        elif 'iso' in units:
            t = dateutil.parser.parse(val)
            return time.mktime(t.timetuple())
        else:
            return val
        


    def build_coverage(self):
        pass

