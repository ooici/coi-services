import calendar
import dateutil.parser
import datetime
import time
import netCDF4
import numpy as np

class TimeUtils(object):

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
            dtg = dateutil.parser.parse(val)
            return calendar.timegm(dtg.timetuple())
        else:
            raise TypeError('Unknown time units')
        


    @classmethod
    def find_nearest(cls, arr, val):
        '''
        The sexiest algorithm for finding the best matching value for a numpy array
        '''
        idx = np.abs(arr-val).argmin()
        return idx
