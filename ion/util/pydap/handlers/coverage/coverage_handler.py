#import sys
import re
import os
import numpy as np
import urllib
from pyon.util.log import log
from email.utils import formatdate
from stat import ST_MTIME

from pydap.handlers.helper import constrain
from coverage_model.coverage import SimplexCoverage
from coverage_model.parameter_types import QuantityType,ConstantRangeType,ArrayType, ConstantType, RecordType, CategoryType, BooleanType
from pydap.model import DatasetType,BaseType, SequenceType
from pydap.handlers.lib import BaseHandler
import time

class Handler(BaseHandler):

    extensions = re.compile(r'^.*[0-9A-Za-z\-]{32}',re.IGNORECASE)

    def __init__(self, filepath):
        self.filepath = filepath
    
    def parse_constraints(self, environ):
        seq_name = 'data'
        base = os.path.split(self.filepath)
        cov = SimplexCoverage.load(base[0], base[1],mode='r')

        last_modified = formatdate(time.mktime(time.localtime(os.stat(self.filepath)[ST_MTIME])))
        environ['pydap.headers'].append(('Last-modified', last_modified))

        dataset = DatasetType(cov.name)
        
        #find time fill index
        time_context = cov.get_parameter_context(cov.temporal_parameter_name)
        time_fill_value = time_context.fill_value
        time_data = cov.get_parameter_values(cov.temporal_parameter_name)
        fill_index = -1
        try:
            fill_index = np.where(time_data == time_fill_value)[0][0]
        except IndexError:
            pass
        fields, queries = environ['pydap.ce']
        queries = filter(bool, queries)  # fix for older version of pydap
        
        all_vars = []
        for name in cov.list_parameters():
            pc = cov.get_parameter_context(name)
            #if isinstance(pc.param_type, QuantityType):
            #if name in ["time", "lon", "lat", "density"]:
            all_vars.append(name)

        if not fields:
            fields = [[(name, ())] for name in all_vars]
        dataset[seq_name] = SequenceType(name=seq_name)
        
        for var in fields:
            while var:
                name, slice_ = var.pop(0)
                name = urllib.unquote(name)
                
                #print >> sys.stderr, "name", name

                if seq_name == name:
                    continue

                slice_ = self.update_slice_object(slice_, fill_index)
                if slice_ is None:
                    continue

                pc = cov.get_parameter_context(name)
                try:
                    attrs = {}
                    try:
                        attrs['units'] = pc.uom
                    except:
                        pass
                    attrs['long_name'] = pc.long_name
                    data = cov.get_parameter_values(name, tdoa=slice_)
                    data = np.asanyarray(data) 
                    if isinstance(pc.param_type, QuantityType):
                        dataset[seq_name][name] = BaseType(name=name, data=data, type=data.dtype.char, attributes=attrs)
                    if isinstance(pc.param_type, ConstantType):
                        dataset[seq_name][name] = BaseType(name=name, data=data, type=data.dtype.char, attributes=attrs)
                    if isinstance(pc.param_type, ConstantRangeType):
                        try:
                            result = []
                            for d in data:
                                f = [str(d[0]),str(d[1])]
                                result.append('_'.join(f))
                            data = np.asanyarray(result)
                        except:
                            data = np.asanyarray(['None' for d in data])
                        dataset[seq_name][name] = BaseType(name=name, data=data, type=data.dtype.char, attributes=attrs)
                    if isinstance(pc.param_type,BooleanType):
                        boolean_values = np.asanyarray(data, dtype='int32')
                        dataset[seq_name][name] = BaseType(name=name, data=boolean_values, type=self.get_numpy_type(boolean_values), attributes=attrs)
                    if isinstance(pc.param_type,CategoryType):
                        dataset[seq_name][name] = BaseType(name=name, data=data, type=self.get_numpy_type(data), attributes=attrs)
                    if isinstance(pc.param_type,ArrayType):
                        dataset[seq_name][name] = BaseType(name=name, data=data, type=self.get_numpy_type(data), attributes=attrs)
                    if isinstance(pc.param_type,RecordType):
                        result = []
                        for ddict in data:
                            d = ['_'.join([k,v]) for k,v in ddict.iteritems()]
                            result = result + d
                        result = np.asanyarray(result)
                        ttype = self.get_numpy_type(result)
                        dataset[seq_name][name] = BaseType(name=name, data=result, type=ttype, attributes=attrs)
                        

                except Exception, e:
                    log.exception('Problem reading cov %s %s', cov.name, e)
                    continue
        return constrain(dataset, environ.get('QUERY_STRING', ''))

    
    def get_numpy_type(self, data):
        result = data.dtype.char
        if self.is_basestring(data):
            result = 'S'
        elif self.is_float(data):
            result = 'd'
        elif self.is_int(data):
            result = 'i'
        elif result not in ('d', 'f', 'h', 'i','b','H','I','B','S'):
            raise TypeNotSupportedError()
        return result

    def is_basestring(self, data):
        for d in data:
            if not isinstance(d, basestring):
                return False
        return True
    
    def is_float(self, data):
        for d in data:
            if not isinstance(d, float):
                return False
        return True
    
    def is_int(self, data):
        for d in data:
            if not isinstance(d, int):
                return False
        return True
    
    def is_collection(self, data):
        for d in data:
            if not isinstance(d, (list,tuple,np.ndarray)):
                return False
        return True

    def update_slice_object(self, slice_, fill_index):

        slice_ = slice_[0] if slice_ else slice(None)

        #need to truncate slice here in case time has fill values
        if slice_.start is None and slice_.stop is None and fill_index >=0:
            slice_ = slice(0, fill_index, 1)

        if slice_.start and slice_.stop is None:

            if fill_index > slice_.start:
                return None

            if fill_index > slice_.stop:
                slice_.stop = fill_index

        if slice_.start is not None and slice_.start == slice_.stop:
            slice_ = slice(slice_.start, slice_.stop+1, slice_.step)
        
        return slice_

class TypeNotSupportedError(Exception):
    pass
