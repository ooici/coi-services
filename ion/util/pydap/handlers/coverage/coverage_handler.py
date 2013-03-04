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
from pydap.model import DatasetType,BaseType, GridType
from pydap.handlers.lib import BaseHandler
import time

class Handler(BaseHandler):

    extensions = re.compile(r'^.*[0-9A-Za-z\-]{32}',re.IGNORECASE)

    def __init__(self, filepath):
        self.filepath = filepath
        
    def get_numpy_type(self, data):
        data = self.none_to_str(data)
        result = data.dtype.char
        if self.is_basestring(data):
            result = 'S'
        elif self.is_float(data):
            result = 'd'
        elif self.is_int(data):
            result = 'i'
        elif result not in ('d','f','h','i','b','H','I','B','S'):
            raise TypeNotSupportedError()
        return result

    def get_attrs(self, cov, name):
        pc = cov.get_parameter_context(name)
        attrs = {}
        try:
            attrs['units'] = pc.uom
        except:
            pass
        attrs['long_name'] = pc.long_name
        return attrs

    def get_data(self,cov, name, slice_):
        pc = cov.get_parameter_context(name)
        data = cov.get_parameter_values(name, tdoa=slice_)
        if isinstance(pc.param_type, ConstantRangeType):
            if isinstance(data, tuple):
                data = [data]
        data = np.asanyarray(data) 
        if not data.shape:
            data.shape = (1,)
        return data
    
    def get_time_data(self, cov, slice_):
        data = cov.get_parameter_values(cov.temporal_parameter_name, tdoa=slice_)
        data = np.asanyarray(data) 
        if not data.shape:
            data.shape = (1,)
        return data

    def make_grid(self, name, data, time_data, attrs, time_attrs, dims, ttype):
        grid = GridType(name=name)
        grid[name] = BaseType(name=name, data=data, type=ttype, attributes=attrs, dimensions=dims, shape=data.shape)
        grid[dims[0]] = BaseType(name=dims[0], data=time_data, type=time_data.dtype.char, attributes=time_attrs, dimensions=dims, shape=time_data.shape)
        return grid    

    def get_dataset(self, cov, fields, fill_index, dataset):
        for var in fields:
            print "var", var
            while var:
                name, slice_ = var.pop(0)
                name = urllib.unquote(name)
                
                slice_ = self.update_slice_object(slice_, fill_index)
                if slice_ is None:
                    continue

                pc = cov.get_parameter_context(name)
                try:
                    param = cov.get_parameter(name)
                    data = self.get_data(cov, name, slice_)
                    time_data = self.get_time_data(cov, slice_)
                    time_attrs  = self.get_attrs(cov, name)
                    attrs  = self.get_attrs(cov, name)
                    dims = (cov.temporal_parameter_name,)

                    if isinstance(pc.param_type, QuantityType) and not param.is_coordinate and cov.temporal_parameter_name != name:
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, data.dtype.char)                
                    if isinstance(pc.param_type, ConstantType):
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, data.dtype.char)                
                    if isinstance(pc.param_type, ConstantRangeType):
                        #convert to string
                        try:
                            result = []
                            for d in data:
                                f = [str(d[0]),str(d[1])]
                                result.append('_'.join(f))
                            data = np.asanyarray(result)
                        except Exception, e:
                            data = np.asanyarray(['None' for d in data])
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, data.dtype.char)                
                    if isinstance(pc.param_type,BooleanType):
                        data = np.asanyarray(data, dtype='int32')
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, data.dtype.char)                
                    if isinstance(pc.param_type,CategoryType):
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, self.get_numpy_type(data))                
                    if isinstance(pc.param_type,ArrayType):
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, self.get_numpy_type(data))                
                    if isinstance(pc.param_type,RecordType):
                        #convert to string
                        try:
                            result = []
                            for ddict in data:
                                d = ['_'.join([k,v]) for k,v in ddict.iteritems()]
                                result = result + d
                            result = np.asanyarray(result)
                        except Exception, e:
                            result = np.asanyarray(['None' for d in data])
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, self.get_numpy_type(data))                
                    if param.is_coordinate and cov.temporal_parameter_name == name:
                        dataset[name] = BaseType(name=name, data=data, type=data.dtype.char, attributes=attrs, shape=data.shape)
                except Exception, e:
                    log.exception('Problem reading cov %s %s', cov.name, e)
                    import traceback
                    traceback.print_exc()
                    continue
        return dataset

    def parse_constraints(self, environ):
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
            all_vars.append(name)
        
        #remove everything except quantity type
        #strings break the erddap interface
        t = []
        for param in all_vars:
            if not isinstance(cov.get_parameter_context(param).param_type, QuantityType):
                t.append(param)
        [all_vars.remove(i) for i in t]
        
        if not fields:
            fields = [[(name, ())] for name in all_vars]
        
        dataset = self.get_dataset(cov, fields, fill_index, dataset)
        
        return constrain(dataset, environ.get('QUERY_STRING', ''))
        
    def none_to_str(self, data):
        for i,d in enumerate(data):
            if d is None:
                data[i] = 'None'
        return data

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

def get_var(coverage,name,slice_):
    data = coverage.get_parameter_values(name,tdoa=slice_)
    if not isinstance(data, np.ndarray):
        data = np.array([data])
    attrs = {'units': coverage.get_parameter_context(name).uom}
    dims = (coverage.temporal_parameter_name,)
    
    
    log.debug( 'name=%s', name)
    log.debug( 'data=%s', data)
    log.debug( 'shape=%s' , data.shape)
    log.debug( 'type=%s', data.dtype.char)
    log.debug( 'dims=%s', dims)
    log.debug( 'attrs=%s', attrs)
    return BaseType(name=name, data=data, shape=data.shape, type=data.dtype.char, dimensions=dims, attributes=attrs)

