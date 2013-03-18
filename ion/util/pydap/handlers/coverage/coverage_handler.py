import re
import os
import numpy as np
import urllib
from pyon.util.log import log
from email.utils import formatdate
from stat import ST_MTIME

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
        if hasattr(pc,'uom'):
            attrs['units'] = pc.uom

        if hasattr(pc,'display_name'):
            attrs['long_name'] = pc.display_name
        return attrs

    def get_data(self,cov, name, slice_):
        #pc = cov.get_parameter_context(name)
        data = cov.get_parameter_values(name, tdoa=slice_)
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

    def get_dataset(self, cov, fields, fill_index, dataset, response):
        for var in fields:
            while var:
                name, slice_ = var.pop(0)
                name = urllib.unquote(name)
                
                slice_ = self.update_slice_object(slice_, fill_index)
                if slice_ is None:
                    continue
                #print name
                #print "slice", slice_
                pc = cov.get_parameter_context(name)
                try:
                    param = cov.get_parameter(name)
                    
                    data = np.array([])
                    time_data = np.array([])
                    if response == "dods":
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
                        #start = time.time()
                        #convert to string
                        try:
                            #scalar case
                            if data.shape == (2,):
                                data = np.atleast_1d('_'.join([str(data[0]), str(data[1])]))
                            else:
                                for i,d in enumerate(data):
                                    f = [str(d[0]),str(d[1])]
                                    data[i] = '_'.join(f)
                        except Exception, e:
                            data = np.asanyarray(['None' for d in data])
                        #print "range end", time.time() - start
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, 'S')                
                    if isinstance(pc.param_type,BooleanType):
                        data = np.asanyarray(data, dtype='int32')
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, data.dtype.char)                
                    if isinstance(pc.param_type,CategoryType):
                        #start = time.time()
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, self.get_numpy_type(data))                
                        #print "category end", time.time() - start
                    if isinstance(pc.param_type,ArrayType):
                        #start = time.time()
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, self.get_numpy_type(data))                
                        #print "array end", time.time() - start
                    if isinstance(pc.param_type,RecordType):
                        #start = time.time()
                        #convert to string
                        try:
                            for i,ddict in enumerate(data):
                                data[i] = str(ddict)
                        except Exception, e:
                            data = np.asanyarray(['None' for d in data])
                        #print "record end", time.time() - start
                        dataset[name] = self.make_grid(name, data, time_data, attrs, time_attrs, dims, 'S')
                    if param.is_coordinate and cov.temporal_parameter_name == name:
                        dataset[name] = BaseType(name=name, data=data, type=data.dtype.char, attributes=attrs, shape=data.shape)
                except Exception, e:
                    log.exception('Problem reading cov %s %s', cov.name, e)
                    continue
        #print dataset
        return dataset

    def parse_constraints(self, environ):
        base = os.path.split(self.filepath)
        coverage = SimplexCoverage.load(base[0], base[1],mode='r')

        last_modified = formatdate(time.mktime(time.localtime(os.stat(self.filepath)[ST_MTIME])))
        environ['pydap.headers'].append(('Last-modified', last_modified))

        atts = {}
        atts['title'] = coverage.name
        dataset = DatasetType(coverage.name) #, attributes=atts)
        fields, queries = environ['pydap.ce']
        response = environ['pydap.response']

        queries = filter(bool, queries)  # fix for older version of pydap

        all_vars = coverage.list_parameters()
        
        fill_index = -1
        if response == "dods":
            time_context = coverage.get_parameter_context(coverage.temporal_parameter_name)
            time_fill_value = time_context.fill_value
            time_data = coverage.get_parameter_values(coverage.temporal_parameter_name)
            try:
                fill_index = np.where(time_data == time_fill_value)[0][0]
            except IndexError:
                pass

        # If no fields have been explicitly requested, of if the sequence
        # has been requested directly, return all variables.
        if not fields:
            fields = [[(name, ())] for name in all_vars]
        
        dataset = self.get_dataset(coverage, fields, fill_index, dataset, response)
        return dataset
    
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
