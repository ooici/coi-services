import re
import os
import numpy as np
import urllib
from pyon.util.log import log
from email.utils import formatdate
from stat import ST_MTIME

from coverage_model.coverage import SimplexCoverage
from coverage_model.parameter_types import QuantityType,ConstantRangeType,ArrayType, ConstantType, RecordType, CategoryType, BooleanType
from pydap.model import DatasetType,BaseType, SequenceType
from pydap.handlers.lib import BaseHandler
import time

class Handler(BaseHandler):

    extensions = re.compile(r'^.*[0-9A-Za-z\-]{36}',re.IGNORECASE)

    def __init__(self, filepath):
        self.filepath = filepath
    
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
        all_vars = cov.list_parameters()
        fields = [[(name, ())] for name in all_vars]
        seq = SequenceType("data")
        for var in fields:
            while var:
                name, slice_ = var.pop(0)
                name = urllib.unquote(name)

                slice_ = self.update_slice_object(slice_, fill_index)
                if slice_ is None:
                    continue
    
                pc = cov.get_parameter_context(name)
                #just dealing with time series data for now make a sequence
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
                        seq[name] = BaseType(name=name, data=data, type=data.dtype.char, attributes=attrs)
                    if isinstance(pc.param_type, ConstantType):
                        seq[name] = BaseType(name=name, data=data, type=data.dtype.char, attributes=attrs)
                    if isinstance(pc.param_type, ConstantRangeType):
                        try:
                            col1,col2 = zip(*data)
                        except:
                            col1 = [d for d in data]
                            col2 = [d for d in data]
                        name_col1 = "_".join([name,"low"])
                        name_col2 = "_".join([name,"high"])
                        col1 = np.array(col1)
                        seq[name_col1] = BaseType(name=name_col1, data=col1, type=col1.dtype.char, attributes=attrs)
                        col2 = np.array(col2)
                        seq[name_col2] = BaseType(name=name_col2, data=col2, type=col2.dtype.char, attributes=attrs)
                    if isinstance(pc.param_type,BooleanType):
                        boolean_values = np.asanyarray(data, dtype='int8')
                        seq[name] = BaseType(name=name, data=boolean_values, type=boolean_values.dtype.char, attributes=attrs)
                    if isinstance(pc.param_type,CategoryType):
                        seq[name] = BaseType(name=name, data=data, type=self.get_numpy_type(data), attributes=attrs)
                    if isinstance(pc.param_type,ArrayType):
                        #TODO: make recursive
                        try:
                            ttype = self.get_numpy_type(data)
                            if ttype == 'S':
                                seq[name] = BaseType(name=name, data=data, type=ttype, attributes=attrs)
                        except TypeNotSupportedError:
                            if self.is_collection(data):
                                for x,idata in enumerate(data):
                                    nname = "_".join([name, str(x)])
                                    seq[nname] = BaseType(name=nname, data=idata, type=self.get_numpy_type(idata), attributes=attrs)
                            else:
                                log.exception("only handle list or string objects")
                                raise TypeNotSupportedError()
                    if isinstance(pc.param_type,RecordType):
                        keys = []
                        values = []
                        for ddict in data:
                            for k,v in ddict.iteritems():
                                keys.append(k)
                                values.append(v)
                        keys = np.asanyarray(keys)
                        seq[name+"_keys"] = BaseType(name=name+"_keys", data=keys, type=self.get_numpy_type(keys), attributes=attrs)
                        values = np.asanyarray(values)
                        seq[name+"_values"] = BaseType(name=name+"_values", data=values, type=self.get_numpy_type(values), attributes=attrs)
                        

                except Exception, e:
                    log.exception('Problem reading cov %s %s', cov.name, e)
                    continue
        
        dataset['data'] = seq 
        dataset._set_id()
        dataset.close = cov.close
        return dataset
    
    def get_numpy_type(self, data):
        result = data.dtype.char
        if self.is_basestring(data) == True:
            result = 'S'
        if result not in ('d', 'f', 'h', 'i','b','H','I','B','S'):
            raise TypeNotSupportedError()
        return result

    def is_basestring(self, data):
        for d in data:
            if not isinstance(d, basestring):
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
