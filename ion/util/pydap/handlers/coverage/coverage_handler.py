import re
import os
import numpy
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
    
    def is_str(self, data):
        for d in data:
            if not isinstance(d, str):
                return False
        return True
    def is_list(self, data):
        for d in data:
            if not isinstance(d, (list,tuple)):
                return False
        return True

    def parse_constraints(self, environ):
        import sys
        base = os.path.split(self.filepath)
        self.cov = SimplexCoverage.load(base[0], base[1],mode='r')

        last_modified = formatdate(time.mktime(time.localtime(os.stat(self.filepath)[ST_MTIME])))
        environ['pydap.headers'].append(('Last-modified', last_modified))

        dataset = DatasetType(self.cov.name)
        
        #find time fill index
        time_context = self.cov.get_parameter_context(self.cov.temporal_parameter_name)
        time_fill_value = time_context.fill_value
        time_data = self.cov.get_parameter_values(self.cov.temporal_parameter_name)
        fill_index = -1
        try:
            fill_index = numpy.where(time_data == time_fill_value)[0][0]
        except IndexError:
            pass
        fields, queries = environ['pydap.ce']
        queries = filter(bool, queries)  # fix for older version of pydap
        #got through data parameters
        all_vars = self.cov.list_parameters()
        fields = [[(name, ())] for name in all_vars]
        seq = SequenceType("data")
        for var in fields:
            while var:
                name, slice_ = var.pop(0)
                name = urllib.unquote(name)

                slice_ = self.update_slice_object(slice_, fill_index)
                if slice_ is None:
                    continue
    
                #print >> sys.stderr, "name",name
                #print "is_coord",param.is_coordinate
                pc = self.cov.get_parameter_context(name)
                #just dealing with time series data for now make a sequence
                try:
                    attrs = {}
                    try:
                        attrs['units'] = pc.uom
                    except:
                        pass
                    attrs['long_name'] = pc.long_name
                    data = self.cov.get_parameter_values(name, tdoa=slice_)
                    data = numpy.array(data) 
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
                        col1 = numpy.array(col1)
                        seq[name_col1] = BaseType(name=name_col1, data=col1, type=col1.dtype.char, attributes=attrs)
                        col2 = numpy.array(col2)
                        seq[name_col2] = BaseType(name=name_col2, data=col2, type=col2.dtype.char, attributes=attrs)
                    if isinstance(pc.param_type,BooleanType):
                        boolean_values = self.transform_boolean(data)
                        boolean_values = numpy.array(boolean_values)
                        seq[name] = BaseType(name=name, data=boolean_values, type=boolean_values.dtype.char, attributes=attrs)
                    if isinstance(pc.param_type,CategoryType):
                        cat_data = self.transform_categories(pc.param_type.categories, data)
                        cat_data = numpy.array(cat_data)
                        bt = BaseType(name=name, data=cat_data, type=cat_data.dtype.char, attributes=attrs)
                        seq[name] = bt
                    if isinstance(pc.param_type,ArrayType):
                        data = numpy.array(data)
                        ttype = numpy.dtype.char
                        #numpy defaults string array to type 'O' which is object which [ydap does nto support
                        #hack to fix it
                        #TODO: handle recursively
                        if self.is_str(data) == True:
                            ttype = 's'
                            seq[name] = BaseType(name=name, data=data, type=ttype, attributes=attrs)
                        elif self.is_list(data):
                            for x,lst in enumerate(data):
                                nname = "_".join([name, str(x)])
                                idata = numpy.array(lst)
                                ttype = numpy.dtype.char
                                if self.is_str(idata) == True:
                                    ttype = 's'
                                seq[nname] = BaseType(name=nname, data=idata, type=ttype, attributes=attrs)
                        else:
                            log.exception("only handle list and string objects")
                    if isinstance(pc.param_type,RecordType):
                        #print "rc data", data
                        keys = []
                        values = []
                        for ddict in data:
                            for k,v in ddict.iteritems():
                                keys.append(k)
                                values.append(v)
                        keys = numpy.array(keys)
                        ttype = keys.dtype.char
                        if self.is_str(keys) == True:
                            ttype = 's'
                        seq[name+"_keys"] = BaseType(name=name+"_keys", data=keys, type=ttype, attributes=attrs)
                        values = numpy.array(values)
                        ttype = values.dtype.char
                        if self.is_str(values) == True:
                            ttype = 's'
                        seq[name+"_values"] = BaseType(name=name+"_values", data=values, type=ttype, attributes=attrs)
                        

                except Exception, e:
                    print "exception", e
                    import traceback
                    print traceback.format_exc()
                    log.exception('Problem reading cov %s %s', self.cov.name, e)
                    continue
        
        dataset['data'] = seq 
        dataset._set_id()
        dataset.close = self.cov.close
        #print "dataset",dataset
        return dataset

    def update_slice_object(self, slice_, fill_index):

        slice_ = slice_[0] if slice_ else slice(None)
        #print 'formed slice ', slice_

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
    
    def transform_categories(self, categories, cat_data):
        result = []
        for d in cat_data:
            for k,v in categories.iteritems():
                if d == v:
                    result.append(k)
        return result

        
    def transform_boolean(self, boolean_values):
        result = []
        for b in boolean_values:
            if b == True:
                result.append(1)
            else:
                result.append(0)
        return result
    
        
