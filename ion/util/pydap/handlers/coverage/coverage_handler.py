import re
import os
import numpy as np
import numexpr as ne
from urllib import unquote
from pyon.util.log import log
from email.utils import formatdate
from stat import ST_MTIME

from coverage_model.coverage import AbstractCoverage
from coverage_model.parameter_types import QuantityType,ConstantRangeType,ArrayType, ConstantType, RecordType 
from coverage_model.parameter_types import CategoryType, BooleanType, ParameterFunctionType, SparseConstantType
from coverage_model.parameter_functions import ParameterFunctionException
from pyon.container.cc import Container
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from pydap.model import DatasetType,BaseType, GridType, SequenceType
from pydap.handlers.lib import BaseHandler
from pyon.public import CFG, PRED
import time
import simplejson as json
import collections
import functools

numpy_boolean = '?'
numpy_integer_types = 'bhilqp'
numpy_uinteger_types = 'BHILQP'
numpy_floats = 'efdg'
numpy_complex = 'FDG'
numpy_object = 'O'
numpy_str = 'SUV'

def exception_wrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            log.exception("Failed handling PyDAP request")
            raise
    return wrapper

def request_profile(enabled=False):
    def profile(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            init = time.time()
            retval = func(*args, **kwargs)
            finished = time.time()
            log.info('Request took %ss', finished-init)
            return retval
        return wrapper
    return profile


class Handler(BaseHandler):
    CACHE_LIMIT = CFG.get_safe('server.pydap.cache_limit', 5)
    CACHE_EXPIRATION = CFG.get_safe('server.pydap.cache_expiration', 5)
    REQUEST_LIMIT = CFG.get_safe('server.pydap.request_limit', 200) # MB
    _coverages = collections.OrderedDict() # Cache has to be a class var because each handler is initialized per request

    extensions = re.compile(r'^.*[0-9A-Za-z\-]{32}',re.IGNORECASE)

    def __init__(self, filepath):
        self.filepath = filepath

    def calculate_bytes(self, bitmask, parameter_num):
        timesteps = np.sum(bitmask)
        # Assume 8 bytes per variable per timestep
        count = 8 * parameter_num * timesteps
        return count

    def is_too_large(self, bitmask, parameter_num):
        requested = self.calculate_bytes(bitmask, parameter_num)
        return requested > (self.REQUEST_LIMIT * 1024**2)

    def get_numpy_type(self, data):
        data = self.none_to_str(data)
        result = data.dtype.char
        if self.is_basestring(data):
            result = 'S'
        elif self.is_float(data):
            result = 'd'
        elif self.is_int(data):
            result = 'i'
        elif result == 'O':
            self.json_dump(data)
            result = 'O'
        elif result == '?':
            result = '?'
        elif result not in ('d','f','h','i','b','H','I','B','S'):
            raise TypeNotSupportedError('Type: %s (%s)' %(result, repr(data)))
        return result

    def json_dump(self, data):
        try:
            return json.dumps([i for i in data])
        except TypeError as e:
            raise TypeNotSupportedError(e)
    
    @classmethod
    def get_coverage(cls, data_product_id):
        '''
        Memoization (LRU) of _get_coverage
        '''
        if not data_product_id:
            return
        try:
            result, ts = cls._coverages.pop(data_product_id)
            if (time.time() - ts) > cls.CACHE_EXPIRATION:
                result.close()
                raise KeyError(data_product_id)
        except KeyError:
            if data_product_id is None:
                return None
            resource_registry = Container.instance.resource_registry
            dataset_ids, _ = resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)
            if not dataset_ids: return None
            dataset_id = dataset_ids[0]
            result = DatasetManagementService._get_coverage(dataset_id, mode='r')
            result.value_caching = False
            ts = time.time()
            if result is None:
                return None
            if len(cls._coverages) >= cls.CACHE_LIMIT:
                key, value = cls._coverages.popitem(0)
                coverage, ts = value
                coverage.close(timeout=5)
        cls._coverages[dataset_id] = result, ts
        return result

    def get_attrs(self, cov, name):
        pc = cov.get_parameter_context(name)
        attrs = {}
        if hasattr(pc,'uom'):
            attrs['units'] = pc.uom

        if hasattr(pc,'display_name'):
            attrs['long_name'] = pc.display_name
        return attrs

    def get_data(self,cov, name, bitmask):
        #pc = cov.get_parameter_context(name)
        try:
            data = self.get_values(cov, name)
            data[bitmask]
            #data = cov._range_value[name][:][bitmask]
        except ParameterFunctionException:
            data = np.empty(cov.num_timesteps(), dtype='object')
        data = np.asanyarray(data) 
        if not data.shape:
            data.shape = (1,)
        return data
    
    def get_time_data(self, cov, slice_):
        return self.get_data(cov, cov.temporal_parameter_name, slice_)

    def make_series(self, response, name, data, attrs, ttype):
        base_type = BaseType(name=name, data=data, type=ttype, attributes=attrs)
        #grid[dims[0]] = BaseType(name=dims[0], data=time_data, type=time_data.dtype.char, attributes=time_attrs, dimensions=dims, shape=time_data.shape)
        return base_type    

    def filter_data(self, data):
        if len(data.shape) > 1:
            return self.ndim_stringify(data), 'S'
        if data.dtype.char in numpy_integer_types + numpy_uinteger_types:
            return data, data.dtype.char
        if data.dtype.char in numpy_floats:
            return data, data.dtype.char
        if data.dtype.char in numpy_boolean:
            return np.asanyarray(data, dtype='int32') ,'i'
        if data.dtype.char in numpy_complex:
            return self.stringify(data), 'S'
        if data.dtype.char in numpy_object:
            return self.stringify_inplace(data), 'S'
        if data.dtype.char in numpy_str:
            return data, 'S'
        return np.asanyarray(['Unsupported Type' for i in data]), 'S'


    def ndim_stringify(self, data):
        retval = np.empty(data.shape[0], dtype='O')
        try:
            if len(data.shape)>1:
                for i in xrange(data.shape[0]):
                    retval[i] = ','.join(map(lambda x : str(x), data[i].tolist()))
                return retval
        except:
            retval = np.asanyarray(['None' for d in data])
        return retval


    def stringify(self, data):
        retval = np.empty(data.shape, dtype='O')
        try:
            for i,obj in enumerate(data):
                retval[i] = str(obj)
        except:
            retval = np.asanyarray(['None' for d in data])
        return retval

    def stringify_inplace(self, data):
        try:
            for i,obj in enumerate(data):
                data[i] = str(obj)
        except:
            data = np.asanyarray(['None' for d in data])
        return data

    def get_bitmask(self, cov, fields, slices, selectors):
        '''
        returns a bitmask appropriate to the values
        '''
        bitmask = np.ones(cov.num_timesteps(), dtype=np.bool)
        for selector in selectors:
            field, operator, value = self.parse_selectors(selector)
            if operator is None:
                continue
            values = self.get_values(cov, field)
            expression = ' '.join(['values', operator, value])
            bitmask = bitmask & ne.evaluate(expression)

        return bitmask

    def get_values(self, cov, field):
        data_dict = cov.get_parameter_values(param_names=[field], fill_empty_params=True, as_record_array=False).get_data()
        data = data_dict[field]
        return data

    def get_dataset(self, cov, fields, slices, selectors, dataset, response):
        seq = SequenceType('data')
        bitmask = self.get_bitmask(cov, fields, slices, selectors)
        if self.is_too_large(bitmask, len(fields)):
            log.error('Client request too large. \nFields: %s\nSelectors: %s', fields, selectors)
            return
        for name in fields:
            # Strip the data. from the field
            if name.startswith('data.'):
                name = name[5:]
            pc = cov.get_parameter_context(name)
            if re.match(r'.*_[a-z0-9]{32}', name):
                continue # Let's not do this
            try:
                data = self.get_data(cov, name, bitmask)
                attrs  = self.get_attrs(cov, name)
                if isinstance(pc.param_type, QuantityType):
                    data, dtype = self.filter_data(data)
                    seq[name] = self.make_series(response, name, data, attrs, dtype)
                elif isinstance(pc.param_type, ConstantType):
                    data, dtype = self.filter_data(data)
                    seq[name] = self.make_series(response, name, data, attrs, dtype)
                elif isinstance(pc.param_type, ConstantRangeType):
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
                    seq[name] = self.make_series(response, name, data, attrs, 'S')
                elif isinstance(pc.param_type,BooleanType):
                    data, dtype = self.filter_data(data)
                    seq[name] = self.make_series(response, name, data, attrs, dtype)
                elif isinstance(pc.param_type,CategoryType):
                    data, dtype = self.filter_data(data)
                    #start = time.time()
                    seq[name] = self.make_series(response, name, data, attrs, dtype)
                elif isinstance(pc.param_type,ArrayType):
                    data, dtype = self.filter_data(data)
                    seq[name] = self.make_series(response, name, data, attrs, dtype)

                elif isinstance(pc.param_type,RecordType):
                    data, dtype = self.filter_data(data)
                    seq[name] = self.make_series(response, name, data, attrs, dtype)

                elif isinstance(pc.param_type, ParameterFunctionType):
                    data, dtype = self.filter_data(data)
                    seq[name] = self.make_series(response, name, data, attrs, dtype)

                elif isinstance(pc.param_type, SparseConstantType):
                    data, dtype = self.filter_data(data)
                    seq[name] = self.make_series(response, name, data, attrs, dtype)
                #dataset[name] = self.make_series(response, name, data, attrs, dtype)
#                elif param.is_coordinate and cov.temporal_parameter_name == name:
#                        dataset[name] = BaseType(name=name, data=data, type=data.dtype.char, attributes=attrs, shape=data.shape)
#                    else:
#                        log.error("Unhandled parameter for parameter (%s) type: %s", name, pc.param_type.__class__.__name__)
                    
            except Exception, e:
                log.exception('Problem reading cov %s %s', cov.name, e.__class__.__name__)
                continue
            dataset['data'] = seq
        return dataset

    def value_encoding_to_dap_type(self, value_encoding):
        if value_encoding is None:
            return 'S'
        
        dt = np.dtype(value_encoding).char
        if dt =='O':
            return 'S'
        return dt

    def dap_type(self, context):
        if isinstance(context.param_type, (ArrayType, ConstantRangeType, CategoryType, RecordType)):
            return 'S'
        return self.value_encoding_to_dap_type(context.param_type.value_encoding)

    def handle_dds(self, coverage, dataset, fields):
        cov = coverage
        seq = SequenceType('data')

        for name in fields:
            # Strip the data. from the field

            if name.startswith('data.'):
                name = name[5:]
                
            if re.match(r'.*_[a-z0-9]{32}', name):
                continue # Let's not do this
            try:
                context = coverage.get_parameter_context(name)
                attrs = self.get_attrs(cov, name)

                #grid[name] = BaseType(name=name, type=self.dap_type(context), attributes=attrs, dimensions=(time_name,), shape=(coverage.num_timesteps,))
                seq[name] = BaseType(name=name, type=self.dap_type(context), attributes=attrs, shape=(coverage.num_timesteps(),))
                #grid[cov.temporal_parameter_name] = time_base
            except Exception:
                log.exception('Problem reading cov %s', str(cov))
                continue
        dataset['data'] = seq
        return dataset


    def parse_query_string(self,query_string):
        tokens = query_string.split('&')
        fields = []
        selectors = []
        slices = []
        dap_selection_operators = ['<', '<=', '>', '>=', '=', '!=', '=~']
        slice_operators = ['[', ':', ']']
        for token in tokens:
            token = unquote(token)
            if not token: # ignore the case where the url ends in nothing or a &
                continue
            token_identified = False
            for selector in dap_selection_operators:
                if selector in token:
                    selectors.append(token)
                    token_identified = True
                    break
            for operator in slice_operators:
                if operator in token:
                    slices.append(token)
                    token_identified = True
                    break
            if not token_identified:
                fields = token.split(',')

        return fields, slices, selectors

    def parse_slices(self,slice_operator):
        pivot = slice_operator.find('[')
        field = slice_operator[:pivot]
        slicer = slice_operator[pivot:]
        # Strip away the outer []s
        slicer = slicer[1:-1]
        # Separte the slice into tokens separated by :
        start,stride,stop = slicer.split(':')
        start = int(start)
        stride = int(stride)
        stop = int(stop)+1
        slice_ = slice(start,stride,stop)
        return field, slice_

    def parse_selectors(self, selector):
        matches = re.match(r'([a-zA-Z0-9-_\.]+)(<=|<|>=|>|=~|!=|=)(.*)', selector)
        field, operator, value = matches.groups()
        value = value.replace('"','')
        value = value.replace("'",'')
        if 'data.' in field: # strip away the data prefix
            field = field[5:]
        if operator is '=':
            operator = '=='
        elif operator is '=~':
            return None,None,None

        return field, operator, value
    
    @request_profile(CFG.get_safe('server.pydap.profile_enabled', True))
    @exception_wrapper
    def parse_constraints(self, environ):
        base, data_product_id = os.path.split(self.filepath)
        coverage = self.get_coverage(data_product_id)

        last_modified = formatdate(time.mktime(time.localtime(os.stat(self.filepath)[ST_MTIME])))
        environ['pydap.headers'].append(('Last-modified', last_modified))

        atts = {}
        atts['title'] = coverage.name
        dataset = DatasetType(coverage.name) #, attributes=atts)
        response = environ['pydap.response']

        if response == 'dods':
            query_string = environ['QUERY_STRING']
            fields, slices, selectors = self.parse_query_string(query_string)
        elif response in ('dds', 'das'):
            fields = [] # All fields
            slices = []
            selectors = []

        all_vars = coverage.list_parameters()
        
        if not fields:
            fields = all_vars
        if response == "dods":
            dataset = self.get_dataset(coverage, fields, slices, selectors, dataset, response)

        elif response in ('dds', 'das'):
            self.handle_dds(coverage, dataset, fields)
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
