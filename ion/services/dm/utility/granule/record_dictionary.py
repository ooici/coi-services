#!/usr/bin/env python

'''
@package ion.services.dm.utility.granule.record_dictionary
@file ion/services/dm/utility/granule/record_dictionary.py
@author Tim Giguere
@author Luke Campbell <LCampbell@ASAScience.com>
@brief https://confluence.oceanobservatories.org/display/CIDev/Record+Dictionary
'''

from pyon.container.cc import Container
from pyon.core.exception import BadRequest, NotFound
from pyon.core.object import IonObjectSerializer
from pyon.core.interceptor.encode import encode_ion
from pyon.util.arg_check import validate_equal
from pyon.util.log import log
from pyon.util.memoize import memoize_lru

from ion.util.stored_values import StoredValueManager

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import Granule, StreamDefinition

from coverage_model import ParameterDictionary, ConstantType, ConstantRangeType, get_value_class, SimpleDomainSet, QuantityType, Span, SparseConstantType
from coverage_model.parameter_functions import ParameterFunctionException
from coverage_model.parameter_values import AbstractParameterValue, ConstantValue
from coverage_model.parameter_types import ParameterFunctionType

import numpy as np
from copy import copy
import msgpack
import time

class RecordDictionaryTool(object):
    """
    A record dictionary is a key/value store which contains records for a particular dataset. The keys are specified by
    a parameter dictionary which map to the fields in the dataset, the data types for the records as well as metadata
    about the fields. Each field in a record dictionary must contain the same number of records. A record can consist of
    a scalar value (typically a NumPy scalar) or it may contain an array or dictionary of values. The data type for each
    field is determined by the parameter dictionary. The length of the first assignment dictates the allowable size for
    the RDT - see the Tip below

    The record dictionary can contain an instance of the parameter dictionary itself or it may contain a reference to
    one by use of a stream definition. A stream definition is a resource persisted by the resource registry and contains
    the parameter dictionary. When the record dictionary is constructed the client will specify either a stream
    definition identifier or a parameter dictionary.

    ParameterDictionaries are inherently large and congest message traffic through RabbitMQ, therefore it is preferred 
    to use stream definitions in lieu of parameter dictionaries directly.
    """

    _rd                 = None
    _pdict              = None
    _shp                = None
    _locator            = None
    _stream_def         = None
    _dirty_shape        = False
    _available_fields   = None
    _creation_timestamp = None
    _stream_config      = {}
    _definition         = None
    connection_id       = ''
    connection_index    = ''


    def __init__(self,param_dictionary=None, stream_definition_id='', locator=None, stream_definition=None):
        """
        """
        if type(param_dictionary) == dict:
            self._pdict = ParameterDictionary.load(param_dictionary)
        
        elif isinstance(param_dictionary,ParameterDictionary):
            self._pdict = param_dictionary
        
        elif stream_definition_id or stream_definition:
            if stream_definition:
                if not isinstance(stream_definition,StreamDefinition):
                    raise BadRequest('Improper StreamDefinition object')
                self._definition = stream_definition

            stream_def_obj = stream_definition or RecordDictionaryTool.read_stream_def(stream_definition_id)
            pdict = stream_def_obj.parameter_dictionary
            self._available_fields = stream_def_obj.available_fields or None
            self._stream_config = stream_def_obj.stream_configuration
            self._pdict = ParameterDictionary.load(pdict)
            self._stream_def = stream_definition_id

        else:
            raise BadRequest('Unable to create record dictionary with improper ParameterDictionary')
        
        if stream_definition_id:
            self._stream_def=stream_definition_id
        
        self._shp = None
        self._rd = {}
        self._locator = locator

        self._setup_params()

    def _pval_callback(self, name, slice_):
        retval = np.atleast_1d(self[name])
        return retval[slice_]

    @classmethod
    def get_paramval(cls, ptype, domain, values):
        paramval = get_value_class(ptype, domain_set=domain)
        if isinstance(ptype,ParameterFunctionType):
            paramval.memoized_values = values
        if isinstance(ptype,SparseConstantType):
            values = np.atleast_1d(values)
            spans = cls.spanify(values)
            paramval.storage._storage = np.array([spans],dtype='object')
        else:
            paramval[:] = values
        paramval.storage._storage.flags.writeable = False
        return paramval

    def lookup_values(self):
        return [i for i in self._lookup_values() if not self.context(i).document_key]

    def _lookup_values(self):
        lookup_values = []
        for field in self.fields:
            if hasattr(self.context(field), 'lookup_value'):
                lookup_values.append(field)
        return lookup_values
    
    @classmethod
    def spanify(cls,arr):
        spans = []
        lastval = None
        for i,val in enumerate(arr):
            if i == 0:
                span = Span(None,None,0,val)
                spans.append(span)
                lastval = val
                continue
            if np.atleast_1d(lastval == val).all():
                continue
            spans[-1].upper_bound = i
            span = Span(i,None,-i,val)
            spans.append(span)
            lastval = val
        return spans


    def fetch_lookup_values(self):
        doc_keys = []
        for lv in self._lookup_values():
            context = self.context(lv)
            if context.document_key:
                document_key = context.document_key
                if '$designator' in context.document_key and 'reference_designator' in self._stream_config:
                    document_key = document_key.replace('$designator',self._stream_config['reference_designator'])
                doc_keys.append(document_key)

        lookup_docs = {}
        if doc_keys:
            svm = StoredValueManager(Container.instance)
            doc_list = svm.read_value_mult(doc_keys)
            lookup_docs = dict(zip(doc_keys, doc_list))

        for lv in self._lookup_values():
            context = self.context(lv)
            if context.document_key:
                document_key = context.document_key
                if '$designator' in context.document_key and 'reference_designator' in self._stream_config:
                    document_key = document_key.replace('$designator',self._stream_config['reference_designator'])
                doc = lookup_docs[document_key]
                if doc is None:
                    log.debug('Reference Document for %s not found', document_key)
                    continue
                if context.lookup_value in doc:
                    self[lv] = [doc[context.lookup_value]] * self._shp[0] if self._shp else doc[context.lookup_value]

    @classmethod
    def load_from_granule(cls, g):
        if g.stream_definition_id:
            instance = cls(stream_definition_id=g.stream_definition_id, locator=g.locator)
        elif g.stream_definition:
            instance = cls(stream_definition=g.stream_definition, locator=g.locator)
        else:
            instance = cls(param_dictionary=g.param_dictionary, locator=g.locator)
        
       
        if g.domain:
            instance._shp = (g.domain[0],)

        if g.creation_timestamp:
            instance._creation_timestamp = g.creation_timestamp

        # Do time first
        time_ord = instance.to_ordinal(instance.temporal_parameter)
        if g.record_dictionary[time_ord] is not None:
            instance._rd[instance.temporal_parameter] = g.record_dictionary[time_ord]

        for k,v in g.record_dictionary.iteritems():
            key = instance.from_ordinal(k)
            if v is not None:
                #ptype = instance._pdict.get_context(key).param_type
                #paramval = cls.get_paramval(ptype, instance.domain, v)
                instance._rd[key] = v
        
        instance.connection_id = g.connection_id
        instance.connection_index = g.connection_index

        return instance

    def to_granule(self, data_producer_id='',provider_metadata_update={}, connection_id='', connection_index=''):
        granule = Granule()
        granule.record_dictionary = {}
        
        for key,val in self._rd.iteritems():
            if val is not None:
                granule.record_dictionary[self.to_ordinal(key)] = self[key]
            else:
                granule.record_dictionary[self.to_ordinal(key)] = None
        
        granule.param_dictionary = {} if self._stream_def else self._pdict.dump()
        if self._definition:
            granule.stream_definition = self._definition
        else:
            granule.stream_definition = None
            granule.stream_definition_id = self._stream_def
        granule.locator = self._locator
        granule.domain = self.domain.shape
        granule.data_producer_id=data_producer_id
        granule.provider_metadata_update=provider_metadata_update
        granule.creation_timestamp = time.time()
        granule.connection_id = connection_id
        granule.connection_index = connection_index
        return granule


    def _setup_params(self):
        for param in self._pdict.keys():
            self._rd[param] = None

    @property
    def fields(self):
        if self._available_fields is not None:
            return list(set(self._available_fields).intersection(self._pdict.keys()))
        return self._pdict.keys()

    @property
    def domain(self):
        dom = SimpleDomainSet(self._shp)
        return dom

    @property
    def temporal_parameter(self):
        return self._pdict.temporal_parameter_name

    def fill_value(self,name):
        return self._pdict.get_context(name).fill_value

    def _replace_hook(self, name,vals):
        if vals is None:
            return None
        if not isinstance(self._pdict.get_context(name).param_type, QuantityType):
            return vals
        if isinstance(vals, (list,tuple)):
            vals = [i if i is not None else self.fill_value(name) for i in vals]
            if all([i is None for i in vals]):
                return None
            return vals
        if isinstance(vals, np.ndarray):
            np.place(vals,vals==np.array(None), self.fill_value(name))
            try:
                if (vals == np.array(self.fill_value(name))).all():
                    return None
            except AttributeError:
                pass
            return np.asanyarray(vals, dtype=self._pdict.get_context(name).param_type.value_encoding)
        return np.atleast_1d(vals)

    def __setitem__(self, name, vals):
        return self._set(name, self._replace_hook(name,vals))

    def _set(self, name, vals):
        """
        Set a parameter
        """
        if name not in self.fields:
            raise KeyError(name)

        if vals is None:
            self._rd[name] = None
            return
        context = self._pdict.get_context(name)

        if self._shp is None and isinstance(context.param_type, (SparseConstantType, ConstantType, ConstantRangeType)):
            self._shp = (1,)
            self._dirty_shape = True
        
        elif self._shp is None or self._dirty_shape:
            if isinstance(vals, np.ndarray):
                self._shp = (vals.shape[0],) # Only support 1-d right now
            elif isinstance(vals, list):
                self._shp = (len(vals),)
            else:
                raise BadRequest('No shape was defined')

            log.trace('Set shape to %s', self._shp)
            if self._dirty_shape:
                self._dirty_shape = False
                self._reshape_const()

        else:
            if isinstance(vals, np.ndarray):
                if not vals.shape:
                    raise BadRequest('Invalid shape on input (dimensionless)')
                validate_equal(vals.shape[0], self._shp[0], 'Invalid shape on input (%s expecting %s)' % (vals.shape, self._shp))
            elif isinstance(vals, list):
                validate_equal(len(vals), self._shp[0], 'Invalid shape on input')

        #paramval = self.get_paramval(context.param_type, dom, vals)
        self._rd[name] = vals

    def param_type(self, name):
        if name in self.fields:
            return self._pdict.get_context(name).param_type
        raise KeyError(name)

    def context(self, name):
        if name in self.fields:
            return self._pdict.get_context(name)
        raise KeyError(name)

    def _reshape_const(self):
        for k in self.fields:
            if isinstance(self._rd[k], ConstantValue):
                self._rd[k].domain_set = self.domain

    def __getitem__(self, name):
        """
        Get an item by nick name from the record dictionary.
        """
        if not self._shp:
            return None
        if self._available_fields and name not in self._available_fields:
            raise KeyError(name)
        ptype = self._pdict.get_context(name).param_type
        if isinstance(ptype, ParameterFunctionType):
            if self._rd[name] is not None and getattr(self._rd[name],'memoized_values',None) is not None:
                return self._rd[name].memoized_values[:]
            try:
                pfv = get_value_class(ptype, self.domain)
                pfv._pval_callback = self._pval_callback
                return pfv[:]
            except ParameterFunctionException:
                log.debug('failed to get parameter function field: %s (%s)', name, self._pdict.keys(), exc_info=True)
        if self._rd[name] is not None:
            return np.atleast_1d(self._rd[name])
        return None

    def iteritems(self):
        """ D.iteritems() -> an iterator over the (key, value) items of D """
        for k,v in self._rd.iteritems():
            if self._available_fields and k not in self._available_fields:
                continue
            if v is not None:
                yield k,v

    def iterkeys(self):
        """ D.iterkeys() -> an iterator over the keys of D """
        for k,v in self._rd.iteritems():
            if v is not None:
                yield k

    def itervalues(self):
        """ D.itervalues() -> an iterator over the values of D """
        for k,v in self._rd.iteritems():
            if v is not None:
                yield v

    def __contains__(self, key):
        """ D.__contains__(k) -> True if D has a key k, else False """
        if self._available_fields:
            return key in self._rd and key in self._available_fields
        return key in self._rd

    def __delitem__(self, y):
        """ x.__delitem__(y) <==> del x[y] """
        self._rd[y] = None

    def __iter__(self):
        """ x.__iter__() <==> iter(x) """
        for k in self._rd.iterkeys():
            yield k


    def __len__(self):
        """ x.__len__() <==> len(x) """
        if self._shp is None:
            return 0
        else:
            return self._shp[0]

    def __repr__(self):
        """ x.__repr__() <==> repr(x) """
        return self.pretty_print()

    def __str__(self):
        return 'Record Dictionary %s' % self._rd.keys()

    __hash__ = None

    def pretty_print(self):
        """
        @brief Pretty Print the record dictionary for debug or log purposes.
        """
        from pprint import pformat
        repr_dict = {}
        for field in self.fields:
            if self[field] is not None:
                repr_dict[field] = self[field][:]
        return pformat(repr_dict)


    def size(self):
        '''
        Truly poor way to calculate the size of a granule...
        returns the size in bytes.
        '''
        granule = self.to_granule()
        serializer = IonObjectSerializer()
        flat = serializer.serialize(granule)
        byte_stream = msgpack.packb(flat, default=encode_ion)
        return len(byte_stream)

    
    def to_ordinal(self, key):
        params = copy(self.fields)
        params.sort()
        try:
            return params.index(key)
        except ValueError:
            raise KeyError(key)
        
    def from_ordinal(self, ordinal):
        params = copy(self.fields)
        params.sort()
        return params[ordinal]


    @staticmethod
    def read_stream_def(stream_def_id):
        pubsub_cli = PubsubManagementServiceClient()
        stream_def_obj = pubsub_cli.read_stream_definition(stream_def_id)
        return stream_def_obj


