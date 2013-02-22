#!/usr/bin/env python

'''
@package ion.services.dm.utility.granule.record_dictionary
@file ion/services/dm/utility/granule/record_dictionary.py
@author Tim Giguere
@author Luke Campbell <LCampbell@ASAScience.com>
@brief https://confluence.oceanobservatories.org/display/CIDev/Record+Dictionary
'''

from pyon.core.exception import BadRequest
from pyon.core.object import IonObjectSerializer
from pyon.core.interceptor.encode import encode_ion
from pyon.util.arg_check import validate_equal
from pyon.util.log import log
from pyon.util.memoize import memoize_lru

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import Granule


from coverage_model import ParameterDictionary, ConstantType, ConstantRangeType, get_value_class, SimpleDomainSet, QuantityType
from coverage_model.parameter_values import AbstractParameterValue, ConstantValue
from coverage_model.parameter_types import ParameterFunctionType

import numpy as np
import msgpack
import time

class RecordDictionaryTool(object):
    """
    A record dictionary is a key/value store which contains records for a particular dataset. The keys are specified by 
    a parameter dictionary which map to the fields in the dataset, the data types for the records as well as metadata about 
    the fields. Each field in a record dictionary must contain the same number of records. A record can consist of a scalar 
    value (typically a NumPy scalar) or it may contain an array or dictionary of values. The data type for each field is 
    determined by the parameter dictionary. The length of the first assignment dictates the allowable size for the RDT - 
    see the Tip below

    The record dictionary can contain an instance of the parameter dictionary itself or it may contain a reference to one 
    by use of a stream definition. A stream definition is a resource persisted by the resource registry and contains the 
    parameter dictionary. When the record dictionary is constructed the client will specify either a stream definition 
    identifier or a parameter dictionary.

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

    def __init__(self,param_dictionary=None, stream_definition_id='', locator=None):
        """
        """
        if type(param_dictionary) == dict:
            self._pdict = ParameterDictionary.load(param_dictionary)
        
        elif isinstance(param_dictionary,ParameterDictionary):
            self._pdict = param_dictionary
        
        elif stream_definition_id:
            stream_def_obj = RecordDictionaryTool.pdict_from_stream_def(stream_definition_id)
            pdict = stream_def_obj.parameter_dictionary
            self._available_fields = stream_def_obj.available_fields or None
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

    @classmethod
    def get_paramval(cls, ptype, domain, values):
        paramval = get_value_class(ptype, domain_set=domain)
        if isinstance(ptype,ParameterFunctionType):
            paramval.memoized_values = values
        else:
            paramval[:] = values
        paramval.storage._storage.flags.writeable = False
        return paramval



    @classmethod
    def load_from_granule(cls, g):
        if isinstance(g.param_dictionary, str):
            instance = cls(stream_definition_id=g.param_dictionary, locator=g.locator)
            stream_def_obj = RecordDictionaryTool.pdict_from_stream_def(g.param_dictionary)
            pdict = stream_def_obj.parameter_dictionary
            instance._available_fields = stream_def_obj.available_fields or None
            instance._pdict = ParameterDictionary.load(pdict)
        
        else:
            instance = cls(param_dictionary=g.param_dictionary, locator=g.locator)
            instance._pdict = ParameterDictionary.load(g.param_dictionary)
        
       
        if g.domain:
            instance._shp = (g.domain[0],)

        if g.creation_timestamp:
            instance._creation_timestamp = g.creation_timestamp
        
        for k,v in g.record_dictionary.iteritems():
            key = instance._pdict.key_from_ord(k)
            if v is not None:
                ptype = instance._pdict.get_context(key).param_type
                paramval = cls.get_paramval(ptype, instance.domain, v)
                instance._rd[key] = paramval
        
        return instance

    def to_granule(self, data_producer_id='',provider_metadata_update={}):
        granule = Granule()
        granule.record_dictionary = {}
        
        for key,val in self._rd.iteritems():
            if val is not None:
                granule.record_dictionary[self._pdict.ord_from_key(key)] = val[:]
            else:
                granule.record_dictionary[self._pdict.ord_from_key(key)] = None
        
        granule.param_dictionary = self._stream_def or self._pdict.dump()
        granule.locator = self._locator
        granule.domain = self.domain.shape
        granule.data_producer_id=data_producer_id
        granule.provider_metadata_update=provider_metadata_update
        granule.creation_timestamp = time.time()
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
        if not isinstance(self._pdict.get_context(name).param_type, QuantityType):
            return vals
        nparray = np.array(vals)
        np.place(nparray,nparray==np.array(None),self.fill_value(name))
        try:
            if (nparray==np.array(self.fill_value(name))).all():
                return None
        except AttributeError:
            return nparray
        return nparray

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

        if self._shp is None and (isinstance(context.param_type, ConstantType) or isinstance(context.param_type, ConstantRangeType)):
            self._shp = (1,)
            self._dirty_shape = True
        
        elif self._shp is None or self._dirty_shape:
            if isinstance(vals, np.ndarray):
                self._shp = vals.shape
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
                validate_equal(vals.shape, self._shp, 'Invalid shape on input (%s expecting %s)' % (vals.shape, self._shp))
            elif isinstance(vals, list):
                validate_equal(len(vals), self._shp[0], 'Invalid shape on input')

        dom = self.domain
        paramval = self.get_paramval(context.param_type, dom, vals)
        self._rd[name] = paramval

    def _reshape_const(self):
        for k in self.fields:
            if isinstance(self._rd[k], ConstantValue):
                self._rd[k].domain_set = self.domain

    def __getitem__(self, name):
        """
        Get an item by nick name from the record dictionary.
        """
        if self._available_fields and name not in self._available_fields:
            raise KeyError(name)
        if self._rd[name] is not None:
            context = self._pdict.get_context(name)
            if isinstance(context.param_type, ParameterFunctionType):
                return self._rd[name].memoized_values[:]
            return self._rd[name][:]
        else:
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
        return pformat(self.__dict__)


    def __eq__(self, comp):
        if self._shp != comp._shp:
            return False
        if self._pdict != comp._pdict:
            return False

        for k,v in self._rd.iteritems():
            if v != comp._rd[k]:
                if isinstance(v, AbstractParameterValue) and isinstance(comp._rd[k], AbstractParameterValue):
                    if (v.content == comp._rd[k].content).all():
                        continue
                return False
        return True

    def __ne__(self, comp):
        return not (self == comp)

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

    
    @staticmethod
    @memoize_lru(maxsize=100)
    def pdict_from_stream_def(stream_def_id):
        pubsub_cli = PubsubManagementServiceClient()
        stream_def_obj = pubsub_cli.read_stream_definition(stream_def_id)
        return stream_def_obj


