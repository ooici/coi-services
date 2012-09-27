#!/usr/bin/env python

'''
@package ion.services.dm.utility.granule.record_dictionary
@file ion/services/dm/utility/granule/record_dictionary.py
@author Tim Giguere
@author Luke Campbell <LCampbell@ASAScience.com>
@brief https://confluence.oceanobservatories.org/display/CIDev/Record+Dictionary
'''

from pyon.core.exception import BadRequest
from coverage_model.parameter import ParameterDictionary
from coverage_model.parameter_values import get_value_class, AbstractParameterValue
from coverage_model.coverage import SimpleDomainSet
from pyon.util.log import log
from pyon.util.arg_check import validate_equal
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import Granule
from pyon.util.memoize import memoize_lru
import numpy as np

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

    _rd          = None
    _pdict       = None
    _shp         = None
    _locator     = None
    _stream_def  = None

    def __init__(self,param_dictionary=None, stream_definition_id='', locator=None):
        """
        """
        if type(param_dictionary) == dict:
            self._pdict = ParameterDictionary.load(param_dictionary)
        
        elif isinstance(param_dictionary,ParameterDictionary):
            self._pdict = param_dictionary
        
        elif stream_definition_id:
            pdict = RecordDictionaryTool.pdict_from_stream_def(stream_definition_id)
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
    def load_from_granule(cls, g):
        if isinstance(g.param_dictionary, str):
            instance = cls(stream_definition_id=g.param_dictionary, locator=g.locator)
            pdict = RecordDictionaryTool.pdict_from_stream_def(g.param_dictionary)
            instance._pdict = ParameterDictionary.load(pdict)
        
        else:
            instance = cls(param_dictionary=g.param_dictionary, locator=g.locator)
            instance._pdict = ParameterDictionary.load(g.param_dictionary)
        
       
        if g.domain['shape']:
            instance._shp = (g.domain['shape'][0],)
        
        for k,v in g.record_dictionary.iteritems():
            if v is not None:
                g.record_dictionary[k]['domain_set'] = g.domain
                ptype = instance._pdict.get_context(k).param_type
                g.record_dictionary[k]['parameter_type'] = ptype.dump()

                instance._rd[k] = AbstractParameterValue.load(g.record_dictionary[k])
        
        return instance


    def to_granule(self, data_producer_id=''):
        granule = Granule()
        granule.record_dictionary = {}
        
        for key,val in self._rd.iteritems():
            if val is not None:
                granule.record_dictionary[key] = val.dump()
                granule.record_dictionary[key]['domain_set'] = None
                granule.record_dictionary[key]['parameter_type'] = None
            else:
                granule.record_dictionary[key] = None
        
        granule.param_dictionary = self._stream_def or self._pdict.dump()
        granule.locator = self._locator
        granule.domain = self.domain.dump()
        granule.data_producer_id=data_producer_id
        return granule


    def _setup_params(self):
        for param in self._pdict.keys():
            self._rd[param] = None

    @property
    def domain(self):
        dom = SimpleDomainSet(self._shp)
        return dom

    def __setitem__(self, name, vals):
        """
        Set a parameter
        """
        if name not in self._rd:
            raise KeyError(name)
        context = self._pdict.get_context(name)
        if self._shp is None: # Not initialized:
            if isinstance(vals, np.ndarray):
                self._shp = vals.shape
            elif isinstance(vals, list):
                self._shp = (len(vals),)
            else:
                raise BadRequest('No shape was defined')

            log.info('Set shape to %s', self._shp)
        else:
            if isinstance(vals, np.ndarray):
                validate_equal(vals.shape, self._shp, 'Invalid shape on input')
            elif isinstance(vals, list):
                validate_equal(len(vals), self._shp[0], 'Invalid shape on input')

        dom = self.domain
        paramval = get_value_class(context.param_type, domain_set = dom)
        paramval[:] = vals
        paramval.storage._storage.flags.writeable = False
        self._rd[name] = paramval



    def __getitem__(self, name):
        """
        Get an item by nick name from the record dictionary.
        """
        if self._rd[name]:
            return self._rd[name][:]
        else:
            return None

    def iteritems(self):
        """ D.iteritems() -> an iterator over the (key, value) items of D """
        for k,v in self._rd.iteritems():
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
    
    @staticmethod
    @memoize_lru(maxsize=100)
    def pdict_from_stream_def(stream_def_id):
        pubsub_cli = PubsubManagementServiceClient()
        stream_def_obj = pubsub_cli.read_stream_definition(stream_def_id)
        return stream_def_obj.parameter_dictionary


