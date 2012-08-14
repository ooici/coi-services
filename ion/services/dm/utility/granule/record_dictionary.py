#!/usr/bin/env python

'''
@package ion.services.dm.utility.granule.record_dictionary
@file ion/services/dm/utility/granule/record_dictionary.py
@author David Stuebe
@author Tim Giguere
@brief https://confluence.oceanobservatories.org/display/CIDev/R2+Construction+Data+Model
'''

import StringIO
from ion.services.dm.utility.granule.taxonomy import TaxyTool, Taxonomy
from coverage_model.parameter import ParameterDictionary
from pyon.util.log import log
import numpy

_NoneType = type(None)
class RecordDictionaryTool(object):
    """
    A granule is a unit of information which conveys part of a coverage. It is composed of a taxonomy and a nested
    structure of record dictionaries.

    The record dictionary is composed of named value sequences and other nested record dictionaries.

    The fact that all of the keys in the record dictionary are handles mapped by the taxonomy should never be exposed.
    The application user refers to everything in the record dictionary by the unique nick name from the taxonomy.

    """
    def __init__(self, taxonomy=None, param_dictionary=None, shape=None):
        """
        @brief Initialize a new instance of Record Dictionary Tool with a taxonomy and an optional fixed length
        @param taxonomy is an instance of a TaxonomyTool or Taxonomy (IonObject) used in this record dictionary
        @param length is an optional fixed length for the value sequences of this record dictionary
        """
        if not isinstance(shape, (_NoneType, int, tuple)):
            raise TypeError('Invalid shape argument, received type "%s"; should be None or int or tuple' % type(shape))


        self._rd = {}
        self._shp = shape

        if isinstance(self._shp, int):
            self._shp = (self._shp,)

        # hold onto the taxonomy - we need it to build the granule...

        #Eventually Taxonomy and TaxyTool will go away, to be replaced with ParameterDictionary from coverage-model
        #For now, keep Taxonomy stuff in until everyone can re-work their code.
        if param_dictionary:
            self._param_dict = param_dictionary
            self._tx = None
        elif isinstance(taxonomy, TaxyTool):
            self._tx = taxonomy
            self._param_dict = None
        elif isinstance(taxonomy, Taxonomy):
            self._tx = TaxyTool(taxonomy)
            self._param_dict = None
        else:
            raise TypeError('Invalid taxonomy argument, received type "%s"; should be ParameterDictionary, Taxonomy or TaxyTool' % type(taxonomy))

    @classmethod
    def load_from_granule(cls, g):
        """
        @brief return an instance of Record Dictionary Tool from a granule. Used when a granule is received in a message
        """
        if g.param_dictionary:
            result = cls(param_dictionary=ParameterDictionary.load(g.param_dictionary))

        else:
            result = cls(TaxyTool(g.taxonomy))

        result._rd = g.record_dictionary
        if result._rd.has_key(0):
            result._shp = result._rd[0].shape
        return result

    def __setitem__(self, name, vals):
        """
        Set an item by nick name in the record dictionary
        """

        #This if block is used if RecordDictionaryTool was initialized using a ParameterDictionary.
        #This is the long term solution using the coverage-model.
        if self._param_dict:
            if isinstance(vals, RecordDictionaryTool):
                assert vals._param_dict == self._param_dict
                self._rd[self._param_dict.ord_from_key(param_name=name)] = vals._rd
            elif isinstance(vals, numpy.ndarray):
                #Otherwise it is a value sequence which should have the correct length

                # Matthew says: Assert only equal shape 5/17/12

                if vals.ndim == 0:
                    raise ValueError('The rank of a value sequence array in a record dictionary must be greater than zero. Got name "%s" with rank "%d"' % (name, vals.ndim))

                # Set _shp if it is None
                if self._shp is None:
                    self._shp = vals.shape

                # Test new value sequence length
                if self._shp != vals.shape:
                    raise ValueError('Invalid array shape "%s" for name "%s"; Record dictionary defined shape is "%s"' % (vals.shape, name, self._shp))

                self._rd[self._param_dict.ord_from_key(param_name=name)] = vals

            else:

                raise TypeError('Invalid type "%s" in Record Dictionary Tool setitem with name "%s". Valid types are numpy.ndarray and RecordDictionaryTool' % (type(vals),name))

        #This if block is used if RecordDictionaryTool was initialized using a Taxonomy or TaxyTool
        #This block will eventually be removed.
        if self._tx:
            if isinstance(vals, RecordDictionaryTool):
                assert vals._tx == self._tx
                self._rd[self._tx.get_handle(name)] = vals._rd
            elif isinstance(vals, numpy.ndarray):
                #Otherwise it is a value sequence which should have the correct length

                # Matthew says: Assert only equal shape 5/17/12

                if vals.ndim == 0:
                    raise ValueError('The rank of a value sequence array in a record dictionary must be greater than zero. Got name "%s" with rank "%d"' % (name, vals.ndim))

                # Set _shp if it is None
                if self._shp is None:
                    self._shp = vals.shape

#                # Test new value sequence length
                if self._shp != vals.shape:
                    raise ValueError('Invalid array shape "%s" for name "%s"; Record dictionary defined shape is "%s"' % (vals.shape, name, self._shp))

                self._rd[self._tx.get_handle(name)] = vals

            else:

                raise TypeError('Invalid type "%s" in Record Dictionary Tool setitem with name "%s". Valid types are numpy.ndarray and RecordDictionaryTool' % (type(vals),name))

    def __getitem__(self, name):
        """
        Get an item by nick name from the record dictionary.
        """

        #This if block is used if RecordDictionaryTool was initialized using a ParameterDictionary.
        #This is the long term solution using the coverage-model.
        if self._param_dict:
            if isinstance(self._rd[self._param_dict.ord_from_key(param_name=name)], dict):
                result = RecordDictionaryTool(taxonomy=self._param_dict)
                result._rd = self._rd[self._param_dict.ord_from_key(param_name=name)]
                return result
            else:
                return self._rd[self._param_dict.ord_from_key(param_name=name)]

        #This if block is used if RecordDictionaryTool was initialized using a Taxonomy or TaxyTool
        #This block will eventually be removed.
        if self._tx:
            if isinstance(self._rd[self._tx.get_handle(name)], dict):
                result = RecordDictionaryTool(taxonomy=self._tx)
                result._rd = self._rd[self._tx.get_handle(name)]
                return result
            else:
                return self._rd[self._tx.get_handle(name)]

    def iteritems(self):
        """ D.iteritems() -> an iterator over the (key, value) items of D """

        #This if block is used if RecordDictionaryTool was initialized using a ParameterDictionary.
        #This is the long term solution using the coverage-model.
        if self._param_dict:
            for k, v in self._rd.iteritems():
                if isinstance(v, dict):
                    result = RecordDictionaryTool(param_dictionary=self._param_dict)
                    result._rd = v
                    yield self._param_dict.key_from_ord(k), result
                else:
                    yield self._param_dict.key_from_ord(k), v

        #This if block is used if RecordDictionaryTool was initialized using a Taxonomy or TaxyTool
        #This block will eventually be removed.
        if self._tx:
            for k, v in self._rd.iteritems():
                if isinstance(v, dict):
                    result = RecordDictionaryTool(taxonomy=self._tx)
                    result._rd = v
                    yield self._tx.get_nick_name(k), result
                else:
                    yield self._tx.get_nick_name(k), v

    def iterkeys(self):
        """ D.iterkeys() -> an iterator over the keys of D """

        #This if block is used if RecordDictionaryTool was initialized using a ParameterDictionary.
        #This is the long term solution using the coverage-model.
        if self._param_dict:
            for k in self._rd.iterkeys():
                yield self._param_dict.key_from_ord(k)

        #This if block is used if RecordDictionaryTool was initialized using a Taxonomy or TaxyTool
        #This block will eventually be removed.
        if self._tx:
            for k in self._rd.iterkeys():
                yield self._tx.get_nick_name(k)

    def itervalues(self):
        """ D.itervalues() -> an iterator over the values of D """

        #This if block is used if RecordDictionaryTool was initialized using a ParameterDictionary.
        #This is the long term solution using the coverage-model.
        if self._param_dict:
            for v in self._rd.itervalues():
                if isinstance(v, dict):
                    result = RecordDictionaryTool(taxonomy=self._param_dict)
                    result._rd = v
                    yield result
                else:
                    yield v

        #This if block is used if RecordDictionaryTool was initialized using a Taxonomy or TaxyTool
        #This block will eventually be removed.
        if self._tx:
            for v in self._rd.itervalues():
                if isinstance(v, dict):
                    result = RecordDictionaryTool(taxonomy=self._tx)
                    result._rd = v
                    yield result
                else:
                    yield v

    #TJG - This may need to be updated
    def update(self, E=None, **F):
        """
        @brief Dictionary update method exposed for Record Dictionaries
        @param E is another record dictionary
        @param F is a dictionary of nicknames and value sequences
        """
        if E:
            if hasattr(E, "keys"):
                for k in E:
                    self[k] = E[k]
            else:
                for k, v in E.iteritems():
                    self[k] = v

        if F:
            for k in F.keys():
                self[k] = F[k]

    def __contains__(self, nick_name):
        """ D.__contains__(k) -> True if D has a key k, else False """

        handle = ''
        try:
            if self._param_dict:
                handle = self._param_dict.ord_from_key(nick_name)

            if self._tx:
                handle = self._tx.get_handle(nick_name)
        except KeyError as ke:
            # if the nick_name is not in the taxonomy, it is certainly not in the record dictionary
            return False

        return handle in self._rd

    def __delitem__(self, y):
        """ x.__delitem__(y) <==> del x[y] """
        #not sure if this is right, might just have to remove the name, not the whole handle
        if self._param_dict:
            del self._rd[self._param_dict.ord_from_key(y)]

        if self._tx:
            del self._rd[self._tx.get_handle(y)]
        #will probably need to delete the name from _tx

    def __iter__(self):
        """ x.__iter__() <==> iter(x) """
        return self.iterkeys()

    def __len__(self):
        """ x.__len__() <==> len(x) """
        return len(self._rd)

    def __repr__(self):
        """ x.__repr__() <==> repr(x) """
        result = "{"
        for k, v in self.iteritems():
            result += "\'{0}\': {1},".format(k, v)

        if len(result) > 1:
            result = result[:-1] + "}"

        return result

    def __str__(self):
        result = "{"
        for k, v in self.iteritems():
            result += "\'{0}\': {1},".format(k, v)

        if len(result) > 1:
            result = result[:-1] + "}"

        return result

    __hash__ = None

    def pretty_print(self):
        """
        @brief Pretty Print the record dictionary for debug or log purposes.
        """
        pass

        fid = StringIO.StringIO()
        # Use string IO inside a try block in case of exceptions or a large return value.
        try:
            fid.write('Start Pretty Print Record Dictionary:\n')
            self._pprint(fid,offset='')
            fid.write('End of Pretty Print')
        except Exception, ex:
            log.exception('Unexpected Exception in Pretty Print Wrapper!')
            fid.write('Exception! %s' % ex)

        finally:
            result = fid.getvalue()
            fid.close()


        return result

    def _pprint(self, fid, offset=None):
        pass
        """
        Utility method for pretty print
        """
        for k, v in self.iteritems():
            if isinstance(v, RecordDictionaryTool):
                fid.write('= %sRDT nick named "%s" contains:\n' % (offset,k))
                new_offset = offset + '+ '
                v._pprint(fid, offset=new_offset)
            else:
                fid.write('= %sRDT nick name: "%s"\n= %svalues: %s\n' % (offset,k, offset, repr(v)))
