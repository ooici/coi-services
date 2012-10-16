#!/usr/bin/env python

'''
@package ion.services.dm.utility.granule.granule
@file ion/services/dm/utility/granule/granule.py
@author David Stuebe
@author Tim Giguere
@author Luke Campbell
@brief https://confluence.oceanobservatories.org/display/CIDev/R2+Construction+Data+Model
'''

from pyon.util.arg_check import validate_is_instance
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool


def build_granule(record_dictionary=None, **kwargs):
    """
    This method is a simple wrapper that knows how to produce a granule IonObject from a RecordDictionaryTool and a TaxonomyTool

    A granule is a unit of information which conveys part of a coverage.

    A granule contains a record dictionary. The record dictionary is composed of named value sequences.
    We want the Granule Builder to have a dictionary like behavior for building record dictionaries, using the taxonomy
    as a map from the name to the ordinal in the record dictionary.
    """
    if record_dictionary is None:
        raise StandardError('Must provide a record dictionary')
    validate_is_instance(record_dictionary,RecordDictionaryTool)
    return record_dictionary.to_granule(**kwargs)

def combine_granules(granule_a, granule_b):
    """
    This is a method that combines granules in a very naive manner
    """
    raise NotImplementedError('Deprecated')



