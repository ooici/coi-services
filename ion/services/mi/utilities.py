#!/usr/bin/env python

"""
@package ion.services.mi.utilities Utility classes for MI work
@file ion/services/mi/utilities.py
@author Steve Foley
@brief Various utilities that assist with MI work
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

# imports go here

class BaseEnum(object):
    """
    Base class for enums. Used to code agent and instrument
    states, events, commands and errors.
    To use, derive a class from this subclass and set values equal to it
    such as:
    @code
    class FooEnum(BaseEnum):
       VALUE1 = "Value 1"
       VALUE2 = "Value 2"
    @endcode
    and address the values as FooEnum.VALUE1 after you import the
    class/package.
    
    Enumerations are part of the code in the MI modules since they are tightly
    coupled with what the drivers can do. By putting the values here, they
    are quicker to execute and more compartmentalized so that code can be
    re-used more easily outside of a capability container as needed.
    """
    
    @classmethod
    def list(cls):
        '''
        List the values of this enum.
        '''
        return [getattr(cls,attr) for attr in dir(cls) if \
            not callable(getattr(cls,attr)) and not attr.startswith('__')]


    @classmethod
    def has(cls, item):
        '''Is the object defined in the class. Use this function to test
        a variable for enum membership. For example,
        @code
        if not FooEnum.has(possible_value)
        @endcode
        
        @param item The attribute value to test for.
        @retval True if one of the class attributes has value item, false
        otherwise.
        '''
        return item in cls.list()

