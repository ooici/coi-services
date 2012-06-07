#!/usr/bin/env python

"""
@file coi-services/ion/idk/common.py
@author Bill French
@brief Common class definitions for the IDK
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

class Singleton(object):
    """
    Singleton interface:
    http://www.python.org/download/releases/2.2.3/descrintro/#__new__
    """
    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.init(*args, **kwds)
        return it

    def init(self, *args, **kwds):
        pass

    def destroy(cls):
        """Destroy the singleton object"""
        it = cls.__dict__.get("__it__")
        if it is not None:
            cls.__dict__["__it__"] = None

