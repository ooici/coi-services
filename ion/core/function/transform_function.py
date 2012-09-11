#!/usr/bin/env python

'''
@author: Tim Giguere <tgiguere@asascience.com>
@file: pyon/ion/transforma.py
@description: New Implementation for TransformFunction classes
'''

<<<<<<< HEAD
from pyon.public import log
=======
>>>>>>> 356b8a9414212dfbe69a852eef72ca5cc67bfd12
from pyon.core.exception import BadRequest
from interface.objects import Granule

class TransformFunction(object):

    """
    The execute function receives an input and several configuration parameters, and returns the processed result.
    @param input Any object, granule, or array of granules
    @param context A dictionary
    @param config A dictionary containing the container configuration. Will usually be empty.
    @param params A dictionary containing input parameters
    @param state A dictionary containing input state
    """

<<<<<<< HEAD
=======
    def validate_inputs(f):
        raise NotImplementedError('Method validate_inputs not implemented')

    @validate_inputs
>>>>>>> 356b8a9414212dfbe69a852eef72ca5cc67bfd12
    @staticmethod
    def execute(input=None, context=None, config=None, params=None, state=None):
        raise NotImplementedError('Method execute not implemented')

class SimpleTransformFunction(TransformFunction):
    """
    This class takes in any single input object, performs a function on it, and then returns any output object.
    """
    @staticmethod
    def validate_inputs(target):
        def validate(*args, **kwargs):
            return target(*args, **kwargs)

        return validate

    @staticmethod
    def execute(input=None, context=None, config=None, params=None, state=None):
        pass

class SimpleGranuleTransformFunction(SimpleTransformFunction):
    """
    This class receives a single granule, processes it, then returns a granule object.
    """
    @staticmethod
    def validate_inputs(target):
        def validate(*args, **kwargs):
            if args[0]:
                if not isinstance(args[0], Granule):
                    raise BadRequest('input parameter must be of type Granule')
            return target(*args, **kwargs)

        return validate

    @staticmethod
    def execute(input=None, context=None, config=None, params=None, state=None):
        pass

class MultiGranuleTransformFunction(SimpleGranuleTransformFunction):
    """
    This class receives multiple granules, processes them, then returns an array of granules.
    """
    @staticmethod
    def validate_inputs(target):
        def validate(*args, **kwargs):
            if args[0]:
                if not isinstance(args[0], list):
                    raise BadRequest('input parameter must be of type List')
                for x in args[0]:
                    if not isinstance(x, Granule):
                        raise BadRequest('input list may only contain Granules')
            return target(*args, **kwargs)

        return validate

    @staticmethod
    def execute(input=None, context=None, config=None, params=None, state=None):
        pass