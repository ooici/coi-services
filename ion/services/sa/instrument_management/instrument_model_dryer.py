#!/usr/bin/env python

"""
@package  ion.services.sa.instrument_management.instrument_model_dryer
@author   Ian Katz
"""



#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class InstrumentModelDryer(IMSsimple):
    """
    @brief resource management for InstrumentModel resources
    """

    def _primary_object_name(self):
        return RT.InstrumentModel

    def _primary_object_label(self):
        return "instrument_model"
