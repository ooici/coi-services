#!/usr/bin/env python

"""
@package  ion.services.sa.instrument_management.platform_model_dryer
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class PlatformModelDryer(IMSsimple):
    """
    @brief resource management for PlatformModel resources
    """

    def _primary_object_name(self):
        return RT.PlatformModel

    def _primary_object_label(self):
        return "platform_model"
