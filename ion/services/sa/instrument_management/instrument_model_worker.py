#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class InstrumentModelWorker(IMSsimple):

    def _primary_object_name(self):
        return RT.InstrumentModel

    def _primary_object_label(self):
        return "instrument_model"
