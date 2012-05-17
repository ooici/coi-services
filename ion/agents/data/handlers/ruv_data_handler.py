#!/usr/bin/env python

"""
@package ion.agents.data.handlers.ruv_data_handler
@file ion/agents/data/handlers/ruv_data_handler
@author Christopher Mueller
@brief 
"""

from pyon.public import log
from pyon.util.containers import get_safe
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler
import numpy as np

class RuvDataHandler(BaseDataHandler):

    @classmethod
    def _new_data_constraints(cls, config):
        return {}

    @classmethod
    def _get_data(cls, config):
        return None

    @classmethod
    def _init_dataset_object(cls, config):
        """
        Initialize a dataset object specific to the data handler
        Result is assigned to dh_cfg.dataset_object
        """
        return None