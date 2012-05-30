#!/usr/bin/env python

"""
@package ion.services.sa.instrument.flag Flags for IMS
@file ion/services/sa/instrument/flag.py
@author Ian katz
@brief Contstants for IMS
"""


from ion.agents.instrument.common import BaseEnum

class KeywordFlag(BaseEnum):
    """attachment keywords used for LCS validation"""

    FLAG_PREFIX = "IONFLAG_"
    fp = FLAG_PREFIX

    EGG_URL                         = fp + "egg_url"
    CERTIFICATION                   = fp + "certification"
    TEST_RESULTS                    = fp + "test_results"
    # INSTRUMENT_AGENT_CERTIFICATION  = fp + "instrument_agent_certification"
    # INSTRUMENT_DEVICE_CERTIFICATION = fp + "instrument_device_certification"
    # PLATFORM_AGENT_CERTIFICATION    = fp + "instrument_agent_certification"
    # PLATFORM_DEVICE_CERTIFICATION   = fp + "instrument_device_certification"
