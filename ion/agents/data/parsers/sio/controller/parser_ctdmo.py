#!/usr/bin/env python

"""
"""

__author__ = 'Michael Meisinger'


import ntplib
import struct
import time

from ooi.logging import log
from ion.agents.data.parsers.parser_utils import FlexDataParticle, ParserException


# 2 days @ 1 record/sec
MAX_RECORDS_PER_GRANULE = 2*24*60*60


class CTDMOChunkParser(object):
    """
    Read binary data chunk
    """
    def __init__(self, chunk):
        self.chunk = chunk
        self.records = self._parse_chunk()

    def _parse_chunk(self):
        records = []
        if "<ERROR" in self.chunk["content"]:
            log.warn("ERROR chunk")
        else:
            print self.chunk
        return records

    def get_records(self, max_count=MAX_RECORDS_PER_GRANULE):
        return self.records[:max_count]
