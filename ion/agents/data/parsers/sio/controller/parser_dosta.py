#!/usr/bin/env python

"""
"""

__author__ = 'Michael Meisinger'


import ntplib
import struct
import time

from ooi.logging import log
from ion.agents.data.parsers.parser_utils import FlexDataParticle, ParserException

STREAM_NAME = "dostad"

# 2 days @ 1 record/sec
MAX_RECORDS_PER_GRANULE = 2*24*60*60


class DOSTAChunkParser(object):
    """
    Read binary data chunk
    """
    def __init__(self, chunk):
        self.chunk = chunk
        self.records = self._parse_chunk()

    def _parse_chunk(self):
        records = []

        sample = self.chunk["content"][4:].strip()
        parts = sample.split("\t")
        record = {
            # TODO: Set correct params
            #'model': parts[0],
            #'serial': parts[1],
            'C': float(parts[2]),
            'D': float(parts[3]),
            'E': float(parts[4]),
            'F': float(parts[5]),
            'G': float(parts[6]),
            'H': float(parts[7]),
            'I': float(parts[8]),
            'J': float(parts[9]),
            'K': float(parts[10]),
            'L': float(parts[11]),
        }
        ntp_timestamp = ntplib.system_to_ntp_time(self.chunk["timestamp"])
        particle = FlexDataParticle(driver_timestamp=ntp_timestamp, stream_name=STREAM_NAME)
        particle.set_data_values(record)
        records.append(particle)

        return records

    def get_records(self, max_count=MAX_RECORDS_PER_GRANULE):
        return self.records[:max_count]
