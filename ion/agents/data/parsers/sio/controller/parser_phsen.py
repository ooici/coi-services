#!/usr/bin/env python

"""
"""

__author__ = 'Michael Meisinger'


import ntplib
import struct
import time

from ooi.logging import log
from ion.agents.data.parsers.parser_utils import FlexDataParticle, ParserException


STREAM_NAME = "phsen"

# 2 days @ 1 record/sec
MAX_RECORDS_PER_GRANULE = 2*24*60*60


class PHSENChunkParser(object):
    """
    Read binary data chunk
    """
    def __init__(self, chunk):
        self.chunk = chunk
        self.records = self._parse_chunk()

    def _parse_chunk(self):
        data = self.chunk["content"]

        record = {
            # TODO: Set correct params
            'raq': data,
        }
        ntp_timestamp = ntplib.system_to_ntp_time(self.chunk["timestamp"])
        particle = FlexDataParticle(driver_timestamp=ntp_timestamp, stream_name=STREAM_NAME)
        particle.set_data_values(record)
        records = []
        records.append(particle)

        return records

    def get_records(self, max_count=MAX_RECORDS_PER_GRANULE):
        return self.records[:max_count]
