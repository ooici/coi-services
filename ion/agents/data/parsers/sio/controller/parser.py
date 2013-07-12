#!/usr/bin/env python

"""
The data are stored in Blocks, with a Header

The Main Controller Data Structure is as follows:

[0x01]<Header, 31Bytes>[0x02]<Data, N Bytes>[0x03]

Offset	 NumBytes	 Value/Type	 Description
00   1   [0x01] SOH, Start of Header
01   2   [A-Z0-9] Identifier for Instrument/DataType
03	 5	 [0-9] ControllerID
08	 2	 [0-9] Number of Instrument / Inductive ID
10	 1	 [_] Spacer
11	 4	 [0-9A-F] Number of Data Bytes, Hex Number
15	 1	 [a-z] Processing Flag (lowercase letter)
16	 8	 [0-9A-F] POSIX Timestamp of Controller, Hex Number
24	 1	 [_] Spacer
25	 2	 [0-9A-F] BlockNumber, HEX, 0 .. 255 (overflow)
27	 1	 [_] Spacer
28	 4	 [0-9A-F] CRC Checksum DataBytes CCITT-16 Reversed (0 x8408)
32	 1	 [0x02] STX, Start of Text
33	 N	        DataBytes
33+N 1	 [0x03]
ETX, End of Text

Instrument/Data Type IDs:
CT CTDMO
AD ADCPS
PS Program Status
CS Controller Status
FL FLORT
DO DOSTA
PH PHSEN
"""

__author__ = 'Michael Meisinger'

import ntplib
import struct
import time

from ooi.logging import log
from pyon.util.containers import named_any
from ion.agents.data.parsers.parser_utils import FlexDataParticle, ParserException


# 2 days @ 1 record/sec
MAX_RECORDS_PER_GRANULE = 2*24*60*60

MAX_INMEMORY_SIZE = 50000000

CHUNK_START = '\x01'
END_OF_FILE = ""
CONTENT_STX = '\x02'
CONTENT_ETX = '\x03'

DEVICE_TYPES = {
    "CT": "ion.agents.data.parsers.sio.controller.parser_ctdmo.CTDMOChunkParser",
    "AD": "ion.agents.data.parsers.sio.controller.parser_adcps.ADCPSChunkParser",
    "PS": "ion.agents.data.parsers.sio.controller.parser.ProgramStatusChunkParser",
    "CS": "ion.agents.data.parsers.sio.controller.parser.ControllerStatusChunkParser",
    "FL": "ion.agents.data.parsers.sio.controller.parser_flort.FLORTChunkParser",
    "DO": "ion.agents.data.parsers.sio.controller.parser_dosta.DOSTAChunkParser",
    "PH": "ion.agents.data.parsers.sio.controller.parser_phsen.PHSENChunkParser",
}

class SIOControllerPackageParser(object):
    """
    Read binary data file
    """

    def __init__(self, url=None, open_file=None, parse_after=0, *args, **kwargs):
        """ raise exception if file does not meet spec, or is too large to read into memory """
        self._chunk_index = 0
        self._record_index = 0
        self._upload_time = time.time()

        self._chunks = []
        self._parse_after = parse_after
        with open_file or open(url, 'rb') as f:
            f.seek(0, 2)
            size = f.tell()
            if size > MAX_INMEMORY_SIZE:
                raise ParserException('file is too big')
            f.seek(0)
            chunk = self._read_chunk(f)
            while chunk:
                if chunk['end'] > self._parse_after:
                    self._chunks.append(chunk)
                chunk = self._read_chunk(f)
                #chunk = None
        log.debug('parsed %s, found %d usable chunks', url, len(self._chunks))

    def _read_chunk(self, f):
        #log.info("Start parsing chunk @%s", f.tell())
        chunk = {}
        skipped = ""
        b = f.read(1)
        while b != CHUNK_START and b != END_OF_FILE:
            skipped += b
            b = f.read(1)
        if b == END_OF_FILE:
            return None
        if skipped:
            log.warn("Skipped %s bytes: %s", len(skipped), skipped)
        dev_type = f.read(2)
        #log.info("Found dev_type: %s", dev_type)
        if dev_type not in DEVICE_TYPES:
            raise ParserException("Unknown device type: %s" % dev_type)
        chunk["dev_type"] = dev_type

        header = f.read(30)

        cid, inum, spc1, clen, pflag, ts, spc2, bnum, spc3, crc, stx = struct.unpack("5s2s1s4s1s8s1s2s1s4s1s", header)
        if not (spc1 == spc2 == spc3 == "_"):
            raise ParserException("Could not parse spacers")
        if not stx == CONTENT_STX:
            raise ParserException("Could not parse content STX")

        chunk.update(dict(
            controller_id=int(cid),
            inst_num=int(inum),
            content_length=int(clen, 16),
            processing_flag=pflag,
            timestamp=int(ts, 16),
            block_num=int(bnum, 16),
            content_crc=int(crc, 16)
        ))
        content = f.read(chunk["content_length"])
        if len(content) != chunk["content_length"]:
            raise ParserException("Content too short: %s instead of %s" % (len(content), chunk["content_length"]))
        if CONTENT_ETX in content:
            log.warn("Content contains ETX marker")
        # if f.read(1) != CONTENT_ETX:
        #     raise ParserException("Content ETX expected")
        b = f.read(1)
        extra = ""
        while b != CONTENT_ETX and b != END_OF_FILE:
            extra += b
            b = f.read(1)
        if b == END_OF_FILE:
            raise ParserException("Unexpected EOF")
        #if len(extra) > 0:
        #    log.warn("Found %s extra content bytes to ETX instread of %s", len(extra), chunk["content_length"])
        content += extra
        chunk["content_length"] = len(content)
        chunk["content"] = content

        #print content

        chunk["end"] = 1

        #import pprint
        #pprint.pprint(chunk)

        log.info("Chunk %(dev_type)s %(controller_id)s.%(inst_num)s f=%(processing_flag)s len=%(content_length)s ts=%(timestamp)s bn=%(block_num)s" % chunk)

        return chunk

    def get_records(self, max_count=MAX_RECORDS_PER_GRANULE):
        records = []
        for ch_num, chunk in enumerate(self._chunks):
            if ch_num < self._chunk_index:
                continue

            before_records = len(records)
            dev_type = chunk["dev_type"]
            dev_parser = DEVICE_TYPES[dev_type]
            try:
                clss = named_any(dev_parser)
                chunk_parser = clss(chunk)
                new_records = chunk_parser.get_records(max_count=max_count)
                start_idx = 0
                if ch_num == self._chunk_index and self._record_index > 0:
                    # Before we stopped in the middle of a chunk
                    new_records = new_records[self._record_index:]
                    start_idx = self._record_index

                self._chunk_index = ch_num
                if before_records + len(new_records) > max_count:
                    records.extend(new_records[:max_count-before_records])
                    self._record_index = start_idx + (max_count - before_records)
                    break
                else:
                    records.extend(new_records)
                    if len(records) == max_count:
                        self._chunk_index += 1
                        self._record_index = 0
                        break

            except Exception as ex:
                log.warn("Error: %s", ex)

        return records

class ProgramStatusChunkParser(object):
    """
    Read binary data chunk
    """
    def __init__(self, chunk):
        self.chunk = chunk
        self.records = self._parse_chunk()

    def _parse_chunk(self):
        records = []
        return records

    def get_records(self, max_count=MAX_RECORDS_PER_GRANULE):
        return self.records[:max_count]

class ControllerStatusChunkParser(object):
    """
    Read binary data chunk
    """
    def __init__(self, chunk):
        self.chunk = chunk
        self.records = self._parse_chunk()

    def _parse_chunk(self):
        records = []
        return records

    def get_records(self, max_count=MAX_RECORDS_PER_GRANULE):
        return self.records[:max_count]

