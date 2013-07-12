#!/usr/bin/env python

"""
WFP CTD profile data are in binary SBE52 format, ieee-le.
File Names are CX######.DAT, where X is the number of the WFP, 1 or 2 at PAPA2013 and ###### is the WFP profile number of its current mission.

SBE52 binary format 11 bytes: ccctttpppoo
1. Conductivity (mmho/cm) = (ccc / 10, 0.5000)
    If ccc < 0.5 decimal, ccc is set to 00000 (hex).
    If ccc > 95.0 decimal, ccc is set to FFFFF (hex).
2. Temperature (deg-C, ITS-90) = (ttt / 10, 5000)
    If ttt < -5 decimal, ttt is set to 00000 (hex).
    If ttt > 35.0 decimal, ttt is set to FFFFF (hex).
3. Pressure (decibars) = (ppp /  10100) \n
    If ppp < -10 decimal, ppp is set to 00000 (hex).
    If ppp > 7000 decimal, ppp is set to FFFFF (hex).\n
4. Optional Oxygen (Hz) = oo

A profile ends with a terminator record (11 bytes 0xFF) and followed by 2 POSIX timestamps, sensor-on time and sensor-off time, UINT32

A CTD file can have multiple profiles.

The data files will be transmitted decimated to the SIO controller, i.e. only every N'th record.
example in HEX-ASCII, see also attached C0000042.DAT (binary) and C0000042.HEX (converted to HEX-ASCII), format may change for PAPA2013:
05df5002370b007ce90000
05df62023719007ce20000
...
05151f0142330388550000
05151f0142310388560000
ffffffffffffffffffffff
4e926d594e928b6f
"""

import ntplib
import struct
import time

from ooi.logging import log
from ion.agents.data.parsers.parser_utils import FlexDataParticle


# 2 days @ 1 record/sec
MAX_RECORDS_PER_GRANULE = 2*24*60*60

MAX_INMEMORY_SIZE = 50000000
CTD_END_PROFILE_DATA = '\xff'*11


class SBE52BinaryCTDParser(object):
    """
    read binary data file,
    split into "profiles" (different periods of monitoring with start/end time),
    loop over profiles and return groups of records from each
    TODO: instead of reading whole file into memory, keep file open and seek/read/buffer/parse as needed
          (complicated a little b/c timestamps are at the end of each profile, and don't know # records)
    """
    _profile_index = 0
    _record_index = 0
    _upload_time = time.time()

    def __init__(self, url=None, open_file=None, parse_after=0, *a, **b):
        """ raise exception if file does not meet spec, or is too large to read into memory """
        self._profiles = []
        self._parse_after = parse_after
        with open_file or open(url, 'rb') as f:
            f.seek(0,2)
            size = f.tell()
            if size > MAX_INMEMORY_SIZE:
                raise Exception('file is too big')
            f.seek(0)
            profile = self._read_profile(f)
            while profile:
                if profile['end'] > self._parse_after:
                    self._profiles.append(profile)
                profile = self._read_profile(f)
        log.debug('parsed %s, found %d usable profiles', url, len(self._profiles))

    def _read_profile(self, f):
        line = f.read(11)
        # EOF here is expected -- no more profiles
        if not line:
            return None
        out = dict(records=[])
        while True:
            if line == CTD_END_PROFILE_DATA:
                break
            elif not line:
                # EOF here is bad -- incomplete profile
                raise Exception('bad file format -- EOF before reached end of profile')
            out['records'].append(line)
            line = f.read(11)
        # after 'ff'*11 marker, next 8 bytes are start/end times
        out['start'], out['end'] = struct.unpack('>II', f.read(8))
        log.trace('read profile [%d-%d] %d records', out['start'], out['end'], len(out['records']))
        return out

    def get_records(self, max_count=MAX_RECORDS_PER_GRANULE):
        """
        return a list of dicts, each dict describes one record in the current profile
        or None if the last profile has been read completely
        """
        if self._profile_index >= len(self._profiles):
            return None
        particle_list = []

        profile = self._profiles[self._profile_index]
        start = profile['start']
        end = profile['end']
        records = profile['records']

        last_index = min(max_count + self._record_index, len(records))
        while self._record_index < last_index:
            data = records[self._record_index]
            time = self._interpolate_time(self._record_index, start, end, len(records))
            ntp_timestamp = ntplib.system_to_ntp_time(time)
            if time > self._parse_after:
                record = {
                    'upload_time': self._upload_time,
                    'conductivity': self._get_conductivity(data),
                    'temp': self._get_temperature(data),
                    'pressure': self._get_pressure(data),
                    'oxygen': self._get_oxygen(data)
                }
                particle = FlexDataParticle(driver_timestamp=ntp_timestamp)
                particle.set_data_values(record)
                particle_list.append(particle.generate(encode=False))
            self._record_index += 1

        if self._record_index == len(records):
            self._record_index = 0
            self._profile_index += 1

        return particle_list

    def _interpolate_time(self, index, start, end, count):
        """ WARNING: we don't really understand how to map start/end time to individual intervals
            Assuming here that last interval ENDS at the end time.
        """
        delta = (end-start)/count
        return start + index*delta

    def _get_conductivity(self, data):
        return self._get_value(data[0:3], 10000, 0.5)

    def _get_temperature(self, data):
        return self._get_value(data[3:6], 10000, 5)

    def _get_pressure(self, data):
        return self._get_value(data[6:9], 100, 10)

    def _get_oxygen(self, data):
        return float(self._unpack_int(data[9:11]))

    def _get_value(self, data, divisor, offset):
        raw = self._unpack_int(data)
        if raw == 0 or raw == 0xFFFFF:
            return float('nan')
        return float(raw)/divisor - offset

    def _unpack_int(self, data):
        # can't use struct.unpack once for the whole field -- these fields are not typical 2, 4 or 8-byte widths
        out = 0
        for char in data[:-1]:
            out += struct.unpack('>B', char)[0]
            out *= 256
        out += struct.unpack('>B', data[-1])[0]
        return out
