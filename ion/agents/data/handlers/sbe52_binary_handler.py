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

from pyon.util.containers import get_safe
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler
from ion.agents.data.handlers.handler_utils import list_file_info, calculate_iteration_count, get_time_from_filename

import struct
import time
from ooi.logging import log
import os

# 2 days @ 1 record/sec
MAX_RECORDS_PER_GRANULE=2*24*60*60

MAX_INMEMORY_SIZE=50000000
CTD_END_PROFILE_DATA='\xff'*11




DH_CONFIG_DETAILS = {
    'ds_desc_params': [
        ('base_url', str, 'base path/url for this dataset'),
        ('pattern', str, 'The filter pattern for this dataset.  If file-based, use shell-style notation; if remote (http, ftp), use regex'),
        ('parser_mod', str, 'The module where the parser is contained'),
        ('parser_cls', str, 'The parser class')
    ],
    }


class SBE52BinaryDataHandler(BaseDataHandler):
    """
    This class was copied from TimG's original implementation in hypm_data_handler but modified to handle:
    - handle multiple profiles within a single datafile
    - correct timestamps within parsers
    - parse binary files instead of hax-ascii

    The original implementation is obsolete but I don't want to lose the current tests while developing the replacement.
    """

    @classmethod
    def _init_acquisition_cycle(cls, config):
        # TODO: Can't build a parser here because we won't have a file name!!  Just a directory :)
        # May not be much to do in this method...
        # maybe just ensure access to the dataset_dir and move the 'buried' params up to the config dict?
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        if not ext_dset_res:
            raise SystemError('external_dataset_res not present in configuration, cannot continue')

        config['ds_params'] = ext_dset_res.dataset_description.parameters

    @classmethod
    def _constraints_for_new_request(cls, config):
        old_list = get_safe(config, 'new_data_check') or []
        # CBM: Fix this when the DotList crap is sorted out
        old_list = list(old_list)  # NOTE that the internal tuples are also DotList objects

        ret = {}
        base_url = get_safe(config, 'ds_params.base_url')
        list_pattern = get_safe(config, 'ds_params.list_pattern')
        date_pattern = get_safe(config, 'ds_params.date_pattern')
        date_extraction_pattern = get_safe(config, 'ds_params.date_extraction_pattern')

        curr_list = list_file_info(base_url, list_pattern)

        #compare the last read files (old_list) with the current directory contents (curr_list)
        #if the file names are the same (curr_file[0] and old_file[0]) check the size of the
        #current file (curr_file[2]) with the file position when the last file was read (old_file[3])
        #if there's more data now that was read last time, add the file to the list
        new_list = []
        for curr_file in curr_list:
            found = False
            for old_file in old_list:
                if curr_file[0] == old_file[0]:      #if filenames are the same, that means the file is still in the directory, and was previously read
                    found = True
                    if curr_file[2] > old_file[3]:   #f2[2] is the current file size, f2[3] is the last read file size
                        new_list.append((curr_file[0], curr_file[1], curr_file[2], old_file[-1]))     #add it in if the current file size is bigger than the last time
            if not found:
                new_list.append(curr_file)

        config['set_new_data_check'] = curr_list

        ret['new_files'] = new_list
        ret['bounding_box'] = {}
        ret['vars'] = []

        return ret

    @classmethod
    def _constraints_for_historical_request(cls, config):
        base_url = get_safe(config, 'ds_params.base_url')
        list_pattern = get_safe(config, 'ds_params.list_pattern')
        date_pattern = get_safe(config, 'ds_params.date_pattern')
        date_extraction_pattern = get_safe(config, 'ds_params.date_extraction_pattern')

        start_time = get_safe(config, 'constraints.start_time')
        end_time = get_safe(config, 'constraints.end_time')

        new_list = []
        curr_list = list_file_info(base_url, list_pattern)

        new_list = curr_list

        #        config['constraints']['new_files'] = new_list
        return {'new_files': new_list}

    @classmethod
    def _get_data(cls, config):
        """
        Iterable function that acquires data from a source iteratively based on constraints provided by config
        Passed into BaseDataHandler._publish_data and iterated to publish samples.
        @param config dict containing configuration parameters, may include constraints, formatters, etc
        @retval an iterable that returns well-formed Granule objects on each iteration
        """
        new_flst = get_safe(config, 'constraints.new_files', [])
        parser_mod = get_safe(config, 'parser_mod', '')
        parser_cls = get_safe(config, 'parser_cls', '')
        module = __import__(parser_mod, fromlist=[parser_cls])
        classobj = getattr(module, parser_cls)

        for f in new_flst:
            try:
                size = os.stat(f[0]).st_size
                try:
                    #find the new data check index in config
                    index = -1
                    for ndc in config['set_new_data_check']:
                        if ndc[0] == f[0]:
                            index = config['set_new_data_check'].index(ndc)
                            break
                except:
                    log.error('File name not found in attachment')

                parser = classobj(f[0], f[3])

                max_rec = get_safe(config, 'max_records', 1)
                stream_def = get_safe(config, 'stream_def')
                while True:
                    records = parser.get_records(max_count=max_rec)
                    if not records:
                        break

                    rdt = RecordDictionaryTool(stream_definition_id=stream_def)
                    for key in records[0]:
                        rdt[key] = [ record[key] for record in records ]

                    g = rdt.to_granule()

# TODO: record files already read for future additions...
#                    #update new data check with the latest file position
                    if 'set_new_data_check' in config and index > -1:
                        # WRONG: should only record this after file finished parsing,
                        # but may not have another yield at that point to trigger update
                        config['set_new_data_check'][index] = (f[0], f[1], f[2], size)

                    yield g

#                parser.close()

            except Exception as ex:
                # TODO: Decide what to do here, raise an exception or carry on
                log.error('Error parsing data file \'{0}\': {1}'.format(f, ex))




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
            if size>MAX_INMEMORY_SIZE:
                raise Exception('file is too big')
            f.seek(0)
            profile = self._read_profile(f)
            while profile:
                if profile['end']>self._parse_after:
                    self._profiles.append(profile)
                profile = self._read_profile(f)
        log.debug('parsed %s, found %d usable profiles', url, len(self._profiles))

    def _read_profile(self, f):
        line = f.read(11)
        # EOF here is expected -- no more profiles
        if not line:
            return None
        out = { 'records': [] }
        while True:
            if line==CTD_END_PROFILE_DATA:
                break
            elif not line:
                # EOF here is bad -- incomplete profile
                raise Exception('bad file format -- EOF before reached end of profile')
            out['records'].append(line)
            line = f.read(11)
        # after 'ff'*11 marker, next 8 bytes are start/end times
        out['start'],out['end'] = struct.unpack('<ii',f.read(8))
        log.trace('read profile [%d-%d] %d records',out['start'],out['end'],len(out['records']))
        return out

    def get_records(self, max_count=MAX_RECORDS_PER_GRANULE):
        """
        return a list of dicts, each dict describes one record in the current profile
        or None if the last profile has been read completely
        """
        if self._profile_index>=len(self._profiles):
            return None
        profile = self._profiles[self._profile_index]
        start = profile['start']
        end = profile['end']
        records = profile['records']

        out = []
        last_index = min(max_count+self._record_index,len(records))
        while self._record_index<last_index:
            data = records[self._record_index]
            time = self._interpolate_time(self._record_index,start,end,len(records))
            if time>self._parse_after:
                record = {
                    'upload_time': self._upload_time,
                    'time': time,
                    'conductivity': self._get_conductivity(data),
                    'temp': self._get_temperature(data),
                    'pressure': self._get_pressure(data),
                    'oxygen': self._get_oxygen(data)
                }
                out.append(record)
            self._record_index+=1

        if self._record_index==len(records):
            self._record_index=0
            self._profile_index+=1
        return out

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
        if raw==0 or raw==0xFFFFF:
            return float('nan')
        return float(raw)/divisor - offset

    def _unpack_int(self, data):
        # can't use struct.unpack once for the whole field -- these fields are not typical 2, 4 or 8-byte widths
        out = 0
        for char in data[:-1]:
            out+=struct.unpack('<B',char)[0]
            out*=256
        out+=struct.unpack('<B',data[-1])[0]
        return out











################################# OLD STUFF BELOW....

class ExternalDatasetParser(object):
    """ define interface for any ExternalDataset Parser """
    def __init__(self, url=None):
        if not url:
            raise HYPMException('Must provide a filename')
        self.f = open(url, 'r')
    def read_next_data(self):
        raise Exception("must be implemented by subclass")
    def close(self):
        if not self.f.closed:
            self.f.close()

class SBE52_Hex_Parser(ExternalDatasetParser):
    """ implement common utility for parsing hex fields in SBE52 files """
    def __init__(self, url, field_positions, eof_marker):
        '''
        @param url:
        @param start_position:
        @param field_positions: dict of param name to list of 2 int values, char position of start and end of field with that name
        @param eof_marker:
        @return:
        '''
        super(SBE52_Hex_Parser,self).__init__(url)
        self.field_positions = field_positions
        self.eof_marker = eof_marker

    def _parse_field(self, name, line):
        return int(line[self.field_positions[name][0]:self.field_positions[name][1]], 16)
    def _parse_line(self, line):
        out = dict()
        for field in self.field_positions:
            pos = self.field_positions[field]
            out[field] = int( line[pos[0]:pos[1]], 16 )
        return out
    def _parse_times(self, line):
        return ( int(line[0:4], 16), int(line[4:8], 16) )
    def _parse_file(self):
        values = []
        line = self.f.readline()
        while line!=self.eof_marker:
            values.append(self._parse_line(line))
        line = self.f.readline()
        start_time, end_time = self._parse_times(line)
        delta_time = end_time - start_time
        for i in xrange(len(values)):
            time = start_time + i*delta_time
            values[i]['time'] = time
        return values

    def read_next_data(self):
        values = self._parse_file()
        # change from list of dicts to dict of lists
        return { key: [ value[key] for value in values ] for key in self.field_positions }

DEFAULT_ACM_FIELDS = {  'VelA': (0, 2),
                        'VelB': (2,4),
                        'VelC': (4,6),
                        'VelD': (6,8),
                        'Mx': (8,10),
                        'My': (10,12),
                        'Mz': (12,14),
                        'Pitch': (14,16),
                        'Roll': (16,18)   }

DEFAULT_ACM_EOF_MARKER = 'ffffffffffffffffffffffffffffffffffff'
class SBE52_ACM_Parser(SBE52_Hex_Parser):
    def __init__(self, url, start_position=0):
        super(SBE52_ACM_Parser,self).__init__(url, DEFAULT_ACM_FIELDS, DEFAULT_ACM_EOF_MARKER)
        line = self.f.readline()
        bytes_per_record = int(line[0:4], 16)
        if bytes_per_record!=18:
            raise HYPMException('data product expects 9 uint16 values, file expected have 18 bytes per record')
        self.number_of_records = int(line[4:8], 16)
        if start_position:
            self.f.seek(start_position)
