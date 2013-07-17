#!/usr/bin/env python

"""
@package ion.agents.data.handlers.hypm_01_wpf_data_handler
@file ion/agents/data/handlers/slocum_data_handler
@author Christopher Mueller
@brief
"""

from pyon.public import log
from pyon.util.containers import get_safe
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler
from ion.agents.data.handlers.handler_utils import list_file_info, calculate_iteration_count, get_time_from_filename
import numpy as np
import struct

DH_CONFIG_DETAILS = {
    'ds_desc_params': [
        ('base_url', str, 'base path/url for this dataset'),
        ('pattern', str, 'The filter pattern for this dataset.  If file-based, use shell-style notation; if remote (http, ftp), use regex'),
        ('parser_mod', str, 'The module where the parser is contained'),
        ('parser_cls', str, 'The parser class')
        ],
    }


class HYPMDataHandler(BaseDataHandler):
    @classmethod
    def _init_acquisition_cycle(cls, config):
        # TODO: Can't build a parser here because we won't have a file name!!  Just a directory :)
        # May not be much to do in this method...
        # maybe just ensure access to the dataset_dir and move the 'buried' params up to the config dict?
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        if not ext_dset_res:
            raise SystemError('external_dataset_res not present in configuration, cannot continue')

        config['ds_params'] = ext_dset_res.dataset_description.parameters

    #        base_url = ext_dset_res.dataset_description.parameters['base_url']
    #        hdr_cnt = get_safe(ext_dset_res.dataset_description.parameters, 'header_count', 17)
    #        pattern = get_safe(ext_dset_res.dataset_description.parameters, 'pattern')
    #        config['header_count'] = hdr_cnt
    #        config['base_url'] = base_url
    #        config['pattern'] = pattern

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
                dprod_id = get_safe(config, 'data_producer_id', 'unknown data producer')

                stream_def = get_safe(config, 'stream_def')

                cnt = calculate_iteration_count(parser.record_count, max_rec)
                file_pos = -1
                for x in xrange(cnt):
                    rdt = RecordDictionaryTool(stream_definition_id=stream_def)
                    all_data = {}
                    all_data.clear()
                    for name in parser.sensor_names:
                        all_data[name] = []

                    for y in xrange(max_rec):
                        data_map, file_pos = parser.read_next_data()
                        if len(data_map.items()):
                            for name in parser.sensor_names:
                                all_data[name].append(data_map[name]) #[x * max_rec:(x + 1) * max_rec]

                    for name in parser.sensor_names:
                        try:
                            rdt[name] = all_data[name]
                        except Exception:
                            log.error('failed to set rdt[%s], all_data=%r', name, all_data)
                            raise

                    g = rdt.to_granule()

                    #update new data check with the latest file position
                    if 'set_new_data_check' in config and index > -1:
                        config['set_new_data_check'][index] = (f[0], f[1], f[2], file_pos)

                    yield g

                parser.close()

            except HYPMException as ex:
                # TODO: Decide what to do here, raise an exception or carry on
                log.error('Error parsing data file \'{0}\': {1}'.format(f, ex))

class HYPM_01_WFP_CTDParser(object):

    termination_string = 'ffffffffffffffffffffff'

    def __init__(self, url=None, start_position=0):
        """
        Constructor for the parser. Initializes headers and data

        @param url the url/filepath of the file
        """
        if not url:
            raise HYPMException('Must provide a filename')

        self.sensor_names = ['time', 'conductivity', 'temperature', 'pressure', 'oxygen']

        self.f = open(url, 'r')
        self.record_count = self.get_record_count()
        self.f.seek(start_position)

    def close(self):
        if not self.f.closed:
            self.f.close()

    def read_next_data(self):

        data_map = {}
        time_step_array = {}
        for name in self.sensor_names:
            time_step_array[name] = []

        try:

            while True:
                #get rid of trailing spaces in the file
                line = self.f.readline().strip()
                if not line:
                    break
                #check to see if the termination string has been hit
                if line != self.termination_string:
                    time_step_array[self.sensor_names[1]].append((float(int(line[0:6], 16)) / 10000) * 0.5)
                    time_step_array[self.sensor_names[2]].append((float(int(line[6:12], 16)) / 10000))
                    time_step_array[self.sensor_names[3]].append((float(int(line[12:18], 16)) / 100) * 10)
                    #check to see if oxygen is included in data
                    if len(line) > 18:
                        time_step_array[self.sensor_names[4]].append(int(line[18:], 16))
                #termination string
                else:
                    #add array of data at this time step to the array type values in the param dict
                    for name in self.sensor_names:
                        if not name is 'time':
                            data_map[name] = time_step_array[name]
                        #get next line which contains timestamp
                    line = self.f.next().strip()
                    data_map['time'] = int(line[0:8], 16)
                    break
        except Exception as ex:
            log.error(ex)
            raise HYPMException('Error getting next data')

        return data_map, self.f.tell()

    def get_record_count(self):
        old_pos = self.f.tell()
        self.f.seek(0)
        lines = self.f.readlines()
        self.f.seek(old_pos)
        return lines.count(self.termination_string + '\n')


class HYPM_01_WFP_ACMParser(object):

    termination_string = 'ffffffffffffffffffffffffffffffffffff'

    def __init__(self, url=None, start_position=0):
        """
        Constructor for the parser. Initializes headers and data

        @param url the url/filepath of the file
        """
        if not url:
            raise HYPMException('Must provide a filename')

        self.sensor_names = ['time', 'upload_time', 'VelA', 'VelB', 'VelC', 'VelD', 'Mx', 'My', 'Mz', 'Pitch', 'Roll']

        self.f = open(url, 'r')
        self.record_count = self.get_record_count()
        self.f.seek(start_position)

    def read_next_data(self):
        data_map = {}
        time_step_array = {}
        for name in self.sensor_names:
            time_step_array[name] = []

        try:
            while True:
                #get rid of trailing spaces in the file
                line = self.f.readline().strip()
                if not line:
                    break

                if not self.initialized:
                    self.bytes_per_record = int(line[0:4], 16)
                    self.number_of_records = int(line[4:8], 16)
                else:
                    #check to see if the termination string has been hit
                    if line != self.termination_string:
                        for i in xrange(self.bytes_per_record / 2):
                            time_step_array[self.sensor_names[i + 1]].append(int(line[i * 4:((i + 1) * 4)],16))
                    else:
                        for name in self.sensor_names:
                            if not name is 'time':
                                data_map[name] = time_step_array[name]
                                #get next line which contains timestamp
                        line = self.f.next().strip()
                        data_map['time'] = int(line[0:8], 16)
        except Exception as ex:
            raise HYPMException('Error reading file: %s', ex)

        return data_map, self.f.tell()

    def close(self):
        if not self.f.closed:
            self.f.close()

    def get_record_count(self):
        self.f.seek(0)
        line = self.f.readline()
        self.bytes_per_record = int(line[0:4], 16)
        self.number_of_records = int(line[4:8], 16)
        self.initialized = True
        return self.number_of_records

class HYPM_01_WFP_ENGParser(object):

    termination_string = 'ffffffff'

    def __init__(self, url=None, start_position=0):
        """
        Constructor for the parser. Initializes headers and data

        @param url the url/filepath of the file
        """
        if not url:
            raise HYPMException('Must provide a filename')

        self.sensor_names = []

        self.components = ['Time', 'Core', 'Fluorometer', 'Turbidity', 'Optode', 'Par', 'Puck', 'BioSuite', 'FLBB']

        self.component_details = {
            'Time': [('Time', 'int', 8)],
            'Core': [('Current', 'float', 8), ('Voltage', 'float', 8), ('Pressure', 'float', 8)],
            'Fluorometer': [('Value', 'float', 8), ('Gain', 'int', 8)],
            'Turbidity': [('Value', 'float', 8), ('Gain', 'int', 8)],
            'Optode': [('Oxygen', 'float', 8), ('Temp', 'float', 8)],
            'Par': [('Value', 'float', 8)],
            'Puck': [('Scatter', 'int', 4), ('Chla', 'int', 4), ('CDOM', 'int', 4)],
            'BioSuite': [('Scatter', 'int', 4), ('Chla', 'int', 4), ('CDOM' 'int', 4), ('Temp', 'int', 4), ('Par', 'int', 4)],
            'FLBB': [('Chla', 'int', 4), ('Turb', 'int', 4), ('Temp', 'int', 4)]
        }

        self.components_on = [False for x in range(0, 9)]
        self.components_on[0] = True

        self.first_line = False
        self.second_line = False

        self.f = open(url, 'r')
        self.record_count = self.get_record_count()
        self.init_components()
        #there are 34 bytes at the end of the file to indicate termination and
        #sensor start and end times. If the start position is > 34, it stands to
        #reason that ingest needed to be restarted, so account for the extra bytes
        #when restarting.
        if start_position > 34:
            self.f.seek(start_position - 34)
        else:
            self.f.seek(start_position)
        self.first_line = (start_position != 0)
        self.second_line = (start_position != 0)

    def init_components(self):
        line = self.f.readline().strip()
        for i in range(1, 9):
            self.components_on[i] = (int(line[(i - 1) * 4:(i * 4)], 16) != 0)

        for i in range(0,9):
            if self.components_on[i]:
                for component in self.component_details[self.components[i]]:
                    name = self.components[i] + '_' + component[0]
                    self.sensor_names.append(name)

    def read_next_data(self):
        data_map = {}

        #get rid of trailing spaces in the file
        line = self.f.readline().strip()
        if not line:
            return data_map, self.f.tell()

        if not self.first_line:
            self.first_line = True
            line = self.f.readline().strip()

        if not self.second_line:
            #2 POSIX timestamps (UINT32), sensor-on time, profile start time
            self.second_line = True
            line = self.f.readline().strip()

        #check to see if the termination string has been hit
        if not (line.startswith(self.termination_string)):
            for i in range(0,9):
                pos = 0
                if self.components_on[i]:
                    for component in self.component_details[self.components[i]]:
                        name = self.components[i] + '_' + component[0]
                        if component[1] == 'int':
                            data_map[name] = int(line[pos:pos+component[2]], 16)
                        elif component[1] == 'float':
                            data_map[name] = struct.unpack('!f', line[pos:pos+component[2]].decode('hex'))[0]
                        pos += component[2]
        else:
            #read the final line of the profile
            line = self.f.readline()

        #Add 34 bytes to the end to account for the last two lines
        #they don't get read ordinarily.
        return data_map, self.f.tell() + 34

    def close(self):
        if not self.f.closed:
            self.f.close()

    def get_record_count(self):
        self.f.seek(0)
        lines = self.f.readlines()
        self.f.seek(0)
        #This needs to be improved upon. A more complete example will be helpful.
        return len(lines) - 4

class HYPMException(Exception):
    pass
