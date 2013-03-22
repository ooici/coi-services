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

        new_list = [x for x in curr_list if x not in old_list]

        #ret['start_time'] = get_time_from_filename(new_list[0][0], date_extraction_pattern, date_pattern)
        #ret['end_time'] = get_time_from_filename(new_list[len(new_list) - 1][0], date_extraction_pattern, date_pattern)

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
    def _get_data_old(cls, config):
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
                parser = classobj(f[0])

                max_rec = get_safe(config, 'max_records', 1)
                dprod_id = get_safe(config, 'data_producer_id', 'unknown data producer')

                stream_def = get_safe(config, 'stream_def')

                cnt = calculate_iteration_count(len(parser.data_map[parser.data_map.keys()[0]]), max_rec)
                for x in xrange(cnt):
                    #rdt = RecordDictionaryTool(taxonomy=ttool)
                    rdt = RecordDictionaryTool(stream_definition_id=stream_def)

                    for name in parser.sensor_names:
                        d = parser.data_map[name][x * max_rec:(x + 1) * max_rec]
                        rdt[name] = d

                    #g = build_granule(data_producer_id=dprod_id, taxonomy=ttool, record_dictionary=rdt)
                    g = rdt.to_granule()
                    yield g
            except HYPMException as ex:
                # TODO: Decide what to do here, raise an exception or carry on
                log.error('Error parsing data file \'{0}\': {1}'.format(f, ex))

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
                parser = classobj(f[0])

                max_rec = get_safe(config, 'max_records', 1)
                dprod_id = get_safe(config, 'data_producer_id', 'unknown data producer')

                stream_def = get_safe(config, 'stream_def')

                cnt = calculate_iteration_count(parser.get_record_count(), max_rec)

                for x in xrange(cnt):
                    rdt = RecordDictionaryTool(stream_definition_id=stream_def)

                    data_map, file_pos = parser.read_next_data()

                    for name in parser.sensor_names:
                        d = data_map[name][x * max_rec:(x + 1) * max_rec]
                        rdt[name] = d

                    g = rdt.to_granule()
                    yield g

                parser.close()

            except HYPMException as ex:
                # TODO: Decide what to do here, raise an exception or carry on
                log.error('Error parsing data file \'{0}\': {1}'.format(f, ex))

class HYPM_01_WFP_CTDParser(object):

    termination_string = 'ffffffffffffffffffffff'

    def __init__(self, url=None):
        """
        Constructor for the parser. Initializes headers and data

        @param url the url/filepath of the file
        """
        if not url:
            raise HYPMException('Must provide a filename')

        self.data_map = {}

        self.sensor_names = ['time', 'conductivity', 'temperature', 'pressure', 'oxygen']

        try:
            time_step_array = {}
            for name in self.sensor_names:
                self.data_map[name] = []
                time_step_array[name] = []

            with open(url, 'r') as f:
                for line in f:
                    #get rid of trailing spaces in the file
                    line = line.strip()
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
                                self.data_map[name].append(time_step_array[name])
                        #get next line which contains timestamp
                        line = f.next().strip()
                        self.data_map['time'].append(int(line[0:8], 16))
        except Exception as ex:
            log.error(ex)
            raise HYPMException('Error reading file: %s', url)

class HYPM_01_WFP_CTDParser2(object):

    termination_string = 'ffffffffffffffffffffff'

    def __init__(self, url=None):
        """
        Constructor for the parser. Initializes headers and data

        @param url the url/filepath of the file
        """
        if not url:
            raise HYPMException('Must provide a filename')

        self.sensor_names = ['time', 'conductivity', 'temperature', 'pressure', 'oxygen']

        self.f = open(url, 'r')

    def close(self):
        if not self.f.closed:
            self.f.close()

    def read_next_data(self):

        data_map = {}
        time_step_array = {}
        for name in self.sensor_names:
            data_map[name] = []
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
                            data_map[name].append(time_step_array[name])
                        #get next line which contains timestamp
                    line = self.f.next().strip()
                    data_map['time'].append(int(line[0:8], 16))
                    break
        except Exception as ex:
            log.error(ex)
            raise HYPMException('Error getting next data')

        return data_map, self.f.tell()

    def get_record_count(self):
        self.f.seek(0)
        lines = self.f.readlines()
        self.f.seek(0)
        return lines.count(self.termination_string + '\n')

class HYPM_01_WFP_ACMParser(object):

    termination_string = 'ffffffffffffffffffffffffffffffffffff'

    def __init__(self, url=None, header_size=17):
        """
        Constructor for the parser. Initializes headers and data

        @param url the url/filepath of the file
        """
        if not url:
            raise HYPMException('Must provide a filename')

        self.data_map = {}

        self.sensor_names = ['VelA', 'VelB', 'VelC', 'VelD', 'Mx', 'My', 'Mz', 'Pitch', 'Roll']
        for name in self.sensor_names:
            self.data_map[name] = []

        initialized = False
        finalized = False
        try:
            with open(url, 'r') as f:
                for line in f:
                    #get rid of trailing spaces in the file
                    line = line.strip()
                    if not initialized:
                        bytes_per_record = int(line[0:4], 16)
                        number_of_records = int(line[4:8], 16)
                        initialized = True
                    elif finalized:
                        #last record is sensor on time and sensor off time
                        pass
                    else:
                        #check to see if the termination string has been hit
                        if line != self.termination_string:
                            for i in xrange(bytes_per_record / 2):
                                self.data_map[self.sensor_names[i]].append(int(line[i * 4:((i + 1) * 4)],16))
                        else:
                            finalized = True
        except Exception as ex:
            raise HYPMException('Error reading file: %s', ex)

class HYPM_01_WFP_ENGParser(object):

    termination_string = 'ffffffff'

    def __init__(self, url=None, header_size=17):
        """
        Constructor for the parser. Initializes headers and data

        @param url the url/filepath of the file
        """
        if not url:
            raise HYPMException('Must provide a filename')

        self.data_map = {}

        self.sensor_names = []

        components = ['Time', 'Core', 'Fluorometer', 'Turbidity', 'Optode', 'Par', 'Puck', 'BioSuite', 'FLBB']

        component_details = {
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

        components_on = [False for x in range(0, 9)]
        components_on[0] = True

        first_line = False
        second_line = False
        #try:
        with open(url, 'r') as f:
            for line in f:
                #get rid of trailing spaces in the file
                line = line.strip()
                if not first_line:
                    #8 Component Flags  (UINT16)
                    for i in range(1, 9):
                        components_on[i] = (int(line[(i - 1) * 4:(i * 4)], 16) != 0)
                    first_line = True
                elif not second_line:
                    #2 POSIX timestamps (UINT32), sensor-on time, profile start time
                    second_line = True
                #check to see if the termination string has been hit
                elif not (line.startswith(self.termination_string)):
                    for i in range(0,9):
                        pos = 0
                        if components_on[i]:
                            for component in component_details[components[i]]:
                                name = components[i] + '_' + component[0]
                                if not name in self.data_map.keys():
                                    self.sensor_names.append(name)
                                    self.data_map[name] = []
                                if component[1] == 'int':
                                    self.data_map[name].append(int(line[pos:pos+component[2]], 16))
                                elif component[1] == 'float':
                                    self.data_map[name].append(struct.unpack('!f', line[pos:pos+component[2]].decode('hex'))[0])
                                pos += component[2]
                else:
                    #exit from loop
                    break
        # except Exception as ex:
        #     log.error(ex)
        #     raise HYPMException('Error reading file: %s', url)

class HYPMException(Exception):
    pass