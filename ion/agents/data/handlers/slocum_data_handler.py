#!/usr/bin/env python

"""
@package ion.agents.data.handlers.slocum_data_handler
@file ion/agents/data/handlers/slocum_data_handler
@author Christopher Mueller
@brief
"""

from pyon.public import log
from pyon.util.containers import get_safe
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler
from ion.agents.data.handlers.handler_utils import list_file_info, get_sbuffer, calculate_iteration_count, get_time_from_filename
import numpy as np

DH_CONFIG_DETAILS = {
    'ds_desc_params': [
        ('base_url', str, 'base path/url for this dataset'),
        ('header_count', int, '# of header lines'),
        ('pattern', str, 'The filter pattern for this dataset.  If file-based, use shell-style notation; if remote (http, ftp), use regex'),
        ],
    }


class SlocumDataHandler(BaseDataHandler):
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

        ret['start_time'] = get_time_from_filename(new_list[0][0], date_extraction_pattern, date_pattern)
        ret['end_time'] = get_time_from_filename(new_list[len(new_list) - 1][0], date_extraction_pattern, date_pattern)

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

        for x in curr_list:
            curr_time = get_time_from_filename(x[0], date_extraction_pattern, date_pattern)
            if start_time <= curr_time <= end_time:
                new_list.append(x)

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
        hdr_cnt = get_safe(config, 'header_count', SlocumParser.DEFAULT_HEADER_SIZE)
        for f in new_flst:
            try:
                parser = SlocumParser(f[0], hdr_cnt)
                #CBM: Not in use yet...
                #            ext_dset_res = get_safe(config, 'external_dataset_res', None)
                #            t_vname = ext_dset_res.dataset_description.parameters['temporal_dimension']
                #            x_vname = ext_dset_res.dataset_description.parameters['zonal_dimension']
                #            y_vname = ext_dset_res.dataset_description.parameters['meridional_dimension']
                #            z_vname = ext_dset_res.dataset_description.parameters['vertical_dimension']
                #            var_lst = ext_dset_res.dataset_description.parameters['variables']

                max_rec = get_safe(config, 'max_records', 1)
                dprod_id = get_safe(config, 'data_producer_id', 'unknown data producer')

                stream_def = get_safe(config, 'stream_def')

                cnt = calculate_iteration_count(len(parser.data_map[parser.data_map.keys()[0]]), max_rec)
                for x in xrange(cnt):
                    #rdt = RecordDictionaryTool(taxonomy=ttool)
                    rdt = RecordDictionaryTool(stream_definition_id=stream_def)

                    for name in parser.sensor_map:
                        d = parser.data_map[name][x * max_rec:(x + 1) * max_rec]
                        rdt[name] = d

                    #g = build_granule(data_producer_id=dprod_id, taxonomy=ttool, record_dictionary=rdt)
                    g = rdt.to_granule()
                    yield g
            except SlocumParseException:
                # TODO: Decide what to do here, raise an exception or carry on
                log.error('Error parsing data file: \'{0}\''.format(f))


class SlocumParser(object):
    # John K's documentation says there are 16 header lines, but I believe there are actually 17
    # The 17th indicating the 'dtype' of the data for that column
    DEFAULT_HEADER_SIZE = 17
    header_map = {}
    sensor_map = {}
    data_map = {}

    def __init__(self, url=None, header_size=17):
        """
        Constructor for the parser. Initializes headers and data

        @param url the url/filepath of the file
        @param header_size number of header lines. This is information is in the header already, so it will be removed
        """
        if not url:
            raise SlocumParseException('Must provide a filename')

        self.header_size = int(header_size)

        sb = None
        try:
            # Get a byte-string generator for use in the data-retrieval loop (to avoid opening the file every time)
            sb = get_sbuffer(url)
            sb.seek(0)
            for x in xrange(self.header_size - 3):
                line = sb.readline()
                key, value = line.split(':', 1)
                self.header_map[key.strip()] = value.strip()

            # Collect the sensor names & units
            sensor_names = sb.readline().split()
            units = sb.readline().split()
            # Keep track of the intended data type for each sensor
            dtypes = []
            for d in sb.readline().split():
                if d is '1':
                    dtypes.append('byte')
                elif d is '2':
                    dtypes.append('short')
                elif d is '4':
                    dtypes.append('float')
                elif d is '8':
                    dtypes.append('double')

            assert len(sensor_names) == len(units) == len(dtypes)

            for i in xrange(len(sensor_names)):
                sb.seek(0)
                self.sensor_map[sensor_names[i]] = (units[i], dtypes[i])
                dat = np.genfromtxt(fname=sb, skip_header=self.header_size, usecols=i, dtype=dtypes[i], missing_values='NaN')  # ,usemask=True)
                self.data_map[sensor_names[i]] = dat

        finally:
            if not sb is None:
                sb.close()


class SlocumParseException(Exception):
    pass
