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

import os

from ooi.logging import log

from pyon.util.containers import get_safe, named_any

from ion.agents.populate_rdt import populate_rdt
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler
from ion.agents.data.handlers.handler_utils import list_file_info, calculate_iteration_count, get_time_from_filename


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
                    particles = parser.get_records(max_count=max_rec)
                    if not particles:
                        break

                    rdt = RecordDictionaryTool(stream_definition_id=stream_def)

                    populate_rdt(rdt, particles)

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




