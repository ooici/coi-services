#!/usr/bin/env python

"""
@package ion.util.direct_coverage_utils
@file ion/util/direct_coverage_utils
@author Christopher Mueller
@brief Utilities for operating with coverages via a direct connection
"""
from interface.objects import ProcessDefinition, ProcessStateEnum, DataProduct
from ion.services.cei.process_dispatcher_service import ProcessStateGate

from pyon.public import log

import os
import yaml
import requests
import numpy as np
from StringIO import StringIO


from pyon.public import RT, PRED
from coverage_model import utils
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient


class DirectCoverageAccess(object):
    def __init__(self):
        self.ingestion_management = IngestionManagementServiceClient()
        self.resource_registry = ResourceRegistryServiceClient()
        self._paused_streams = []
        self._w_covs = {}
        self._ro_covs = {}

        self._context_managed = False

    def __enter__(self):
        self._context_managed = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Close any open read-only coverages
        for dsid, c in self._ro_covs.iteritems():
            c.close()

        # Close any open write coverages
        for sid, c in self._w_covs.iteritems():
            c.close()

        # Resume any paused ingestion workers
        for s in self._paused_streams:
            self.resume_ingestion(s)

    def get_ingestion_config(self):
        '''
        Grab the ingestion configuration from the resource registry
        '''
        # The ingestion configuration should have been created by the bootstrap service
        # which is configured through r2deploy.yml

        ingest_configs, _ = self.resource_registry.find_resources(restype=RT.IngestionConfiguration,id_only=True)
        return ingest_configs[0]

    def get_coverage_path(self, dataset_id):
        pth = DatasetManagementService._get_coverage_path(dataset_id)
        if not os.path.exists(pth):
            raise ValueError('Coverage with id \'{0}\' does not exist!'.format(dataset_id))

    def pause_ingestion(self, stream_id):
        if not self._context_managed:
            print "Warning: Pausing ingestion when not using a context manager is potentially unsafe - " \
                  "be sure to resume ingestion for all stream ids in self._paused_streams"

        if stream_id not in self._paused_streams:
            self.ingestion_management.pause_data_stream(stream_id, self.get_ingestion_config())
            self._paused_streams.append(stream_id)

    def resume_ingestion(self, stream_id):
        if stream_id in self._paused_streams:
            self.ingestion_management.resume_data_stream(stream_id, self.get_ingestion_config())
            self._paused_streams.remove(stream_id)

    def get_stream_id(self, dataset_id):
        sid, _ = self.resource_registry.find_objects(dataset_id, predicate=PRED.hasStream, id_only=True)
        return sid[0] if len(sid) > 0 else None

    def get_read_only_coverage(self, dataset_id):
        if not self._context_managed:
            print "Warning: Coverages will remain open until they are closed or go out of scope - " \
                  "be sure to close coverage instances when you're finished working with them"

        if dataset_id in self._ro_covs:
            return self._ro_covs[dataset_id]
        else:
            self._ro_covs[dataset_id] = DatasetManagementService._get_coverage(dataset_id, mode='r')

        return self._ro_covs[dataset_id]

    def get_editable_coverage(self, dataset_id):
        sid = self.get_stream_id(dataset_id)
        if sid in self._paused_streams:
            return self._w_covs[sid]

        self.pause_ingestion(sid)
        self._w_covs[sid] = DatasetManagementService._get_simplex_coverage(dataset_id, mode='w')
        return self._w_covs[sid]

    @classmethod
    def get_parser(cls, data_file_path, config_path=None):
        return SimpleCSVParser.get_parser(data_file_path, config_path=config_path)

    def manual_upload(self, dataset_id, data_file_path, config_path=None):
        # First, ensure we can get a parser and parse the data file
        parser = self.get_parser(data_file_path, config_path)
        dat = parser.parse()

        # Get the coverage
        with self.get_editable_coverage(dataset_id) as cov:
            # Find the indices for the times in the data file
            try:
                time_dat = dat[cov.temporal_parameter_name]
            except ValueError, ve:
                if ve.message == 'field named %s not found.' % cov.temporal_parameter_name:
                    raise ValueError('Temporal parameter name {0} not in upload data'.format(cov.temporal_parameter_name))
                else:
                    raise
            cov_times = cov.get_time_values()
            tinds = [utils.find_nearest_index(cov_times, ti) for ti in time_dat]

            sl = (tinds,)
            cparams = cov.list_parameters()
            for n in dat.dtype.names:
                if n != cov.temporal_parameter_name:
                    if n in cparams:
                        cov.set_parameter_values(n, dat[n], sl)
                    else:
                        log.warn('Skipping column \'%s\': matching parameter not found in coverage!', n)

    def upload_calibration_coefficients(self, dataset_id, data_file_path, config_path=None):
        # First, ensure we can get a parser and parse the data file
        parser = self.get_parser(data_file_path, config_path)
        dat = parser.parse()

        # Get the coverage
        with self.get_editable_coverage(dataset_id) as cov:
            cparams = cov.list_parameters()
            for n in dat.dtype.names:
                if n != cov.temporal_parameter_name:
                    if n in cparams:
                        cov.set_parameter_values(n, dat[n])
                    else:
                        log.warn('Skipping column \'%s\': matching parameter not found in coverage!', n)


class SimpleCSVParser(object):

    def __init__(self, data_url, num_columns=None, column_map=None, header_size=0, delimiter=',',
                 use_column_names=True, dtype='float32', fill_val=-999):
        if column_map is not None and not isinstance(column_map, dict):
            raise ValueError('If specified, \'column_map\' must be type<dict>')

        self._dat_buffer = self._get_sbuffer(data_url)
        self.data_url = data_url
        self.num_columns = num_columns
        self.column_map = column_map
        self.header_size = header_size
        self.use_column_names = use_column_names
        self.delimiter = delimiter
        self.dtype = dtype
        self.fill_val = fill_val

    @property
    def properties(self):
        props = ['data_url', 'header_size', 'delimiter', 'num_columns', 'use_column_names', 'dtype', 'fill_val', 'column_map']
        out = []
        for p in props:
            out.append('{0}: {1}'.format(p, getattr(self, p)))

        return '\n'.join(out)

    def parse(self):
        """
        Parse the file located at self.data_url

        @return: A structured numpy array; field names can be listed with ret.dtype.names
        """

        # If not specified, sort out how many columns
        if self.num_columns is None:
            # Ensure we start at the beginning
            self._dat_buffer.seek(0)
            # Skip all the header lines
            for l in xrange(self.header_size):
                self._dat_buffer.readline()
            # Get the length of the next line split by the delimiter
            self.num_columns = len(self._dat_buffer.readline().split(self.delimiter))

        fill_vals = [self.fill_val] * self.num_columns
        dtypes = [self.dtype] * self.num_columns
        if self.use_column_names:
            names = True
        else:
            names = ['f{0}'.format(i) for i in xrange(self.num_columns)]

        # Iterate over the members of column_map and substitute properties as appropriate
        if self.column_map is not None:
            for i, d in self.column_map.iteritems():
                i = int(i)
                if not self.use_column_names:
                    if 'name' in d:
                        names[i] = d['name']

                if 'dtype' in d:
                    dtypes[i] = d['dtype']
                if 'fill_val' in d:
                    fill_vals[i] = d['fill_val']

        dtypes = ','.join(dtypes)

        self._dat_buffer.seek(0)
        dat = np.genfromtxt(fname=self._dat_buffer,
                            skip_header=self.header_size,
                            delimiter=self.delimiter,
                            names=names,
                            dtype=dtypes,
                            filling_values=fill_vals)

        return dat

    def _get_sbuffer(self, url):
        # If type isn't specified, attempt to determine based on url

        # Switch on type
        if url.startswith('http://'):
            response = requests.get(url)
            buf = StringIO(response.content)
        elif os.path.exists(url):
            buf = StringIO(open(url).read())
        else:
            raise ValueError('Data file \'{0}\' not found'.format(url))

        return buf

    @classmethod
    def get_parser(self, data_file_path, config_path=None):
        config = None
        if config_path is not None:
            config = yaml.load(open(config_path))

        if config is None:  # Empty configuration, do not pass to parser
            parser = self(data_file_path)
        else:
            parser = self(data_file_path, **config)

        return parser

    @classmethod
    def write_default_parser_config(cls, out_path):
        config_contents = '''#######################################################################################################
# File properties
# 'header_size': the number of lines to skip before beginning to parse data
# 'delimiter': the character used to delimit data columns
# 'num_columns': the number of data columns; if None (default), calculated from first line after header
#######################################################################################################
#header_size: 0 # Defaults to 0 if not specified
#delimiter: ',' # Defaults to ',' if not specified
#num_columns: None # Defaults to None

########################################################################################
# Global column properties - supply defaults for the data columns
# 'use_column_names': Attempts to use the first row after the header as the column names
#       If false and the 'name' property for each column is not specified below,
#       sequential names of 'f<col#>' are used
# 'dtype': The data type for the column - supported types are those supported by numpy
# 'fill_val': The fill value to use for missing data - must match the data type
########################################################################################
#use_column_names: true # Defaults to true if not specified
#dtype: float32 # Defaults to float32 if not specified
#fill_val: -999 # Defaults to -999 if not specified

#####################################################################################
# Column-by-column properties - override the 'name', 'dtype' and 'fill_val' defaults
# A dictionary where the key is the column index and the value is a dictionary
# Supported keys for the inner dictionaries are: 'name', 'dtype' and 'fill_val'
# Rules for 'dtype' and 'fill_val' are the same as above
# 'name' is ignored if 'use_column_names' == true
#####################################################################################
#column_map: # Defaults to None, meaning no per-column overriding is applied
  #0: {name: a, dtype: float32, fill_val: -999}'''

        with open(out_path, 'w') as f:
            f.write(config_contents)

