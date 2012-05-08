#!/usr/bin/env python

"""
@package ion.agents.data.handlers.netcdf_data_handler
@file ion/agents/data/handlers/netcdf_data_handler.py
@author Christopher Mueller
@brief

"""

from pyon.public import log
from pyon.util.containers import get_safe

import time

from ion.agents.data.handlers.base_data_handler import BaseDataHandler, DataHandlerParameter
from netCDF4 import Dataset

PACKET_CONFIG = {
    'data_stream' : ('prototype.sci_data.stream_defs', 'ctd_stream_packet')
}

class NetcdfDataHandler(BaseDataHandler):

    @classmethod
    def _new_data_constraints(cls, config):
        """
        Returns a constraints dictionary with
        @param config Dict of configuration parameters - may be used to generate the returned 'constraints' dict
        """
        #TODO: Sort out what the config needs to look like - dataset_in??
        ext_dset_res = get_safe(config, 'dh_cfg.external_dataset_res', None)
        log.debug('ExternalDataset Resource: {0}'.format(ext_dset_res))
        if ext_dset_res:
            #TODO: Use the external dataset resource to determine what data is new (i.e. pull 'old' fingerprint from here)
            log.debug('ext_dset_res.dataset_description = {0}'.format(ext_dset_res.dataset_description))

        return {'temporal_slice':'(slice(0,10))'}

    @classmethod
    def _get_data(cls, config):
        """
        Retrieves config['constraints']['count'] number of random samples of length config['constraints']['array_len']
        @param config Dict of configuration parameters - must contain ['constraints']['count'] and ['constraints']['count']
        """
        ext_dset_res = get_safe(config, 'dh_cfg.external_dataset_res', None)
        if ext_dset_res:
            ds_url = ext_dset_res.dataset_description.parameters['dataset_path']
            log.debug('External Dataset URL: \'{0}\''.format(ds_url))

            x_vname = ext_dset_res.dataset_description.parameters['zonal_dimension']
            y_vname = ext_dset_res.dataset_description.parameters['meridional_dimension']
            t_vname = ext_dset_res.dataset_description.parameters['temporal_dimension']
            var_lst = ext_dset_res.dataset_description.parameters['variables']

            t_slice = get_safe(config, 'constraints.temporal_slice', (slice(0,1)))
            #TODO: Using 'eval' here is BAD - need to find a less sketchy way to pass constraints
            if isinstance(t_slice,str):
                t_slice=eval(t_slice)

            # Open the netcdf file, obtain the appropriate variables
            ds=Dataset(ds_url)
            lat = ds.variables[y_vname][0]
            lon = ds.variables[x_vname][0]

            t_arr = ds.variables[t_vname][t_slice]
            data_arrays = {}
            for varn in var_lst:
                data_arrays[varn] = ds.variables[varn][t_slice]

            for i in xrange(t_arr.size):
                time.sleep(0.1)
                #TODO: Build and return granule here
                ret = {
                    t_vname:t_arr[i],
                    y_vname:lat,
                    x_vname:lon,
                }
                for key, arr in data_arrays.iteritems():
                    ret[key] = arr[i]

                yield ret

            ds.close()
