#!/usr/bin/env python

"""
@package ion.agents.data.handlers.netcdf_data_handler
@file ion/agents/data/handlers/netcdf_data_handler.py
@author Christopher Mueller
@brief

"""

from pyon.public import log
from pyon.util.containers import get_safe
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler, DataHandlerParameter
import numpy as np

import time

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
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
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
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        if ext_dset_res:
            ds_url = ext_dset_res.dataset_description.parameters['dataset_path']
            log.debug('External Dataset URL: \'{0}\''.format(ds_url))

            t_vname = ext_dset_res.dataset_description.parameters['temporal_dimension']
            x_vname = ext_dset_res.dataset_description.parameters['zonal_dimension']
            y_vname = ext_dset_res.dataset_description.parameters['meridional_dimension']
            z_vname = ext_dset_res.dataset_description.parameters['vertical_dimension']
            var_lst = ext_dset_res.dataset_description.parameters['variables']

            t_slice = get_safe(config, 'constraints.temporal_slice', (slice(0,1)))
            #TODO: Using 'eval' here is BAD - need to find a less sketchy way to pass constraints
            if isinstance(t_slice,str):
                t_slice=eval(t_slice)

            # Open the netcdf file, obtain the appropriate variables
            ds=Dataset(ds_url)
            lon = ds.variables[x_vname][:]
            lat = ds.variables[y_vname][:]
            z = ds.variables[z_vname][:]

            t_arr = ds.variables[t_vname][t_slice]
            data_arrays = {}
            for varn in var_lst:
                data_arrays[varn] = ds.variables[varn][t_slice]

            max_rec = get_safe(config, 'max_records', 1)
            dprod_id = get_safe(config, 'data_producer_id', 'unknown data producer')
            tx = get_safe(config, 'taxonomy')
            ttool = TaxyTool(tx) #CBM: Assertion inside RDT.__setitem__ requires same instance of TaxyTool

            cnt = cls._calc_iter_cnt(t_arr.size, max_rec)
            for x in xrange(cnt):
                ta = t_arr[x*max_rec:(x+1)*max_rec]

                # Make a 'master' RecDict
                rdt = RecordDictionaryTool(taxonomy=ttool)
                # Make a 'coordinate' RecDict
                rdt_c = RecordDictionaryTool(taxonomy=ttool)
                # Make a 'data' RecDict
                rdt_d = RecordDictionaryTool(taxonomy=ttool)

                # Assign values to the coordinate RecDict
                rdt_c[x_vname] = lon
                rdt_c[y_vname] = lat
                rdt_c[z_vname] = z

                # Assign values to the data RecDict
                rdt_d[t_vname] = ta
                for key, arr in data_arrays.iteritems():
                    d = arr[x*max_rec:(x+1)*max_rec]
                    rdt_d[key] = d

                # Add the coordinate and data RecDicts to the master RecDict
                rdt['coords'] = rdt_c
                rdt['data'] = rdt_d

                # Build and return a granule
                # CBM: ttool must be passed
                g = build_granule(data_producer_id=dprod_id, taxonomy=ttool, record_dictionary=rdt)
                yield g

            ds.close()
