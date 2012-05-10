#!/usr/bin/env python

"""
@package ion.agents.data.handlers.netcdf_data_handler
@file ion/agents/data/handlers/netcdf_data_handler.py
@author Christopher Mueller
@brief

"""

from pyon.public import log
from pyon.util.containers import get_safe
from pyon.ion.granule.granule import build_granule
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.record_dictionary import RecordDictionaryTool

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
            lon = ds.variables[x_vname][0]
            lat = ds.variables[y_vname][0]
            z = ds.variables[z_vname][0]

            t_arr = ds.variables[t_vname][t_slice]
            data_arrays = {}
            for varn in var_lst:
                data_arrays[varn] = ds.variables[varn][t_slice]

            dprod_id = get_safe(config, 'data_producer_id', 'unknown data producer')
            tx = get_safe(config, 'taxonomy')
            ttool = TaxyTool(tx) #CBM: Assertion inside RDT.__setitem__ requires same instance of TaxyTool

            for i in xrange(t_arr.size):
                time.sleep(0.1)
                #TODO: Build and return granule here
                rdt = RecordDictionaryTool(taxonomy=ttool)
                rdt_c = RecordDictionaryTool(taxonomy=ttool)
                rdt_d = RecordDictionaryTool(taxonomy=ttool)
                #CBM: RDict handling of numpy?? Need to convert to a list - len() is called on it - of python primitives
                rdt_c[t_vname] = [t_arr[i].tolist()]
                rdt_c[x_vname] = [lon.tolist()]
                rdt_c[y_vname] = [lat.tolist()]
                rdt_c[z_vname] = [z.tolist()]

                for key, arr in data_arrays.iteritems():
                    rdt_d[key] = [arr[i].tolist()]

                rdt['coords'] = rdt_c
                rdt['data'] = rdt_d

                # CBM: ttool must be passed
                g = build_granule(data_producer_id=dprod_id, taxonomy=ttool, record_dictionary=rdt)
                yield g

            ds.close()
