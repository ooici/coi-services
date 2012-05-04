#!/usr/bin/env python

"""
@package ion.agents.eoi.handler.netcdf_data_handler
@file ion/agents/eoi/handler/netcdf_data_handler.py
@author Christopher Mueller
@brief

"""

from pyon.util import log
from pyon.util.containers import get_safe

import time

from ion.agents.eoi.handler.base_data_handler import BaseDataHandler

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

        return {'temporal_slice':'(slice(0,10))'}

    @classmethod
    def _get_data(cls, config):
        """
        Retrieves config['constraints']['count'] number of random samples of length config['constraints']['array_len']
        @param config Dict of configuration parameters - must contain ['constraints']['count'] and ['constraints']['count']
        """
        t_slice = get_safe(config, 'constraints.temporal_slice', (slice(0,2)))
        if isinstance(t_slice,str):
            t_slice=eval(t_slice)

        # Open the netcdf file, obtain the appropriate variables
        from netCDF4 import Dataset as DS
        ds=DS('test_data/usgs.nc')
        lat = ds.variables['lat'][0]
        lon = ds.variables['lon'][0]

        t = ds.variables['time'][t_slice]
        temp = ds.variables['water_temperature'][t_slice]
        sflow = ds.variables['streamflow'][t_slice]

        for i in xrange(t.size):
            time.sleep(0.1)
            yield {'time':t,'temp':temp,'sflow':sflow}

        ds.close()
