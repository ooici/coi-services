#!/usr/bin/env python

"""
@package ion.agents.data.handlers.slocum_data_handler
@file ion/agents/data/handlers/slocum_data_handler
@author Christopher Mueller
@brief 
"""

from pyon.public import log
from pyon.util.containers import get_safe
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler
import numpy as np


PACKET_CONFIG = {
    'data_stream' : ('prototype.sci_data.stream_defs', 'ctd_stream_packet')
}

class SlocumParser(object):

    # John K's documentation says there are 16 header lines, but I believe there are actually 17
    # The 17th indicating the 'dtype' of the data for that column
    header_size = 17
    header_map = {}
    sensor_map = {}
    data_map = {}

    def __init__(self, filename=None):
        if not filename:
            raise SystemError('Must provide a filename')

        with open(filename, 'r') as f:
            for x in xrange(self.header_size-3):
                line = f.readline()
                key,value=line.split(':',1)
                self.header_map[key.strip()]=value.strip()

            # Collect the sensor names & units
            sensor_names = f.readline().split()
            units = f.readline().split()
            # Keep track of the intended data type for each sensor
            dtypes=[]
            for d in f.readline().split():
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
            self.sensor_map[sensor_names[i]]=(units[i],dtypes[i])
            dat = np.genfromtxt(fname=filename,skip_header=self.header_size,usecols=i,dtype=dtypes[i],missing_values='NaN')#,usemask=True)
            self.data_map[sensor_names[i]]=dat


class SlocumDataHandler(BaseDataHandler):

    @classmethod
    def _new_data_constraints(cls, config):
        return {}

    @classmethod
    def _get_data(cls, config):
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        if ext_dset_res:
            ds_url = ext_dset_res.dataset_description.parameters['dataset_path']
            parser = SlocumParser(ds_url)

            log.debug('External Dataset URL: \'{0}\''.format(ds_url))

            #CBM: Not in use yet...
#            t_vname = ext_dset_res.dataset_description.parameters['temporal_dimension']
#            x_vname = ext_dset_res.dataset_description.parameters['zonal_dimension']
#            y_vname = ext_dset_res.dataset_description.parameters['meridional_dimension']
#            z_vname = ext_dset_res.dataset_description.parameters['vertical_dimension']
            var_lst = ext_dset_res.dataset_description.parameters['variables']

            max_rec = get_safe(config, 'max_records', 1)
            dprod_id = get_safe(config, 'data_producer_id', 'unknown data producer')
            tx_yml = get_safe(config, 'taxonomy')
            ttool = TaxyTool.load(tx_yml) #CBM: Assertion inside RDT.__setitem__ requires same instance of TaxyTool

            cnt = cls._calc_iter_cnt(len(parser.sensor_map), max_rec)
            for x in xrange(cnt):
                rdt = RecordDictionaryTool(taxonomy=ttool)

                for name in parser.sensor_map:
                    d = parser.data_map[name][x*max_rec:(x+1)*max_rec]
                    rdt[name]=d

                g = build_granule(data_producer_id=dprod_id, taxonomy=ttool, record_dictionary=rdt)
                yield g


