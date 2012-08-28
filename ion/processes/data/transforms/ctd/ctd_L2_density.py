'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L2_density.py
@description Transforms CTD parsed data into L2 product for density
'''

import re
from pyon.ion.transforma import TransformDataProcess, TransformAlgorithm
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, L2_density_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.util.containers import get_safe
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
import numpy as np

class DensityTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the conductivity, density and Temperature value and calculates density
    according to the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''

    # Make the stream definitions of the transform class attributes... best available option I can think of?
    incoming_stream_def = SBE37_CDM_stream_definition()
    outgoing_stream_def = L2_density_stream_definition()

    def on_start(self):
        super(DensityTransform, self).on_start()

        if self.CFG.process.publish_streams.has_key('density'):
            self.dens_stream = self.CFG.process.publish_streams.density
        elif self.CFG.process.publish_streams.has_key('output'):
            self.dens_stream = self.CFG.process.publish_streams.output

    def recv_packet(self, msg, headers):
        log.warn('ctd_L2_desnity.recv_packet: {0}'.format(msg))
        stream_id = headers['routing_key']
        stream_id = re.sub(r'\.data', '', stream_id)
        self.receive_msg(msg, stream_id)

    def publish(self, msg, stream_id):
        self.publisher.publish(msg=msg, stream_id=stream_id)

    def receive_msg(self, granule, stream_id):
        """Processes incoming data!!!!
        """
        rdt = RecordDictionaryTool.load_from_granule(granule)

        temperature = get_safe(rdt, 'temp')
        conductivity = get_safe(rdt, 'conductivity')
        pressure = get_safe(rdt, 'pres') #psd.get_values('pressure')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        depth = get_safe(rdt, 'depth')

        # Use the constructor to put data into a granule
        #psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])
        ### Assumes the config argument for output streams is known and there is only one 'output'.
        ### the stream id is part of the metadata which much go in each stream granule - this is awkward to do at the
        ### application level like this!

        parameter_dictionary = self._create_parameter()
        root_rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)

        root_rdt['density'] = ctd_L2_density_algorithm.execute(conductivity, pressure, temperature, longitude, latitude)
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['depth'] = depth

        g = build_granule(data_producer_id='ctd_L2_density', param_dictionary=parameter_dictionary, record_dictionary=root_rdt)
        self.publish(msg=g, stream_id=self.dens_stream)

        return g

    def _create_parameter(self):

        pdict = ParameterDictionary()

        pdict = self._add_location_time_ctxt(pdict)

        dens_ctxt = ParameterContext('density', param_type=QuantityType(value_encoding=np.float32))
        dens_ctxt.uom = 'unknown'
        dens_ctxt.fill_value = 0x0
        pdict.add_context(dens_ctxt)

        return pdict

    def _add_location_time_ctxt(self, pdict):

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0
        pdict.add_context(lon_ctxt)

        depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=np.float32))
        depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        depth_ctxt.uom = 'meters'
        depth_ctxt.fill_value = 0e0
        pdict.add_context(depth_ctxt)

        return pdict

class ctd_L2_density_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        cond = args[0]
        pres = args[1]
        temp = args[2]
        lon = args[3]
        lat = args[4]
        sp = SP_from_cndr(r=cond/cte.C3515, t=temp, p=pres)
        sa = SA_from_SP(sp, pres, lon, lat)
        return rho(sa, temp, pres)