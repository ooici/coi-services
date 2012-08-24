'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_temperature.py
@description Transforms CTD parsed data into L1 product for temperature
'''
import re
from pyon.ion.transforma import TransformDataProcess, TransformAlgorithm
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log
from decimal import *

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from prototype.sci_data.stream_defs import L1_temperature_stream_definition, L0_temperature_stream_definition

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor
from seawater.gibbs import SP_from_cndr
from seawater.gibbs import cte

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.util.containers import get_safe
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
import numpy as np

class CTDL1TemperatureTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the temperature vaule and scales it accroding to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''

    # Make the stream definitions of the transform class attributes... best available option I can think of?
    incoming_stream_def = L0_temperature_stream_definition()
    outgoing_stream_def = L1_temperature_stream_definition()


    def on_start(self):
        super(CTDL1TemperatureTransform, self).on_start()
        self.temp_stream = self.CFG.process.publish_streams.temperature

    def recv_packet(self, msg, headers):
        log.warn('ctd_L1_temperature.recv_packet: {0}'.format(msg))
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
        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        depth = get_safe(rdt, 'depth')

        parameter_dictionary = self._create_parameter()
        root_rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)

        root_rdt['temp'] = ctd_L1_temperature_algorithm.execute(temperature)
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['depth'] = depth

        g = build_granule(data_producer_id='ctd_L1_temperature', param_dictionary=parameter_dictionary, record_dictionary=root_rdt)
        self.publish(msg=g, stream_id=self.temp_stream)

        return g

    def _create_parameter(self):

        pdict = ParameterDictionary()

        pdict = self._add_location_time_ctxt(pdict)

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
        temp_ctxt.uom = 'degree_Celsius'
        temp_ctxt.fill_value = 0e0
        pdict.add_context(temp_ctxt)

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

class ctd_L1_temperature_algorithm(TransformAlgorithm):
    '''
    The L1 temperature data product algorithm takes the L0 temperature data product and converts it into Celcius.
    Once the hexadecimal string is converted to decimal, only scaling (dividing by a factor and adding an offset) is
    required to produce the correct decimal representation of the data in Celsius.
    The scaling function differs by CTD make/model as described below.
        SBE 37IM, Output Format 0
        1) Standard conversion from 5-character hex string (Thex) to decimal (tdec)
        2) Scaling: T [C] = (tdec / 10,000) - 10
    '''

    @staticmethod
    def execute(*args, **kwargs):
        print args[0]
        return (args[0] / 10000.0) - 10