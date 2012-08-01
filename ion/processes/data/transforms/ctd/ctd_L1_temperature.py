'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_temperature.py
@description Transforms CTD parsed data into L1 product for temperature
'''

from pyon.ion.transform import TransformFunction
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
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.util.containers import get_safe
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
import numpy as np

class CTDL1TemperatureTransform(TransformFunction):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the temperature vaule and scales it accroding to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''

    def on_start(self):

        # Make the stream definitions of the transform class attributes... best available option I can think of?
        self.incoming_stream_def = L0_temperature_stream_definition()
        self.outgoing_stream_def = L1_temperature_stream_definition()

        ### Parameter dictionaries
        self.defining_parameter_dictionary()

    def defining_parameter_dictionary(self):

        # Define the parameter context objects

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0

        height_ctxt = ParameterContext('height', param_type=QuantityType(value_encoding=np.float32))
        height_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        height_ctxt.uom = 'meters'
        height_ctxt.fill_value = 0e0

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
        temp_ctxt.uom = 'degree_Celsius'
        temp_ctxt.fill_value = 0e0


        data_ctxt = ParameterContext('data', param_type=QuantityType(value_encoding=np.int8))
        data_ctxt.uom = 'byte'
        data_ctxt.fill_value = 0x0

        # Define the parameter dictionary objects

        self.temp = ParameterDictionary()
        self.temp.add_context(t_ctxt)
        self.temp.add_context(lat_ctxt)
        self.temp.add_context(lon_ctxt)
        self.temp.add_context(height_ctxt)
        self.temp.add_context(temp_ctxt)
        self.temp.add_context(data_ctxt)

    def execute(self, granule):
        """Processes incoming data!!!!
        """

        rdt = RecordDictionaryTool.load_from_granule(granule)
        #todo: use only flat dicts for now, may change later...
#        rdt0 = rdt['coordinates']
#        rdt1 = rdt['data']

        temperature = get_safe(rdt, 'temp')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        height = get_safe(rdt, 'height')

        log.warn('Got temperature: %s' % str(temperature))


        # The L1 temperature data product algorithm takes the L0 temperature data product and converts it into Celcius.
        # Once the hexadecimal string is converted to decimal, only scaling (dividing by a factor and adding an offset) is
        # required to produce the correct decimal representation of the data in Celsius.
        # The scaling function differs by CTD make/model as described below.
        #    SBE 37IM, Output Format 0
        #    1) Standard conversion from 5-character hex string (Thex) to decimal (tdec)
        #    2) Scaling: T [C] = (tdec / 10,000) - 10

        root_rdt = RecordDictionaryTool(param_dictionary=self.temp)

        #todo: use only flat dicts for now, may change later...
#        data_rdt = RecordDictionaryTool(taxonomy=self.tx)
#        coord_rdt = RecordDictionaryTool(taxonomy=self.tx)

        scaled_temperature = temperature

        for i in xrange(len(temperature)):
            scaled_temperature[i] = ( temperature[i] / 10000.0) - 10

        root_rdt['temp'] = scaled_temperature
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['height'] = height

        #todo: use only flat dicts for now, may change later...
#        root_rdt['coordinates'] = coord_rdt
#        root_rdt['data'] = data_rdt

        return build_granule(data_producer_id='ctd_L1_temperature', param_dictionary=self.temp, record_dictionary=root_rdt)
  