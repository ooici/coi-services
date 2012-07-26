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

class CTDL1TemperatureTransform(TransformFunction):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the temperature vaule and scales it accroding to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''


    # Make the stream definitions of the transform class attributes... best available option I can think of?
    incoming_stream_def = L0_temperature_stream_definition()
    outgoing_stream_def = L1_temperature_stream_definition()

    ### Taxonomies are defined before hand out of band... somehow.
    tx = TaxyTool()
    tx.add_taxonomy_set('temp','long name for temperature')
    tx.add_taxonomy_set('lat','long name for latitude')
    tx.add_taxonomy_set('lon','long name for longitude')
    tx.add_taxonomy_set('height','long name for height')
    tx.add_taxonomy_set('time','long name for time')
    # This is an example of using groups it is not a normative statement about how to use groups
    tx.add_taxonomy_set('coordinates','This group contains coordinates...')
    tx.add_taxonomy_set('data','This group contains data...')


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

        root_rdt = RecordDictionaryTool(taxonomy=self.tx)

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

        return build_granule(data_producer_id='ctd_L1_temperature', taxonomy=self.tx, record_dictionary=root_rdt)
  