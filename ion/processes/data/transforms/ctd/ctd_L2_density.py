'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L2_density.py
@description Transforms CTD parsed data into L2 product for density
'''

from pyon.ion.transform import TransformFunction
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
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.util.containers import get_safe


class DensityTransform(TransformFunction):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the conductivity, pressure and Temperature value and calculates density
    according to the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''


    # Make the stream definitions of the transform class attributes... best available option I can think of?
    incoming_stream_def = SBE37_CDM_stream_definition()
    outgoing_stream_def = L2_density_stream_definition()

    ### Taxonomies are defined before hand out of band... somehow.
    tx = TaxyTool()
    tx.add_taxonomy_set('density','long name for density')
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

        temperature = get_safe(rdt, 'pres')
        conductivity = get_safe(rdt, 'cond')
        pressure = get_safe(rdt, 'temp')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        height = get_safe(rdt, 'height')


        log.warn('Got conductivity: %s' % str(conductivity))
        log.warn('Got pressure: %s' % str(pressure))
        log.warn('Got temperature: %s' % str(temperature))


        sp = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pressure)

        sa = SA_from_SP(sp, pressure, longitude, latitude)

        density = rho(sa, temperature, pressure)

        log.warn('Got density: %s' % str(density))

        # Use the constructor to put data into a granule
        #psc = PointSupplementConstructor(point_definition=self.outgoing_stream_def, stream_id=self.streams['output'])
        ### Assumes the config argument for output streams is known and there is only one 'output'.
        ### the stream id is part of the metadata which much go in each stream granule - this is awkward to do at the
        ### application level like this!

        root_rdt = RecordDictionaryTool(taxonomy=self.tx)
        #todo: use only flat dicts for now, may change later...
#        data_rdt = RecordDictionaryTool(taxonomy=self.tx)
#        coord_rdt = RecordDictionaryTool(taxonomy=self.tx)

        root_rdt['density'] = density
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['height'] = height

#        root_rdt['coordinates'] = coord_rdt
#        root_rdt['data'] = data_rdt

        return build_granule(data_producer_id='ctd_L2_density', taxonomy=self.tx, record_dictionary=root_rdt)


  