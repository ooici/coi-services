'''
@author David Stuebe
@file ion/services/dm/transformation/example/ion_seawater.py
@description Transforms using the csiro/gibbs seawater toolbox
'''

from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

from prototype.sci_data.stream_parser import PointSupplementStreamParser
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, L2_density_stream_definition, L2_practical_salinity_stream_definition

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
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

class SalinityTransform(TransformFunction):
    '''
    L2 Transform for CTD Data.
    Input is conductivity temperature and salsure delivered as a single packet.
    Output is Practical Salinity as calculated by the Gibbs Seawater package
    '''

    def __init__(self):

        self.outgoing_stream_def = L2_practical_salinity_stream_definition()

        self.incoming_stream_def = SBE37_CDM_stream_definition()

#        ### Taxonomies are defined before hand out of band... somehow.
#        tx = TaxyTool()
#        tx.add_taxonomy_set('salinity','long name for salinity')
#        tx.add_taxonomy_set('lat','long name for latitude')
#        tx.add_taxonomy_set('lon','long name for longitude')
#        tx.add_taxonomy_set('height','long name for height')
#        tx.add_taxonomy_set('time','long name for time')
#        # This is an example of using groups it is not a normative statement about how to use groups
#        tx.add_taxonomy_set('coordinates','This group contains coordinates...')
#        tx.add_taxonomy_set('data','This group contains data...')

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

        sal_ctxt = ParameterContext('cond', param_type=QuantityType(value_encoding=np.float32))
        sal_ctxt.uom = 'unknown'
        sal_ctxt.fill_value = 0e0

        data_ctxt = ParameterContext('data', param_type=QuantityType(value_encoding=np.int8))
        data_ctxt.uom = 'byte'
        data_ctxt.fill_value = 0x0

        # Define the parameter dictionary objects

        self.sal = ParameterDictionary()
        self.sal.add_context(t_ctxt)
        self.sal.add_context(lat_ctxt)
        self.sal.add_context(lon_ctxt)
        self.sal.add_context(height_ctxt)
        self.sal.add_context(sal_ctxt)
        self.sal.add_context(data_ctxt)

    def execute(self, granule):
        """Processes incoming data!!!!
        """

        rdt = RecordDictionaryTool.load_from_granule(granule)
        #todo: use only flat dicts for now, may change later...
#        rdt0 = rdt['coordinates']
#        rdt1 = rdt['data']

        temperature = get_safe(rdt, 'sal')
        conductivity = get_safe(rdt, 'cond')
        salsure = get_safe(rdt, 'temp')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        height = get_safe(rdt, 'height')

        log.warn('Got conductivity: %s' % str(conductivity))
        log.warn('Got salsure: %s' % str(salsure))
        log.warn('Got temperature: %s' % str(temperature))

        salinity = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=salsure)

        log.warn('Got salinity: %s' % str(salinity))


        root_rdt = RecordDictionaryTool(param_dictionary=self.sal)
        #todo: use only flat dicts for now, may change later...
#        data_rdt = RecordDictionaryTool(taxonomy=self.tx)
#        coord_rdt = RecordDictionaryTool(taxonomy=self.tx)

        root_rdt['salinity'] = salinity
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['height'] = height

#        root_rdt['coordinates'] = coord_rdt
#        root_rdt['data'] = data_rdt

        return build_granule(data_producer_id='ctd_L2_salinity', param_dictionary=self.sal, record_dictionary=root_rdt)


