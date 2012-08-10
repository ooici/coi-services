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
from ion.services.dm.utility.granule_utils import CoverageCraft

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.util.containers import get_safe
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
import numpy as np

craft = CoverageCraft
sdom, tdom = craft.create_domains()
sdom = sdom.dump()
tdom = tdom.dump()
parameter_dictionary = craft.create_parameters()

class SalinityTransform(TransformFunction):
    '''
    L2 Transform for CTD Data.
    Input is conductivity temperature and pres delivered as a single packet.
    Output is Practical Salinity as calculated by the Gibbs Seawater package
    '''

    outgoing_stream_def = L2_practical_salinity_stream_definition()
    incoming_stream_def = SBE37_CDM_stream_definition()

    def __init__(self):
        super(SalinityTransform, self).__init__()

    def execute(self, granule):
        """Processes incoming data!!!!
        """

        rdt = RecordDictionaryTool.load_from_granule(granule)
        #todo: use only flat dicts for now, may change later...
#        rdt0 = rdt['coordinates']
#        rdt1 = rdt['data']

        temperature = get_safe(rdt, 'temp')
        conductivity = get_safe(rdt, 'conductivity')
        pres = get_safe(rdt, 'pressure')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')
        depth = get_safe(rdt, 'depth')

        log.warn('Got conductivity: %s' % str(conductivity))
        log.warn('Got pres: %s' % str(pres))
        log.warn('Got temperature: %s' % str(temperature))

        salinity = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pres)

        log.warn('Got salinity: %s' % str(salinity))


        root_rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)
        #todo: use only flat dicts for now, may change later...
#        data_rdt = RecordDictionaryTool(taxonomy=self.tx)
#        coord_rdt = RecordDictionaryTool(taxonomy=self.tx)

        root_rdt['salinity'] = salinity
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude
        root_rdt['depth'] = depth

#        root_rdt['coordinates'] = coord_rdt
#        root_rdt['data'] = data_rdt

        return build_granule(data_producer_id='ctd_L2_salinity', param_dictionary=parameter_dictionary, record_dictionary=root_rdt)


