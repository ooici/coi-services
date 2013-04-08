
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L2_density.py
@description Transforms CTD parsed data into L2 product for density
'''
from pyon.util.log import log
from pyon.core.exception import BadRequest
from ion.core.process.transform import TransformDataProcess
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class DensityTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    '''
    output_bindings = ['density']

    def on_start(self):
        super(DensityTransform, self).on_start()

        self.dens_stream = self.CFG.process.publish_streams.values()[0]

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.dens_stream)

    def recv_packet(self, packet, stream_route, stream_id):
        """
        Processes incoming data!!!!
        """
        if packet == {}:
            return
        log.debug("L2 density transform received granule with record dict: %s", packet.record_dictionary)

        granule = CTDL2DensityTransformAlgorithm.execute(packet, params=self.stream_definition._id)

        log.debug("L2 density transform publishing granule with record dict: %s", granule.record_dictionary)

        self.publisher.publish(msg=granule)


class CTDL2DensityTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)
        out_rdt = RecordDictionaryTool(stream_definition_id=params)

        conductivity = rdt['conductivity']
        pressure = rdt['pressure']
        temperature = rdt['temp']

        longitude = rdt['lon'] if rdt['lon'] is not None else 0
        latitude = rdt['lat'] if rdt['lat'] is not None else 0

        sp = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pressure)

        log.debug("Density algorithm calculated the sp (practical salinity) values: %s", sp)

        sa = SA_from_SP(sp, pressure, longitude, latitude)

        log.debug("Density algorithm calculated the sa (actual salinity) values: %s", sa)

        dens_value = rho(sa, temperature, pressure)

        for key, value in rdt.iteritems():
            if key in out_rdt:
                if key=='conductivity' or key=='temp' or key=='pressure':
                    continue
                out_rdt[key] = value[:]

        out_rdt['density'] = dens_value

        log.debug("Density algorithm returning density values: %s", out_rdt['density'])

        return out_rdt.to_granule()
