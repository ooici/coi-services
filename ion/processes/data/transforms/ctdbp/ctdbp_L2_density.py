
'''
@author Swarbhanu Chatterjee
@file ion/processes/data/transforms/ctdbp/ctdbp_L2_density.py
@description Transforms incoming L1 product into L2 product for density through the L2 stream
'''
from pyon.util.log import log
from pyon.core.exception import BadRequest
from ion.core.process.transform import TransformDataProcess
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

class CTDBP_DensityTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    '''
    output_bindings = ['density']

    def on_start(self):
        super(CTDBP_DensityTransform, self).on_start()

        if not self.CFG.process.publish_streams.has_key('density'):
            raise BadRequest("For CTD transforms, please send the stream_id "
                             "using a special keyword (ex: density)")
        self.dens_stream_id = self.CFG.process.publish_streams.density

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.dens_stream_id)

    def recv_packet(self, packet, stream_route, stream_id):
        """
        Processes incoming data!!!!
        """
        if packet == {}:
            return
        log.debug("CTDBP L2 density transform received granule with record dict: %s", packet.record_dictionary)

        granule = CTDBP_DensityTransformAlgorithm.execute(packet, params=self.stream_definition._id)

        log.debug("CTDBP L2 density transform publishing granule with record dict: %s", granule.record_dictionary)

        self.density.publish(msg=granule)


class CTDBP_DensityTransformAlgorithm(SimpleGranuleTransformFunction):

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

        log.debug("CTDBP Density algorithm calculated the sp (practical salinity) values: %s", sp)

        sa = SA_from_SP(sp, pressure, longitude, latitude)

        log.debug("CTDBP Density algorithm calculated the sa (actual salinity) values: %s", sa)

        dens_value = rho(sa, temperature, pressure)

        for key, value in rdt.iteritems():
            if key in out_rdt:
                if key=='conductivity' or key=='temp' or key=='pressure':
                    continue
                out_rdt[key] = value[:]

        out_rdt['density'] = dens_value

        return out_rdt.to_granule()
