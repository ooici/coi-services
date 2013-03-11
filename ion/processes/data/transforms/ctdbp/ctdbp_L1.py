
"""
@author Swarbhanu Chatterjee
@file ion/processes/data/transforms/ctdbp/ctd_L1.py
@description Transforms incoming L0 product into L1 product for conductivity, temperature and pressure through the L1 stream
"""

from pyon.public import log
from pyon.core.exception import BadRequest

from ion.core.process.transform import TransformDataProcess
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction

import numpy as np

class CTDBP_L1_Transform(TransformDataProcess):
    """
    L1 takes the L0 stream and the passed in Calibration Coefficients and performs the appropriate calculations
    to output the L1 values.
    """
    output_bindings = ['L1_stream']

    def on_start(self):
        super(CTDBP_L1_Transform, self).on_start()

        #  Validate the CFG used to launch the transform has all the required fields
        if not self.CFG.process.publish_streams.has_key('L1_stream'):
            raise BadRequest("For CTDBP transforms, please send the stream_id for the L1_stream using "
                     "a special keyword (L1_stream)")

        self.L1_stream_id = self.CFG.process.publish_streams.L1_stream

        calibration_coeffs = {}
        calibration_coeffs['temp_calibration_coeffs'] = self.CFG.process.calibration_coeffs.temp_calibration_coeffs
        calibration_coeffs['pres_calibration_coeffs'] = self.CFG.process.calibration_coeffs.pres_calibration_coeffs
        calibration_coeffs['cond_calibration_coeffs'] = self.CFG.process.calibration_coeffs.cond_calibration_coeffs

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.L1_stream_id)

        self.params = {}
        self.params['stream_def_id'] = self.stream_definition._id
        self.params['calibration_coeffs'] = calibration_coeffs

    def recv_packet(self, packet, stream_route, stream_id):
        """Processes incoming data!!!!
        """

        log.debug("CTDBP L1 transform received a packet: %s", packet)

        if packet == {}:
            return

        granule = CTDBP_L1_TransformAlgorithm.execute(packet, params= self.params)
        self.L1_stream.publish(msg=granule)

class CTDBP_L1_TransformAlgorithm(SimpleGranuleTransformFunction):
    """
    Compute conductivity, temperature and pressure using the calibration coefficients

    Reference
    ---------
    The calculations below are based on the following spreadsheet document:
    https://docs.google.com/spreadsheet/ccc?key=0Au7PUzWoCKU4dDRMeVI0RU9yY180Z0Y5U0hyMUZERmc#gid=0

    """
    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)
        out_rdt = RecordDictionaryTool(stream_definition_id=params['stream_def_id'])

        # Fill the time values
        out_rdt['time'] = rdt['time']

        # The calibration coefficients
        temp_calibration_coeffs= params['calibration_coeffs']['temp_calibration_coeffs']
        pres_calibration_coeffs= params['calibration_coeffs']['pres_calibration_coeffs']
        cond_calibration_coeffs = params['calibration_coeffs']['cond_calibration_coeffs']

        # Set the temperature values for the output granule
        out_rdt = CTDBP_L1_TransformAlgorithm.calculate_temperature(    input_rdt = rdt,
                                                                        out_rdt = out_rdt,
                                                                        temp_calibration_coeffs= temp_calibration_coeffs )

        # Set the pressure values for the output granule
        out_rdt = CTDBP_L1_TransformAlgorithm.calculate_pressure(   input_rdt= rdt,
                                                                    out_rdt = out_rdt,
                                                                    pres_calibration_coeffs= pres_calibration_coeffs)

        # Set the conductivity values for the output granule
        # Note that since the conductivity caculation depends on whether TEMPWAT_L1, PRESWAT_L1 have been calculated, we need to do this last
        out_rdt = CTDBP_L1_TransformAlgorithm.calculate_conductivity(   input_rdt = rdt,
                                                                        out_rdt = out_rdt,
                                                                        cond_calibration_coeffs = cond_calibration_coeffs
        )

        # build the granule for the L1 stream
        return out_rdt.to_granule()

    @staticmethod
    def calculate_conductivity(input_rdt = None, out_rdt = None, cond_calibration_coeffs = None):
        """
        Dependencies: conductivity calibration coefficients, TEMPWAT_L1, PRESWAT_L1
        """

        log.debug("L1 transform applying conductivity algorithm")

        CONDWAT_L0 = input_rdt['conductivity']
        TEMPWAT_L1 = out_rdt['temp']
        PRESWAT_L1 = out_rdt['pressure']


        #------------  CALIBRATION COEFFICIENTS FOR CONDUCTIVITY  --------------
        g = cond_calibration_coeffs['g']
        h = cond_calibration_coeffs['h']
        I = cond_calibration_coeffs['I']
        j = cond_calibration_coeffs['j']
        CTcor = cond_calibration_coeffs['CTcor']
        CPcor = cond_calibration_coeffs['CPcor']

        if not (g and h and I and j and CTcor and CPcor):
            raise BadRequest("All the conductivity calibration coefficients (g,h,I,j,CTcor, CPcor) were not passed through"
                             "the config. Example: config.process.calibration_coeffs.cond_calibration_coeffs['g']")

        #------------  Computation -------------------------------------
        freq = (CONDWAT_L0 / 256) / 1000
        CONDWAT_L1 = (g + h * freq**2 + I * freq**3 + j * freq**4) / (1 + CTcor * TEMPWAT_L1 + CPcor * PRESWAT_L1)


        #------------  Update the output record dictionary with the values -------
        for key, value in input_rdt.iteritems():
            if key in out_rdt:
                out_rdt[key] = value[:]

        #------------------------------------------------------------------------
        # Update the conductivity values
        #------------------------------------------------------------------------
        out_rdt['conductivity'] = CONDWAT_L1

        log.debug("L1 transform conductivity algorithm returning: %s", out_rdt)

        return out_rdt

    @staticmethod
    def calculate_temperature(input_rdt = None, out_rdt = None, temp_calibration_coeffs = None):
        """
        Dependencies: temperature calibration coefficients
        """
        log.debug("L1 transform applying temperature algorithm")

        TEMPWAT_L0 = input_rdt['temperature']

        #------------  CALIBRATION COEFFICIENTS FOR TEMPERATURE  --------------
        a0 = temp_calibration_coeffs['a0']
        a1 = temp_calibration_coeffs['a1']
        a2 = temp_calibration_coeffs['a2']
        a3 = temp_calibration_coeffs['a3']

        if not (a0 and a1 and a2 and a3):
            raise BadRequest("All the temperature calibration coefficients (a0,a1,a2,a3) were not passed through"
                             "the config. Example: config.process.calibration_coeffs.temp_calibration_coeffs['a0']")

        #------------  Computation -------------------------------------
        MV = (TEMPWAT_L0 - 524288) / 1.6e+007
        R = (MV * 2.900e+009 + 1.024e+008) / (2.048e+004 - MV * 2.0e+005)
        TEMPWAT_L1 = 1 / (a0 + a1 * np.log(R) + a2 * (np.log(R))**2 + a3 * (np.log(R))**3) - 273.15

        #------------  Update the output record dictionary with the values --------------
        for key, value in input_rdt.iteritems():
            if key in out_rdt:
                out_rdt[key] = value[:]

        out_rdt['temp'] = TEMPWAT_L1

        log.debug("L1 transform temperature algorithm returning: %s", out_rdt)

        return out_rdt

    @staticmethod
    def calculate_pressure(input_rdt = None, out_rdt = None, pres_calibration_coeffs = None):
        """
            Dependencies: TEMPWAT_L0, PRESWAT_L0, pressure calibration coefficients
        """

        log.debug("L1 transform applying pressure algorithm")

        PRESWAT_L0 = input_rdt['pressure']
        TEMPWAT_L0 = input_rdt['temperature']

        #------------  CALIBRATION COEFFICIENTS FOR TEMPERATURE  --------------
        PTEMPA0 = pres_calibration_coeffs['PTEMPA0']
        PTEMPA1 = pres_calibration_coeffs['PTEMPA1']
        PTEMPA2 = pres_calibration_coeffs['PTEMPA2']

        PTCA0 = pres_calibration_coeffs['PTCA0']
        PTCA1 = pres_calibration_coeffs['PTCA1']
        PTCA2 = pres_calibration_coeffs['PTCA2']

        PTCB0 = pres_calibration_coeffs['PTCB0']
        PTCB1 = pres_calibration_coeffs['PTCB1']
        PTCB2 = pres_calibration_coeffs['PTCB2']

        PA0 = pres_calibration_coeffs['PTCB0']
        PA1 = pres_calibration_coeffs['PA1']
        PA2 = pres_calibration_coeffs['PA2']

        cond = PTEMPA0 and PTEMPA0 and PTEMPA2 and PTCA0 and PTCA1 and PTCA2 and PTCB0 and PTCB1 and PTCB2
        cond = cond and PA0 and PA1 and PA2

        if not cond:
            raise BadRequest("All the pressure calibration coefficients were not passed through"
                             "the config. Example: config.process.calibration_coeffs.pres_calibration_coeffs['PTEMPA0']")

        #------------  Computation -------------------------------------
        tvolt = TEMPWAT_L0 / 13107
        temp = PTEMPA0 + PTEMPA1 * tvolt + PTEMPA2 * tvolt**2
        x = PRESWAT_L0 - PTCA0 - PTCA1 * temp - PTCA2 * temp**2
        n = x * PTCB0 / (PTCB0 + PTCB1 * temp + PTCB2 * temp**2)
        absolute_pressure = PA0 + PA1 * n + PA2 * n**2
        PRESWAT_L1 = (absolute_pressure * 0.689475729) - 10.1325

        #------------  Update the output record dictionary with the values ---------
        for key, value in input_rdt.iteritems():
            if key in out_rdt:
                out_rdt[key] = value[:]

        out_rdt['pressure'] = PRESWAT_L1

        log.debug("L1 transform pressure algorithm returning: %s", out_rdt)

        return out_rdt
