
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

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        stream_def = pubsub.read_stream_definition(stream_id=self.L1_stream_id)
        self.stream_definition_id = stream_def._id

        self.temp_calibration_coeffs = self.CFG.process.calibration_coeffs['temp_calibration_coeffs']
        self.pres_calibration_coeffs = self.CFG.process.calibration_coeffs['pres_calibration_coeffs']
        self.cond_calibration_coeffs = self.CFG.process.calibration_coeffs['cond_calibration_coeffs']

    def recv_packet(self, packet, stream_route, stream_id):
        if packet == {}:
            return

        l0_values = RecordDictionaryTool.load_from_granule(packet)
        l1_values = RecordDictionaryTool(stream_definition_id=self.stream_definition_id)
        log.debug("CTDBP L1 transform using L0 values: tempurature %s, pressure %s, conductivity %s",
                  l0_values['temperature'], l0_values['pressure'], l0_values['conductivity'])

        #for key, value in 'lat', 'lon', 'time', ...:   <-- do we want to be a little more specific here?
        for key, value in l0_values.iteritems():
            if key in l1_values:
                l1_values[key] = value[:]

        l1_values['temp'] = self.calculate_temperature(l0=l0_values)
        l1_values['pressure'] = self.calculate_pressure(l0=l0_values)
        l1_values['conductivity'] = self.calculate_conductivity(l0=l0_values, l1=l1_values)

        log.debug('calculated L1 values: temp %s, pressure %s, conductivity %s',
                  l1_values['temp'], l1_values['pressure'], l1_values['conductivity'])
        self.L1_stream.publish(msg=l1_values.to_granule())

    def calculate_temperature(self, l0):
        TEMPWAT_L0 = l0['temperature']

        #------------  CALIBRATION COEFFICIENTS FOR TEMPERATURE  --------------
        a0 = self.temp_calibration_coeffs['TA0']
        a1 = self.temp_calibration_coeffs['TA1']
        a2 = self.temp_calibration_coeffs['TA2']
        a3 = self.temp_calibration_coeffs['TA3']

        #------------  Computation -------------------------------------
        MV = (TEMPWAT_L0 - 524288) / 1.6e+007
        R = (MV * 2.900e+009 + 1.024e+008) / (2.048e+004 - MV * 2.0e+005)
        logR = np.log(R)
        TEMPWAT_L1 = 1 / (a0 + a1 * logR + a2 * logR**2 + a3 * logR**3) - 273.15

        return TEMPWAT_L1

    def calculate_pressure(self, l0):
        PRESWAT_L0 = l0['pressure']
        TEMPWAT_L0 = l0['temperature']

        #------------  CALIBRATION COEFFICIENTS FOR TEMPERATURE  --------------
        PTEMPA0 = self.pres_calibration_coeffs['PTEMPA0']
        PTEMPA1 = self.pres_calibration_coeffs['PTEMPA1']
        PTEMPA2 = self.pres_calibration_coeffs['PTEMPA2']

        PTCA0 = self.pres_calibration_coeffs['PTCA0']
        PTCA1 = self.pres_calibration_coeffs['PTCA1']
        PTCA2 = self.pres_calibration_coeffs['PTCA2']

        PTCB0 = self.pres_calibration_coeffs['PTCB0']
        PTCB1 = self.pres_calibration_coeffs['PTCB1']
        PTCB2 = self.pres_calibration_coeffs['PTCB2']

        PA0 = self.pres_calibration_coeffs['PA0']
        PA1 = self.pres_calibration_coeffs['PA1']
        PA2 = self.pres_calibration_coeffs['PA2']

        #------------  Computation -------------------------------------
        tvolt = TEMPWAT_L0 / 13107.0
        temp = PTEMPA0 + PTEMPA1 * tvolt + PTEMPA2 * tvolt**2
        x = PRESWAT_L0 - PTCA0 - PTCA1 * temp - PTCA2 * temp**2
        n = x * PTCB0 / (PTCB0 + PTCB1 * temp + PTCB2 * temp**2)
        absolute_pressure = PA0 + PA1 * n + PA2 * n**2
        PRESWAT_L1 = (absolute_pressure * 0.689475729) - 10.1325

        return PRESWAT_L1

    def calculate_conductivity(self, l0, l1):
        CONDWAT_L0 = l0['conductivity']
        TEMPWAT_L1 = l1['temp']
        PRESWAT_L1 = l1['pressure']

        #------------  CALIBRATION COEFFICIENTS FOR CONDUCTIVITY  --------------
        g = self.cond_calibration_coeffs['G']
        h = self.cond_calibration_coeffs['H']
        I = self.cond_calibration_coeffs['I']
        j = self.cond_calibration_coeffs['J']
        CTcor = self.cond_calibration_coeffs['CTCOR']
        CPcor = self.cond_calibration_coeffs['CPCOR']

        log.debug('g %e, h %e, i %e, j %e, CTcor %e, CPcor %e', g, h, I, j, CTcor, CPcor)

        #------------  Computation -------------------------------------
        freq = (CONDWAT_L0 / 256000.0)
        numerator = (g + h * freq**2 + I * freq**3 + j * freq**4)
        denominator = (1 + CTcor * TEMPWAT_L1 + CPcor * PRESWAT_L1)
        log.debug('freq %s, cond = %s / %s', freq, numerator, denominator)
        CONDWAT_L1 = numerator / denominator
#        CONDWAT_L1 = (g + h * freq**2 + I * freq**3 + j * freq**4) / (1 + CTcor * TEMPWAT_L1 + CPcor * PRESWAT_L1)

        #------------------------------------------------------------------------
        # Update the conductivity values
        #------------------------------------------------------------------------
        return CONDWAT_L1

