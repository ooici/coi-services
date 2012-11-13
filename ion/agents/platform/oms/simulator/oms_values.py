#!/usr/bin/env python

"""
@package ion.agents.platform.oms.simulator.oms_values
@file    ion/agents/platform/oms/simulator/oms_values.py
@author  Carlos Rueda
@brief   Platform attribute value generators for the OMS simulator.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import time
import ntplib
import math

#
# TODO reported timestamps are NTP but need to handle the input time
# parameters in the same way.
#

# time begins now for purposes of reporting
_START_TIME = time.time()

# maximum value array size for a single generation call
_MAX_RESULT_SIZE = 1000

# next value for generators created by _create_simple_generator
_next_value = 990000

def _create_simple_generator(gen_period):
    """
    Returns a simple generator that reports incremental values every given
    time period.

    @param gen_period   discretize the time axis by this period
    """
    def _gen(from_time, to_time):
        global _next_value

        if from_time < _START_TIME:
            from_time = _START_TIME

        # t: initial abscissa coordinate
        l_from_time = long(from_time - 2*gen_period)
        t = float((l_from_time / gen_period) * gen_period)
        while t < from_time:
            t += gen_period

        values = []
        while t < to_time:
            val = _next_value
            _next_value += 1

            timestamp = ntplib.system_to_ntp_time(t)
            values.append((val, timestamp))
            t += gen_period

            if len(values) == _MAX_RESULT_SIZE:
                break

        return values

    return _gen


def _create_sine_generator(sine_period, gen_period, min_val, max_val):
    """
    Returns a sine stream fluctuating between min_val and max_val.

    @param sine_period   Sine period in time units
    @param gen_period    discretize the time axis by this period
    @param min_val
    @param max_val
    """

    twopi = 2 * math.pi
    def _gen(from_time, to_time):
        if from_time < _START_TIME:
            from_time = _START_TIME

        # t: initial abscissa coordinate
        l_from_time = long(from_time - 2*gen_period)
        t = float((l_from_time / gen_period) * gen_period)
        while t < from_time:
            t += gen_period

        range2 = (max_val - min_val) / 2
        values = []
        while t < to_time:
            s = math.sin(t / sine_period * twopi)
            val = s * range2 + (max_val + min_val) / 2
            timestamp = ntplib.system_to_ntp_time(t)
            values.append((val, timestamp))
            t += gen_period

            if len(values) == _MAX_RESULT_SIZE:
                break

        return values

    return _gen


_default_generator = _create_simple_generator(gen_period=5)

# for simplicity, use the same parameterized sine generator for
# all "input_voltage" regardless of the platform:
_input_voltage_generator = _create_sine_generator(
                                        sine_period=30,
                                        gen_period=5,
                                        min_val=-500,
                                        max_val=+500)

# concrete generators per platform/attribute:
_plat_attr_generators = {
    # we used to have a couple here, but now none for the moment.

    # An example would be:
#    ('LJ01D', 'input_voltage'): _create_sine_generator(
#                                        sine_period=30,
#                                        gen_period=5,
#                                        min_val=-500,
#                                        max_val=+500),

}


def generate_values(platform_id, attr_id, from_time, to_time):
    """
    Generates synthetic values

    @param platform_id
    @param attr_id
    @param from_time    lower limit (inclusive) of desired time range
    @param to_time      upper limit (exclusive) of desired time range
    """
    if 'input_voltage' == attr_id:
        gen = _input_voltage_generator
    else:
        gen = _plat_attr_generators.get((platform_id, attr_id), _default_generator)

    return gen(from_time, to_time)


if __name__ == "__main__":  # pragma: no cover
    # do not restrict the absolute from_time for this demo program:
    _START_TIME = 0

    import sys

    cur_time = time.time()

    platform_id = sys.argv[1]
    attr_id     = sys.argv[2]
    delta_from  = float(sys.argv[3])
    delta_to    = float(sys.argv[4])

    from_time   = cur_time + delta_from
    to_time     = cur_time + delta_to

    values = generate_values(platform_id, attr_id, from_time, to_time)
    print("generated %d values from %s to %s:" % (len(values), from_time, to_time))
    for n, (val, t) in enumerate(values):
        print("\t%2d: %5.2f -> %+4.3f" % (n, t, val))

"""
Test program

$ bin/python  ion/agents/platform/oms/simulator/oms_values.py Node1A input_voltage -35 0
generated 7 values from 1352830601.65 to 1352830636.65:
	 0: 3561819405.00 -> +0.000
	 1: 3561819410.00 -> -433.013
	 2: 3561819415.00 -> -433.013
	 3: 3561819420.00 -> -0.000
	 4: 3561819425.00 -> +433.013
	 5: 3561819430.00 -> +433.013
	 6: 3561819435.00 -> -0.000
"""
