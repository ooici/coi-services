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
import math

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

        l_from_time = long(from_time - 2*gen_period)

        t = float((l_from_time / gen_period) * gen_period)
        while t < from_time:
            t += gen_period

        values = []
        while t < to_time:
            val = _next_value
            _next_value += 1

            values.append((val, t))
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
        l_from_time = long(from_time)
        if l_from_time % gen_period == 0:
            t = from_time
        else:
            t = float((1 + l_from_time / gen_period) * gen_period)

        range2 = (max_val - min_val) / 2
        values = []
        while t < to_time:
            s = math.sin(t / sine_period * twopi)
            val = s * range2 + (max_val + min_val) / 2
            values.append((val, t))
            t += gen_period

            if len(values) == _MAX_RESULT_SIZE:
                break

        return values

    return _gen


_default_generator = _create_simple_generator(gen_period=5)

_generators = {
    ('Node1A', 'input_voltage'): _create_sine_generator(
                                        sine_period=30,
                                        gen_period=5,
                                        min_val=-500,
                                        max_val=+500),

    ('Node1D', 'input_voltage'): _create_sine_generator(
                                        sine_period=20,
                                        gen_period=2,
                                        min_val=50,
                                        max_val=100),
}


def generate_values(platform_id, attr_id, from_time, to_time):
    """
    Generates synthetic values

    @param platform_id
    @param attr_id
    @param from_time    lower limit (inclusive) of desired time range
    @param to_time      upper limit (exclusive) of desired time range
    """
    gen = _generators.get((platform_id, attr_id), _default_generator)
    return gen(from_time, to_time)


if __name__ == "__main__":
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
generated 7 values from 1351802359.33 to 1351802394.33:
	 0: 1351802360.00 -> -433.013
	 1: 1351802365.00 -> -433.013
	 2: 1351802370.00 -> -0.000
	 3: 1351802375.00 -> +433.013
	 4: 1351802380.00 -> +433.013
	 5: 1351802385.00 -> -0.000
	 6: 1351802390.00 -> -433.013
"""
