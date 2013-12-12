#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.simulator.oms_values
@file    ion/agents/platform/rsn/simulator/oms_values.py
@author  Carlos Rueda
@brief   Platform attribute value generators for the RSN OMS simulator.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import time
import ntplib
import math


# time begins a few secs ago from now for purposes of reporting
_START_TIME = ntplib.system_to_ntp_time(time.time() - 30)

# maximum value array size for a single generation call
_MAX_RESULT_SIZE = 1000

# next value for generators created by _create_simple_generator
_next_value = 990000


def _create_simple_generator(gen_period):
    """
    Returns a simple generator that reports incremental values every given
    time period.

    @param gen_period   discretize the time axis by this period in secs

    @retval A function to be called with parameters (from_time, to_time) where
            from_time and to_time are the lower and upper limits (both
            inclusive) of desired time window (NTP).
    """
    def _gen(from_time, to_time):
        global _next_value

        if from_time < _START_TIME:
            from_time = _START_TIME

        # t: initial abscissa coordinate within the time window
        l_from_time = long(from_time - 2*gen_period)
        t = float((l_from_time / gen_period) * gen_period)
        while t < from_time:
            t += gen_period

        values = []
        while t <= to_time:
            val = _next_value
            _next_value += 1

            timestamp = t
            values.append((val, timestamp))
            t += gen_period

            if len(values) == _MAX_RESULT_SIZE:
                break

        return values

    return _gen


def _create_sine_generator(sine_period, gen_period, min_val, max_val):
    """
    Returns a sine stream fluctuating between min_val and max_val.

    @param sine_period   Sine period in secs
    @param gen_period    discretize the time axis by this period in secs
    @param min_val       min value
    @param max_val       max value

    @retval A function to be called with parameters (from_time, to_time) where
            from_time and to_time are the lower and upper limits (both
            inclusive) of desired time window (NTP).
    """

    twopi = 2 * math.pi

    def _gen(from_time, to_time):
        if from_time < _START_TIME:
            from_time = _START_TIME

        # t: initial abscissa coordinate within the time window
        l_from_time = long(from_time - 2*gen_period)
        t = float((l_from_time / gen_period) * gen_period)
        while t < from_time:
            t += gen_period

        range2 = (max_val - min_val) / 2
        values = []
        while t <= to_time:
            s = math.sin(t / sine_period * twopi)
            val = s * range2 + (max_val + min_val) / 2
            timestamp = t
            values.append((val, timestamp))
            t += gen_period

            if len(values) == _MAX_RESULT_SIZE:
                break

        return values

    return _gen


# generators per platform-ID/attribute-name:
_plat_attr_generators = {
    # we used to have a couple here, but now none for the moment.

    # An example would be:
    # ('LJ01D', 'input_voltage'): _create_sine_generator(sine_period=30,
    #                                                    gen_period=2.5,
    #                                                    min_val=-500,
    #                                                    max_val=+500),

}


# generators per attribute name:
_attribute_generators = {

    'input_voltage':
    _create_sine_generator(sine_period=30,
                           gen_period=2.5,
                           min_val=-500,
                           max_val=+500),

    'input_bus_current':
    _create_sine_generator(sine_period=50,
                           gen_period=5,
                           min_val=-300,
                           max_val=+300),

    'MVPC_temperature':
    _create_sine_generator(sine_period=20,
                           gen_period=4,
                           min_val=-200,
                           max_val=+200),

    'MVPC_pressure_1':
    _create_sine_generator(sine_period=20,
                           gen_period=4,
                           min_val=-100,
                           max_val=+100),
    }


_default_generator = _create_simple_generator(gen_period=5)


def generate_values(platform_id, attr_id, from_time, to_time):
    """
    Generates synthetic values within a given time window (both ends are
    inclusive). Times are NTP.

    @param platform_id  Platform ID
    @param attr_id      Attribute ID. Only the name part is considered. See OOIION-1551.
    @param from_time    lower limit of desired time window
    @param to_time      upper limit of desired time window
    """

    # get the attribute name from the given ID:
    separator = attr_id.rfind('|')
    attr_name = attr_id[:separator] if separator >= 0 else attr_id

    # try by platform/attribute:
    if (platform_id, attr_name) in _plat_attr_generators:
        gen = _plat_attr_generators[(platform_id, attr_name)]

    # else: try by the attribute only:
    elif attr_name in _attribute_generators:
        gen = _attribute_generators[attr_name]

    else:
        gen = _default_generator

    return gen(from_time, to_time)


if __name__ == "__main__":  # pragma: no cover
    # do not restrict the absolute from_time for this demo program:
    _START_TIME = 0

    import sys

    if len(sys.argv) < 5:
        print("""
        USAGE:
            oms_values.py platform_id attr_id delta_from delta_to

            Generates values in window [curr_time + delta_from, curr_time + delta_to]

        Example:
            oms_values.py Node1A input_voltage -35 0
        """)
        exit()

    cur_time = ntplib.system_to_ntp_time(time.time())

    platform_id = sys.argv[1]
    attr_id     = sys.argv[2]
    delta_from  = float(sys.argv[3])
    delta_to    = float(sys.argv[4])

    from_time   = cur_time + delta_from
    to_time     = cur_time + delta_to

    values = generate_values(platform_id, attr_id, from_time, to_time)
    print("Generated %d values in time window [%s, %s]:" % (
        len(values), from_time, to_time))
    for n, (val, t) in enumerate(values):
        print("\t%2d: %5.2f -> %+4.3f" % (n, t, val))

"""
$ bin/python  ion/agents/platform/rsn/simulator/oms_values.py Node1A other_attr -35 0
Generated 7 values in time window [3561992754.4, 3561992789.4]:
	 0: 3561992755.00 -> +990000.000
	 1: 3561992760.00 -> +990001.000
	 2: 3561992765.00 -> +990002.000
	 3: 3561992770.00 -> +990003.000
	 4: 3561992775.00 -> +990004.000
	 5: 3561992780.00 -> +990005.000
	 6: 3561992785.00 -> +990006.000

$ bin/python  ion/agents/platform/rsn/simulator/oms_values.py Node1A input_voltage -35 0
Generated 7 values in time window [3561992757.86, 3561992792.86]:
	 0: 3561992760.00 -> -0.000
	 1: 3561992765.00 -> +433.013
	 2: 3561992770.00 -> +433.013
	 3: 3561992775.00 -> +0.000
	 4: 3561992780.00 -> -433.013
	 5: 3561992785.00 -> -433.013
	 6: 3561992790.00 -> -0.000

"""
