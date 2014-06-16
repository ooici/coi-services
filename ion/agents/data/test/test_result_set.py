#!/usr/bin/env python

"""
@package mi.idk.test.test_result_set
@file mi.idk/test/test_result_set.py
@author Bill French
@brief Read a result set file and test the verification methods
"""

__author__ = 'Bill French'


import os
import re
import numpy as np
import uuid

from pyon.public import log
from nose.plugins.attrib import attr
from mock import Mock
from ion.agents.data.result_set import ResultSet
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType, ArrayType, RecordType
from ion.agents.populate_rdt import populate_rdt

@attr('UNIT', group='mi')
class TestResultSet(IonIntegrationTestCase):
    """
    Test the metadata object
    """
    def _get_result_set_file(self, filename):
        """
        return the full path to the result_set_file in
        the same directory as the test file.
        """
        test_dir = os.path.dirname(__file__)
        return os.path.join(test_dir, filename)

    def setUp(self):
        """
        Setup the test case
        """

    def test_ntp_conversion(self):
        rs = ResultSet(self._get_result_set_file("result_set_file.yml"))
        ts = rs._string_to_ntp_date_time("1970-01-01T00:00:00.00Z")
        self.assertEqual(ts, 2208988800.0)

        ts = rs._string_to_ntp_date_time("1970-01-01T00:00:00.00")
        self.assertEqual(ts, 2208988800.0)

        ts = rs._string_to_ntp_date_time("1970-01-01T00:00:00")
        self.assertEqual(ts, 2208988800.0)

        ts = rs._string_to_ntp_date_time("1970-01-01T00:00:00Z")
        self.assertEqual(ts, 2208988800.0)

        ts = rs._string_to_ntp_date_time("1970-01-01T00:01:00.101Z")
        self.assertEqual(ts, 2208988860.101)

        self.assertRaises(ValueError, rs._string_to_ntp_date_time, "09/05/2013 02:47:21.000")

    def get_param_dict(self):
        pdict = ParameterDictionary()

        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.float64))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0
        pdict.add_context(cond_ctxt)

        pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=np.float64))
        pres_ctxt.uom = 'unknown'
        pres_ctxt.fill_value = 0x0
        pdict.add_context(pres_ctxt)

        temp_ctxt = ParameterContext('temperature', param_type=QuantityType(value_encoding=np.float64))
        temp_ctxt.uom = 'unknown'
        temp_ctxt.fill_value = 0x0
        pdict.add_context(temp_ctxt)

        oxy_ctxt = ParameterContext('oxygen', param_type=QuantityType(value_encoding=np.float64))
        oxy_ctxt.uom = 'unknown'
        oxy_ctxt.fill_value = 0x0
        pdict.add_context(oxy_ctxt)

        internal_ts_ctxt = ParameterContext(name='internal_timestamp', param_type=QuantityType(value_encoding=np.float64))
        internal_ts_ctxt._derived_from_name = 'time'
        internal_ts_ctxt.uom = 'seconds'
        internal_ts_ctxt.fill_value = -1
        pdict.add_context(internal_ts_ctxt, is_temporal=True)

        driver_ts_ctxt = ParameterContext(name='driver_timestamp', param_type=QuantityType(value_encoding=np.float64))
        driver_ts_ctxt._derived_from_name = 'time'
        driver_ts_ctxt.uom = 'seconds'
        driver_ts_ctxt.fill_value = -1
        pdict.add_context(driver_ts_ctxt)

        return pdict

    def get_particle(self, timestamp, cond, pres, temp, oxygen, new_sequence = False):
        return {
            'quality_flag': 'ok',
            'preferred_timestamp': 'internal_timestamp',
            'stream_name': 'ctdpf_parsed',
            'pkt_format_id': 'JSON_Data',
            'pkt_version': 1,
            'internal_timestamp': timestamp,
            'values':
                [
                    {'value_id': 'temperature', 'value': temp},
                    {'value_id': 'conductivity', 'value': cond},
                    {'value_id': 'pressure', 'value': pres},
                    {'value_id': 'oxygen', 'value': oxygen}
                ],
            'driver_timestamp': timestamp+1,
            'new_sequence': new_sequence
        }

    def create_test_granules(self, buffer_data=False):
        """
        Generate test granules from particles.  If buffer data is set to true then
        try to buffer data into a granule.  If the particle has the new sequence
        flag set then a new granule will be generated.  This method emulates the
        agent_stream_publisher module.
        :return: list of granules generated.
        """
        base_timestamp = 3583861263.0
        connection_index = 0

        particles = []
        particles.append(self.get_particle(base_timestamp, 10.5914, 161.06, 4.1870, 2693.0))
        particles.append(self.get_particle(base_timestamp+1, 10.5915, 161.07, 4.1871, 2693.1))
        particles.append(self.get_particle(base_timestamp+2, 10.5916, 161.08, 4.1872, 2693.2))
        particles.append(self.get_particle(base_timestamp+3, 10.5917, 161.09, 4.1873, 2693.3, True))
        particles.append(self.get_particle(base_timestamp+4, 10.5918, 161.10, 4.1874, 2693.4))

        data_groups = []
        result_granules = []
        data_groups_index = 0

        for particle in particles:
            # If we need a new connection then start a new group, but only if we have found
            # something in the current group
            if (particle.get('new_sequence', False) or buffer_data == False) and \
               (len(data_groups) > 0 and len(data_groups[data_groups_index]) > 0):
                data_groups_index += 1

            if len(data_groups) <= data_groups_index:
                data_groups.append([])

            data_groups[data_groups_index].append(particle)

        log.debug("Granules to create: %s", len(data_groups))

        for data in data_groups:
            connection_id = uuid.uuid4()
            connection_index += 1
            rdt = RecordDictionaryTool(param_dictionary=self.get_param_dict())

            rdt = populate_rdt(rdt, data)

            g = rdt.to_granule(data_producer_id='agent_res_id', connection_id=connection_id.hex,
                               connection_index=str(connection_index))

            result_granules.append(g)

        return result_granules

    def test_extract_data(self):
        rs = ResultSet(self._get_result_set_file("result_set_file.yml"))

        granules = self.create_test_granules()
        self.assertEqual(len(granules), 5)

        result = rs._extract_granule_data(granules)
        self.assertEqual(len(result), 5)

        granules = self.create_test_granules(True)
        self.assertEqual(len(granules), 2)
        result = rs._extract_granule_data(granules)
        self.assertEqual(len(result), 5)

    def test_simple_set(self):
        rs = ResultSet(self._get_result_set_file("result_set_file.yml"))

        granules = self.create_test_granules(True)
        self.assertEqual(len(granules), 2)
        self.assertTrue(rs.verify(granules))

