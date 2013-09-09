#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_particle_conversion
@file ion/agents.instrument/test_particle_conversion.py
@author Edward Hunter
@brief Confirm all data partles are converted to granules.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Import pyon first for monkey patching.

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

from nose.plugins.attrib import attr
from mock import patch

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase


from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.dm.utility.granule_utils import RecordDictionaryTool

from ion.agents.populate_rdt import populate_rdt

"""
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_particle_conversion.py:TestParticleConversion
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_particle_conversion.py:TestParticleConversion.test_sbe37_particles
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_particle_conversion.py:TestParticleConversion.test_vel3d_particles
"""

@attr('INT', group='mi')
class TestParticleConversion(IonIntegrationTestCase):
    """
    Test cases to confirm all data particles convert to granules successfully.
    """
    
    ############################################################################
    # Setup, teardown.
    ############################################################################
        
    def setUp(self):

        # Start container.
        log.info('Staring capability container.')
        self._start_container()

        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        
        # Create a pubsub client to create streams.
        self.pubsub_client = PubsubManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()
    
    def create_granule(self, stream_name, param_dict_name, particle_list):
        pd_id = self.dataset_management.read_parameter_dictionary_by_name(param_dict_name, id_only=True)
        stream_def_id = self.pubsub_client.create_stream_definition(name=stream_name, parameter_dictionary_id=pd_id)        
        stream_def = self.pubsub_client.read_stream_definition(stream_def_id)
        rdt = RecordDictionaryTool(stream_definition=stream_def)
        rdt = populate_rdt(rdt, particle_list)
        log.trace("RDT: %s", str(rdt))
        g = rdt.to_granule(data_producer_id='fake_agent_id')
        return g
        
    def test_sbe37_particles(self):
        """
        test_particles
        """
        stream_name = 'parsed'
        param_dict_name = 'ctd_parsed_param_dict'
        particle_list = [{u'quality_flag': u'ok',
                          u'preferred_timestamp': u'port_timestamp',
                          u'stream_name': u'parsed',
                          u'port_timestamp': 3578927139.3578925,
                          u'pkt_format_id': u'JSON_Data',
                          u'pkt_version': 1,
                          u'values': [{u'value_id': u'temp', u'value': 68.5895},
                                    {u'value_id': u'conductivity', u'value': 26.72304},
                                    {u'value_id': u'pressure', u'value': 733.303}],
                          u'driver_timestamp': 3578927139.4226017}]
        
        try:
            g = self.create_granule(stream_name, param_dict_name, particle_list)
            self.assert_granule_time(g, particle_list[0]['port_timestamp'])
            
        except Exception as e:
            errmsg = 'Granule creation failed: %s' % str(e)
            errmsg += '\n stream_name: ' + stream_name
            errmsg += '\n param_dict_name: ' + param_dict_name
            errmsg += '\n particle list: %s' % str(particle_list)
            self.fail(errmsg)
            
        stream_name = 'raw'
        param_dict_name = 'ctd_raw_param_dict'
        particle_list = [{u'quality_flag': u'ok',
                          u'preferred_timestamp': u'port_timestamp',
                          u'stream_name': u'raw',
                          u'port_timestamp': 3578927113.3578925,
                          u'pkt_format_id': u'JSON_Data',
                          u'pkt_version': 1,
                          u'values': [{u'binary': True,
                                       u'value_id': u'raw',
                                       u'value': u'ZAA='},
                                    {u'value_id': u'length',
                                     u'value': 2},
                                    {u'value_id': u'type',
                                     u'value': 1},
                                    {u'value_id': u'checksum',
                                     u'value': None}],
                          u'driver_timestamp': 3578927113.75216}]

        try:
            g = self.create_granule(stream_name, param_dict_name, particle_list)
            self.assert_granule_time(g, particle_list[0]['port_timestamp'])
            
        except Exception as e:
            errmsg = 'Granule creation failed: %s' % str(e)
            errmsg += '\n stream_name: ' + stream_name
            errmsg += '\n param_dict_name: ' + param_dict_name
            errmsg += '\n particle list: %s' % str(particle_list)
            self.fail(errmsg)

    def test_internal_time_particle(self):
        """
        Test a particle that has an internal time listed for its preferred time
        """
        stream_name = 'parsed'
        param_dict_name = 'ctd_parsed_param_dict'
        particle_list = [{u'quality_flag': u'ok',
                          u'preferred_timestamp': u'internal_timestamp',
                          u'stream_name': u'parsed',
                          u'port_timestamp': 3578927139.3578925,
                          u'internal_timestamp': 3578927039.3178925,
                          u'pkt_format_id': u'JSON_Data',
                          u'pkt_version': 1,
                          u'values': [{u'value_id': u'temp', u'value': 68.5895},
                                    {u'value_id': u'conductivity', u'value': 26.72304},
                                    {u'value_id': u'pressure', u'value': 733.303}],
                          u'driver_timestamp': 3578927139.4226017}]
        
        try:
            g = self.create_granule(stream_name, param_dict_name, particle_list)
            self.assert_granule_time(g, particle_list[0]['internal_timestamp'])
            
        except Exception as e:
            errmsg = 'Granule creation failed: %s' % str(e)
            errmsg += '\n stream_name: ' + stream_name
            errmsg += '\n param_dict_name: ' + param_dict_name
            errmsg += '\n particle list: %s' % str(particle_list)
            self.fail(errmsg)

    def test_driver_time_particle(self):
        """
        Test a particle that has a driver time listed for its preferred time
        """
        stream_name = 'parsed'
        param_dict_name = 'ctd_parsed_param_dict'
        particle_list = [{u'quality_flag': u'ok',
                          u'preferred_timestamp': u'driver_timestamp',
                          u'stream_name': u'parsed',
                          u'port_timestamp': 3578927139.3578925,
                          u'internal_timestamp': 3578927039.3178925,
                          u'pkt_format_id': u'JSON_Data',
                          u'pkt_version': 1,
                          u'values': [{u'value_id': u'temp', u'value': 68.5895},
                                    {u'value_id': u'conductivity', u'value': 26.72304},
                                    {u'value_id': u'pressure', u'value': 733.303}],
                          u'driver_timestamp': 3578927139.4226017}]
        
        try:
            g = self.create_granule(stream_name, param_dict_name, particle_list)
            self.assert_granule_time(g, particle_list[0]['driver_timestamp'])
            
        except Exception as e:
            errmsg = 'Granule creation failed: %s' % str(e)
            errmsg += '\n stream_name: ' + stream_name
            errmsg += '\n param_dict_name: ' + param_dict_name
            errmsg += '\n particle list: %s' % str(particle_list)
            self.fail(errmsg)

    def test_vel3d_particles(self):
        """
        test_particles
        """
        
        
        stream_name = 'vel3d_parsed'
        param_dict_name = 'vel3d_b_sample'
        particle_list = [
            {
                "driver_timestamp": 3579022766.361967,
                "internal_timestamp": 3579047922.0,
                "pkt_format_id": "JSON_Data",
                "pkt_version": 1,
                "port_timestamp": 3579022762.357902,
                "preferred_timestamp": "port_timestamp",
                "quality_flag": "ok",
                "stream_name": "vel3d_b_sample",
                "values": [
                    {"value": 3579047922.0, "value_id": "date_time_string"},
                    {"value": 5, "value_id": "fractional_second"},
                    {"value": "8000", "value_id": "velocity_beam_a"},
                    {"value": "8000", "value_id": "velocity_beam_b"},
                    {"value": "8000", "value_id": "velocity_beam_c"},
                    {"value": "8000", "value_id": "velocity_beam_d"},
                    {"value": 999.0, "value_id": "turbulent_velocity_east"},
                    {"value": 999.0, "value_id": "turbulent_velocity_north"},
                    {"value": 999.0, "value_id": "turbulent_velocity_up"},
                    {"value": 2.16, "value_id": "temperature"},
                    {"value": 1.0, "value_id": "mag_comp_x"},
                    {"value": -0.0, "value_id": "mag_comp_y"},
                    {"value": -7.9, "value_id": "pitch"},
                    {"value": -78.2, "value_id": "roll"}]
            }
        ]
        class RDT(dict):
            def __init__(self):
                super(RDT, self).__init__()
                self.temporal_parameter = None

        rdt = RDT()
        for x in particle_list[0]['values']:
            rdt[x['value_id']] = None
        
        rdt = populate_rdt(rdt, particle_list)
        
    def assert_granule_time(self, granule, target_time):
        """
        Assert that the granule's time matches the target time
        @param granule The granule to be searched
        @param target_time The time that should match the granule's overall time
        """
        rdt = RecordDictionaryTool.load_from_granule(granule)
        rdt_time = rdt['time'][0]
        log.debug("assert_granule_time granule time: %s", rdt_time)
        log.debug("assert_granule_time target timestamp: %s", target_time)
        self.assertEqual(rdt_time, target_time)

"""

Record Dictionary [
    'quality_flag',
    'grt_min_value_f99940f67e53441ebf6eaded5881a83b',
    'tempwat_stuckvl_qc',
    'density_stuckvl_qc',
    'grt_min_value_c4b779547a3c4edb8d14dbd8a520e8fa',
    'pracsal_stuckvl_qc',
    'svt_n_51b44c0182a44be1ad8a827afc403c44',
    'preswat_spketst_qc',
    'spike_l_23f7af5297644c419ec77e6c96777b4a',
    'preswat_glblrng_qc',
    'svt_n_39da4ac3f5ef4152abdd50d3472efe55',
    'acc_651a70a20cac4e3e953c2a15d73ef171',
    'preswat_stuckvl_qc',
    'conductivity',
    'condwat_spketst_qc',
    'spike_n_eb059db2d3264b23bb3609684a41a333',
    'grt_max_value_6357f533576f44ada6d4c8f30e274089',
    'driver_timestamp',
    'spike_n_7b3dce333a494b9b83fe5cd5d7d5ef01',
    'pracsal_glblrng_qc',
    'density', '
    tempwat_spketst_qc', '
    lon', '
    tempwat_glblrng_qc',
    'spike_n_c25fb874f00f4084a47b2edc008486b2',
    'acc_6780ea4e572b430a8934a48847150830',
    'internal_timestamp',
    'grt_min_value_f86c37ad7e0941dc9cce92f1e4d50d2c',
    'spike_l_a2e04d47f5c04f119eb07159e3db7ede',
    'spike_l_43f0eb0bbbc34e668301a563f1e3585f',
    'acc_2aa1216c834a46a98e962c29913922ea',
    'svt_resolution_855bb319e4c347528bf674e6c70b6188',
    'spike_l_fb2cd584034e44458e075b6c912d82c9',
    'svt_resolution_b9bc906bd0ab4ba0a3b980f347f2bbe6',
    'svt_n_ea91be735c00412fb285134bb8f9a2bc',
    'ingestion_timestamp',
    'port_timestamp',
    'grt_max_value_acf779e1867e48aeae401ca18ebb950f',
    'svt_resolution_9290d7d9fdad4d288f75016fec58dde9',
    'grt_max_value_df91866a9f534b7aad977df31dfd690d',
    'acc_7b142c686b114a1b9dcd7d5d6ce448a5',
    'grt_max_value_4e1f0fddee90478b92a652c3d8b96338',
    'pressure',
    'svt_resolution_1ec434c55b3d4510b2397b756c06ca44',
    'lat', 'spike_l_64e7bc7a88ea4987b239b8f0c00fd36e',
    'svt_n_3e74b0295024435b9f5789f496f6ebe6',
    'spike_n_4fce82015283474cb7792defa838affb',
    'preferred_timestamp',
    'pracsal_spketst_qc',
    'svt_n_a5f28b56d08f42a6af2909815a8f19dd',
    'density_glblrng_qc',
    'temp',
    'grt_min_value_9a408b073a7242f8a83e235f8f9772a8',
    'grt_min_value_3aa45de0bfb9494d8cce4609536a572c',
    'svt_resolution_958b8a79fe954bd7a8cd230d6d64a0b6',
    'condwat_glblrng_qc',
    'spike_n_4e9a461de275475085a7aa7e30477b8c',
    'salinity',
    'condwat_stuckvl_qc',
    'grt_max_value_d7e98b8bc4e7406f820b8a2d5ca7082a',
    'acc_f2f27937060a4b93bcd47e9f35acd12f',
    'time', '
    density_spketst_qc'
    ]


{"driver_timestamp": 3579022768.101987,
 "internal_timestamp": 3579047923.0,
 "pkt_format_id": "JSON_Data",
 "pkt_version": 1,
 "port_timestamp": 3579022764.357902,
 "preferred_timestamp": "port_timestamp",
 "quality_flag": "ok",
 "stream_name": "vel3d_b_sample",
 "values": [
    {"value": 3579047923.0, "value_id": "date_time_string"},
    {"value": 6, "value_id": "fractional_second"},
    {"value": "8000", "value_id": "velocity_beam_a"},
    {"value": "8000", "value_id": "velocity_beam_b"},
    {"value": "8000", "value_id": "velocity_beam_c"},
    {"value": "8000", "value_id": "velocity_beam_d"},
    {"value": 999.0, "value_id": "turbulent_velocity_east"},
    {"value": 999.0, "value_id": "turbulent_velocity_north"},
    {"value": 999.0, "value_id": "turbulent_velocity_up"},
    {"value": 2.16, "value_id": "temperature"},
    {"value": 1.0, "value_id": "mag_comp_x"},
    {"value": -0.01, "value_id": "mag_comp_y"},
    {"value": -8.0, "value_id": "pitch"},
    {"value": -78.0, "value_id": "roll"}]}

[
    'turbulent_velocity_east: <coverage_model.parameter_values.NumericValue object at 0x10e811e10>',
    'pitch: <coverage_model.parameter_values.NumericValue object at 0x10e811890>',
    'velocity_beam_c: <coverage_model.parameter_values.ArrayValue object at 0x10c8fc410>',
    'velocity_beam_b: <coverage_model.parameter_values.ArrayValue object at 0x10c8fc950>',
    'velocity_beam_a: <coverage_model.parameter_values.ArrayValue object at 0x10c8fc810>',
    'driver_timestamp: <coverage_model.parameter_values.NumericValue object at 0x10e811ad0>',
    'velocity_beam_d: <coverage_model.parameter_values.ArrayValue object at 0x10ebc76d0>',
    'temperature: <coverage_model.parameter_values.NumericValue object at 0x10eb30050>',
    'internal_timestamp: <coverage_model.parameter_values.NumericValue object at 0x10eb30c10>',
    'mag_comp_x: <coverage_model.parameter_values.NumericValue object at 0x10ebc7090>',
    'mag_comp_y: <coverage_model.parameter_values.NumericValue object at 0x10c209c50>',
    'roll: <coverage_model.parameter_values.NumericValue object at 0x10e811510>',
    'date_time_string: <coverage_model.parameter_values.ArrayValue object at 0x10e8115d0>',
    'port_timestamp: <coverage_model.parameter_values.NumericValue object at 0x10e811650>',
    'turbulent_velocity_north: <coverage_model.parameter_values.NumericValue object at 0x10e8119d0>',
    'preferred_timestamp: <coverage_model.parameter_values.CategoryValue object at 0x10e811850>',
    'turbulent_velocity_up: <coverage_model.parameter_values.NumericValue object at 0x10e811d50>',
    'time: <coverage_model.parameter_values.NumericValue object at 0x10c8fc890>']

{
    "driver_timestamp": 3579022766.361967,
    "internal_timestamp": 3579047922.0,
    "pkt_format_id": "JSON_Data",
    "pkt_version": 1,
    "port_timestamp": 3579022762.357902,
    "preferred_timestamp": "port_timestamp",
    "quality_flag": "ok",
    "stream_name": "vel3d_b_sample",
    "values": [
        {"value": 3579047922.0, "value_id": "date_time_string"},
        {"value": 5, "value_id": "fractional_second"},
        {"value": "8000", "value_id": "velocity_beam_a"},
        {"value": "8000", "value_id": "velocity_beam_b"},
        {"value": "8000", "value_id": "velocity_beam_c"},
        {"value": "8000", "value_id": "velocity_beam_d"},
        {"value": 999.0, "value_id": "turbulent_velocity_east"},
        {"value": 999.0, "value_id": "turbulent_velocity_north"},
        {"value": 999.0, "value_id": "turbulent_velocity_up"},
        {"value": 2.16, "value_id": "temperature"},
        {"value": 1.0, "value_id": "mag_comp_x"},
        {"value": -0.0, "value_id": "mag_comp_y"},
        {"value": -7.9, "value_id": "pitch"},
        {"value": -78.2, "value_id": "roll"}]
}

[
    'turbulent_velocity_east: <coverage_model.parameter_values.NumericValue object at 0x10069fb90>',
    'pitch: <coverage_model.parameter_values.NumericValue object at 0x10069f6d0>',
    'velocity_beam_c: <coverage_model.parameter_values.ArrayValue object at 0x100696ed0>',
    'velocity_beam_b: <coverage_model.parameter_values.ArrayValue object at 0x100696f10>',
    'velocity_beam_a: <coverage_model.parameter_values.ArrayValue object at 0x1006968d0>',
    'driver_timestamp: <coverage_model.parameter_values.NumericValue object at 0x10c1fea10>',
    'velocity_beam_d: <coverage_model.parameter_values.ArrayValue object at 0x100696e50>',
    'temperature: <coverage_model.parameter_values.NumericValue object at 0x100696fd0>',
    'internal_timestamp: <coverage_model.parameter_values.NumericValue object at 0x100696550>',
    'mag_comp_x: <coverage_model.parameter_values.NumericValue object at 0x100696c90>',
    'roll: <coverage_model.parameter_values.NumericValue object at 0x10069f190>',
    'date_time_string: <coverage_model.parameter_values.ArrayValue object at 0x10069f150>',
    'port_timestamp: <coverage_model.parameter_values.NumericValue object at 0x10069f490>',
    'turbulent_velocity_north: <coverage_model.parameter_values.NumericValue object at 0x10eb50dd0>',
    'preferred_timestamp: <coverage_model.parameter_values.CategoryValue object at 0x10cb2c6d0>',
    'turbulent_velocity_up: <coverage_model.parameter_values.NumericValue object at 0x10069fb50>',
    'time: <coverage_model.parameter_values.NumericValue object at 0x100696b10>']
"""
