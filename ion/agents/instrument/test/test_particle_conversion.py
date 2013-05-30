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
        stream_name = 'parsed'
        param_dict_name = 'ctd_parsed_param_dict'
        pd_id = dataset_management.read_parameter_dictionary_by_name(param_dict_name, id_only=True)
        stream_def_id = pubsub_client.create_stream_definition(name=stream_name, parameter_dictionary_id=pd_id)
        stream_def = pubsub_client.read_stream_definition(stream_def_id)

                rdt = RecordDictionaryTool(stream_definition_id=stream_def)

                rdt = RecordDictionaryTool(stream_definition=stream_def)

            g = rdt.to_granule(data_producer_id=self._agent.resource_id, connection_id=self._connection_ID.hex,
                    connection_index=str(self._connection_index[stream_name]))
"""

"""
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_particle_conversion.py:TestParticleConversion
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_particle_conversion.py:TestParticleConversion.test_sbe37_particles
"""


TEST_PARTICLES = {
    'ctd_parsed' : {
        'param_dict_name' : 'ctd_parsed_param_dict',
        'particles' : []
    }
}

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
            #self.assertSBE37ParsedGranule(g)
            
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
            #self.assertSBE37RawGranule(g)
            
        except Exception as e:
            errmsg = 'Granule creation failed: %s' % str(s)
            errmsg += '\n stream_name: ' + stream_name
            errmsg += '\n param_dict_name: ' + param_dict_name
            errmsg += '\n particle list: %s' % str(particle_list)
            self.fail(errmsg)


