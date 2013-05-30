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
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_particle_conversion.py:TestParticleConversion.test_particles
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
    
    
    def test_particles(self):
        """
        test_particles
        """
        pass




