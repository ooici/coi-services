#!/usr/bin/env python

"""
@package ion.agents.data.test.test_hypm_ctd_0_0_1
@file ion/agents/data/test_hypm_ctd_0_0_1.py
@author Bill French
@brief End to end testing for hypm ctd version 0.0.1
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import gevent
from pyon.public import log
from nose.plugins.attrib import attr

from ion.agents.data.test.dataset_test import DatasetAgentTestCase
from ion.services.dm.test.dm_test_case import breakpoint
import unittest

###############################################################################
# Global constants.
###############################################################################


@attr('INT', group='sa')
class HypmCTDTest(DatasetAgentTestCase):
    """
    Verify dataset agent can harvest data fails, parse the date, publish,
    ingest and retrieve stored data.
    """
    def setUp(self):
        self.test_config.initialize(
            instrument_device_name = 'CTDPF',
            preload_scenario= 'CTDPF',
            stream_name= 'ctdpf_parsed',

            # Uncomment this line to load driver from a locak repository
            #mi_repo = '/Users/wfrench/Workspace/code/wfrench/marine-integrations'
        )

        super(HypmCTDTest, self).setUp()

    def test_parse(self):
        """
        Verify file import and connection ids
        """
        self.assert_initialize()
        self.assert_set_pubrate(0)

        self.create_sample_data("hypm_ctdpf/test_data_1.txt", "DATA0003.txt")
        self.create_sample_data("hypm_ctdpf/test_data_4.txt", "DATA0004.txt")

        granules = self.get_samples(self.test_config.stream_name, 9)
        self.assert_data_values(granules, 'hypm_ctdpf/merged_result.yml')

    def test_large_file(self):
        """
        Verify a large file import with no buffering
        """
        self.assert_initialize()
        self.assert_set_pubrate(5)

        self.create_sample_data("hypm_ctdpf/DAT0003.txt", "DAT004.txt")
        self.get_samples(self.test_config.stream_name, sample_count=436, timeout=120)
        self.assert_sample_queue_size(self.test_config.stream_name, 0)

        self.assert_reset()

    def test_capabilities(self):
        self.assert_agent_capabilities()
