#!/usr/bin/env python

"""
@package ion.agents.data.test.test_moas_ctdgv
@file ion/agents/data/test_moas_ctdgv
@author Bill French
@brief End to end testing for moas ctdgv
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import gevent
import os
from pyon.public import log
from nose.plugins.attrib import attr

from ion.agents.data.test.dataset_test import DatasetAgentTestCase
from ion.services.dm.test.dm_test_case import breakpoint
from pyon.agent.agent import ResourceAgentState
import unittest

###############################################################################
# Global constants.
###############################################################################


@attr('INT', group='sa')
class GliderCTDTest(DatasetAgentTestCase):
    """
    Verify dataset agent can harvest data fails, parse the date, publish,
    ingest and retrieve stored data.
    """
    def setUp(self):
        self.test_config.initialize(
            instrument_device_name = 'CTDGV-01',
            preload_scenario= 'GENG,CTDGV',
            stream_name= 'ggldr_ctdgv_delayed',

            # Uncomment this line to load driver from a local repository
            #mi_repo = '/Users/wfrench/Workspace/code/wfrench/marine-integrations'
        )

        super(GliderCTDTest, self).setUp()

    def test_parse(self):
        """
        Verify file import and connection ids
        """
        self.assert_initialize()

        self.create_sample_data("moas_ctdgv/file_1.mrg", "unit_363_2013_245_6_6.mrg")
        self.create_sample_data("moas_ctdgv/file_2.mrg", "unit_363_2013_245_10_6.mrg")

        granules = self.get_samples(self.test_config.stream_name, 4)
        self.assert_data_values(granules, 'moas_ctdgv/merged.result.yml')
        self.assert_sample_queue_size(self.test_config.stream_name, 0)

    def test_large_file(self):
        """
        Verify a large file import with no buffering
        """
        self.assert_initialize()

        self.create_sample_data("moas_ctdgv/unit_363_2013_199_0_0.mrg", "unit_363_2013_199_0_0.mrg")
        gevent.sleep(10)
        self.assert_sample_queue_size(self.test_config.stream_name, 1)

        self.create_sample_data("moas_ctdgv/unit_363_2013_199_1_0.mrg", "unit_363_2013_199_1_0.mrg")
        gevent.sleep(10)
        self.assert_sample_queue_size(self.test_config.stream_name, 2)

        self.create_sample_data("moas_ctdgv/unit_363_2013_245_6_6.mrg", "unit_363_2013_245_6_6.mrg")
        self.get_samples(self.test_config.stream_name, 171, 180)
        self.assert_sample_queue_size(self.test_config.stream_name, 0)

    def test_capabilities(self):
        self.assert_agent_capabilities()

    def test_lost_connection(self):
        """
        Test a parser exception and verify that the lost connection logic works
        """
        self.assert_initialize()

        path = self.create_sample_data("moas_ctdgv/file_1.mrg", "unit_363_2013_245_6_6.mrg")
        os.chmod(path, 0000)

        self.assert_state_change(ResourceAgentState.LOST_CONNECTION)

        # Sleep long enough to let the first reconnect happen and fail again.
        gevent.sleep(65)

        # Resolve the issue
        os.chmod(path, 0755)

        # We should transition back to streaming and stay there.
        self.assert_state_change(ResourceAgentState.STREAMING, timeout=180)
        self.create_sample_data("moas_ctdgv/file_2.mrg", "unit_363_2013_245_10_6.mrg")

        granules = self.get_samples(self.test_config.stream_name, 4, timeout=30)
        self.assert_data_values(granules, 'moas_ctdgv/merged.result.yml')
