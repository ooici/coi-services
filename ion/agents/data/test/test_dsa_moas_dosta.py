#!/usr/bin/env python

"""
@package ion.agents.data.test.test_moas_dosta
@file ion/agents/data/test_moas_dosta
@author Bill French
@brief End to end testing for moas dosta
"""

__author__ = 'Bill French'


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
class HypmDOSTATest(DatasetAgentTestCase):
    """
    Verify dataset agent can harvest data fails, parse the date, publish,
    ingest and retrieve stored data.
    """
    def setUp(self):
        self.test_config.initialize(
            instrument_device_name = 'DOSTA-01',
            preload_scenario= 'GENG,DOSTA',
            stream_name= 'ggldr_dosta_delayed',

            # Uncomment this line to load driver from a locak repository
            #mi_repo = '/Users/wfrench/Workspace/code/wfrench/marine-integrations'
        )

        super(HypmDOSTATest, self).setUp()

    def test_parse(self):
        """
        Verify file import and connection ids
        """
        self.assert_initialize()

        self.create_sample_data("moas_dosta/file_1.mrg", "unit_363_2013_245_6_6.mrg")
        self.create_sample_data("moas_dosta/file_2.mrg", "unit_363_2013_245_10_6.mrg")

        granules = self.get_samples(self.test_config.stream_name, 4)
        self.assert_data_values(granules, 'moas_dosta/merged.result.yml')

    def test_large_file(self):
        """
        Verify a large file import with no buffering
        """
        self.assert_initialize()

        self.create_sample_data("moas_dosta/unit_363_2013_199_0_0.mrg", "unit_363_2013_199_0_0.mrg")
        gevent.sleep(10)
        self.assert_sample_queue_size(self.test_config.stream_name, 1)

        self.create_sample_data("moas_dosta/unit_363_2013_199_1_0.mrg", "unit_363_2013_199_1_0.mrg")
        gevent.sleep(10)
        self.assert_sample_queue_size(self.test_config.stream_name, 2)

        self.create_sample_data("moas_dosta/unit_363_2013_245_6_6.mrg", "unit_363_2013_245_6_6.mrg")
        self.get_samples(self.test_config.stream_name, 171, 180)
        self.assert_sample_queue_size(self.test_config.stream_name, 0)

    def test_capabilities(self):
        self.assert_agent_capabilities()
