#!/usr/bin/env python

"""
@package ion.agents.data.test.test_hypm_ctd_0_0_1
@file ion/agents/data/test_hypm_ctd_0_0_1.py
@author Bill French
@brief End to end testing for hypm ctd version 0.0.1
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

from pyon.public import log
from nose.plugins.attrib import attr

from ion.agents.data.test.dataset_test import DatasetAgentTestCase
from ion.services.dm.test.dm_test_case import breakpoint

###############################################################################
# Global constants.
###############################################################################


@attr('INT', group='mi')
class HypmCTDTest(DatasetAgentTestCase):
    """
    Verify dataset agent can harvest data fails, parse the date, publish,
    ingest and retrieve stored data.
    """
    def setUp(self):
        self.test_config.initialize(
            instrument_device_name = 'CTDPF',
            preload_scenario= 'CTDPF'
        )

        super(HypmCTDTest, self).setUp()

    def test_init(self):
        """
        """
        self.assert_initialize()

        self.create_sample_data("hypm_ctdpf/DAT0003.txt")

        self.assert_reset()
