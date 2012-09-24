#!/usr/bin/env python

"""
@package ion.agents.platform.test.test_platform_launcher
@file    ion/agents/platform/test/test_platform_launcher.py
@author  Carlos Rueda
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.agents.platform.platform_agent_launcher import LauncherFactory
from pyon.util.int_test import IonIntegrationTestCase

from gevent import sleep

from nose.plugins.attrib import attr


@attr('INT', group='sa')
class Test(IonIntegrationTestCase):

    def test(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        launcher = LauncherFactory.createLauncher()

        platform_id = "My_platformId"
        PA_RESOURCE_ID = 'My_platformId_001'
        agent_config =  {
            'agent'         : {'resource_id': PA_RESOURCE_ID},
            'stream_config' : {}
        }

        pid = launcher.launch(platform_id, agent_config)

        sleep(2)

        launcher.cancel_process(pid)
