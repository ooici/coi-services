import gevent
from mock import Mock
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest
from gevent import queue
import os
import shutil
import tempfile
import functools

from pyon.net.endpoint import RPCClient
from pyon.service.service import BaseService
from pyon.util.containers import DotDict
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.core.exception import NotFound, BadRequest
from pyon.public import log
from pyon.event.event import EventSubscriber
from pyon.agent.agent import ResourceAgentClient

from interface.services.icontainer_agent import ContainerAgentClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget, ProcessStateEnum

from ion.agents.cei.execution_engine_agent import ExecutionEngineAgentClient


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id = ''
    process_type = ''


def needs_eeagent(test):

    @functools.wraps(test)
    def wrapped(*args, **kwargs):
        try:
            import eeagent
            return test(*args, **kwargs)
        except ImportError:
            raise SkipTest("Need eeagent to run this test.")
    return wrapped


class TestProcess(BaseService):
    """Test process to deploy via EEA
    """
    name = __name__ + "test"

    def on_init(self):
        pass


@attr('INT', group='cei')
class ExecutionEngineAgentSupdIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')

        self.resource_id = "eeagent_1234"
        self._eea_name = "eeagent"

        self.supd_directory = tempfile.mkdtemp()

        self.agent_config = {
            'eeagent': {
              'heartbeat': 0,
              'slots': 100,
              'name': 'pyon_eeagent',
              'launch_type': {
                'name': 'supd',
                'pyon_directory': os.getcwd(),
                'supd_directory': self.supd_directory,
                'supdexe': 'bin/supervisord'
              },
            },
            'agent': {'resource_id': self.resource_id},
            'logging': {
            'loggers': {
              'eeagent': {
                'level': 'DEBUG',
                'handlers': ['console']
              }
            },
            'root': {
              'handlers': ['console']
            },
          }
        }

        # Start eeagent.
        self._eea_pid = None

        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        self._eea_pid = self.container_client.spawn_process(name=self._eea_name,
            module="ion.agents.cei.execution_engine_agent",
            cls="ExecutionEngineAgent", config=self.agent_config)
        log.info('Agent pid=%s.', str(self._eea_pid))

        # Start a resource agent client to talk with the instrument agent.
        self._eea_pyon_client = ResourceAgentClient(self.resource_id, process=FakeProcess())
        log.info('Got eea client %s.', str(self._eea_pyon_client))

        self.eea_client = ExecutionEngineAgentClient(self._eea_pyon_client)

    def tearDown(self):
        self.container.terminate_process(self._eea_pid)
        shutil.rmtree(self.supd_directory)

    @needs_eeagent
    def test_basics(self):
        true_u_pid = "test0"
        round = 0
        run_type = "supd"
        true_parameters = {'exec': 'true', 'argv': []}
        self.eea_client.launch_process(true_u_pid, round, run_type, true_parameters)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, true_u_pid)

        self.assertEqual(proc.get('state'), [800, 'EXITED'])

        cat_u_pid = "test1"
        round = 0
        run_type = "supd"
        cat_parameters = {'exec': 'cat', 'argv': []}
        self.eea_client.launch_process(cat_u_pid, round, run_type, cat_parameters)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, cat_u_pid)
        self.assertEqual(proc.get('state'), [500, 'RUNNING'])

        self.eea_client.terminate_process(cat_u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, cat_u_pid)
        self.assertEqual(proc.get('state'), [700, 'TERMINATED'])


@attr('INT', group='cei')
class ExecutionEngineAgentPyonSingleIntTest(IonIntegrationTestCase):
    from ion.agents.cei.execution_engine_agent import ExecutionEngineAgentClient

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')

        self.resource_id = "eeagent_123456"
        self._eea_name = "eeagent"

        self.supd_directory = tempfile.mkdtemp()

        self.agent_config = {
            'eeagent': {
              'heartbeat': 0,
              'slots': 100,
              'name': 'pyon_eeagent',
              'launch_type': {
                'name': 'pyon_single',
                'pyon_directory': os.getcwd(),
                'supd_directory': self.supd_directory,
                'supdexe': 'bin/supervisord'
              },
            },
            'agent': {'resource_id': self.resource_id},
            'logging': {
            'loggers': {
              'eeagent': {
                'level': 'DEBUG',
                'handlers': ['console']
              }
            },
            'root': {
              'handlers': ['console']
            },
          }
        }

        # Start eeagent.
        self._eea_pid = None

        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        self._eea_pid = self.container_client.spawn_process(name=self._eea_name,
            module="ion.agents.cei.execution_engine_agent",
            cls="ExecutionEngineAgent", config=self.agent_config)
        log.info('Agent pid=%s.', str(self._eea_pid))

        # Start a resource agent client to talk with the instrument agent.
        self._eea_pyon_client = ResourceAgentClient(self.resource_id, process=FakeProcess())
        log.info('Got eea client %s.', str(self._eea_pyon_client))

        self.eea_client = ExecutionEngineAgentClient(self._eea_pyon_client)

    def tearDown(self):
        self.container.terminate_process(self._eea_pid)
        shutil.rmtree(self.supd_directory)

    @needs_eeagent
    def test_basics(self):
        u_pid = "test0"
        round = 0
        run_type = "pyon_single"
        proc_name = 'test_transform'
        module = 'ion.agents.cei.test.test_eeagent'
        cls = 'TestProcess'
        parameters = {'name': proc_name, 'module': module, 'cls': cls}
        self.eea_client.launch_process(u_pid, round, run_type, parameters)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

        self.assertEqual(proc.get('state'), [500, 'RUNNING'])

        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)
        self.assertEqual(proc.get('state'), [700, 'TERMINATED'])


@attr('INT', group='cei')
class ExecutionEngineAgentPyonIntTest(IonIntegrationTestCase):
    from ion.agents.cei.execution_engine_agent import ExecutionEngineAgentClient

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')

        self.resource_id = "eeagent_123456789"
        self._eea_name = "eeagent"

        self.persistence_directory = tempfile.mkdtemp()

        self.agent_config = {
            'eeagent': {
              'heartbeat': 0,
              'slots': 100,
              'name': 'pyon_eeagent',
              'launch_type': {
                'name': 'pyon',
                'persistence_directory': self.persistence_directory,
              },
            },
            'agent': {'resource_id': self.resource_id},
            'logging': {
            'loggers': {
              'eeagent': {
                'level': 'DEBUG',
                'handlers': ['console']
              }
            },
            'root': {
              'handlers': ['console']
            },
          }
        }

        self._start_eeagent()

    def _start_eeagent(self):
        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        self.container = self.container_client._get_container_instance()

        # Start eeagent.
        self._eea_pid = self.container_client.spawn_process(name=self._eea_name,
            module="ion.agents.cei.execution_engine_agent",
            cls="ExecutionEngineAgent", config=self.agent_config)
        log.info('Agent pid=%s.', str(self._eea_pid))

        # Start a resource agent client to talk with the instrument agent.
        self._eea_pyon_client = ResourceAgentClient(self.resource_id, process=FakeProcess())
        log.info('Got eea client %s.', str(self._eea_pyon_client))

        self.eea_client = ExecutionEngineAgentClient(self._eea_pyon_client)

    def tearDown(self):
        self.container.terminate_process(self._eea_pid)
        shutil.rmtree(self.persistence_directory)

    @needs_eeagent
    def test_basics(self):
        u_pid = "test0"
        round = 0
        run_type = "pyon"
        proc_name = 'test_x'
        module = 'ion.agents.cei.test.test_eeagent'
        cls = 'TestProcess'
        parameters = {'name': proc_name, 'module': module, 'cls': cls}
        self.eea_client.launch_process(u_pid, round, run_type, parameters)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

        self.assertIsNotNone(proc, "There is no state retrieved from eeagent")
        self.assertEqual(proc.get('state'), [500, 'RUNNING'])

        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

    @needs_eeagent
    def test_kill_and_revive(self):
        """test_kill_and_revive
        Ensure that when an eeagent dies, it pulls the processes it owned from
        persistence, and marks them as failed, so the PD can figure out what to
        do with them
        """
        u_pid = "test0"
        round = 0
        run_type = "pyon"
        proc_name = 'test_transform'
        module = 'ion.agents.cei.test.test_eeagent'
        cls = 'TestProcess'
        parameters = {'name': proc_name, 'module': module, 'cls': cls}
        self.eea_client.launch_process(u_pid, round, run_type, parameters)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

        self.assertIsNotNone(proc, "There is no state retrieved from eeagent")
        self.assertEqual(proc.get('state'), [500, 'RUNNING'])

        # Kill and restart eeagent. Also, kill proc started by eea to simulate
        # a killed container
        old_eea_pid = str(self._eea_pid)
        self.container.terminate_process(self._eea_pid)
        proc_to_kill = self.container.proc_manager.procs_by_name.get(proc_name)
        self.assertIsNotNone(proc_to_kill)
        self.container.terminate_process(proc_to_kill.id)

        self._start_eeagent()

        self.assertNotEqual(old_eea_pid, self._eea_pid)

        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

        self.assertIsNotNone(proc, "There is no state retrieved from eeagent")
        self.assertEqual(proc.get('state'), [850, 'FAILED'])


def get_proc_for_upid(state, upid):

    for proc_state in state.get('processes', []):
        if proc_state.get('upid') == upid:
            return proc_state
    else:
        return None
