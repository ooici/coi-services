import gevent
from mock import Mock
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest
from gevent import queue
import os
import uuid
import os.path
import shutil
import tempfile
import socket
import functools

from random import randint
from BaseHTTPServer import HTTPServer
import SimpleHTTPServer

from pyon.service.service import BaseService
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from pyon.public import log
from pyon.agent.simple_agent import SimpleResourceAgentClient
from pyon.core.exception import Timeout

from interface.services.icontainer_agent import ContainerAgentClient

from ion.agents.cei.execution_engine_agent import ExecutionEngineAgentClient, HeartBeater


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

class TestProcessFail(BaseService):
    """Test process to deploy via EEA
    """
    name = __name__ + "test"

    def on_init(self):
        raise Exception("KABOOM")

class TestProcessSlowStart(BaseService):
    """Test process to deploy via EEA
    """
    name = __name__ + "test"

    def on_init(self):
        log.info("Waiting for TestProcessSlowStart to start")
        gevent.sleep(2)
        log.info("TestProcessSlowStart started")


@attr('INT', group='cei')
class ExecutionEngineAgentSupdIntTest(IonIntegrationTestCase):

    @needs_eeagent
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
        self._eea_pyon_client = SimpleResourceAgentClient(self.resource_id, process=FakeProcess())
        log.info('Got eea client %s.', str(self._eea_pyon_client))

        self.eea_client = ExecutionEngineAgentClient(self._eea_pyon_client)

    def tearDown(self):
        self.container.terminate_process(self._eea_pid)
        shutil.rmtree(self.supd_directory)

    def wait_for_state(self, upid, desired_state, timeout=5):
        attempts = 0
        while timeout > attempts:
            state = self.eea_client.dump_state().result
            proc = get_proc_for_upid(state, upid)
            if proc.get('state') == desired_state:
                return
            gevent.sleep(1)
            attempts += 1

        assert False, "Process %s took too long to get to %s" % (upid, desired_state)

    @needs_eeagent
    def test_basics(self):
        true_u_pid = "test0"
        round = 0
        run_type = "supd"
        true_parameters = {'exec': 'true', 'argv': []}
        self.eea_client.launch_process(true_u_pid, round, run_type, true_parameters)

        self.wait_for_state(true_u_pid, [800, 'EXITED'])

        cat_u_pid = "test1"
        round = 0
        run_type = "supd"
        cat_parameters = {'exec': 'cat', 'argv': []}
        self.eea_client.launch_process(cat_u_pid, round, run_type, cat_parameters)
        self.wait_for_state(cat_u_pid, [500, 'RUNNING'])

        self.eea_client.terminate_process(cat_u_pid, round)
        self.wait_for_state(cat_u_pid, [700, 'TERMINATED'])


@attr('INT', group='cei')
class ExecutionEngineAgentPyonSingleIntTest(IonIntegrationTestCase):

    @needs_eeagent
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
        self._eea_pyon_client = SimpleResourceAgentClient(self.resource_id, process=FakeProcess())
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

    _webserver = None

    @needs_eeagent
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
              }
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
        self._eea_pyon_client = SimpleResourceAgentClient(self.resource_id, process=FakeProcess())
        log.info('Got eea client %s.', str(self._eea_pyon_client))

        self.eea_client = ExecutionEngineAgentClient(self._eea_pyon_client)

    def tearDown(self):
        self._stop_webserver()
        self.container.terminate_process(self._eea_pid)
        shutil.rmtree(self.persistence_directory)

    def _start_webserver(self, directory_to_serve, port=None):
        """ Start a webserver for testing code download
        Note: tries really hard to get a port, and if it can't use
        the suggested port, randomly picks another, and returns it
        """
        def log_message(self, format, *args):
            #swallow log massages
            pass

        class Server(HTTPServer):

            requests = 0

            def serve_forever(self):
                self._serving = 1
                while self._serving:
                    self.handle_request()
                    self.requests += 1

            def stop(self):
                self._serving = 0

        if port is None:
            port = 8008
        Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
        Handler.log_message = log_message

        for i in range(0, 100):
            try:
                self._webserver = Server(("localhost", port), Handler)
            except socket.error:
                print "port %s is in use, picking another" % port
                port = randint(8000, 10000)
                continue
            else:
                break

        self._web_glet = gevent.spawn(self._webserver.serve_forever)
        return port

    def _stop_webserver(self):
        if self._webserver is not None:
            self._web_glet.kill()

    def _enable_code_download(self, whitelist=None):

        if whitelist is None:
            whitelist = []

        self.container.terminate_process(self._eea_pid)
        self.agent_config['eeagent']['code_download'] = {
            'enabled': True,
            'whitelist': whitelist
        }
        self._start_eeagent()

    def wait_for_state(self, upid, desired_state, timeout=30):
        attempts = 0
        last_state = None
        while timeout > attempts:
            try:
                state = self.eea_client.dump_state().result
            except Timeout:
                log.warn("Timeout calling EEAgent dump_state. retrying.")
                continue
            proc = get_proc_for_upid(state, upid)
            last_state = proc.get('state')
            if last_state == desired_state:
                return
            gevent.sleep(1)
            attempts += 1

        assert False, "Process %s took too long to get to %s, had %s" % (upid, desired_state, last_state)

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
        self.wait_for_state(u_pid, [500, 'RUNNING'])

        state = self.eea_client.dump_state().result
        assert len(state['processes']) == 1

        self.eea_client.terminate_process(u_pid, round)
        self.wait_for_state(u_pid, [700, 'TERMINATED'])

        state = self.eea_client.dump_state().result
        assert len(state['processes']) == 1

        self.eea_client.cleanup_process(u_pid, round)
        state = self.eea_client.dump_state().result
        assert len(state['processes']) == 0

    @needs_eeagent
    def test_restart(self):
        u_pid = "test0"
        round = 0
        run_type = "pyon"
        proc_name = 'test_x'
        module = 'ion.agents.cei.test.test_eeagent'
        cls = 'TestProcess'
        parameters = {'name': proc_name, 'module': module, 'cls': cls}

        self.eea_client.launch_process(u_pid, round, run_type, parameters)
        self.wait_for_state(u_pid, [500, 'RUNNING'])

        state = self.eea_client.dump_state().result
        assert len(state['processes']) == 1

        # Start again with incremented round. eeagent should restart the process
        round += 1

        self.eea_client.launch_process(u_pid, round, run_type, parameters)
        self.wait_for_state(u_pid, [500, 'RUNNING'])

        state = self.eea_client.dump_state().result
        ee_round = state['processes'][0]['round']
        assert round == int(ee_round)

        # TODO: this test is disabled, as the restart op is disabled
        # Run restart with incremented round. eeagent should restart the process
        #round += 1

        #self.eea_client.restart_process(u_pid, round)
        #self.wait_for_state(u_pid, [500, 'RUNNING'])

        #state = self.eea_client.dump_state().result
        #ee_round = state['processes'][0]['round']
        #assert round == int(ee_round)

        self.eea_client.terminate_process(u_pid, round)
        self.wait_for_state(u_pid, [700, 'TERMINATED'])

        state = self.eea_client.dump_state().result
        assert len(state['processes']) == 1

        self.eea_client.cleanup_process(u_pid, round)
        state = self.eea_client.dump_state().result
        assert len(state['processes']) == 0

    @needs_eeagent
    def test_failing_process(self):
        u_pid = "testfail"
        round = 0
        run_type = "pyon"
        proc_name = 'test_x'
        module = 'ion.agents.cei.test.test_eeagent'
        cls = 'TestProcessFail'
        parameters = {'name': proc_name, 'module': module, 'cls': cls}
        self.eea_client.launch_process(u_pid, round, run_type, parameters)

        self.wait_for_state(u_pid, [850, 'FAILED'])

        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

    @needs_eeagent
    def test_slow_to_start(self):
        upids = map(lambda i: str(uuid.uuid4().hex), range(0, 10))
        round = 0
        run_type = "pyon"
        proc_name = 'test_x'
        module = 'ion.agents.cei.test.test_eeagent'
        cls = 'TestProcessSlowStart'
        parameters = {'name': proc_name, 'module': module, 'cls': cls}
        for upid in upids:
            self.eea_client.launch_process(upid, round, run_type, parameters)

        for upid in upids:
            self.wait_for_state(upid, [500, 'RUNNING'], timeout=60)

    @needs_eeagent
    def test_start_cancel(self):
        upid = str(uuid.uuid4().hex)
        round = 0
        run_type = "pyon"
        proc_name = 'test_x'
        module = 'ion.agents.cei.test.test_eeagent'
        cls = 'TestProcessSlowStart'
        parameters = {'name': proc_name, 'module': module, 'cls': cls}
        self.eea_client.launch_process(upid, round, run_type, parameters)
        self.wait_for_state(upid, [400, 'PENDING'])
        self.eea_client.terminate_process(upid, round)
        self.wait_for_state(upid, [700, 'TERMINATED'])


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

        self.wait_for_state(u_pid, [500, 'RUNNING'])

        # Kill and restart eeagent. Also, kill proc started by eea to simulate
        # a killed container
        old_eea_pid = str(self._eea_pid)
        self.container.terminate_process(self._eea_pid)
        proc_to_kill = self.container.proc_manager.procs_by_name.get(proc_name)
        self.assertIsNotNone(proc_to_kill)
        self.container.terminate_process(proc_to_kill.id)

        self._start_eeagent()

        self.assertNotEqual(old_eea_pid, self._eea_pid)

        self.wait_for_state(u_pid, [850, 'FAILED'])

    @needs_eeagent
    def test_download_code(self):

        self._enable_code_download(whitelist=['*'])

        u_pid = "test0"
        round = 0
        run_type = "pyon"
        proc_name = 'test_transform'
        module = "ion.my.module.to.download"
        module_uri = 'file://%s/downloads/module_to_download.py' % get_this_directory()
        bad_module_uri = 'file:///tmp/notreal/module_to_download.py'

        cls = 'TestDownloadProcess'

        parameters = {'name': proc_name, 'module': module, 'module_uri': bad_module_uri, 'cls': cls}
        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)

        print response
        assert response.status == 404
        assert "Unable to download" in response.result

        parameters = {'name': proc_name, 'module': module, 'module_uri': module_uri, 'cls': cls}
        self.eea_client.launch_process(u_pid, round, run_type, parameters)

        self.wait_for_state(u_pid, [500, 'RUNNING'])

        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

    @needs_eeagent
    def test_whitelist(self):

        downloads_directory = os.path.join(get_this_directory(), "downloads")
        http_port = 8910
        http_port = self._start_webserver(downloads_directory, port=http_port)

        while self._webserver is None:
            print "Waiting for webserver to come up"
            gevent.sleep(1)

        assert self._webserver.requests == 0

        u_pid = "test0"
        round = 0
        run_type = "pyon"
        proc_name = 'test_transform'
        module = "ion.my.module"
        module_uri = "http://localhost:%s/ion/agents/cei/test/downloads/module_to_download.py" % http_port
        cls = 'TestDownloadProcess'
        parameters = {'name': proc_name, 'module': module, 'module_uri': module_uri, 'cls': cls}
        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)

        assert response.status == 401
        assert "Code download not enabled" in response.result

        # Test no whitelist
        self._enable_code_download()

        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)

        print response
        assert response.status == 401
        assert "not in code_download whitelist" in response.result

        # Test not matching
        self._enable_code_download(whitelist=['blork'])

        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)

        assert response.status == 401
        assert "not in code_download whitelist" in response.result

        # Test exact matching
        self._enable_code_download(whitelist=['localhost'])

        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)

        self.wait_for_state(u_pid, [500, 'RUNNING'])

        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

        # Test wildcard
        self._enable_code_download(whitelist=['*'])

        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)

        self.wait_for_state(u_pid, [500, 'RUNNING'])

        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

    @needs_eeagent
    def test_caching(self):

        downloads_directory = os.path.join(get_this_directory(), "downloads")
        http_port = 8910
        http_port = self._start_webserver(downloads_directory, port=http_port)

        while self._webserver is None:
            print "Waiting for webserver to come up"
            gevent.sleep(1)

        self._enable_code_download(['*'])
        assert self._webserver.requests == 0

        u_pid = "test0"
        round = 0
        run_type = "pyon"
        proc_name = 'test_transform'
        module = "ion.my.module"
        module_uri = "http://localhost:%s/ion/agents/cei/test/downloads/module_to_download.py" % http_port
        cls = 'TestDownloadProcess'
        parameters = {'name': proc_name, 'module': module, 'module_uri': module_uri, 'cls': cls}

        # Launch a process, check that webserver is hit
        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)
        self.wait_for_state(u_pid, [500, 'RUNNING'])
        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

        assert self._webserver.requests == 1


        # Launch another process, check that webserver is still only hit once
        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)

        self.wait_for_state(u_pid, [500, 'RUNNING'])

        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

        assert self._webserver.requests == 1

        u_pid = "test5"
        round = 0
        run_type = "pyon"
        proc_name = 'test_transformx'
        module = "ion.agents.cei.test.test_eeagent"
        module_uri = "http://localhost:%s/ion/agents/cei/test/downloads/module_to_download.py" % http_port
        cls = 'TestProcess'
        parameters = {'name': proc_name, 'module': module, 'module_uri': module_uri, 'cls': cls}

        # Test that a module that is already available in tarball won't trigger a download
        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)
        self.wait_for_state(u_pid, [500, 'RUNNING'])
        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

        assert self._webserver.requests == 1

        u_pid = "test9"
        round = 0
        run_type = "pyon"
        proc_name = 'test_transformx'
        module = "ion.agents.cei.test.test_eeagent"
        module_uri = "http://localhost:%s/ion/agents/cei/test/downloads/module_to_download.py" % http_port
        cls = 'TestProcessNotReal'
        parameters = {'name': proc_name, 'module': module, 'module_uri': module_uri, 'cls': cls}

        # Test behaviour of a non existant class with no download
        response = self.eea_client.launch_process(u_pid, round, run_type, parameters)
        self.wait_for_state(u_pid, [850, 'FAILED'])
        self.eea_client.terminate_process(u_pid, round)
        state = self.eea_client.dump_state().result
        proc = get_proc_for_upid(state, u_pid)

@attr('UNIT', group='cei')
class HeartbeaterMockTest(PyonTestCase):
    """Tests which mock out parts of the EE Agent
    """

    def setUp(self):

        self.cfg = DotDict()
        self.cfg.eeagent = DotDict()
        self.cfg.eeagent.heartbeat = 1
        self.factory = Mock()

        self.process_id = 'fake'
        self.process = Mock()

        self.heartbeater = HeartBeater(self.cfg, self.factory, self.process_id, self.process)

    def tearDown(self):
        pass

    def test_heartbeater_waits_for_start(self):

        self.heartbeater.beat = Mock()

        self.heartbeater.poll()
        self.assertFalse(self.heartbeater.beat.called)

        self.heartbeater._started = True

        self.heartbeater.poll()
        self.assertTrue(self.heartbeater.beat.called)


def get_this_directory():
    return os.path.dirname(os.path.abspath(__file__))


def get_proc_for_upid(state, upid):

    for proc_state in state.get('processes', []):
        if proc_state.get('upid') == upid:
            return proc_state
    else:
        return None
