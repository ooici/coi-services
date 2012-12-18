import gevent
import functools
import BaseHTTPServer
import socket
import md5
from BaseHTTPServer import HTTPServer
from random import randint

from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest
from mock import Mock
from uuid import uuid4

from pyon.agent.simple_agent import SimpleResourceAgentClient
from pyon.public import log
from pyon.service.service import BaseService
from pyon.util.containers import DotDict
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.core import bootstrap
from pyon.core.exception import Timeout, BadRequest, NotFound

from ion.agents.cei.high_availability_agent import HighAvailabilityAgentClient, \
    ProcessDispatcherSimpleAPIClient
from ion.services.cei.test import ProcessStateWaiter, get_dashi_uri_from_cfg

from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessStateEnum, ProcessDefinition, ServiceStateEnum


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id = ''
    process_type = ''


def needs_epu(test):

    @functools.wraps(test)
    def wrapped(*args, **kwargs):
        try:
            import epu
            assert epu
            return test(*args, **kwargs)
        except ImportError:
            raise SkipTest("Need epu to run this test.")
    return wrapped


class TestProcess(BaseService):
    """Test process to deploy via EEA
    """
    name = __name__ + "test"

    def on_init(self):
        pass


@attr('INT', group='cei')
class HighAvailabilityAgentTest(IonIntegrationTestCase):

    @needs_epu
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')
        #self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)
        self.pd_cli = ProcessDispatcherServiceClient(to_name="process_dispatcher")

        self.process_definition_id = uuid4().hex
        self.process_definition_name = 'test'
        self.process_definition =  ProcessDefinition(name=self.process_definition_name, executable={
                'module': 'ion.agents.cei.test.test_haagent',
                'class': 'TestProcess'
        })
        self.pd_cli.create_process_definition(self.process_definition, self.process_definition_id)

        self.resource_id = "haagent_1234"
        self._haa_name = "high_availability_agent"
        self._haa_dashi_name = "dashi_haa_" + uuid4().hex
        self._haa_dashi_uri = get_dashi_uri_from_cfg()
        self._haa_dashi_exchange = "%s.hatests" % bootstrap.get_sys_name()
        self._haa_config = {
            'highavailability': {
                'policy': {
                    'interval': 1,
                    'name': 'npreserving',
                    'parameters': {
                        'preserve_n': 0
                    }
                },
                'process_definition_id': self.process_definition_id,
                'dashi_messaging' : True,
                'dashi_exchange' : self._haa_dashi_exchange,
                'dashi_name': self._haa_dashi_name
            },
            'agent': {'resource_id': self.resource_id},
        }

        self._base_services, _ = self.container.resource_registry.find_resources(
                restype="Service", name=self.process_definition_name)

        self._base_procs = self.pd_cli.list_processes()

        self.waiter = ProcessStateWaiter()
        self.waiter.start()

        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        self._haa_pid = self.container_client.spawn_process(name=self._haa_name,
            module="ion.agents.cei.high_availability_agent",
            cls="HighAvailabilityAgent", config=self._haa_config)

        # Start a resource agent client to talk with the instrument agent.
        self._haa_pyon_client = SimpleResourceAgentClient(self.resource_id, process=FakeProcess())
        log.info('Got haa client %s.', str(self._haa_pyon_client))

        self.haa_client = HighAvailabilityAgentClient(self._haa_pyon_client)


    def tearDown(self):
        self.waiter.stop()
        try:
            self.container.terminate_process(self._haa_pid)
        except BadRequest:
            log.warning("Couldn't terminate HA Agent in teardown (May have been terminated by a test)")
        self._stop_container()

    def get_running_procs(self):
        """returns a normalized set of running procs (removes the ones that
        were there at setup time)
        """

        base = self._base_procs
        base_pids = [proc.process_id for proc in base]
        current = self.pd_cli.list_processes()
        current_pids = [proc.process_id for proc in current]
        print "filtering base procs %s from %s" % (base_pids, current_pids)
        normal = [cproc for cproc in current if cproc.process_id not in base_pids and cproc.process_state == ProcessStateEnum.RUNNING]
        return normal

    def get_new_services(self):

        base = self._base_services
        base_names = [i.name for i in base]
        services_registered, _ = self.container.resource_registry.find_resources(
                restype="Service", name=self.process_definition_name)
        current_names = [i.name for i in services_registered]
        normal = [cserv for cserv in services_registered if cserv.name not in base_names]
        return normal

    def await_ha_state(self, want_state, timeout=10):

        for i in range(0, timeout):
            status = self.haa_client.status().result
            if status == want_state:
                return
            gevent.sleep(1)

        raise Exception("Took more than %s to get to ha state %s" % (timeout, want_state))


    def test_features(self):
        status = self.haa_client.status().result
        # Ensure HA hasn't already failed
        assert status in ('PENDING', 'READY', 'STEADY')


        # verifies L4-CI-CEI-RQ44
        # Note: the HA agent is started in the setUp() method, with config
        # pointing to the test "service". The initial config is set to preserve
        # 0 service processes. With this reconfigure step below, we change that
        # to launch 1.

        new_policy = {'preserve_n': 1}
        self.haa_client.reconfigure_policy(new_policy)

        result = self.haa_client.dump().result
        self.assertEqual(result['policy'], new_policy)

        self.waiter.await_state_event(state=ProcessStateEnum.RUNNING)

        self.assertEqual(len(self.get_running_procs()), 1)

        for i in range(0, 5):
            status = self.haa_client.status().result
            try:
                self.assertEqual(status, 'STEADY')
                break
            except:
                gevent.sleep(1)
        else:
            assert False, "HA Service took too long to get to state STEADY"

        # Ensure Service object has the correct state
        result = self.haa_client.dump().result
        service_id = result.get('service_id')
        service = self.container.resource_registry.read(service_id)
        self.assertEqual(service.state, ServiceStateEnum.STEADY)

        # verifies L4-CI-CEI-RQ122 and L4-CI-CEI-RQ124

        new_policy = {'preserve_n': 2}
        self.haa_client.reconfigure_policy(new_policy)

        self.waiter.await_state_event(state=ProcessStateEnum.RUNNING)

        self.assertEqual(len(self.get_running_procs()), 2)

        new_policy = {'preserve_n': 1}
        self.haa_client.reconfigure_policy(new_policy)

        self.waiter.await_state_event(state=ProcessStateEnum.TERMINATED)

        self.assertEqual(len(self.get_running_procs()), 1)

        new_policy = {'preserve_n': 0}
        self.haa_client.reconfigure_policy(new_policy)

        self.waiter.await_state_event(state=ProcessStateEnum.TERMINATED)
        self.assertEqual(len(self.get_running_procs()), 0)

    def test_associations(self):

        # Ensure that once the HA Agent starts, there is a Service object in
        # the registry
        result = self.haa_client.dump().result
        service_id = result.get('service_id')
        self.assertIsNotNone(service_id)
        service = self.container.resource_registry.read(service_id)
        self.assertIsNotNone(service)

        # Ensure that once a process is started, there is an association between
        # it and the service
        new_policy = {'preserve_n': 1}
        self.haa_client.reconfigure_policy(new_policy)
        self.waiter.await_state_event(state=ProcessStateEnum.RUNNING)
        self.assertEqual(len(self.get_running_procs()), 1)

        self.await_ha_state('STEADY')

        proc = self.get_running_procs()[0]

        processes_associated, _ = self.container.resource_registry.find_resources(
                restype="Process", name=proc.process_id)
        self.assertEqual(len(processes_associated), 1)

        has_processes = self.container.resource_registry.find_associations(
            service, "hasProcess")
        self.assertEqual(len(has_processes), 1)

        self.await_ha_state('STEADY')

        # Ensure that once we terminate that process, there are no associations
        new_policy = {'preserve_n': 0}
        self.haa_client.reconfigure_policy(new_policy)

        self.waiter.await_state_event(state=ProcessStateEnum.TERMINATED)
        self.assertEqual(len(self.get_running_procs()), 0)

        processes_associated, _ = self.container.resource_registry.find_resources(
                restype="Process", name=proc.process_id)
        self.assertEqual(len(processes_associated), 0)

        has_processes = self.container.resource_registry.find_associations(
            service, "hasProcess")
        self.assertEqual(len(has_processes), 0)

        # Ensure that once we terminate that HA Agent, the Service object is
        # cleaned up
        self.container.terminate_process(self._haa_pid)

        with self.assertRaises(NotFound):
            service = self.container.resource_registry.read(service_id)

    def test_dashi(self):

        import dashi

        dashi_conn = dashi.DashiConnection("something", self._haa_dashi_uri,
            self._haa_dashi_exchange)

        status = dashi_conn.call(self._haa_dashi_name, "status")
        assert status in ('PENDING', 'READY', 'STEADY')

        new_policy = {'preserve_n': 0}
        dashi_conn.call(self._haa_dashi_name, "reconfigure_policy",
            new_policy=new_policy)


@attr('UNIT', group='cei')
class ProcessDispatcherSimpleAPIClientTest(PyonTestCase):

    def setUp(self):
        self.mock_real_client = DotDict()
        self.mock_real_client.read_process_definition = Mock()
        self.mock_real_client.create_process = Mock()
        self.mock_real_client.schedule_process = Mock()
        self.mock_real_client.read_process = Mock()
        self.mock_eventpub = DotDict()
        self.mock_eventpub.publish_event = Mock()
        self.mock_container = Mock()

        self.client = ProcessDispatcherSimpleAPIClient('fake',
            real_client=self.mock_real_client, container=self.mock_container)
        self.client.event_pub = self.mock_eventpub

    def test_schedule(self):

        upid = 'my_pid'
        definition_id = 'my_def'
        configuration = {'some': 'value'}

        self.client.schedule_process(upid, definition_id, configuration=configuration)

        self.assertEqual(self.mock_real_client.schedule_process.call_count, 1)
        args, kwargs = self.mock_real_client.schedule_process.call_args

        self.assertEqual(args[0], definition_id)
        self.assertEqual(kwargs['configuration'], configuration)


@attr('INT', group='cei')
class HighAvailabilityAgentSensorPolicyTest(IonIntegrationTestCase):

    def _start_webserver(self, port=None):
        """ Start a webserver for testing code download
        Note: tries really hard to get a port, and if it can't use
        the suggested port, randomly picks another, and returns it
        """
        def log_message(self, format, *args):
            #swallow log massages
            pass

        class TestRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
            server_version = 'test_server'
            extensions_map = ''
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.send_header("Content-Length", len(self.server.response))
                self.end_headers()
                self.wfile.write(self.server.response)


        class Server(HTTPServer):

            response = ''

            def serve_forever(self):
                self._serving = 1
                while self._serving:
                    self.handle_request()

            def stop(self):
                self._serving = 0

        if port is None:
            port = 8008
        Handler = TestRequestHandler
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
            self._webserver.stop()
            gevent.sleep(2)
            self._web_glet.kill()

    @needs_epu
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')
        self.pd_cli = ProcessDispatcherServiceClient(to_name="process_dispatcher")

        self.process_definition_id = uuid4().hex
        self.process_definition =  ProcessDefinition(name='test', executable={
                'module': 'ion.agents.cei.test.test_haagent',
                'class': 'TestProcess'
        })

        self.pd_cli.create_process_definition(self.process_definition,
                self.process_definition_id)


        http_port = 8919
        http_port = self._start_webserver(port=http_port)

        self.resource_id = "haagent_4567"
        self._haa_name = "high_availability_agent"
        self._haa_config = {
            'highavailability': {
                'policy': {
                    'interval': 1,
                    'name': 'sensor',
                    'parameters': {
                        'metric': 'app_attributes:ml',
                        'sample_period': 600,
                        'sample_function': 'Average',
                        'cooldown_period': 5,
                        'scale_up_threshold': 2.0,
                        'scale_up_n_processes': 1,
                        'scale_down_threshold': 1.0,
                        'scale_down_n_processes': 1,
                        'maximum_processes': 5,
                        'minimum_processes': 1,
                    }
                },
                'aggregator': {
                    'type': 'trafficsentinel',
                    'host': 'localhost',
                    'port': http_port,
                    'protocol': 'http',
                    'username': 'user',
                    'password': 'pw'
                },
                'process_definition_id': self.process_definition_id,
                "process_dispatchers": [
                    'process_dispatcher'
                ]
            },
            'agent': {'resource_id': self.resource_id},
        }


        self._base_procs = self.pd_cli.list_processes()

        self.waiter = ProcessStateWaiter()
        self.waiter.start()

        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        self._haa_pid = self.container_client.spawn_process(name=self._haa_name,
            module="ion.agents.cei.high_availability_agent",
            cls="HighAvailabilityAgent", config=self._haa_config)

        # Start a resource agent client to talk with the instrument agent.
        self._haa_pyon_client = SimpleResourceAgentClient(self.resource_id, process=FakeProcess())
        log.info('Got haa client %s.', str(self._haa_pyon_client))

        self.haa_client = HighAvailabilityAgentClient(self._haa_pyon_client)


    def tearDown(self):
        self.waiter.stop()
        self.container.terminate_process(self._haa_pid)
        self._stop_webserver()
        self._stop_container()

    def get_running_procs(self):
        """returns a normalized set of running procs (removes the ones that
        were there at setup time)
        """

        base = self._base_procs
        base_pids = [proc.process_id for proc in base]
        current = self.pd_cli.list_processes()
        current_pids = [proc.process_id for proc in current]
        print "filtering base procs %s from %s" % (base_pids, current_pids)
        normal = [cproc for cproc in current if cproc.process_id not in base_pids and cproc.process_state == ProcessStateEnum.RUNNING]
        return normal

    def _get_managed_upids(self):
        result = self.haa_client.dump().result
        upids = result['managed_upids']
        return upids

    def _set_response(self, response):
        self._webserver.response = response

    def test_sensor_policy(self):
        status = self.haa_client.status().result
        # Ensure HA hasn't already failed
        assert status in ('PENDING', 'READY', 'STEADY')

        self.waiter.await_state_event(state=ProcessStateEnum.RUNNING)

        self.assertEqual(len(self.get_running_procs()), 1)

        for i in range(0, 5):
            status = self.haa_client.status().result
            try:
                self.assertEqual(status, 'STEADY')
                break
            except:
                gevent.sleep(1)
        else:
            assert False, "HA Service took too long to get to state STEADY"

        # Set ml for each proc such that we scale up
        upids = self._get_managed_upids()
        response = ""
        for upid in upids:
            response += "pid=%s&ml=5\n" % upid
        self._set_response(response)

        self.waiter.await_state_event(state=ProcessStateEnum.RUNNING)

        self.assertEqual(len(self.get_running_procs()), 2)

        # Set ml so we stay steady
        upids = self._get_managed_upids()
        response = ""
        for upid in upids:
            response += "pid=%s&ml=1.5\n" % upid
        self._set_response(response)

        self.assertEqual(len(self.get_running_procs()), 2)

        for i in range(0, 5):
            status = self.haa_client.status().result
            try:
                self.assertEqual(status, 'STEADY')
                break
            except:
                gevent.sleep(1)
        else:
            assert False, "HA Service took too long to get to state STEADY"

        # Set ml so we scale down
        upids = self._get_managed_upids()
        response = ""
        for upid in upids:
            response += "pid=%s&ml=0.5\n" % upid
        self._set_response(response)

        self.waiter.await_state_event(state=ProcessStateEnum.TERMINATED)

        self.assertEqual(len(self.get_running_procs()), 1)

        for i in range(0, 5):
            status = self.haa_client.status().result
            try:
                self.assertEqual(status, 'STEADY')
                break
            except:
                gevent.sleep(1)
        else:
            assert False, "HA Service took too long to get to state STEADY"
