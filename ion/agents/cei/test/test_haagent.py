import gevent
import functools
from gevent import queue

from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from pyon.agent.simple_agent import SimpleResourceAgentClient
from pyon.event.event import EventSubscriber
from pyon.public import log
from pyon.service.service import BaseService
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from interface.services.icontainer_agent import ContainerAgentClient
from ion.agents.cei.high_availability_agent import HighAvailabilityAgentClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget, ProcessStateEnum


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

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')
        #self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)
        self.pd_cli = ProcessDispatcherServiceClient(to_name="process_dispatcher")

        self.resource_id = "haagent_1234"
        self._haa_name = "high_availability_agent"
        self._haa_config = {
            'highavailability': {
                'policy': {
                    'interval': 1,
                    'name': 'npreserving',
                    'parameters': {
                        'preserve_n': 0
                    }
                },
                'process_spec': {
                    'name': 'test',
                    'executable':{
                        'module': 'ion.agents.cei.test.test_haagent',
                        'class': 'TestProcess'
                    }
                },
                "process_dispatchers": [
                    'process_dispatcher'
                ]
            },
            'agent': {'resource_id': self.resource_id},
        }

        self._base_procs = self.pd_cli.list_processes()

        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        self._haa_pid = self.container_client.spawn_process(name=self._haa_name,
            module="ion.agents.cei.high_availability_agent",
            cls="HighAvailabilityAgent", config=self._haa_config)

        # Start a resource agent client to talk with the instrument agent.
        self._haa_pyon_client = SimpleResourceAgentClient(self.resource_id, process=FakeProcess())
        log.info('Got haa client %s.', str(self._haa_pyon_client))

        self.haa_client = HighAvailabilityAgentClient(self._haa_pyon_client)

        self.event_queue = queue.Queue()
        self.event_sub = None

    def tearDown(self):
        self.container.terminate_process(self._haa_pid)
        self._stop_container()

    def _event_callback(self, event, *args, **kwargs):
        self.event_queue.put(event)

    def subscribe_events(self, origin):
        self.event_sub = EventSubscriber(event_type="ProcessLifecycleEvent",
            callback=self._event_callback, origin_type="DispatchedProcess")
        self.event_sub.start()

    def await_state_event(self, pid, state):
        event = self.event_queue.get(timeout=30)
        log.debug("Got event: %s", event)
        self.assertTrue(event.origin.startswith(pid))
        self.assertEqual(event.state, state)
        return event

    def get_running_procs(self):
        """returns a normalized set of running procs (removes the ones that 
        were there at setup time)
        """

        base = self._base_procs
        base_pids = [proc.process_id for proc in base]
        current = self.pd_cli.list_processes()
        current_pids = [proc.process_id for proc in current]
        print "filtering base procs %s from %s" % (base_pids, current_pids)
        normal = [cproc for cproc in current if cproc.process_id not in base_pids and cproc.process_state == ProcessStateEnum.SPAWN]
        return normal

    @needs_epu
    def test_features(self):
        status = self.haa_client.status().result
        self.assertEqual(status, 'PENDING')

        new_policy = {'preserve_n': 1}
        self.haa_client.reconfigure_policy(new_policy)

        result = self.haa_client.dump().result
        self.assertEqual(result['policy'], new_policy)

        self.subscribe_events(None)
        self.await_state_event("test", ProcessStateEnum.SPAWN)

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

        # verifies L4-CI-CEI-RQ122 and L4-CI-CEI-RQ124

        new_policy = {'preserve_n': 2}
        self.haa_client.reconfigure_policy(new_policy)

        self.await_state_event("test", ProcessStateEnum.SPAWN)

        self.assertEqual(len(self.get_running_procs()), 2)

        new_policy = {'preserve_n': 1}
        self.haa_client.reconfigure_policy(new_policy)

        self.await_state_event("test", ProcessStateEnum.TERMINATE)

        self.assertEqual(len(self.get_running_procs()), 1)

        new_policy = {'preserve_n': 0}
        self.haa_client.reconfigure_policy(new_policy)

        self.await_state_event("test", ProcessStateEnum.TERMINATE)
        self.assertEqual(len(self.get_running_procs()), 0)
