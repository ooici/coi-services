import gevent
from mock import Mock
from nose.plugins.attrib import attr
from gevent import queue

from pyon.net.endpoint import RPCClient
from pyon.service.service import BaseService
from pyon.util.containers import DotDict
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import NotFound, BadRequest
from pyon.public import log
from pyon.event.event import EventSubscriber

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget, ProcessStateEnum

from ion.services.cei.process_dispatcher_service import ProcessDispatcherService,\
    PDLocalBackend, PDBridgeBackend


@attr('UNIT', group='cei')
class ProcessDispatcherServiceTest(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('process_dispatcher')
        self.pd_service = ProcessDispatcherService()
        self.pd_service.clients = mock_clients
        self.pd_service.container = DotDict()
        self.pd_service.container['spawn_process'] = Mock()
        self.pd_service.container['id'] = 'mock_container_id'
        self.pd_service.container['proc_manager'] = DotDict()
        self.pd_service.container.proc_manager['terminate_process'] = Mock()
        self.pd_service.container.proc_manager['procs'] = {}
        # CRUD Shortcuts
        self.mock_rr_create = self.pd_service.clients.resource_registry.create
        self.mock_rr_read = self.pd_service.clients.resource_registry.read
        self.mock_rr_update = self.pd_service.clients.resource_registry.update
        self.mock_rr_delete = self.pd_service.clients.resource_registry.delete
        self.mock_rr_find = self.pd_service.clients.resource_registry.find_objects
        self.mock_rr_find_res = self.pd_service.clients.resource_registry.find_resources
        self.mock_rr_assoc = self.pd_service.clients.resource_registry.find_associations
        self.mock_rr_create_assoc = self.pd_service.clients.resource_registry.create_association
        self.mock_rr_del_assoc = self.pd_service.clients.resource_registry.delete_association

        self.mock_cc_spawn = self.pd_service.container.spawn_process
        self.mock_cc_terminate = self.pd_service.container.proc_manager.terminate_process
        self.mock_cc_procs = self.pd_service.container.proc_manager.procs

    def test_local_create_schedule(self):

        self.pd_service.init()
        self.assertIsInstance(self.pd_service.backend, PDLocalBackend)

        event_pub = Mock()
        self.pd_service.backend.event_pub = event_pub

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class'}
        self.mock_rr_read.return_value = proc_def
        self.mock_cc_spawn.return_value = '123'

        pid = self.pd_service.create_process("fake-process-def-id")

        # not used for anything in local mode
        proc_schedule = DotDict()

        configuration = {"some": "value"}

        self.pd_service.schedule_process("fake-process-def-id",
            proc_schedule, configuration, pid)

        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)
        self.assertEqual(self.mock_cc_spawn.call_count, 1)
        call_args, call_kwargs = self.mock_cc_spawn.call_args
        self.assertFalse(call_args)

        # name should be def name followed by a uuid
        name = call_kwargs['name']
        self.assertEqual(name, pid)
        self.assertEqual(len(call_kwargs), 5)
        self.assertEqual(call_kwargs['module'], 'my_module')
        self.assertEqual(call_kwargs['cls'], 'class')

        called_config = call_kwargs['config']
        self.assertEqual(called_config, configuration)

        self.assertEqual(event_pub.publish_event.call_count, 1)

        process = self.pd_service.read_process('123')
        self.assertEqual(process.process_id, '123')
        self.assertEqual(process.process_state, ProcessStateEnum.SPAWN)

    def test_schedule_process_notfound(self):
        proc_schedule = DotDict()
        configuration = {}

        self.mock_rr_read.side_effect = NotFound()

        with self.assertRaises(NotFound):
            self.pd_service.schedule_process("not-a-real-process-id",
                proc_schedule, configuration)

        self.mock_rr_read.assert_called_once_with("not-a-real-process-id", "")

    def test_local_cancel(self):
        self.pd_service.init()
        self.assertIsInstance(self.pd_service.backend, PDLocalBackend)

        ok = self.pd_service.cancel_process("process-id")

        self.assertTrue(ok)
        self.mock_cc_terminate.assert_called_once_with("process-id")

    def test_bridge_create_schedule(self):
        pdcfg = dict(uri="amqp://hello", topic="pd", exchange="123")
        self.pd_service.CFG = DotDict()
        self.pd_service.CFG['process_dispatcher_bridge'] = pdcfg
        self.pd_service.init()
        self.assertIsInstance(self.pd_service.backend, PDBridgeBackend)

        event_pub = Mock()
        self.pd_service.backend.event_pub = event_pub

        # sneak in and replace dashi connection method
        mock_dashi = Mock()
        mock_dashi.consume.return_value = lambda: None
        self.pd_service.backend._init_dashi = lambda: mock_dashi

        self.pd_service.start()
        self.assertEqual(mock_dashi.handle.call_count, 1)

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class'}
        self.mock_rr_read.return_value = proc_def

        pid = self.pd_service.create_process("fake-process-def-id")

        proc_schedule = DotDict()
        proc_schedule['target'] = DotDict()
        proc_schedule.target['constraints'] = {"hats": 4}

        configuration = {"some": "value"}

        pid2 = self.pd_service.schedule_process("fake-process-def-id",
            proc_schedule, configuration, pid)

        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)
        self.assertEqual(pid, pid2)
        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)
        self.assertEqual(mock_dashi.call.call_count, 1)
        call_args, call_kwargs = mock_dashi.call.call_args
        self.assertEqual(set(call_kwargs),
            set(['upid', 'spec', 'subscribers', 'constraints']))
        self.assertEqual(call_kwargs['constraints'],
            proc_schedule.target['constraints'])
        self.assertEqual(call_kwargs['subscribers'],
            self.pd_service.backend.pd_process_subscribers)
        self.assertEqual(call_args, ("pd", "dispatch_process"))
        self.assertEqual(event_pub.publish_event.call_count, 0)

        # trigger some fake async state updates from dashi. first
        # should not trigger an event

        self.pd_service.backend._process_state(dict(upid=pid,
            state="400-PENDING"))
        self.assertEqual(event_pub.publish_event.call_count, 0)

        self.pd_service.backend._process_state(dict(upid=pid,
            state="500-RUNNING"))
        self.assertEqual(event_pub.publish_event.call_count, 1)

    def test_bridge_cancel(self):
        pdcfg = dict(uri="amqp://hello", topic="pd", exchange="123")
        self.pd_service.CFG = DotDict()
        self.pd_service.CFG['process_dispatcher_bridge'] = pdcfg
        self.pd_service.init()
        self.assertIsInstance(self.pd_service.backend, PDBridgeBackend)

        event_pub = Mock()
        self.pd_service.backend.event_pub = event_pub

        # sneak in and replace dashi connection method
        mock_dashi = Mock()
        mock_dashi.consume.return_value = lambda: None
        self.pd_service.backend._init_dashi = lambda: mock_dashi

        self.pd_service.start()

        ok = self.pd_service.cancel_process("process-id")

        self.assertTrue(ok)
        mock_dashi.call.assert_called_once_with("pd", "terminate_process",
            upid="process-id")
        self.assertEqual(event_pub.publish_event.call_count, 0)

        self.pd_service.backend._process_state(dict(upid="process-id",
            state="700-TERMINATED"))
        self.assertEqual(event_pub.publish_event.call_count, 1)


class TestProcess(BaseService):
    """Test process to deploy via PD
    """
    name = __name__ + "test"

    def on_init(self):
        self.i = 0

    def count(self):
        self.i += 1
        return self.i


class TestClient(RPCClient):
    def __init__(self, to_name=None, node=None, **kwargs):
        to_name = to_name or __name__ + "test"
        RPCClient.__init__(self, to_name=to_name, node=node, **kwargs)

    def count(self, headers=None, timeout=None):
        return self.request({}, op='count', headers=headers, timeout=timeout)


@attr('INT', group='cei')
class ProcessDispatcherServiceIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')

        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        self.process_definition = ProcessDefinition(name='test_process')
        self.process_definition.executable = {'module': 'ion.services.cei.test.test_process_dispatcher',
                                              'class': 'TestProcess'}
        self.process_definition_id = self.pd_cli.create_process_definition(self.process_definition)
        self.event_queue = queue.Queue()

        self.event_sub = None

    def tearDown(self):
        if self.event_sub:
            self.event_sub.stop()

    def _event_callback(self, event, *args, **kwargs):
        self.event_queue.put(event)

    def subscribe_events(self, origin):
        self.event_sub = EventSubscriber(event_type="ProcessLifecycleEvent",
            callback=self._event_callback, origin=origin, origin_type="DispatchedProcess")
        self.event_sub.start()

    def await_state_event(self, pid, state):
        event = self.event_queue.get(timeout=5)
        log.debug("Got event: %s", event)
        self.assertEqual(event.origin, pid)
        self.assertEqual(event.state, state)
        return event

    def test_create_schedule_cancel(self):
        process_schedule = ProcessSchedule()

        pid = self.pd_cli.create_process(self.process_definition_id)
        self.subscribe_events(pid)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid)
        self.assertEqual(pid, pid2)

        self.await_state_event(pid, ProcessStateEnum.SPAWN)

        proc = self.pd_cli.read_process(pid)
        self.assertEqual(proc.process_id, pid)
        self.assertEqual(proc.process_configuration, {})
        self.assertEqual(proc.process_state, ProcessStateEnum.SPAWN)

        # now try communicating with the process to make sure it is really running
        test_client = TestClient()
        for i in range(5):
            # this timeout may be too low
            self.assertEqual(i + 1, test_client.count(timeout=1))

        # kill the process and start it again
        self.pd_cli.cancel_process(pid)

        self.await_state_event(pid, ProcessStateEnum.TERMINATE)

        oldpid = pid

        pid = self.pd_cli.create_process(self.process_definition_id)
        self.subscribe_events(pid)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid)
        self.assertEqual(pid, pid2)
        self.assertNotEqual(oldpid, pid)

        self.await_state_event(pid, ProcessStateEnum.SPAWN)

        for i in range(5):
            # this timeout may be too low
            self.assertEqual(i + 1, test_client.count(timeout=1))

        # kill the process for good
        self.pd_cli.cancel_process(pid)
        self.await_state_event(pid, ProcessStateEnum.TERMINATE)

    def test_schedule_bad_config(self):

        process_schedule = ProcessSchedule()

        # a non-JSON-serializable IonObject
        o = ProcessTarget()

        with self.assertRaises(BadRequest) as ar:
            self.pd_cli.schedule_process(self.process_definition_id,
                process_schedule, configuration={"bad": o})
        self.assertTrue(ar.exception.message.startswith("bad configuration"))
