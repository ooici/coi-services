import shutil
import tempfile
import uuid
import unittest
import os

from mock import Mock, patch, DEFAULT
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
from pyon.core import bootstrap

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget,\
    ProcessStateEnum, ProcessQueueingMode, ProcessRestartMode
from interface.services.icontainer_agent import ContainerAgentClient

from ion.services.cei.process_dispatcher_service import ProcessDispatcherService,\
    PDLocalBackend, PDNativeBackend, PDBridgeBackend, get_dashi, get_pd_dashi_name

try:
    from epu.states import InstanceState
except ImportError:
    pass

# NOTE: much of the Process Dispatcher functionality is tested directly in the
# epu repository where the code resides. This file only attempts to test the
# Pyon interface itself as well as some integration testing to validate
# communication.


@attr('UNIT', group='cei')
class ProcessDispatcherServiceLocalTest(PyonTestCase):
    """Tests the local backend of the PD
    """

    def setUp(self):
        self.pd_service = ProcessDispatcherService()
        self.pd_service.container = DotDict()
        self.pd_service.container['spawn_process'] = Mock()
        self.pd_service.container['id'] = 'mock_container_id'
        self.pd_service.container['proc_manager'] = DotDict()
        self.pd_service.container['resource_registry'] = Mock()
        self.pd_service.container.proc_manager['terminate_process'] = Mock()
        self.pd_service.container.proc_manager['procs'] = {}

        self.mock_cc_spawn = self.pd_service.container.spawn_process
        self.mock_cc_terminate = self.pd_service.container.proc_manager.terminate_process
        self.mock_cc_procs = self.pd_service.container.proc_manager.procs

        self.pd_service.init()
        self.assertIsInstance(self.pd_service.backend, PDLocalBackend)
        self.pd_service.backend.rr = self.mock_rr = Mock()

    def test_create_schedule(self):

        event_pub = Mock()
        self.pd_service.backend.event_pub = event_pub

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class'}
        self.mock_rr.read.return_value = proc_def
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

        self.mock_rr.read.side_effect = NotFound()

        with self.assertRaises(NotFound):
            self.pd_service.schedule_process("not-a-real-process-id",
                proc_schedule, configuration)

        self.mock_rr.read.assert_called_once_with("not-a-real-process-id")

    def test_local_cancel(self):

        ok = self.pd_service.cancel_process("process-id")

        self.assertTrue(ok)
        self.mock_cc_terminate.assert_called_once_with("process-id")


@attr('UNIT', group='cei')
class ProcessDispatcherServiceNativeTest(PyonTestCase):
    """Tests the Pyon backend of the PD
    """

    def setUp(self):
        self.pd_service = ProcessDispatcherService()
        self.pd_service.container = DotDict()
        self.pd_service.container['spawn_process'] = Mock()
        self.pd_service.container['id'] = 'mock_container_id'
        self.pd_service.container['proc_manager'] = DotDict()
        self.pd_service.container['resource_registry'] = Mock()
        self.pd_service.container.proc_manager['terminate_process'] = Mock()
        self.pd_service.container.proc_manager['procs'] = {}

        pdcfg = dict(dashi_uri="amqp://hello", dashi_exchange="123",
            static_resources=True, backend="native")
        self.pd_service.CFG = DotDict()
        self.pd_service.CFG['processdispatcher'] = pdcfg

        self.mock_dashi = Mock()

        with patch.multiple('ion.services.cei.process_dispatcher_service',
                get_dashi=DEFAULT, ProcessDispatcherCore=DEFAULT,
                ProcessDispatcherStore=DEFAULT, EngineRegistry=DEFAULT,
                PDMatchmaker=DEFAULT) as mocks:
            mocks['get_dashi'].return_value = self.mock_dashi
            mocks['ProcessDispatcherStore'].return_value = self.mock_store = Mock()
            mocks['ProcessDispatcherCore'].return_value = self.mock_core = Mock()
            mocks['PDMatchmaker'].return_value = self.mock_matchmaker = Mock()
            mocks['EngineRegistry'].return_value = self.mock_engineregistry = Mock()

            self.pd_service.init()

        # replace the core and matchmaker with mocks
        self.pd_service.backend.beat_subscriber = self.mock_beat_subscriber = Mock()
        self.assertIsInstance(self.pd_service.backend, PDNativeBackend)
        self.pd_service.backend.rr = self.mock_rr = Mock()

        self.event_pub = Mock()
        self.pd_service.backend.event_pub = self.event_pub

        self.pd_service.start()
        self.assertEqual(self.mock_dashi.handle.call_count, 1)
        self.mock_matchmaker.start_election.assert_called_once_with()
        self.mock_beat_subscriber.activate.assert_called_one_with()

    def test_create_schedule(self):

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class'}
        mock_read_definition = Mock()
        mock_read_definition.return_value = proc_def
        self.pd_service.backend.read_definition = mock_read_definition

        pid = self.pd_service.create_process("fake-process-def-id")

        proc_schedule = DotDict()
        proc_schedule['target'] = DotDict()
        proc_schedule.target['constraints'] = {"hats": 4}
        proc_schedule.target['node_exclusive'] = None
        proc_schedule.target['execution_engine_id'] = None

        configuration = {"some": "value"}

        pid2 = self.pd_service.schedule_process("fake-process-def-id",
            proc_schedule, configuration, pid)

        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)
        self.assertEqual(pid, pid2)
        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)

        self.assertEqual(self.mock_core.schedule_process.call_count, 1)

    def test_queueing_mode(self):

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class'}
        mock_read_definition = Mock()
        mock_read_definition.return_value = proc_def
        self.pd_service.backend.read_definition = mock_read_definition

        pid = self.pd_service.create_process("fake-process-def-id")

        pyon_queueing_mode = ProcessQueueingMode.ALWAYS
        core_queueing_mode = "ALWAYS"

        proc_schedule = ProcessSchedule()
        proc_schedule.queueing_mode = pyon_queueing_mode

        configuration = {"some": "value"}

        pid2 = self.pd_service.schedule_process("fake-process-def-id",
            proc_schedule, configuration, pid)

        self.assertEqual(self.mock_core.schedule_process.call_count, 1)
        call_args, call_kwargs = self.mock_core.schedule_process.call_args
        self.assertEqual(call_kwargs['queueing_mode'], core_queueing_mode)

    def test_restart_mode(self):

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class'}
        mock_read_definition = Mock()
        mock_read_definition.return_value = proc_def
        self.pd_service.backend.read_definition = mock_read_definition

        pid = self.pd_service.create_process("fake-process-def-id")

        pyon_restart_mode = ProcessRestartMode.ABNORMAL
        core_restart_mode = "ABNORMAL"

        proc_schedule = ProcessSchedule()
        proc_schedule.restart_mode = pyon_restart_mode

        configuration = {"some": "value"}

        pid2 = self.pd_service.schedule_process("fake-process-def-id",
            proc_schedule, configuration, pid)

        self.assertEqual(self.mock_core.schedule_process.call_count, 1)
        call_args, call_kwargs = self.mock_core.schedule_process.call_args
        self.assertEqual(call_kwargs['restart_mode'], core_restart_mode)

    def test_node_exclusive_eeid(self):

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class'}
        mock_read_definition = Mock()
        mock_read_definition.return_value = proc_def
        self.pd_service.backend.read_definition = mock_read_definition

        pid = self.pd_service.create_process("fake-process-def-id")

        node_exclusive = "someattr"
        ee_id = "some_ee"

        proc_schedule = ProcessSchedule()
        proc_schedule.target.node_exclusive = node_exclusive
        proc_schedule.target.execution_engine_id = ee_id

        configuration = {"some": "value"}

        pid2 = self.pd_service.schedule_process("fake-process-def-id",
            proc_schedule, configuration, pid)

        self.assertEqual(self.mock_core.schedule_process.call_count, 1)
        call_args, call_kwargs = self.mock_core.schedule_process.call_args
        self.assertEqual(call_kwargs['execution_engine_id'], ee_id)
        self.assertEqual(call_kwargs['node_exclusive'], node_exclusive)

    def test_cancel(self):

        ok = self.pd_service.cancel_process("process-id")

        self.assertTrue(ok)
        self.assertEqual(self.mock_core.terminate_process.call_count, 1)

    def test_definitions(self):

        executable = {'module': 'my_module', 'class': 'class'}
        definition = ProcessDefinition(name="someprocess", executable=executable)
        pd_id = self.pd_service.create_process_definition(definition)
        assert self.mock_core.create_definition.called
        self.assertTrue(pd_id)
        assert self.mock_rr.create.called_once_with(definition, object_id=pd_id)

        self.mock_core.describe_definition.return_value = dict(name="someprocess",
            executable=executable)

        definition2 = self.pd_service.read_process_definition("someprocess")
        assert self.mock_core.describe_definition.called
        self.assertEqual(definition2.name, "someprocess")
        self.assertEqual(definition2.executable, executable)

        self.pd_service.delete_process_definition("someprocess")
        assert self.mock_core.remove_definition.called
        assert self.mock_rr.delete.called_once_with(pd_id)

    def test_read_process(self):

        self.mock_core.describe_process.return_value = dict(upid="processid",
            state="500-RUNNING")
        proc = self.pd_service.read_process("processid")
        assert self.mock_core.describe_process.called

        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.SPAWN)
        self.assertEqual(proc.process_configuration, {})

    def test_read_process_with_config(self):
        config = {"hats": 4}
        self.mock_core.describe_process.return_value = dict(upid="processid",
            state="500-RUNNING", configuration=config)
        proc = self.pd_service.read_process("processid")
        assert self.mock_core.describe_process.called

        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.SPAWN)
        self.assertEqual(proc.process_configuration, config)

    def test_list_processes(self):
        core_procs = [dict(upid="processid", state="500-RUNNING")]
        self.mock_core.describe_processes.return_value = core_procs

        procs = self.pd_service.list_processes()
        proc = procs[0]
        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.SPAWN)


@attr('UNIT', group='cei')
class ProcessDispatcherServiceBridgeTest(PyonTestCase):
    """Tests the bridge backend of the PD
    """
    def setUp(self):
        self.pd_service = ProcessDispatcherService()
        self.pd_service.container = DotDict()
        self.pd_service.container['resource_registry'] = Mock()

        pdcfg = dict(uri="amqp://hello", topic="pd", exchange="123")
        self.pd_service.CFG = DotDict()
        self.pd_service.CFG['process_dispatcher_bridge'] = pdcfg
        self.pd_service.init()
        self.assertIsInstance(self.pd_service.backend, PDBridgeBackend)
        self.pd_service.backend.rr = self.mock_rr = Mock()

        self.event_pub = Mock()
        self.pd_service.backend.event_pub = self.event_pub

        # sneak in and replace dashi connection method
        mock_dashi = Mock()
        mock_dashi.consume.return_value = lambda: None
        self.pd_service.backend._init_dashi = lambda: mock_dashi
        self.mock_dashi = mock_dashi

        self.pd_service.start()
        self.assertEqual(mock_dashi.handle.call_count, 1)

    def tearDown(self):
        self.pd_service.quit()

    def test_create_schedule(self):

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class'}
        mock_read_def = Mock()
        mock_read_def.return_value = proc_def
        self.pd_service.backend.read_definition = mock_read_def

        pid = self.pd_service.create_process("fake-process-def-id")
        mock_read_def.assert_called_once_with("fake-process-def-id")

        proc_schedule = DotDict()
        proc_schedule['target'] = DotDict()
        proc_schedule.target['constraints'] = {"hats": 4}
        proc_schedule.target['node_exclusive'] = None
        proc_schedule.target['execution_engine_id'] = None

        configuration = {"some": "value"}

        pid2 = self.pd_service.schedule_process("fake-process-def-id",
            proc_schedule, configuration, pid)

        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)
        self.assertEqual(pid, pid2)
        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)
        self.assertEqual(self.mock_dashi.call.call_count, 1)
        call_args, call_kwargs = self.mock_dashi.call.call_args
        self.assertEqual(set(call_kwargs),
            set(['upid', 'definition_id', 'configuration', 'subscribers', 'constraints']))
        self.assertEqual(call_kwargs['constraints'],
            proc_schedule.target['constraints'])
        self.assertEqual(call_kwargs['subscribers'],
            self.pd_service.backend.pd_process_subscribers)
        self.assertEqual(call_args, ("pd", "schedule_process"))
        self.assertEqual(self.event_pub.publish_event.call_count, 0)

        # trigger some fake async state updates from dashi. first
        # should not trigger an event

        self.pd_service.backend._process_state(dict(upid=pid,
            state="400-PENDING"))
        self.assertEqual(self.event_pub.publish_event.call_count, 0)

        self.pd_service.backend._process_state(dict(upid=pid,
            state="500-RUNNING"))
        self.assertEqual(self.event_pub.publish_event.call_count, 1)

    def test_cancel(self):

        ok = self.pd_service.cancel_process("process-id")

        self.assertTrue(ok)
        self.mock_dashi.call.assert_called_once_with("pd", "terminate_process",
            upid="process-id")
        self.assertEqual(self.event_pub.publish_event.call_count, 0)

        self.pd_service.backend._process_state(dict(upid="process-id",
            state="700-TERMINATED"))
        self.assertEqual(self.event_pub.publish_event.call_count, 1)

    def test_definitions(self):
        executable = {'module': 'my_module', 'class': 'class'}
        definition = ProcessDefinition(name="someprocess", executable=executable)
        pd_id = self.pd_service.create_process_definition(definition)
        assert self.mock_dashi.call.called
        self.assertTrue(pd_id)
        assert self.mock_rr.create.called_once_with(definition, object_id=pd_id)

        self.mock_dashi.call.reset_mock()
        self.mock_dashi.call.return_value = dict(name="someprocess",
            executable=executable)

        definition2 = self.pd_service.read_process_definition("someprocess")
        assert self.mock_dashi.call.called
        self.assertEqual(definition2.name, "someprocess")
        self.assertEqual(definition2.executable, executable)

        self.mock_dashi.call.reset_mock()
        self.pd_service.delete_process_definition("someprocess")
        assert self.mock_dashi.call.called
        assert self.mock_rr.delete.called_once_with(pd_id)

    def test_read_process(self):

        self.mock_dashi.call.return_value = dict(upid="processid",
            state="500-RUNNING")
        proc = self.pd_service.read_process("processid")
        assert self.mock_dashi.call.called

        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.SPAWN)
        self.assertEqual(proc.process_configuration, {})

    def test_read_process_with_config(self):
        config = {"hats": 4}
        self.mock_dashi.call.return_value = dict(upid="processid",
            state="500-RUNNING", configuration=config)
        proc = self.pd_service.read_process("processid")
        assert self.mock_dashi.call.called

        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.SPAWN)
        self.assertEqual(proc.process_configuration, config)

    def test_list_processes(self):

        core_procs = [dict(upid="processid", state="500-RUNNING")]
        self.mock_dashi.call.return_value = core_procs
        procs = self.pd_service.list_processes()
        proc = procs[0]
        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.SPAWN)


class TestProcess(BaseService):
    """Test process to deploy via PD
    """
    name = __name__ + "test"

    def on_init(self):
        self.i = 0
        self.response = self.CFG.test_response

    def count(self):
        self.i += 1
        return self.i

    def query(self):
        return self.response


class TestClient(RPCClient):
    def __init__(self, to_name=None, node=None, **kwargs):
        to_name = to_name or __name__ + "test"
        RPCClient.__init__(self, to_name=to_name, node=node, **kwargs)

    def count(self, headers=None, timeout=None):
        return self.request({}, op='count', headers=headers, timeout=timeout)

    def query(self, headers=None, timeout=None):
        return self.request({}, op='query', headers=headers, timeout=timeout)



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
        self._stop_container()

    def _event_callback(self, event, *args, **kwargs):
        self.event_queue.put(event)

    def subscribe_events(self, origin):
        self.event_sub = EventSubscriber(event_type="ProcessLifecycleEvent",
            callback=self._event_callback, origin=origin, origin_type="DispatchedProcess")
        self.event_sub.start()

    def await_state_event(self, pid, state):
        event = self.event_queue.get(timeout=30)
        log.debug("Got event: %s", event)
        self.assertEqual(event.origin, pid)
        self.assertEqual(event.state, state)
        return event

    def test_create_schedule_cancel(self):
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        pid = self.pd_cli.create_process(self.process_definition_id)
        self.subscribe_events(pid)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid)
        self.assertEqual(pid, pid2)

        # verifies L4-CI-CEI-RQ141 and L4-CI-CEI-RQ142

        self.await_state_event(pid, ProcessStateEnum.SPAWN)

        proc = self.pd_cli.read_process(pid)
        self.assertEqual(proc.process_id, pid)
        self.assertEqual(proc.process_configuration, {})
        self.assertEqual(proc.process_state, ProcessStateEnum.SPAWN)

        # now try communicating with the process to make sure it is really running
        test_client = TestClient()
        for i in range(5):
            self.assertEqual(i + 1, test_client.count(timeout=10))

        # verifies L4-CI-CEI-RQ147

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
            self.assertEqual(i + 1, test_client.count(timeout=10))

        # kill the process for good
        self.pd_cli.cancel_process(pid)
        self.await_state_event(pid, ProcessStateEnum.TERMINATE)

    def test_schedule_with_config(self):

        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        pid = self.pd_cli.create_process(self.process_definition_id)
        self.subscribe_events(pid)

        # verifies L4-CI-CEI-RQ66

        # feed in a string that the process will return -- verifies that
        # configuration actually makes it to the instantiated process
        test_response = uuid.uuid4().hex
        configuration = {"test_response" : test_response}

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration=configuration, process_id=pid)
        self.assertEqual(pid, pid2)

        self.await_state_event(pid, ProcessStateEnum.SPAWN)

        test_client = TestClient()

        # verifies L4-CI-CEI-RQ139
        # assure that configuration block (which can contain inputs, outputs,
        # and arbitrary config) 1) makes it to the process and 2) is returned
        # in process queries

        self.assertEqual(test_client.query(), test_response)

        proc = self.pd_cli.read_process(pid)
        self.assertEqual(proc.process_id, pid)
        self.assertEqual(proc.process_configuration, configuration)

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

pd_config = {
    'processdispatcher': {
        'backend': "native",
        'static_resources': True,
        'heartbeat_queue': "hbeatq",
        'dashi_uri': "amqp://guest:guest@localhost/",
        'dashi_exchange': "%s.pdtests" % bootstrap.get_sys_name(),
        "engines": {
            "default": {
                "deployable_type": "eeagent_pyon",
                "launch_type": {
                    "name": "pyon_single",
                    "pyon_directory": "/home/cc/coi-services/",
                    "container_args": "--noshell",
                    "supd_directory": "/tmp"
                },
                "slots": 100,
                "base_need": 1
            }
        }
    }
}


@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
@attr('LOCOINT', group='cei')
class ProcessDispatcherEEAgentIntTest(ProcessDispatcherServiceIntTest):
    """Run the basic int tests again, with a different environment
    """

    def setUp(self):
        self._start_container()
        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        self.container = self.container_client._get_container_instance()

        app = dict(name="process_dispatcher", processapp=("process_dispatcher",
                               "ion.services.cei.process_dispatcher_service",
                               "ProcessDispatcherService"))
        self.container.start_app(app, config=pd_config)

        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        self.process_definition = ProcessDefinition(name='test_process')
        self.process_definition.executable = {'module': 'ion.services.cei.test.test_process_dispatcher',
                                              'class': 'TestProcess'}
        self.process_definition_id = self.pd_cli.create_process_definition(self.process_definition)
        self.event_queue = queue.Queue()

        self.event_sub = None

        self.resource_id = "eeagent_123456789"
        self._eea_name = "eeagent"

        self.persistence_directory = tempfile.mkdtemp()

        self.agent_config = {
            'eeagent': {
                'heartbeat': 1,
                'heartbeat_queue': 'hbeatq',
                'slots': 100,
                'name': 'pyon_eeagent',
                'node_id': 'somenodeid',
                'launch_type': {
                    'name': 'pyon',
                    'persistence_directory': self.persistence_directory,
                    },
                },
            'agent': {'resource_id': self.resource_id},
        }

        #send a fake dt_state message to PD's dashi binding.
        dashi = get_dashi(uuid.uuid4().hex,
            pd_config['processdispatcher']['dashi_uri'],
            pd_config['processdispatcher']['dashi_exchange'])
        dt_state = dict(node_id="somenodeid", state=InstanceState.RUNNING,
            deployable_type="eeagent_pyon")
        dashi.fire(get_pd_dashi_name(), "dt_state", args=dt_state)

        self._eea_pid = self.container_client.spawn_process(name=self._eea_name,
            module="ion.agents.cei.execution_engine_agent",
            cls="ExecutionEngineAgent", config=self.agent_config)
        log.info('Agent pid=%s.', str(self._eea_pid))

    def _start_eeagent(self):
        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        self.container = self.container_client._get_container_instance()

        # Start eeagent.
        self._eea_pid = self.container_client.spawn_process(name=self._eea_name,
            module="ion.agents.cei.execution_engine_agent",
            cls="ExecutionEngineAgent", config=self.agent_config)
        log.info('Agent pid=%s.', str(self._eea_pid))

    def tearDown(self):
        self.container.terminate_process(self._eea_pid)
        shutil.rmtree(self.persistence_directory)

        if self.event_sub:
            self.event_sub.stop()
        self._stop_container()

