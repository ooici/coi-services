import shutil
import tempfile
import uuid
import unittest
import os

from mock import Mock, patch, DEFAULT
from nose.plugins.attrib import attr

from pyon.net.endpoint import RPCClient
from pyon.service.service import BaseService
from pyon.util.containers import DotDict
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import NotFound, BadRequest
from pyon.public import log
from pyon.core import bootstrap

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget,\
    ProcessStateEnum, ProcessQueueingMode, ProcessRestartMode, ProcessDefinitionType
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from ion.services.cei.process_dispatcher_service import ProcessDispatcherService,\
    PDLocalBackend, PDNativeBackend, PDBridgeBackend, get_dashi, get_pd_dashi_name,\
    PDDashiHandler
from ion.services.cei.test import ProcessStateWaiter

try:
    from epu.states import InstanceState
    from epu.processdispatcher.engines import domain_id_from_engine
    _HAS_EPU = True
except ImportError:
    InstanceState = None
    domain_id_from_engine = None
    _HAS_EPU = False

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
        backend = self.pd_service.backend
        assert isinstance(backend, PDLocalBackend)

        event_pub = Mock()
        backend.event_pub = event_pub

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class'}
        self.mock_rr.read.return_value = proc_def
        self.mock_cc_spawn.return_value = '123'

        pid = self.pd_service.create_process("fake-process-def-id")

        # not used for anything in local mode
        proc_schedule = DotDict()

        configuration = {"some": "value"}

        if backend.SPAWN_DELAY:

            with patch("gevent.spawn_later") as mock_gevent:
                self.pd_service.schedule_process("fake-process-def-id",
                    proc_schedule, configuration, pid)

                self.assertTrue(mock_gevent.called)

                self.assertEqual(mock_gevent.call_args[0][0], backend.SPAWN_DELAY)
                self.assertEqual(mock_gevent.call_args[0][1], backend._inner_spawn)
                spawn_args = mock_gevent.call_args[0][2:]

            # now call the delayed spawn directly
            backend._inner_spawn(*spawn_args)

        else:
            self.pd_service.schedule_process("fake-process-def-id", proc_schedule,
                configuration, pid)

        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)
        self.assertEqual(self.mock_cc_spawn.call_count, 1)
        call_args, call_kwargs = self.mock_cc_spawn.call_args
        self.assertFalse(call_args)

        # name should be def name followed by a uuid
        name = call_kwargs['name']
        assert name.startswith(proc_def['name'])
        self.assertEqual(len(call_kwargs), 5)
        self.assertEqual(call_kwargs['module'], 'my_module')
        self.assertEqual(call_kwargs['cls'], 'class')
        self.assertEqual(call_kwargs['process_id'], pid)

        called_config = call_kwargs['config']
        self.assertEqual(called_config, configuration)

        # PENDING followed by RUNNING
        self.assertEqual(event_pub.publish_event.call_count, 2)

        process = self.pd_service.read_process(pid)
        self.assertEqual(process.process_id, pid)
        self.assertEqual(process.process_state, ProcessStateEnum.RUNNING)

    def test_read_process_notfound(self):
        with self.assertRaises(NotFound):
            self.pd_service.read_process("processid")

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
class ProcessDispatcherServiceDashiHandlerTest(PyonTestCase):
    """Tests the dashi frontend of the PD
    """

    #TODO: add some more thorough tests

    def setUp(self):

        self.mock_backend = DotDict()
        self.mock_backend['create_definition'] = Mock()
        self.mock_backend['read_definition'] = Mock()
        self.mock_backend['read_definition_by_name'] = Mock()
        self.mock_backend['delete_definition'] = Mock()
        self.mock_backend['spawn'] = Mock()
        self.mock_backend['read_process'] = Mock()
        self.mock_backend['list'] = Mock()
        self.mock_backend['cancel'] = Mock()

        self.mock_dashi = DotDict()
        self.mock_dashi['handle'] = Mock()

        self.pd_dashi_handler = PDDashiHandler(self.mock_backend, self.mock_dashi)

    def test_process_definitions(self):

        definition_id = "hello"
        definition_type = ProcessDefinitionType.PYON_STREAM
        executable = {'class': 'SomeThing', 'module': 'some.module'}
        name = "whataname"
        description = "describing stuff"

        self.pd_dashi_handler.create_definition(definition_id, definition_type,
                executable, name, description)
        self.assertEqual(self.mock_backend.create_definition.call_count, 1)

        self.pd_dashi_handler.describe_definition(definition_id)
        self.assertEqual(self.mock_backend.read_definition.call_count, 1)

        raised = False
        try:
            self.pd_dashi_handler.update_definition(definition_id, definition_type,
                executable, name, description)
        except BadRequest:
            raised = True
        assert raised, "update_definition didn't raise badrequest"

        self.pd_dashi_handler.remove_definition(definition_id)
        self.assertEqual(self.mock_backend.delete_definition.call_count, 1)

        raised = False
        try:
            self.pd_dashi_handler.list_definitions()
        except BadRequest:
            raised = True
        assert raised, "list_definitions didn't raise badrequest"

    def test_schedule(self):

        self.mock_backend['read_definition'].return_value = ProcessDefinition()

        upid = 'myupid'
        name = 'myname'
        definition_id = 'something'
        queueing_mode = 'RESTART_ONLY'
        restart_mode = 'ABNORMAL'

        self.pd_dashi_handler.schedule_process(upid, definition_id, queueing_mode=queueing_mode, restart_mode=restart_mode, name=name)

        self.assertEqual(self.mock_backend.spawn.call_count, 1)
        args, kwargs = self.mock_backend.spawn.call_args
        passed_schedule = args[2]
        assert passed_schedule.queueing_mode == ProcessQueueingMode.RESTART_ONLY
        assert passed_schedule.restart_mode == ProcessRestartMode.ABNORMAL
        passed_name = args[4]
        assert passed_name == name

    def test_schedule_by_name(self):
        pdef = ProcessDefinition()
        pdef._id = "someprocessid"
        self.mock_backend['read_definition_by_name'].return_value = pdef

        upid = 'myupid'
        definition_name = 'something'
        queueing_mode = 'RESTART_ONLY'
        restart_mode = 'ABNORMAL'

        self.pd_dashi_handler.schedule_process(upid, definition_name=definition_name,
            queueing_mode=queueing_mode, restart_mode=restart_mode)

        self.mock_backend.read_definition_by_name.assert_called_once_with(definition_name)
        self.assertFalse(self.mock_backend.read_definition.call_count)

        self.assertEqual(self.mock_backend.spawn.call_count, 1)
        args, kwargs = self.mock_backend.spawn.call_args
        passed_schedule = args[2]
        assert passed_schedule.queueing_mode == ProcessQueueingMode.RESTART_ONLY
        assert passed_schedule.restart_mode == ProcessRestartMode.ABNORMAL



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
                get_processdispatcher_store=DEFAULT, EngineRegistry=DEFAULT,
                PDMatchmaker=DEFAULT) as mocks:
            mocks['get_dashi'].return_value = self.mock_dashi
            mocks['get_processdispatcher_store'].return_value = self.mock_store = Mock()
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
        self.mock_beat_subscriber.start.assert_called_once_with()

    def test_create_schedule(self):

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module': 'my_module', 'class': 'class', 'url': 'myurl'}
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
        self.assertEqual(proc.process_state, ProcessStateEnum.RUNNING)
        self.assertEqual(proc.process_configuration, {})

    def test_read_process_notfound(self):

        self.mock_core.describe_process.return_value = None

        with self.assertRaises(NotFound):
            proc = self.pd_service.read_process("processid")
        assert self.mock_core.describe_process.called

    def test_read_process_with_config(self):
        config = {"hats": 4}
        self.mock_core.describe_process.return_value = dict(upid="processid",
            state="500-RUNNING", configuration=config)
        proc = self.pd_service.read_process("processid")
        assert self.mock_core.describe_process.called

        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.RUNNING)
        self.assertEqual(proc.process_configuration, config)

    def test_list_processes(self):
        core_procs = [dict(upid="processid", state="500-RUNNING")]
        self.mock_core.describe_processes.return_value = core_procs

        procs = self.pd_service.list_processes()
        proc = procs[0]
        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.RUNNING)


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
        name = 'allthehats'

        pid2 = self.pd_service.schedule_process("fake-process-def-id",
            proc_schedule, configuration, pid, name=name)

        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)
        self.assertEqual(pid, pid2)
        self.assertTrue(pid.startswith(proc_def.name) and pid != proc_def.name)
        self.assertEqual(self.mock_dashi.call.call_count, 1)
        call_args, call_kwargs = self.mock_dashi.call.call_args
        self.assertEqual(set(call_kwargs),
            set(['upid', 'definition_id', 'configuration', 'subscribers', 'constraints', 'name']))
        self.assertEqual(call_kwargs['constraints'],
            proc_schedule.target['constraints'])
        self.assertEqual(call_kwargs['subscribers'],
            self.pd_service.backend.pd_process_subscribers)
        self.assertEqual(call_args, ("pd", "schedule_process"))
        self.assertEqual(self.event_pub.publish_event.call_count, 0)

        # trigger some fake async state updates from dashi

        self.pd_service.backend._process_state(dict(upid=pid,
            state="400-PENDING"))
        self.assertEqual(self.event_pub.publish_event.call_count, 1)

        self.pd_service.backend._process_state(dict(upid=pid,
            state="500-RUNNING"))
        self.assertEqual(self.event_pub.publish_event.call_count, 2)

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
        self.assertEqual(proc.process_state, ProcessStateEnum.RUNNING)
        self.assertEqual(proc.process_configuration, {})

    def test_read_process_notfound(self):

        self.mock_dashi.call.return_value = None

        with self.assertRaises(NotFound):
            self.pd_service.read_process("processid")
        assert self.mock_dashi.call.called

    def test_read_process_with_config(self):
        config = {"hats": 4}
        self.mock_dashi.call.return_value = dict(upid="processid",
            state="500-RUNNING", configuration=config)
        proc = self.pd_service.read_process("processid")
        assert self.mock_dashi.call.called

        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.RUNNING)
        self.assertEqual(proc.process_configuration, config)

    def test_list_processes(self):

        core_procs = [dict(upid="processid", state="500-RUNNING")]
        self.mock_dashi.call.return_value = core_procs
        procs = self.pd_service.list_processes()
        proc = procs[0]
        self.assertEqual(proc.process_id, "processid")
        self.assertEqual(proc.process_state, ProcessStateEnum.RUNNING)


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

    def get_process_name(self, pid=None):
        if pid is None:
            return
        proc = self.container.proc_manager.procs.get(pid)
        if proc is None:
            return
        return proc._proc_name


class TestClient(RPCClient):
    def __init__(self, to_name=None, node=None, **kwargs):
        to_name = to_name or __name__ + "test"
        RPCClient.__init__(self, to_name=to_name, node=node, **kwargs)

    def count(self, headers=None, timeout=None):
        return self.request({}, op='count', headers=headers, timeout=timeout)

    def query(self, headers=None, timeout=None):
        return self.request({}, op='query', headers=headers, timeout=timeout)

    def get_process_name(self, pid=None, headers=None, timeout=None):
        return self.request({'pid': pid}, op='get_process_name', headers=headers, timeout=timeout)



@attr('INT', group='cei')
class ProcessDispatcherServiceIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')

        self.rr_cli  = ResourceRegistryServiceClient()
        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        self.process_definition = ProcessDefinition(name='test_process')
        self.process_definition.executable = {'module': 'ion.services.cei.test.test_process_dispatcher',
                                              'class': 'TestProcess'}
        self.process_definition_id = self.pd_cli.create_process_definition(self.process_definition)

        self.waiter = ProcessStateWaiter()

    def tearDown(self):
        self.waiter.stop()

    def test_create_schedule_cancel(self):
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        proc_name = 'myreallygoodname'
        pid = self.pd_cli.create_process(self.process_definition_id)
        self.waiter.start(pid)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid, name=proc_name)
        self.assertEqual(pid, pid2)

        # verifies L4-CI-CEI-RQ141 and L4-CI-CEI-RQ142
        self.waiter.await_state_event(pid, ProcessStateEnum.RUNNING)

        proc = self.pd_cli.read_process(pid)
        self.assertEqual(proc.process_id, pid)
        self.assertEqual(proc.process_configuration, {})
        self.assertEqual(proc.process_state, ProcessStateEnum.RUNNING)

        # make sure process is readable directly from RR (mirrored)
        # verifies L4-CI-CEI-RQ63
        # verifies L4-CI-CEI-RQ64
        proc = self.rr_cli.read(pid)
        self.assertEqual(proc.process_id, pid)

        # now try communicating with the process to make sure it is really running
        test_client = TestClient()
        for i in range(5):
            self.assertEqual(i + 1, test_client.count(timeout=10))

        # verifies L4-CI-CEI-RQ147

        # check the process name was set in container
        got_proc_name = test_client.get_process_name(pid=pid2)
        self.assertEqual(proc_name, got_proc_name)

        # kill the process and start it again
        self.pd_cli.cancel_process(pid)

        self.waiter.await_state_event(pid, ProcessStateEnum.TERMINATED)
        self.waiter.stop()

        oldpid = pid

        pid = self.pd_cli.create_process(self.process_definition_id)
        self.waiter.start(pid)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid)
        self.assertEqual(pid, pid2)
        self.assertNotEqual(oldpid, pid)

        self.waiter.await_state_event(pid, ProcessStateEnum.RUNNING)

        for i in range(5):
            self.assertEqual(i + 1, test_client.count(timeout=10))

        # kill the process for good
        self.pd_cli.cancel_process(pid)
        self.waiter.await_state_event(pid, ProcessStateEnum.TERMINATED)

    def test_schedule_with_config(self):

        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        pid = self.pd_cli.create_process(self.process_definition_id)
        self.waiter.start(pid)

        # verifies L4-CI-CEI-RQ66

        # feed in a string that the process will return -- verifies that
        # configuration actually makes it to the instantiated process
        test_response = uuid.uuid4().hex
        configuration = {"test_response" : test_response}

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration=configuration, process_id=pid)
        self.assertEqual(pid, pid2)

        self.waiter.await_state_event(pid, ProcessStateEnum.RUNNING)

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
        self.waiter.await_state_event(pid, ProcessStateEnum.TERMINATED)

    def test_schedule_bad_config(self):

        process_schedule = ProcessSchedule()

        # a non-JSON-serializable IonObject
        o = ProcessTarget()

        with self.assertRaises(BadRequest) as ar:
            self.pd_cli.schedule_process(self.process_definition_id,
                process_schedule, configuration={"bad": o})
        self.assertTrue(ar.exception.message.startswith("bad configuration"))

    def test_create_invalid_definition(self):
        # create process definition missing module and class
        # verifies L4-CI-CEI-RQ137
        executable = dict(url="http://somewhere.com/something.py")
        definition = ProcessDefinition(name="test_process", executable=executable)
        with self.assertRaises(BadRequest) as ar:
            self.pd_cli.create_process_definition(definition)


pd_config = {
    'processdispatcher': {
        'backend': "native",
        'static_resources': True,
        'heartbeat_queue': "hbeatq",
        'dashi_uri': "amqp://guest:guest@localhost/",
        'dashi_exchange': "%s.pdtests" % bootstrap.get_sys_name(),
        'default_engine': "engine1",
        "engines": {
            "engine1": {
                "slots": 100,
                "base_need": 1
            },
            "engine2": {
                "slots": 100,
                "base_need": 1
            }
        }
    }
}

def _get_eeagent_config(node_id, persistence_dir, slots=100, resource_id=None):

    resource_id = resource_id or uuid.uuid4().hex

    return {
        'eeagent': {
            'code_download': {
                'enabled': True,
                'whitelist': ['*']
            },
            'heartbeat': 1,
            'heartbeat_queue': 'hbeatq',
            'slots': slots,
            'name': 'pyon_eeagent',
            'node_id': node_id,
            'launch_type': {
                'name': 'pyon',
                'persistence_directory': persistence_dir,
                },
            },
        'agent': {'resource_id': resource_id},
        }

@unittest.skipIf(_HAS_EPU is False, 'epu dependency not available')
@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
@attr('LOCOINT', group='cei')
class ProcessDispatcherEEAgentIntTest(ProcessDispatcherServiceIntTest):
    """Run the basic int tests again, with a different environment
    """

    def setUp(self):
        self.dashi = None
        self._start_container()
        self.container_client = ContainerAgentClient(node=self.container.node,
            name=self.container.name)
        self.container = self.container_client._get_container_instance()

        app = dict(name="process_dispatcher", processapp=("process_dispatcher",
                               "ion.services.cei.process_dispatcher_service",
                               "ProcessDispatcherService"))
        self.container.start_app(app, config=pd_config)

        self.rr_cli = self.container.resource_registry

        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        self.process_definition = ProcessDefinition(name='test_process')
        self.process_definition.executable = {'module': 'ion.services.cei.test.test_process_dispatcher',
                                              'class': 'TestProcess'}
        self.process_definition_id = self.pd_cli.create_process_definition(self.process_definition)

        self._eea_pids = []
        self._tmpdirs = []

        self.dashi = get_dashi(uuid.uuid4().hex,
            pd_config['processdispatcher']['dashi_uri'],
            pd_config['processdispatcher']['dashi_exchange'])

        #send a fake node_state message to PD's dashi binding.
        self.node1_id = uuid.uuid4().hex
        self._send_node_state("engine1", self.node1_id)
        self._start_eeagent(self.node1_id)

        self.waiter = ProcessStateWaiter()

    def _send_node_state(self, engine_id, node_id=None):
        node_id = node_id or uuid.uuid4().hex
        node_state = dict(node_id=node_id, state=InstanceState.RUNNING,
            domain_id=domain_id_from_engine(engine_id))
        self.dashi.fire(get_pd_dashi_name(), "node_state", args=node_state)

    def _start_eeagent(self, node_id):
        persistence_dir = tempfile.mkdtemp()
        self._tmpdirs.append(persistence_dir)
        agent_config = _get_eeagent_config(node_id, persistence_dir)
        pid = self.container_client.spawn_process(name="eeagent",
            module="ion.agents.cei.execution_engine_agent",
            cls="ExecutionEngineAgent", config=agent_config)
        log.info('Agent pid=%s.', str(pid))
        self._eea_pids.append(pid)

    def tearDown(self):
        for pid in self._eea_pids:
            self.container.terminate_process(pid)
        for d in self._tmpdirs:
            shutil.rmtree(d)

        self.waiter.stop()
        if self.dashi:
            self.dashi.cancel()


    def test_requested_ee(self):

        # request non-default engine

        process_target = ProcessTarget(execution_engine_id="engine2")
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS
        process_schedule.target = process_target

        pid = self.pd_cli.create_process(self.process_definition_id)
        self.waiter.start()

        self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, process_id=pid)

        self.waiter.await_state_event(pid, ProcessStateEnum.WAITING)


        # request unknown engine, with NEVER queuing mode. The request
        # should be rejected.
        # verifies L4-CI-CEI-RQ52

        process_target = ProcessTarget(execution_engine_id="not-a-real-ee")
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.NEVER
        process_schedule.target = process_target

        rejected_pid = self.pd_cli.create_process(self.process_definition_id)

        self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, process_id=rejected_pid)

        self.waiter.await_state_event(rejected_pid, ProcessStateEnum.REJECTED)

        # now add a node and eeagent for engine2. original process should leave
        # queue and start running
        node2_id = uuid.uuid4().hex
        self._send_node_state("engine2", node2_id)
        self._start_eeagent(node2_id)

        self.waiter.await_state_event(pid, ProcessStateEnum.RUNNING)

        # spawn another process. it should start immediately.

        process_target = ProcessTarget(execution_engine_id="engine2")
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.NEVER
        process_schedule.target = process_target

        pid2 = self.pd_cli.create_process(self.process_definition_id)

        self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, process_id=pid2)

        self.waiter.await_state_event(pid2, ProcessStateEnum.RUNNING)

        # one more with node exclusive

        process_target = ProcessTarget(execution_engine_id="engine2",
            node_exclusive="hats")
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.NEVER
        process_schedule.target = process_target

        pid3 = self.pd_cli.create_process(self.process_definition_id)

        self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, process_id=pid3)

        self.waiter.await_state_event(pid3, ProcessStateEnum.RUNNING)

        # kill the processes for good
        self.pd_cli.cancel_process(pid)
        self.waiter.await_state_event(pid, ProcessStateEnum.TERMINATED)
        self.pd_cli.cancel_process(pid2)
        self.waiter.await_state_event(pid2, ProcessStateEnum.TERMINATED)
        self.pd_cli.cancel_process(pid3)
        self.waiter.await_state_event(pid3, ProcessStateEnum.TERMINATED)

    def test_node_exclusive(self):

        # the node_exclusive constraint is used to ensure multiple processes
        # of the same "kind" each get a VM exclusive of each other. Other
        # processes may run on these VMs, just not processes with the same
        # node_exclusive tag. Since we cannot directly query the contents
        # of each node in this test, we prove the capability by scheduling
        # processes one by one and checking their state.

        # verifies L4-CI-CEI-RQ121
        # verifies L4-CI-CEI-RQ57

        # first off, setUp() created a single node and eeagent.
        # We schedule two processes with the same "abc" node_exclusive
        # tag. Since there is only one node, the first process should run
        # and the second should be queued.

        process_target = ProcessTarget(execution_engine_id="engine1")
        process_target.node_exclusive = "abc"
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS
        process_schedule.target = process_target

        pid1 = self.pd_cli.create_process(self.process_definition_id)
        self.waiter.start()

        self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, process_id=pid1)

        self.waiter.await_state_event(pid1, ProcessStateEnum.RUNNING)

        pid2 = self.pd_cli.create_process(self.process_definition_id)
        self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, process_id=pid2)
        self.waiter.await_state_event(pid2, ProcessStateEnum.WAITING)

        # now demonstrate that the node itself is not full by launching
        # a third process without a node_exclusive tag -- it should start
        # immediately

        process_target.node_exclusive = None
        pid3 = self.pd_cli.create_process(self.process_definition_id)
        self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, process_id=pid3)
        self.waiter.await_state_event(pid3, ProcessStateEnum.RUNNING)

        # finally, add a second node to the engine. pid2 should be started
        # since there is an exclusive "abc" node free.
        node2_id = uuid.uuid4().hex
        self._send_node_state("engine1", node2_id)
        self._start_eeagent(node2_id)
        self.waiter.await_state_event(pid2, ProcessStateEnum.RUNNING)

        # kill the processes for good
        self.pd_cli.cancel_process(pid1)
        self.waiter.await_state_event(pid1, ProcessStateEnum.TERMINATED)
        self.pd_cli.cancel_process(pid2)
        self.waiter.await_state_event(pid2, ProcessStateEnum.TERMINATED)
        self.pd_cli.cancel_process(pid3)
        self.waiter.await_state_event(pid3, ProcessStateEnum.TERMINATED)

    def test_code_download(self):
        # create a process definition that has no URL; only module and class.
        process_definition_no_url = ProcessDefinition(name='test_process_nodownload')
        process_definition_no_url.executable = {'module': 'ion.my.test.process',
                'class': 'TestProcess'}
        process_definition_id_no_url = self.pd_cli.create_process_definition(process_definition_no_url)

        # create another that has a URL of the python file (this very file)
        # verifies L4-CI-CEI-RQ114
        url = "file://%s" % os.path.join(os.path.dirname(__file__), 'test_process_dispatcher.py')
        process_definition = ProcessDefinition(name='test_process_download')
        process_definition.executable = {'module': 'ion.my.test.process',
                'class': 'TestProcess', 'url': url}
        process_definition_id = self.pd_cli.create_process_definition(process_definition)

        process_target = ProcessTarget()
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS
        process_schedule.target = process_target

        self.waiter.start()

        # Test a module with no download fails
        pid_no_url = self.pd_cli.create_process(process_definition_id_no_url)

        self.pd_cli.schedule_process(process_definition_id_no_url,
            process_schedule, process_id=pid_no_url)

        self.waiter.await_state_event(pid_no_url, ProcessStateEnum.FAILED)

        # Test a module with a URL runs
        pid = self.pd_cli.create_process(process_definition_id)

        self.pd_cli.schedule_process(process_definition_id,
            process_schedule, process_id=pid)

        self.waiter.await_state_event(pid, ProcessStateEnum.RUNNING)
