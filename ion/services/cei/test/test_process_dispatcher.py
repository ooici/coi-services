import shutil
import tempfile
import uuid
import unittest
import os

from mock import Mock, patch, DEFAULT
from nose.plugins.attrib import attr

from pyon.net.endpoint import RPCClient
from pyon.service.service import BaseService
from pyon.util.containers import DotDict, get_safe
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import NotFound, BadRequest, Conflict, IonException
from pyon.public import log
from pyon.core import bootstrap

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget,\
    ProcessStateEnum, ProcessQueueingMode, ProcessRestartMode, ProcessDefinitionType
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from ion.services.cei.process_dispatcher_service import ProcessDispatcherService,\
    PDLocalBackend, PDNativeBackend, get_dashi, get_pd_dashi_name, PDDashiHandler,\
    Notifier
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
        self.pd_service.backend.event_pub = self.mock_event_pub = Mock()

    def test_create_schedule(self):

        backend = self.pd_service.backend

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
        self.assertEqual(self.mock_event_pub.publish_event.call_count, 2)

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
        pid = self.pd_service.create_process("fake-process-def-id")

        ok = self.pd_service.cancel_process(pid)

        self.assertTrue(ok)
        self.mock_cc_terminate.assert_called_once_with(pid)


class FakeDashiNotFoundError(Exception):
    pass


class FakeDashiBadRequestError(Exception):
    pass


class FakeDashiWriteConflictError(Exception):
    pass


@attr('UNIT', group='cei')
class ProcessDispatcherServiceDashiHandlerTest(PyonTestCase):
    """Tests the dashi frontend of the PD
    """
    def setUp(self):

        self.mock_backend = DotDict()
        self.mock_backend['create_definition'] = Mock()
        self.mock_backend['read_definition'] = Mock()
        self.mock_backend['read_definition_by_name'] = Mock()
        self.mock_backend['update_definition'] = Mock()
        self.mock_backend['delete_definition'] = Mock()
        self.mock_backend['create'] = Mock()
        self.mock_backend['schedule'] = Mock()
        self.mock_backend['read_process'] = Mock()
        self.mock_backend['list'] = Mock()
        self.mock_backend['cancel'] = Mock()
        self.mock_backend['set_system_boot'] = Mock()

        self.mock_dashi = DotDict()
        self.mock_dashi['handle'] = Mock()

        self.mock_pyon_dashi_exc_map = {
            NotFound: FakeDashiNotFoundError,
            BadRequest: FakeDashiBadRequestError,
            Conflict: FakeDashiWriteConflictError
            }

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

        self.pd_dashi_handler.describe_definition(definition_name=name)
        self.assertEqual(self.mock_backend.read_definition_by_name.call_count, 1)

        self.pd_dashi_handler.update_definition(definition_id, definition_type,
            executable, name, description)
        self.assertEqual(self.mock_backend.update_definition.call_count, 1)

        self.pd_dashi_handler.remove_definition(definition_id)
        self.assertEqual(self.mock_backend.delete_definition.call_count, 1)

        with patch('ion.services.cei.process_dispatcher_service._PYON_DASHI_EXC_MAP',
                self.mock_pyon_dashi_exc_map):

            with self.assertRaises(FakeDashiBadRequestError):
                self.pd_dashi_handler.list_definitions()

            # need to specify either definition id or name
            with self.assertRaises(FakeDashiBadRequestError):
                self.pd_dashi_handler.describe_definition()

    def test_exception_map(self):
        # only testing one of the handlers. assuming they all share the decorator
        with patch('ion.services.cei.process_dispatcher_service._PYON_DASHI_EXC_MAP',
                self.mock_pyon_dashi_exc_map):

            self.mock_backend.read_definition.side_effect = NotFound()
            with self.assertRaises(FakeDashiNotFoundError):
                self.pd_dashi_handler.describe_definition("some-def")

            self.mock_backend.read_definition.side_effect = Conflict()
            with self.assertRaises(FakeDashiWriteConflictError):
                self.pd_dashi_handler.describe_definition("some-def")

            self.mock_backend.read_definition.side_effect = BadRequest()
            with self.assertRaises(FakeDashiBadRequestError):
                self.pd_dashi_handler.describe_definition("some-def")

            # try with an unmapped IonException. should get passed through directly
            self.mock_backend.read_definition.side_effect = IonException()
            with self.assertRaises(IonException):
                self.pd_dashi_handler.describe_definition("some-def")

    def test_schedule(self):

        self.mock_backend['read_definition'].return_value = ProcessDefinition()

        upid = 'myupid'
        name = 'myname'
        definition_id = 'something'
        queueing_mode = 'RESTART_ONLY'
        restart_mode = 'ABNORMAL'

        self.pd_dashi_handler.schedule_process(upid, definition_id,
            queueing_mode=queueing_mode, restart_mode=restart_mode, name=name)

        self.assertEqual(self.mock_backend.schedule.call_count, 1)
        args, kwargs = self.mock_backend.schedule.call_args
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

        self.assertEqual(self.mock_backend.schedule.call_count, 1)
        args, kwargs = self.mock_backend.schedule.call_args
        passed_schedule = args[2]
        assert passed_schedule.queueing_mode == ProcessQueueingMode.RESTART_ONLY
        assert passed_schedule.restart_mode == ProcessRestartMode.ABNORMAL

    def test_set_system_boot(self):
        self.pd_dashi_handler.set_system_boot(False)
        self.mock_backend.set_system_boot.assert_called_once_with(False)


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
                PDMatchmaker=DEFAULT, PDDoctor=DEFAULT) as mocks:
            mocks['get_dashi'].return_value = self.mock_dashi
            mocks['get_processdispatcher_store'].return_value = self.mock_store = Mock()
            mocks['ProcessDispatcherCore'].return_value = self.mock_core = Mock()
            mocks['PDMatchmaker'].return_value = self.mock_matchmaker = Mock()
            mocks['PDDoctor'].return_value = self.mock_doctor = Mock()
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

    def test_schedule_haagent_name(self):
        haa_proc_def = DotDict()
        haa_proc_def['name'] = "haagent"
        haa_proc_def['executable'] = {'module': 'my_module', 'class': 'class'}

        payload_proc_def = DotDict()
        payload_proc_def['name'] = "payload_process"
        payload_proc_def['executable'] = {'module': 'my_module', 'class': 'class'}

        proc_defs = {"haa_proc_def_id": haa_proc_def,
                     "payload_proc_def_id": payload_proc_def}

        read_definition_mock = Mock()
        read_definition_mock.side_effect = proc_defs.get
        self.pd_service.backend.read_definition = read_definition_mock

        # not used for anything in local mode
        proc_schedule = DotDict()

        configuration = {"highavailability": {"process_definition_id": "payload_proc_def_id"}}
        self.pd_service.schedule_process("haa_proc_def_id", proc_schedule, configuration)

        self.assertEqual(self.mock_core.schedule_process.call_count, 1)
        name = self.mock_core.schedule_process.call_args[1]['name']
        self.assertTrue(name.startswith("payload_process-ha"))

        # now try with scheduling by process definition name instead of ID
        self.mock_core.schedule_process.reset_mock()
        configuration = {"highavailability": {"process_definition_name": "payload_process"}}
        self.pd_service.schedule_process("haa_proc_def_id", proc_schedule, configuration)

        self.assertEqual(self.mock_core.schedule_process.call_count, 1)
        name = self.mock_core.schedule_process.call_args[1]['name']
        self.assertTrue(name.startswith("payload_process-ha"))

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


class TestProcess(BaseService):
    """Test process to deploy via PD
    """
    name = __name__ + "test"

    def on_init(self):
        self.i = 0
        self.response = self.CFG.test_response
        self.restart = get_safe(self.CFG, "process.start_mode") == "RESTART"

    def count(self):
        self.i += 1
        return self.i

    def query(self):
        return self.response

    def is_restart(self):
        return self.restart

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

    def is_restart(self, headers=None, timeout=None):
        return self.request({}, op='is_restart', headers=headers, timeout=timeout)


@attr('INT', group='cei')
class ProcessDispatcherServiceIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')

        self.rr_cli = ResourceRegistryServiceClient()
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

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid)
        self.assertEqual(pid, pid2)

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
        configuration = {"test_response": test_response}

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

    def test_cancel_notfound(self):
        with self.assertRaises(NotFound):
            self.pd_cli.cancel_process("not-a-real-process-id")

    def test_create_invalid_definition(self):
        # create process definition missing module and class
        # verifies L4-CI-CEI-RQ137
        executable = dict(url="http://somewhere.com/something.py")
        definition = ProcessDefinition(name="test_process", executable=executable)
        with self.assertRaises(BadRequest):
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


@attr('INT', group='cei')
class ProcessDispatcherNotifierTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()

    def test_event_publish_unicode_error(self):
        process = Mock()
        process.process_id = "some_process_id"
        process.state = "500-RUNNING"
        notifier = Notifier()
        notifier.event_pub = Mock()

        notifier.event_pub.publish_event.side_effect = Exception()
        notifier.notify_process(process)


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
        from pyon.public import CFG

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
        self._eea_pid_to_resource_id = {}
        self._eea_pid_to_persistence_dir = {}
        self._tmpdirs = []

        self.dashi = get_dashi(uuid.uuid4().hex,
            pd_config['processdispatcher']['dashi_uri'],
            pd_config['processdispatcher']['dashi_exchange'],
            sysname=CFG.get_safe("dashi.sysname")
            )

        #send a fake node_state message to PD's dashi binding.
        self.node1_id = uuid.uuid4().hex
        self._send_node_state("engine1", self.node1_id)
        self._initial_eea_pid = self._start_eeagent(self.node1_id)

        self.waiter = ProcessStateWaiter()

    def _send_node_state(self, engine_id, node_id=None):
        node_id = node_id or uuid.uuid4().hex
        node_state = dict(node_id=node_id, state=InstanceState.RUNNING,
            domain_id=domain_id_from_engine(engine_id))
        self.dashi.fire(get_pd_dashi_name(), "node_state", args=node_state)

    def _start_eeagent(self, node_id, resource_id=None, persistence_dir=None):
        if not persistence_dir:
            persistence_dir = tempfile.mkdtemp()
            self._tmpdirs.append(persistence_dir)
        resource_id = resource_id or uuid.uuid4().hex
        agent_config = _get_eeagent_config(node_id, persistence_dir,
            resource_id=resource_id)
        pid = self.container_client.spawn_process(name="eeagent",
            module="ion.agents.cei.execution_engine_agent",
            cls="ExecutionEngineAgent", config=agent_config)
        log.info('Agent pid=%s.', str(pid))
        self._eea_pids.append(pid)
        self._eea_pid_to_resource_id[pid] = resource_id
        self._eea_pid_to_persistence_dir[pid] = persistence_dir
        return pid

    def _kill_eeagent(self, pid):
        self.assertTrue(pid in self._eea_pids)
        self.container.terminate_process(pid)
        self._eea_pids.remove(pid)
        del self._eea_pid_to_resource_id[pid]
        del self._eea_pid_to_persistence_dir[pid]

    def tearDown(self):
        for pid in list(self._eea_pids):
            self._kill_eeagent(pid)
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

    def _add_test_process(self, restart_mode=None):
        process_schedule = ProcessSchedule()
        if restart_mode is not None:
            process_schedule.restart_mode = restart_mode
        pid = self.pd_cli.create_process(self.process_definition_id)

        pid_listen_name = "PDtestproc_%s" % uuid.uuid4().hex
        config = {'process': {'listen_name': pid_listen_name}}

        self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, process_id=pid, configuration=config)

        client = TestClient(to_name=pid_listen_name)
        return pid, client

    def test_restart(self):
        self.waiter.start()

        restartable_pids = []
        nonrestartable_pids = []
        clients = {}
        # start 10 processes with RestartMode.ALWAYS
        for _ in range(10):
            pid, client = self._add_test_process(ProcessRestartMode.ALWAYS)
            restartable_pids.append(pid)
            clients[pid] = client

        # and 10 processes with RestartMode.ABNORMAL
        for _ in range(10):
            pid, client = self._add_test_process(ProcessRestartMode.ABNORMAL)
            restartable_pids.append(pid)
            clients[pid] = client

        # and 10 with RestartMode.NEVER
        for _ in range(10):
            pid, client = self._add_test_process(ProcessRestartMode.NEVER)
            nonrestartable_pids.append(pid)
            clients[pid] = client

        all_pids = restartable_pids + nonrestartable_pids

        self.waiter.await_many_state_events(all_pids, ProcessStateEnum.RUNNING)

        for pid in all_pids:
            client = clients[pid]
            self.assertFalse(client.is_restart())
            self.assertEqual(client.count(), 1)

        # now kill the whole eeagent and restart it. processes should
        # show up as FAILED in the next heartbeat.
        resource_id = self._eea_pid_to_resource_id[self._initial_eea_pid]
        persistence_dir = self._eea_pid_to_persistence_dir[self._initial_eea_pid]
        log.debug("Restarting eeagent %s", self._initial_eea_pid)
        self._kill_eeagent(self._initial_eea_pid)

        # manually kill the processes to simulate a real container failure
        for pid in all_pids:
            self.container.terminate_process(pid)

        self._start_eeagent(self.node1_id, resource_id=resource_id,
            persistence_dir=persistence_dir)

        # wait for restartables to restart
        self.waiter.await_many_state_events(restartable_pids, ProcessStateEnum.RUNNING)

        # query the processes again. it should have restart mode config
        for pid in restartable_pids:
            client = clients[pid]
            self.assertTrue(client.is_restart())
            self.assertEqual(client.count(), 1)

        # meanwhile some procs should not have restarted
        for pid in nonrestartable_pids:
            proc = self.pd_cli.read_process(pid)
            self.assertEqual(proc.process_state, ProcessStateEnum.FAILED)

        # guard against extraneous events we were receiving as part of a bug:
        # processes restarting again after they were already restarted
        self.waiter.await_nothing(timeout=5)

    def test_idempotency(self):
        # ensure every operation can be safely retried
        process_schedule = ProcessSchedule()
        process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        proc_name = 'myreallygoodname'
        pid = self.pd_cli.create_process(self.process_definition_id)
        self.waiter.start(pid)

        # note: if we import UNSCHEDULED state into ProcessStateEnum,
        # this assertion will need to change.
        proc = self.pd_cli.read_process(pid)
        self.assertEqual(proc.process_id, pid)
        self.assertEqual(proc.process_state, ProcessStateEnum.REQUESTED)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid, name=proc_name)
        self.assertEqual(pid, pid2)

        self.waiter.await_state_event(pid, ProcessStateEnum.RUNNING)

        # repeating schedule is harmless
        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid, name=proc_name)
        self.assertEqual(pid, pid2)

        proc = self.pd_cli.read_process(pid)
        self.assertEqual(proc.process_id, pid)
        self.assertEqual(proc.process_configuration, {})
        self.assertEqual(proc.process_state, ProcessStateEnum.RUNNING)

        self.pd_cli.cancel_process(pid)
        self.waiter.await_state_event(pid, ProcessStateEnum.TERMINATED)

        # repeating cancel is harmless
        self.pd_cli.cancel_process(pid)
        proc = self.pd_cli.read_process(pid)
        self.assertEqual(proc.process_id, pid)
        self.assertEqual(proc.process_configuration, {})
        self.assertEqual(proc.process_state, ProcessStateEnum.TERMINATED)
