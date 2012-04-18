import gevent
from mock import Mock
from unittest import SkipTest
from nose.plugins.attrib import attr
import unittest, os
from gevent import queue

from pyon.util.containers import DotDict
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import NotFound, BadRequest
from pyon.public import CFG, log
from pyon.event.event import EventSubscriber

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget, ProcessStateEnum

from ion.processes.data.transforms.transform_example import TransformExample
from ion.services.cei.process_dispatcher_service import ProcessDispatcherService,\
    PDLocalBackend, PDBridgeBackend


@attr('UNIT',group='cei')
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
        proc_def['executable'] = {'module':'my_module', 'class':'class'}
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
        mock_dashi.consume.return_value = lambda : None
        self.pd_service.backend._init_dashi = lambda : mock_dashi

        self.pd_service.start()
        self.assertEqual(mock_dashi.handle.call_count, 1)

        proc_def = DotDict()
        proc_def['name'] = "someprocess"
        proc_def['executable'] = {'module':'my_module', 'class':'class'}
        self.mock_rr_read.return_value = proc_def

        pid = self.pd_service.create_process("fake-process-def-id")

        proc_schedule = DotDict()
        proc_schedule['target'] = DotDict()
        proc_schedule.target['constraints'] = {"hats" : 4}

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

        # sneak in and replace dashi connection method
        mock_dashi = Mock()
        mock_dashi.consume.return_value = lambda : None
        self.pd_service.backend._init_dashi = lambda : mock_dashi

        self.pd_service.start()

        ok = self.pd_service.cancel_process("process-id")

        self.assertTrue(ok)
        mock_dashi.call.assert_called_once_with("pd", "terminate_process",
            upid="process-id")
        self.assertEqual(event_pub.publish_event.call_count, 0)

        self.pd_service.backend._process_state(dict(upid=pid,
            state="700-TERMINATED"))
        self.assertEqual(event_pub.publish_event.call_count, 1)


@attr('INT', group='cei')
class ProcessDispatcherServiceLocalIntTest(IonIntegrationTestCase):
    """Integration tests for the "local" mode of the PD

    In local mode processes are directly started on the local container. This
    is provided to allow use of PD functionality when launching via a single
    rel file instead of via a "real" CEI launch.
    """

    def setUp(self):

        # ensure bridge mode is disabled
        if 'process_dispatcher_bridge' in CFG:
            del CFG['process_dispatcher_bridge']

        # set up the container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')

        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        self.process_definition = ProcessDefinition(name='basic_transform_definition')
        self.process_definition.executable = {'module': 'ion.processes.data.transforms.transform_example',
                                              'class':'TransformExample'}
        self.process_definition_id = self.pd_cli.create_process_definition(self.process_definition)

        self.event_queue = queue.Queue()

        self.event_sub = None

    def tearDown(self):
        if self.event_sub:
            self.event_sub.deactivate()

    def _event_callback(self, event, *args, **kwargs):
        self.event_queue.put(event)

    def subscribe_events(self, origin):
        self.event_sub =  EventSubscriber(event_type="ProcessLifecycleEvent",
            callback=self._event_callback, origin=origin, origin_type="DispatchedProcess")
        self.event_sub.activate()

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_schedule_cancel(self):

        process_schedule = ProcessSchedule()

        pid = self.pd_cli.create_process(self.process_definition_id)
        self.subscribe_events(pid)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration={}, process_id=pid)
        self.assertEqual(pid, pid2)

        event = self.event_queue.get(timeout=5)
        self.assertEqual(event.origin, pid)
        self.assertEqual(event.state, ProcessStateEnum.SPAWN)
        log.debug("Got event: %s", event)

        proc = self.container.proc_manager.procs.get(pid)
        self.assertIsInstance(proc, TransformExample)

        # failures could theoretically leak processes but I don't think that
        # matters since everything gets torn down between tests
        self.pd_cli.cancel_process(pid)

        event = self.event_queue.get(timeout=5)
        log.debug("Got event: %s", event)
        self.assertEqual(event.origin, pid)
        self.assertEqual(event.state, ProcessStateEnum.TERMINATE)

        self.assertNotIn(pid, self.container.proc_manager.procs)

    def test_schedule_bad_config(self):

        process_schedule = ProcessSchedule()

        # a non-JSON-serializable IonObject
        o = ProcessTarget()

        with self.assertRaises(BadRequest) as ar:
            self.pd_cli.schedule_process(self.process_definition_id,
                process_schedule, configuration={"bad" : o})
        self.assertTrue(ar.exception.message.startswith("bad configuration"))


@attr('INT', group='cei')
class ProcessDispatcherServiceBridgeIntTest(IonIntegrationTestCase):
    """Integration tests for the "bridge" mode of the PD

    In bridge mode requests are bridged to a backend Process Dispatcher
    service running outside of the container and launched out of band.
    """

    dashi_uri = "memory://local"
    dashi_exchange = "test_pd_bridge_exchange"
    dashi_pd_topic = "processdispatcher"

    def setUp(self):

        # set up a fake dashi consumer to act as the PD
        try:
            import dashi
        except ImportError:
            raise SkipTest("Process Dispatcher Bridge integration test "+
                           "requires the dashi library. Skipping.")

        self.fake_pd = FakePD(dashi.DashiConnection(self.dashi_pd_topic,
            self.dashi_uri, self.dashi_exchange))
        self.fake_pd.consume_in_thread()

        # set up the container
        self._start_container()

        CFG['process_dispatcher_bridge'] = dict(uri="memory://local",
            exchange="test_pd_bridge_exchange", topic="processdispatcher")

        self.container.start_rel_from_url('res/deploy/r2cei.yml')

        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        self.process_definition = ProcessDefinition(name='basic_transform_definition')
        self.process_definition.executable = {'module': 'ion.processes.data.transforms.transform_example',
                                              'class':'TransformExample'}
        self.process_definition_id = self.pd_cli.create_process_definition(self.process_definition)

        self.event_queue = queue.Queue()

        self.event_sub = None

    def _event_callback(self, event, *args, **kwargs):
        self.event_queue.put(event)

    def subscribe_events(self, origin):
        self.event_sub =  EventSubscriber(event_type="ProcessLifecycleEvent",
            callback=self._event_callback, origin=origin, origin_type="DispatchedProcess")
        self.event_sub.activate()

    def tearDown(self):
        if hasattr(self, "fake_pd") and self.fake_pd:
            self.fake_pd.shutdown()
        if self.event_sub:
            self.event_sub.deactivate()

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_schedule_cancel(self):
        process_schedule = ProcessSchedule()
        process_schedule.target = ProcessTarget()
        process_schedule.target.constraints = {'site' : 'chicago'}

        config = {'some': "value"}

        pid = self.pd_cli.create_process(self.process_definition_id)

        self.subscribe_events(pid)

        pid2 = self.pd_cli.schedule_process(self.process_definition_id,
            process_schedule, configuration=config, process_id=pid)

        self.assertEqual(pid, pid2)

        self.assertEqual(self.fake_pd.dispatch_process.call_count, 1)
        args, kwargs = self.fake_pd.dispatch_process.call_args
        self.assertFalse(args)
        self.assertEqual(set(kwargs),
            set(['upid', 'spec', 'subscribers', 'constraints']))

        spec = kwargs['spec']
        self.assertEqual(spec['run_type'], 'pyon_single')
        self.assertEqual(spec['parameters']['rel']['apps'][0]['config'],
            config)

        self.fake_pd.send_process_state("processdispatcher_bridge",
            "process_state", pid, "500-RUNNING")

        event = self.event_queue.get(timeout=5)
        self.assertEqual(event.origin, pid)
        self.assertEqual(event.state, ProcessStateEnum.SPAWN)
        log.debug("Got event: %s", event)

        self.pd_cli.cancel_process(pid)

        self.fake_pd.send_process_state("processdispatcher_bridge",
            "process_state", pid, "700-TERMINATED")

        event = self.event_queue.get(timeout=5)
        self.assertEqual(event.origin, pid)
        self.assertEqual(event.state, ProcessStateEnum.TERMINATE)
        log.debug("Got event: %s", event)

        self.fake_pd.terminate_process.assert_called_once_with(upid=pid)


class FakePD(object):
    """object which uses CEI messaging to simulate the backend PD service

    We cannot stand up the real service for true integration testing but
    this at least verifies that the messaging works.
    """
    consume_timeout = 5

    def __init__(self, dashi):
        self.dashi = dashi

        # return values do not match service responses yet
        self.dispatch_process = Mock()
        self.dispatch_process.return_value = {}
        self.dashi.handle(self.dispatch_process, 'dispatch_process')

        self.terminate_process = Mock()
        self.terminate_process.return_value = {}
        self.dashi.handle(self.terminate_process, 'terminate_process')

    def consume_in_thread(self):
        self.consumer_thread = gevent.spawn(self.dashi.consume)

    def send_process_state(self, dest, op, upid, state):
        proc_state = dict(upid=upid, state=state)
        self.dashi.fire(dest, op, process=proc_state)

    def shutdown(self):
        if self.consumer_thread:
            self.dashi.cancel(block=False)
            self.consumer_thread.kill()
            self.consumer_thread = None
