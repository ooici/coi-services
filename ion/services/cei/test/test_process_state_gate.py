
from ion.services.cei.process_dispatcher_service import ProcessStateGate

from nose.plugins.attrib import attr
from gevent import queue, spawn_later
from gevent.queue import Empty


from pyon.net.endpoint import RPCClient
from pyon.service.service import BaseService
from pyon.util.containers import DotDict
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import log
from pyon.public import IonObject
from pyon.event.event import EventSubscriber

from pyon.ion.resource import OT

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

from interface.objects import ProcessStateEnum, ProcessQueueingMode


try:
    from epu.states import InstanceState
except ImportError:
    pass




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
class ProcessStateGateIntTest(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2cei.yml')

        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)

        self.process_definition = IonObject(OT.ProcessDefinition, name='test_process')
        self.process_definition.executable = {'module': 'ion.services.cei.test.test_process_state_gate',
                                              'class': 'TestProcess'}
        self.process_definition_id = self.pd_cli.create_process_definition(self.process_definition)
        self.event_queue = queue.Queue()

        self.process_schedule = IonObject(OT.ProcessSchedule)
        self.process_schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        self.pid = self.pd_cli.create_process(self.process_definition_id)

        self.event_queue = queue.Queue()

        self.event_sub = EventSubscriber(event_type="ProcessLifecycleEvent",
                                         callback=self._event_callback,
                                         origin=self.pid,
                                         origin_type="DispatchedProcess")

    def tearDown(self):
        #stop subscriber if its running
        if self.event_sub and self.event_sub._cbthread:
            self.event_sub.stop()
        self._stop_container()


    def _event_callback(self, event, *args, **kwargs):
        self.event_queue.put(event)


    def latest_event(self, timeout=10):
        # get latest event from our local event subscriber
        try:
            event = self.event_queue.get(timeout=timeout)
        except Empty:
            event = None
        return event


    def await_state(self, state, timeout=10):

        print "Emptying event queue"
        while True:
            event = self.latest_event(0)
            if event:
                print "State %s from event %s" % (event.state, event)
            else:
                break
        self.event_sub.start()

        #wait for process state
        print "Setting up %s gate" % ProcessStateEnum._str_map[state]
        gate = ProcessStateGate(self.pd_cli.read_process,
                                self.pid,
                                state)
        print "Waiting"
        ret = gate.await(timeout)

        print "Await got %s" % ret
        event = self.latest_event(timeout=1)

        # check false positives/negatives
        if ret and gate._get_first_chance() is None and event is None:
            self.fail("ProcessStateGate got an event that EventSubscriber didnt....")

        self.event_sub.stop()

        if (not ret) or gate._get_last_chance():
            if event and event.state == state:
                self.fail("EventSubscriber got state event %s for process %s, ProcessStateGate missed it" %
                          (ProcessStateEnum._str_map[event.state], self.pid))


        return ret

    def process_start(self):
        print "Scheduling process...",
        self.pd_cli.schedule_process(self.process_definition_id,
                                     self.process_schedule,
                                     configuration={},
                                     process_id=self.pid)
        print "Done scheduling process."


    def process_stop(self):
        print "STOPPING process...",
        self.pd_cli.cancel_process(self.pid)
        print "Done stopping process"


    def test_process_state_gate(self):

        self.assertFalse(self.await_state(ProcessStateEnum.RUNNING, 1),
                         "The process was reported as spawned, but we didn't yet")

        print "GOING TO ACTUALLY START PROCESS NOW"
        spawn_later(1, self.process_start)

        self.assertTrue(self.await_state(ProcessStateEnum.RUNNING),
                        "The process did not spawn")

        self.assertFalse(self.await_state(ProcessStateEnum.TERMINATED, 1),
                        "The process claims to have terminated, but we didn't kill it")


        print "communicating with the process to make sure it is really running"
        test_client = TestClient()
        for i in range(5):
            self.assertEqual(i + 1, test_client.count(timeout=10))

        spawn_later(1, self.process_stop)

        self.assertTrue(self.await_state(ProcessStateEnum.TERMINATED),
                        "The process failed to be reported as terminated when it was terminated")

        self.assertFalse(self.await_state(ProcessStateEnum.RUNNING, 1),
                         "The process was reported as spawned, but we killed it")


        #self.fail("to get log")

