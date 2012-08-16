#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/transforms/poll.py 
@date Tue Aug  7 15:07:18 EDT 2012
@description Contains class to handle polling against queues
'''
from pyon.core.exception import Timeout, BadRequest
from pyon.ion.transforma import TransformStreamListener
from pyon.ion.stream import SimpleStreamSubscriber
from pyon.util.fsm import FSM
from pyon.util.log import log
import gevent

class TransformPoll(TransformStreamListener):
    '''
    Transform Extension of Stream Listener which allows the transform to poll 
    and buffer data from the queue, in lieu of consuming automatically.
    '''
    MESSAGE_BUFFER = 1024
    TIMEOUT        = 2.0
    S_INIT = 'INIT'
    S_READY = 'READY'
    S_FETCH = 'FETCHED'
    S_EXCEPTION = 'EXCEPTION'
    I_READY = 'RESET'
    I_FETCH = 'FETCH'
    I_EXCEPTION = 'EXCEPT'


    def on_start(self):
        self.queue_name  = self.CFG.get_safe('process.queue_name', self.id)
        self.subscriber  = SimpleStreamSubscriber.new_subscriber(self.container, self.queue_name, self.recv_packet)
        self._msg_buffer = []

        self._fsm = FSM(self.S_INIT)
        self._fsm.add_transition(self.I_READY, self.S_INIT, None, self.S_READY)
        self._fsm.add_transition(self.I_FETCH, self.S_READY, None, self.S_FETCH)
        self._fsm.add_transition(self.I_READY, self.S_FETCH, self._reset, self.S_READY)
        self._fsm.add_transition(self.I_EXCEPTION, self.S_FETCH, None, self.S_EXCEPTION)
        self._fsm.add_transition(self.I_READY, self.S_EXCEPTION, self._reset, self.S_READY)
        self.subscriber.initialize()
        self.done = gevent.event.Event()
        self._fsm.process(self.I_READY)
        self.greenlet = gevent.spawn(self.activate)

    def on_quit(self):
        self._ack_all()
        self.subscriber.close()
        if self.subscriber._chan._amq_chan is not None:
            log.error('Channel is still attached, forcing closure.')
            self.subscriber._chan.close_impl()

        self.done.set()
        self.greenlet.join(5)
        self.greenlet = None


    def _reset(self, fsm):
        self._ack_all()

    def _ack_all(self):
        while self._msg_buffer:
            msg = self._msg_buffer.pop()
            try:
                msg.ack()
            except:
                log.critical('Failed to ack message')

    def ack_all(self):
        '''
        Acknowledge all the messages in the current buffer.
        '''
        self._fsm.process(self.I_READY)

    def reject_all(self):
        '''
        Reject all the messages in the curernt buffer.
        '''
        self._reject_all()
        self._fsm.process(self.I_READY)

    def _reject_all(self):
        while self._msg_buffer:
            msg = self._msg_buffer.pop()
            try:
                msg.reject()
            except:
                log.critical('Failed to reject message')

    def poll_trigger(self):
        '''
        Conditional method for determining when to fetch, meant to be overridden.
        '''
        n = self.poll()
        if n:
            return True
        else:
            return False

    def poll(self):
        '''
        Returns the number of available messages
        '''
        return self.subscriber.get_stats()[0]

    def fetch(self):
        '''
        Method for fetching
        '''
        n = self.poll()
        return self._fetch(n)

    def _fetch(self, n):
        '''
        Fetches n messages from the queue,
        The messages must be acknowledged before another fetch can take place
        '''
        self._fsm.process(self.I_FETCH)
        try:
            if len(self._msg_buffer) + n >= self.MESSAGE_BUFFER:
                raise BadRequest('Request exceeds maximum buffer space')
            try:
                self._msg_buffer.extend( self.subscriber.get_n_msgs(n, self.TIMEOUT))
            except gevent.Timeout:
                raise Timeout
        except:
            self._fsm.process(self.I_EXCEPTION)
            return []
        return [(msg.body, msg.headers) for msg in self._msg_buffer]

    def activate(self):
        pass


'''
Example of summation transform:
    pid = cc.spawn_process('klp', 'ion.processes.data.transforms.poll', 'TransformSummation', {}, 'klp')
    klp = cc.proc_manager.procs[pid]
    from pyon.ion.stream import SimpleStreamPublisher
    pub = SimpleStreamPublisher.new_publisher(cc, 'nop', 'example')
    klp.subscriber.xn.bind('example.data', pub.exchange_point)
    [pub.publish(i) for i in xrange(11)]
'''
class TransformSummation(TransformPoll):

    def process(self, msg, header):
        return msg

    def validate(self, msgs):
        val = sum([int(a[0]) for a in msgs])

        log.info('Summation from polled messages: %s',val)
        return True

    def activate(self):
        while not self.done.is_set():
            if self.poll_trigger():
                try:
                    msgs = self.fetch()
                    if self.validate(msgs):
                        self.ack_all()
                    else:
                        self.reject_all()
                except:
                    log.exception('whiskee tango foxtrot')
                    self.reject_all()

            gevent.sleep(2)


