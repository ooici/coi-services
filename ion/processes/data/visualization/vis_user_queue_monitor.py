#!/usr/bin/env python
import gevent

from pyon.ion.process import SimpleProcess
from logging import getLogger
from pyon.util.log import log
from pyon.util.containers import get_safe

class VisUserQueueMonitor(SimpleProcess):
    def on_start(self):
        try:
            SimpleProcess.on_start(self)

            # Get config params
            self.monitor_timeout = get_safe(self.CFG, 'user_queue_monitor_timeout', 300)
            self.monitor_queue_size = get_safe(self.CFG, 'user_queue_monitor_size', 100)

            print " >>>>>>>>>>>>>>>  user_queue_monitor_timeout : ", self.monitor_timeout
            print " >>>>>>>>>>>>>>>  user_queue_monitor_size : ", self.monitor_queue_size

            self.monitor_event = gevent.event.Event()
            self.monitor_event.clear()

            self._process.thread_manager.spawn(self.user_vis_queue_monitor)

        except:
            log.exception('Unable to start VisUserQueueMonitor')
            raise


    def on_quit(self):
        self.monitor_event.set()
        super(VisUserQueueMonitor,self).on_quit()

    def user_vis_queue_monitor(self, **kwargs):

        log.debug("Starting Monitor Loop worker: %s timeout=%s" , self.id,  self.monitor_timeout)
        print "Starting Monitor Loop worker: %s timeout=%s" , self.id,  self.monitor_timeout

        while not self.monitor_event.wait(timeout=self.monitor_timeout):

            if self.container.is_terminating():
                break

            #get the list of queues and message counts on the broker for the user vis queues
            queues = []
            try:
                queues = self.container.ex_manager.list_queues(name=USER_VISUALIZATION_QUEUE, return_columns=['name', 'messages'], use_ems=False)
            except Exception, e:
                log.warn('Unable to get queue information from broker management plugin: ' + e.message)
                pass

            log.debug( "In Monitor Loop worker: %s", self.id)
            for queue in queues:

                log.debug('queue name: %s, messages: %d', queue['name'], queue['messages'])

                #Check for queues which are getting too large and clean them up if need be.
                if queue['messages'] > self.monitor_queue_size:
                    vis_token = queue['name'][queue['name'].index('UserVisQueue'):]

                    try:
                        log.warn("Real-time visualization queue %s had too many messages %d, so terminating this queue and associated resources.", queue['name'], queue['messages'] )

                        #Clear out the queue
                        msgs = self.get_realtime_visualization_data(query_token=vis_token)

                        #Now terminate it
                        self.terminate_realtime_visualization_data(query_token=vis_token)
                    except NotFound, e:
                        log.warn("The token %s could not not be found by the terminate_realtime_visualization_data operation; another worked may have cleaned it up already", vis_token)
                    except Exception, e1:
                        #Log errors and keep going!
                        log.exception(e1)

        log.debug('Exiting user_vis_queue_monitor')