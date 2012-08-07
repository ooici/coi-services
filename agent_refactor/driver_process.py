#!/usr/bin/env python

"""
@package ion.services.mi.driver_process
@file ion/services/mi/driver_process.py
@author Edward Hunter
@brief Messaing enabled driver processes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import logging
from threading import Thread
from subprocess import Popen
from subprocess import PIPE
import signal
import os
import sys
import time
import traceback
from mi.core.exceptions import InstrumentException, InstrumentCommandException
from mi.core.instrument.instrument_driver import DriverAsyncEvent

from mi.core.log import get_logger
log = get_logger()

class DriverProcess(object):
    """
    Base class for messaging enabled OS-level driver processes. Provides
    run loop, dynamic driver import and construction and interface
    for messaging implementation subclasses.
    """
    
    @staticmethod
    def launch_process(cmd_str):
        """
        Base class static constructor. Launch the calling class as a
        separate OS level process. This method combines the derived class
        command string with the common python interpreter command.
        @param cmd_string The python command sequence to import, create and
        run a derived class object.
        @retval a Popen object representing the dirver process.
        """

        # Launch a separate python interpreter, executing the calling
        # class command string.
        spawnargs = ['bin/python', '-c', cmd_str]
        return Popen(spawnargs, close_fds=True)
        
    def __init__(self, driver_module, driver_class, ppid):
        """
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        """
        self.driver_module = driver_module
        self.driver_class = driver_class
        self.ppid = ppid
        self.driver = None
        self.events = []
        self.messaging_started = False
        
    def construct_driver(self):
        """
        Attempt to import and construct the driver object based on
        configuration.
        @retval True if successful, False otherwise.
        """
        import_str = 'import %s as dvr_mod' % self.driver_module
        ctor_str = 'driver = dvr_mod.%s(self.send_event)' % self.driver_class
        try:
            exec import_str
            log.info('Imported driver module %s' % self.driver_module)
            exec ctor_str
            log.info('Constructed driver %s' % self.driver_class)
            
        except (ImportError, NameError, AttributeError) as e:
            log.error('Could not import/construct driver module %s, class %s.' %
                      (self.driver_module, self.driver_class))
            log.error('%s' % str(e))
            return False

        else:
            self.driver = driver
            return True
            
    def start_messaging(self):
        """
        Initialize and start messaging resources for the driver, blocking
        until messaging terminates. Overridden in subclasses for
        specific messaging technologies. 
        """
        pass

    def stop_messaging(self):
        """
        Close messaging resource for the driver. Overridden in subclasses
        for specific messaging technologies.
        """
        pass

    def shutdown(self):
        """
        Shutdown function prior to process exit.
        """
        log.info('Driver process shutting down.')
        self.driver_module = None
        self.driver_class = None
        self.driver = None

    def check_parent(self):
        """
        Test for existence of original parent process, if ppid specified.
        """
        if self.ppid:
            try:
                os.kill(self.ppid, 0)
                
            except OSError:
                log.info('Driver process COULD NOT DETECT PARENT.')
                return False
        
        return True

    def cmd_driver(self, msg):
        """
        Process a command message against the driver. If the command
        exists as a driver attribute, call it passing supplied args and
        kwargs and returning the driver result. Special messages that are
        not forwarded to the driver are:
        'stop_driver_process' - signal to close messaging and terminate.
        'test_events' - populate event queue with test data.
        'process_echo' - echos the message back.
        If the command is not found in the driver, an echo message is
        replied to the client.
        @param msg A driver command message.
        @retval The driver command result.
        """
        cmd = msg.get('cmd', None)
        args = msg.get('args', None)
        kwargs = msg.get('kwargs', None)
        cmd_func = getattr(self.driver, cmd, None)
        log.debug("DriverProcess.cmd_driver(): cmd=%s, cmd_func=%s" %(cmd, cmd_func))
        if cmd == 'stop_driver_process':
            self.stop_messaging()
            return'stop_driver_process'
        elif cmd == 'test_events':
            events = kwargs['events']
            self.events += events
            reply = 'test_events'
        elif cmd == 'process_echo':
            reply = 'ping from resource ppid:%s, resource:%s' % (str(self.ppid), str(self.driver))
            #try:
            #    msg = args[0]
            #except IndexError:
            #    msg = 'no message to echo'
            # reply = 'process_echo: %s' % msg
        elif cmd_func:
            try:
                reply = cmd_func(*args, **kwargs)
            except Exception as e:
                reply = e
                # Command error events are better handled in the agent directly.
                #event = {
                #    'type' : DriverAsyncEvent.ERROR,
                #    'value' : str(e),
                #    'exception' : e,
                #    'time' : time.time()
                #}
                #self.send_event(event)
                if not isinstance(e, InstrumentException):
                    trace = traceback.format_exc()
                    log.critical("Python error, Trace follows: \n%s" %trace)
                
                
        else:
            reply = InstrumentCommandException('Unknown driver command.')
            # Command error events are better handled in the agent directly.
            #event = {
            #    'type' : DriverAsyncEvent.ERROR,
            #    'value' : str(reply),
            #    'exception' : reply,
            #    'time' : time.time()
            #}
            #self.send_event(event)
        
        return reply        
            
    def send_event(self, evt):
        """
        Append an event to the list to be sent by the event threaed.
        """
        self.events.append(evt)
            
    def run(self):
        """
        Process entry point. Construct driver and start messaging loops.
        Periodically check messaging is going and parent exists if
        specified.
        """

        from mi.core.log import LoggerManager
        LoggerManager()

        log.info('Driver process started.')
        
        def shand(signum, frame):
            log.info('DRIVER GOT SIGINT')        
        signal.signal(signal.SIGINT, shand)

        if self.construct_driver():
            self.start_messaging()
            while self.messaging_started:
                if self.check_parent():
                    time.sleep(2)
                else:
                    self.stop_messaging()
                    break
            
        self.shutdown()
        time.sleep(1)
        os._exit(0)
        
