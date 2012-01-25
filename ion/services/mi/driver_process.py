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
from subprocess import Popen

mi_logger = logging.getLogger('mi_logger')

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
        return Popen(spawnargs)
        
    def __init__(self, driver_module, driver_class):
        """
        @param driver_module The python module containing the driver code.
        @param driver_class The python driver class.
        """
        self.driver_module = driver_module
        self.driver_class = driver_class
        self.driver = None
        self.events = []
        
    def construct_driver(self):
        """
        Attempt to import and construct the driver object based on
        configuration.
        @retval True if successful, False otherwise.
        """
        
        import_str = 'import %s as dvr_mod' % self.driver_module
        ctor_str = 'driver = dvr_mod.%s()' % self.driver_class
        
        try:
            exec import_str
            mi_logger.info('Imported driver module %s', self.driver_module)
            exec ctor_str
            mi_logger.info('Constructed driver %s', self.driver_class)
            
        except (ImportError, NameError, AttributeError):
            mi_logger.error('Could not import/construct driver module %s, class %s.',
                      self.driver_module, self.driver_class)
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
        mi_logger.info('Driver process shutting down.')
        self.driver_module = None
        self.driver_class = None
        self.driver = None

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
        if cmd == 'stop_driver_process':
            self.stop_messaging()
            return'stop_driver_process'
        elif cmd == 'test_events':
            events = kwargs['events']
            self.events += events
            #self.events.append('I am event number 1!')
            #self.events.append('And I am event number 2!')
            reply = 'test_events'
        elif cmd == 'process_echo':
            reply = msg
        elif cmd_func:
            reply = cmd_func(*args, **kwargs)
        else:
            reply = 'Unknown driver command'
        
        return reply        
            
    def run(self):
        """
        Process entry point. Construct driver and start messaging loops.
        Call shutdown when messaging terminates amd then end process.
        """
        
        mi_logger.info('Driver process started.')

        if self.construct_driver():
            self.start_messaging()

        self.shutdown()

