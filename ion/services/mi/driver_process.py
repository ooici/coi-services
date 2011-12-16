#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'


from multiprocessing import Process
import time

class DriverProcess(Process):
    """
    class docstring
    """
    
    def __init__(self, interval):
        """
        method docstring
        """
        Process.__init__(self)
        self.interval = interval

    def run(self):
        """
        method docstring
        """
        
        while True:
            print('The time is %s' % time.ctime())
            time.sleep(self.interval)


    