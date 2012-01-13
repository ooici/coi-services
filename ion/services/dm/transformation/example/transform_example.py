'''
@author Luke Campbell
@file ion/services/dm/transformation/example/transform_example.py
@description an Example of a transform
'''
from multiprocessing import Queue
from pyon.public import log
from pyon.ion.process import PythonIonProcess
from pyon.core.process import PyonProcess
import multiprocessing as mp

class TransformExample(PythonIonProcess):

    def __init__(self,name='',description='',version='', processapp={}):
        PyonProcess.__init__(self, target=self.callback)
        self.name = name
        self.description = description
        self.version = version
        self.processapp = processapp
        self.queue = mp.Queue(1024)


    def __str__(self):
        state_info = '  process_definition_id: ' + str(self.process_definition_id) + \
                     '\n  in_subscription_id: ' + str(self.in_subscription_id) + \
                     '\n  out_stream_id: ' + str(self.out_stream_id)
        return state_info

    def callback(self):
        log.debug('Transform Process is working')





