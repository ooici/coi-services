'''
@author Luke Campbell
@file ion/services/dm/transformation/example/transform_example.py
@description an Example of a transform
'''
from pyon.ion.streamproc import StreamProcess
from pyon.public import log

class TransformExample(StreamProcess):

    def __init__(self, *args, **kwargs):
        super(TransformExample,self).__init__()

#    def __str__(self):
#        state_info = '  process_definition_id: ' + str(self.process_definition_id) + \
#                     '\n  in_subscription_id: ' + str(self.in_subscription_id) + \
#                     '\n  out_stream_id: ' + str(self.out_stream_id)
#        return state_info

    def callback(self):
        log.debug('Transform Process is working')


    def on_start(self):
        StreamProcess.on_start(self)

        log.debug('Transform Example started %s ' % self.CFG)


    def process(self, packet):
        """Processes incoming data!!!!
        """
        log.debug('transform example has received data')
