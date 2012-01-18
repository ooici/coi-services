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
        # randomize name if none provided
        self.name = self.CFG.get('process',{}).get('name','1234')
        log.debug('Transform Example started %s ' % self.CFG)
        self.output_streams = list(k for k in self.CFG.get('process',{}).get('publish_streams',{}))


    def process(self, packet):
        """Processes incoming data!!!!
        """
        log.debug(self.output_streams)
        publisher = getattr(self,self.output_streams[0],None)
        log.debug('%s' % publisher)
        input = int(packet.get('num',0))
        output = input + 1
        log.debug('(%s): Transform: %f' % (self.name,output))
        msg = dict(num=output)
        log.debug('(%s): Message Outgoing: %s' % (self.name, msg))
        if publisher:
            publisher.publish(msg)
        with open('/tmp/transform_output', 'a') as f:

            f.write('(%s): Received Packet: %s\n' % (self.name,packet))
            f.write('(%s):   - Transform - %d\n' % (self.name,output))




