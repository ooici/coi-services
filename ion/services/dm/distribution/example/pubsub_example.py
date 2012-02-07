'''
@author Tim Giguere
@file ion/services/dm/distribution/example/pubsub_example.py
@description an Example of a transform
'''
from pyon.ion.streamproc import StreamProcess
from pyon.public import log

class PubSubExample(StreamProcess):

    def __init__(self, *args, **kwargs):
        super(PubSubExample,self).__init__()

    #    def __str__(self):
    #        state_info = '  process_definition_id: ' + str(self.process_definition_id) + \
    #                     '\n  in_subscription_id: ' + str(self.in_subscription_id) + \
    #                     '\n  out_stream_id: ' + str(self.out_stream_id)
    #        return state_info

    def callback(self):
        log.debug('PubSub Process is working')


    def on_start(self):
        StreamProcess.on_start(self)
        # randomize name if none provided
        self.name = self.CFG.get('process',{}).get('name','1234')
        log.debug('PubSub Example started %s ' % self.CFG)
        self.output_streams = list(k for k in self.CFG.get('process',{}).get('publish_streams',{}))


    def process(self, packet):
        """Processes incoming data!!!!
        """
        log.debug(self.output_streams)
        publisher = getattr(self,self.output_streams[0],None)
        log.debug('%s' % publisher)
        input = int(packet.get('num',0))
        output = input + 1
        log.debug('(%s): PubSub:     %f' % (self.name,output))
        msg = dict(num=output)
        log.debug('(%s): Message Outgoing: %s' % (self.name, msg))
        publisher.publish(msg)
        with open('/tmp/pubsub_output', 'a') as f:

            f.write('Received Packet: %s\n' % packet)
            f.write('  - PubSub - %d\n' % output)