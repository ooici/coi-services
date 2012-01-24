'''
@author Luke Campbell
@file ion/services/dm/transformation/example/transform_example.py
@description an Example of a transform
'''
import threading
import time
from pyon.ion.streamproc import StreamProcess
from pyon.public import log
from pyon.ion.transform import TransformDataProcess


class TransformExampleProducer(StreamProcess):
    """
    id_p = cc.spawn_process('myproducer', 'ion.services.dm.transformation.example.transform_example', 'TransformExampleProducer', {'process':{'type':'stream_process','publish_streams':{'out_stream':'forced'}},'stream_producer':{'interval':4000}})
    cc.proc_manager.procs['%s.%s' %(cc.id,id_p)].start()
    """

    def on_init(self):
        log.debug("StreamProducer init. Self.id=%s" % self.id)


    def start(self):

        log.debug("StreamProducer start")
        # Threads become efficent Greenlets with gevent
        streams = self.CFG.get('process',{}).get('publish_streams',None)
        if streams:
            self.output_streams = list(k for k in streams)
        else:
            self.output_streams = None

        self.producer_proc = threading.Thread(target=self._trigger_func)


        self.producer_proc.start()


    def process(self, packet):
        pass

    def on_quit(self):
        log.debug("StreamProducer quit")

    def _trigger_func(self):
        interval = self.CFG.get('stream_producer').get('interval')
        stream_route = self.CFG.get('stream_producer').get('stream_route')
        if self.output_streams:
            pub = getattr(self,self.output_streams[0],None)
        else:
            pub = None
        num = 1
        while True:
            msg = dict(num=str(num))
            pub.publish(msg)
            log.debug("Message %s published", num)
            num += 1
            time.sleep(interval/1000.0)


class TransformExample(TransformDataProcess):

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
        super(TransformExample,self).on_start()
        self.has_output = (len(self.streams)>0)


    def process(self, packet):
        """Processes incoming data!!!!
        """
        output = int(packet.get('num',0)) + 1
        log.debug('(%s) Processing Packet: %s',self.name,packet)
        log.debug('(%s) Transform Complete: %s', self.name, output)

        if self.has_output:
            self.publish(dict(num=str(output)))


        with open('/tmp/transform_output', 'a') as f:

            f.write('(%s): Received Packet: %s\n' % (self.name,packet))
            f.write('(%s):   - Transform - %d\n' % (self.name,output))




