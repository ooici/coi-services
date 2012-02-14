'''
@author Swarbhanu Chatterjee
@file ion/services/dm/ingestion/ingestion_example.py
@description an IngestionExampleProducer class used to produce streams, IngestionExample class that allows ingestion workers to
handle the streams.
'''
import time
from pyon.ion.streamproc import StreamProcess
from pyon.ion.transform import TransformDataProcess
from pyon.public import log
from gevent.greenlet import Greenlet

class IngestionExampleProducer(StreamProcess):
    """
    Replace this with a generic example
    id_p = cc.spawn_process('myproducer', 'ion.services.dm.ingestion.ingestion_example', 'IngestionExampleProducer', {'process':{'type':'stream_process','publish_streams':{'out_stream':'forced'}},'stream_producer':{'interval':4000}})
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

        self.producer_proc = Greenlet(self._trigger_func)
        self.producer_proc.start()

    def process(self, packet):
        pass

    def on_quit(self):
        log.debug("StreamProducer quit")
        self.producer_proc.kill()

    def _trigger_func(self):
        interval = self.CFG.get('stream_producer').get('interval')
        stream_route = self.CFG.get('stream_producer').get('stream_route')
        if self.output_streams:
            pub = getattr(self,self.output_streams[0],None)
        else:
            pub = None
        num = 1
        while num < 20:
            msg = dict(num=str(num))
            if pub:
                pub.publish(msg)
            log.debug("Message %s published", num)
            num += 1
            time.sleep(interval/1000.0)

#            with open('/tmp/ingestion_publisher_process', 'a') as f:
#                f.write('Published packet: %s\n' % num)




class IngestionExample(TransformDataProcess):

    def __init__(self, *args, **kwargs):
        super(IngestionExample,self).__init__()

    def callback(self):
        log.debug('Ingestion Process is working')


    def on_start(self):
        super(IngestionExample,self).on_start()
#        self.has_output = (len(self.streams)>0)

    def process(self, packet):
        """Processes incoming data!!!!
        """
        output = int(packet.get('num',0))
        log.debug('(%s) Processing Packet: %s',self.name,packet)
        log.debug('(%s) Transform Complete: %s', self.name, output)

#        if self.has_output:
#            self.publish(dict(num=str(output)))

        with open('/tmp/ingestion_subscriber_output', 'a') as f:

            f.write('(%s): Received Packet: %s\n' % (self.name,packet))
            f.write('(%s):   - Transform - %d\n' % (self.name,output))

        #-------------------------------------------------------------------------------------------------
        # Test that the ingestion workers are handling packets in round robin
        #  Works when the packet is a num that is incremented by one.
        #  Modify for other cases
        if output > 2:
            if (output - self.num == 2):
                log.debug('INGESTION WORKERS ARE HANDLING PACKETS IN ROUND ROBIN! GOOD!')
            else:
                raise AssertionError('ERROR: ROUND ROBIN HANDLING IS NOT WORKING!')

        self.num = output

        # ensure messages are handled in a round robin manner
#        log.debug('Previous worker: %s, Current worker: %s' % (self.previous_worker_name, self.name))
#        if self.name == self.previous_worker_name:
#            log.debug("Round Robin Error: Ingestion workers are not working in round robbin. Previous worker: %s, Current worker: %s"\
#            % (self.previous_worker_name,self.name))
#        self.previous_worker_name = self.name


#        if self.name == self.previous_worker_name:
#            same_worker+=1
#        if (same_worker/input)>0.5:
#            raise Assertion("Ingestion workers are not working in round robbin")
#        self.previous_worker_name = self.name




