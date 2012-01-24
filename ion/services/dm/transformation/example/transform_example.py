'''
@author Luke Campbell
@file ion/services/dm/transformation/example/transform_example.py
@description an Example of a transform
'''
import threading
import time
from pyon.ion.streamproc import StreamProcess
from pyon.ion.transform import TransformDataProcess
from pyon.ion.transform import TransformProcessAdaptor
from pyon.ion.transform import TransformFunction
from pyon.service.service import BaseService

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.public import IonObject, RT, log, AT



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





class ExternalTransform(TransformProcessAdaptor):
    pass

class ReverseTransform(TransformFunction):
    def execute(self, input):
        retval = input
        retval.reverse()

        return retval


class TransformExampleLauncher(BaseService):
    def on_start(self):
        self.name = self.CFG.get('name')

        # Parse config for which example script to run
        run_str = 'run_' + self.CFG.get('example','basic_transform')
        script = getattr(self,run_str)
        script()
    def run_basic_transform(self):
        pubsub_cli = PubsubManagementServiceClient(node=self.container.node)
        tms_cli = TransformManagementServiceClient(node=self.container.node)
        rr_cli = ResourceRegistryServiceClient(node=self.container.node)

        #-------------------------------
        # Process Definition
        #-------------------------------

        process_definition = IonObject(RT.ProcessDefinition, name='transform_process_definition')
        process_definition.executable = {
            'module': 'ion.services.dm.transformation.example.transform_example',
            'class':'TransformExample'
        }
        process_definition_id, _ = rr_cli.create(process_definition)

        #-------------------------------
        # First Transform
        #-------------------------------

        # Create a dummy output stream from a 'ctd' instrument
        ctd_output_stream = IonObject(RT.Stream,name='ctd1 output', description='output from a ctd')
        ctd_output_stream.original = True
        ctd_output_stream.mimetype = 'hdf'
        ctd_output_stream_id = pubsub_cli.create_stream(ctd_output_stream)

        # Create the subscription to the ctd_output_stream
        ctd_subscription = IonObject(RT.Subscription,name='ctd1 subscription', description='subscribe to this if you want ctd1 data')
        ctd_subscription.query['stream_id'] = ctd_output_stream_id
        ctd_subscription.exchange_name = 'a queue'
        ctd_subscription_id = pubsub_cli.create_subscription(ctd_subscription)

        # Create an output stream for the transform
        transform_output_stream = IonObject(RT.Stream,name='transform output', description='output from the transform process')
        transform_output_stream.original = True
        transform_output_stream.mimetype='raw'
        transform_output_stream_id = pubsub_cli.create_stream(transform_output_stream)


        configuration = {}


        # Launch the first transform process
        transform_id = tms_cli.create_transform( name='basic_transform',
            in_subscription_id=ctd_subscription_id,
            out_streams={'output':transform_output_stream_id},
            process_definition_id=process_definition_id,
            configuration=configuration)
        tms_cli.activate_transform(transform_id)


        #-------------------------------
        # Second Transform
        #-------------------------------

        # Create a SUBSCRIPTION to this output stream for the second transform
        second_subscription = IonObject(RT.Subscription,name='second_subscription', description='the subscription to the first transforms data')
        second_subscription.query['stream_id'] = transform_output_stream_id
        second_subscription.exchange_name = 'final output'
        second_subscription_id = pubsub_cli.create_subscription(second_subscription)

        # Create a final output stream
        final_output = IonObject(RT.Stream,name='final_output_stream',description='Final output')
        final_output.original = True
        final_output.mimetype='raw'
        final_output_id = pubsub_cli.create_stream(final_output)


        configuration = {}

        second_transform_id = tms_cli.create_transform( name='second_transform',
            in_subscription_id=second_subscription_id,
            out_streams={'output':final_output_id},
            process_definition_id=process_definition_id,
            configuration=configuration)
        tms_cli.activate_transform(second_transform_id)

        #-------------------------------
        # Producer (Sample Input)
        #-------------------------------

        # Create a producing example process
        id_p = self.container.spawn_process('myproducer', 'ion.services.dm.transformation.example.transform_example', 'TransformExampleProducer', {'process':{'type':'stream_process','publish_streams':{'out_stream':ctd_output_stream_id}},'stream_producer':{'interval':4000}})
        self.container.proc_manager.procs['%s.%s' %(self.container.id,id_p)].start()

    def run_reverse_transform(self):
        pubsub_cli = PubsubManagementServiceClient(node=self.container.node)
        tms_cli = TransformManagementServiceClient(node=self.container.node)
        rr_cli = ResourceRegistryServiceClient(node=self.container.node)


        #-------------------------------
        # Process Definition
        #-------------------------------

        process_definition = IonObject(RT.ProcessDefinition, name='transform_process_definition')
        process_definition.executable = {
            'module': 'ion.services.dm.transformation.example.transform_example',
            'class':'ReverseTransform'
        }
        process_definition_id, _ = rr_cli.create(process_definition)





        #-------------------------------
        # Execute Transform
        #-------------------------------
        input = [1,2,3,4]
        retval = tms_cli.execute_transform(process_definition_id=process_definition_id,
            data=[1,2,3,4],
            configuration={})
        log.debug('Transform Input: %s', input)
        log.debug('Transform Output: %s', retval)