'''
@author Luke Campbell
@file ion/services/dm/transformation/example/transform_example.py
@description an Example of a transform
'''
import threading
import time
from interface.objects import ProcessDefinition, StreamQuery
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

class TransformEvenOdd(TransformDataProcess):
    '''A simple transform that takes the input of a number and maps an even and odd sequence
    to two separate streams, even and odd
    '''
    def on_start(self):
        super(TransformEvenOdd,self).on_start()
        assert len(self.streams)==2

    def process(self, packet):
        input = int(packet.get('num'))

        even = input * 2
        odd = (input * 2) + 1
        self.even.publish(dict(num=even))
        self.odd.publish(dict(num=odd))
        log.debug('(%s) Processing Packet: %s', self.name, packet)
        log.debug('(%s) Even Transform: %s', self.name, even)
        log.debug('(%s) Odd Transform: %s', self.name, odd)

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
    """
    This Service Launches and controls the execution of various transform examples.
    """


    def __init__(self, *args, **kwargs):
        super(TransformExampleLauncher,self).__init__(*args,**kwargs)

        #-------------------------------
        # Process Definitions
        #-------------------------------

        transform_example_definition = ProcessDefinition(name='transform_example_definition')
        transform_example_definition.executable['module'] = 'ion.services.dm.transformation.transform_example'
        transform_example_definition.executable['class'] = 'TransformExample'

    #-------------------------------
    # on_start()
    #-------------------------------
    def on_start(self):
        ''' Parses the example configuration parameter and prepends 'run_' on it, then executes the function
        '''
        self.name = self.CFG.get('name')

        # Parse config for which example script to run
        run_str = 'run_' + self.CFG.get('example','basic_transform')
        script = getattr(self,run_str)
        script()

    #-------------------------------
    # run_basic_transform()
    #-------------------------------
    def run_basic_transform(self):
        ''' Runs a basic example of a transform. It chains two transforms together, each add 1 to their input

        Producer -> A -> B
        Producer generates a number every four seconds and publishes it on the 'ctd_output_stream'
          the producer is acting as a CTD or instrument in this example.
        A is a basic transform that increments its input and publishes it on the 'transform_output' stream.
        B is a basic transform that receives input.
        All transforms write logging data to '/tmp/transform_output' so you can visually see activity of the transforms
        '''

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
        ctd_output_stream_id = pubsub_cli.create_stream(name='ctd_output_stream', original=True)

        # Create the subscription to the ctd_output_stream
        query = StreamQuery(stream_ids=[ctd_output_stream_id])
        ctd_subscription_id = pubsub_cli.create_subscription(query=query, exchange_name='ctd_output')

        # Create an output stream for the transform
        transform_output_stream_id = pubsub_cli.create_stream(name='transform_output', original=True)

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
        query = StreamQuery(stream_ids=[transform_output_stream_id])
        second_subscription_id = pubsub_cli.create_subscription(query=query, exchange_name='final_output')

        # Create a final output stream
        final_output_id = pubsub_cli.create_stream(name='final_output', original=True)


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

    #-------------------------------
    # run_reverse_transform()
    #-------------------------------
    def run_reverse_transform(self):
        ''' Runs a reverse transform example and displays the results of performing the transform
        '''
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

    #-------------------------------
    # run_even_odd_transform()
    #-------------------------------
    def run_even_odd_transform(self):
        '''
        This example script runs a chained three way transform:
            B
        A <
            C
        Where A is the even_odd transform (generates a stream of even and odd numbers from input)
        and B and C are the basic transforms that receive even and odd input
        '''
        pubsub_cli = PubsubManagementServiceClient(node=self.container.node)
        tms_cli = TransformManagementServiceClient(node=self.container.node)
        rr_cli = ResourceRegistryServiceClient(node=self.container.node)


        #-------------------------------
        # Process Definition
        #-------------------------------
        # Create the process definition for the basic transform
        process_definition = IonObject(RT.ProcessDefinition, name='basic_transform_definition')
        process_definition.executable = {
            'module': 'ion.services.dm.transformation.example.transform_example',
            'class':'TransformExample'
        }
        basic_transform_definition_id, _ = rr_cli.create(process_definition)

        # Create The process definition for the TransformEvenOdd
        process_definition = IonObject(RT.ProcessDefinition, name='basic_transform_definition')
        process_definition.executable = {
            'module': 'ion.services.dm.transformation.example.transform_example',
            'class':'TransformEvenOdd'
        }
        evenodd_transform_definition_id, _ = rr_cli.create(process_definition)

        #-------------------------------
        # Streams
        #-------------------------------
        input_stream_id = pubsub_cli.create_stream(name='input_stream', original=True)

        even_stream_id = pubsub_cli.create_stream(name='even_stream', original=True)

        odd_stream_id = pubsub_cli.create_stream(name='odd_stream', original=True)

        #-------------------------------
        # Subscriptions
        #-------------------------------

        query = StreamQuery(stream_ids=[input_stream_id])
        input_subscription_id = pubsub_cli.create_subscription(query=query, exchange_name='input_queue')

        query = StreamQuery(stream_ids = [even_stream_id])
        even_subscription_id = pubsub_cli.create_subscription(query=query, exchange_name='even_queue')

        query = StreamQuery(stream_ids = [odd_stream_id])
        odd_subscription_id = pubsub_cli.create_subscription(query=query, exchange_name='odd_queue')


        #-------------------------------
        # Launch the EvenOdd Transform
        #-------------------------------

        evenodd_id = tms_cli.create_transform(name='even_odd',
            in_subscription_id=input_subscription_id,
            out_streams={'even':even_stream_id, 'odd':odd_stream_id},
            process_definition_id=evenodd_transform_definition_id,
            configuration={})
        tms_cli.activate_transform(evenodd_id)


        #-------------------------------
        # Launch the Even Processing Transform
        #-------------------------------

        even_transform_id = tms_cli.create_transform(name='even_transform',
            in_subscription_id = even_subscription_id,
            process_definition_id=basic_transform_definition_id,
            configuration={})
        tms_cli.activate_transform(even_transform_id)

        #-------------------------------
        # Launch the Odd Processing Transform
        #-------------------------------

        odd_transform_id = tms_cli.create_transform(name='odd_transform',
            in_subscription_id = odd_subscription_id,
            process_definition_id=basic_transform_definition_id,
            configuration={})
        tms_cli.activate_transform(odd_transform_id)

        #-------------------------------
        # Spawn the Streaming Producer
        #-------------------------------

        id_p = self.container.spawn_process('myproducer', 'ion.services.dm.transformation.example.transform_example', 'TransformExampleProducer', {'process':{'type':'stream_process','publish_streams':{'out_stream':input_stream_id}},'stream_producer':{'interval':4000}})
        self.container.proc_manager.procs['%s.%s' %(self.container.id,id_p)].start()
