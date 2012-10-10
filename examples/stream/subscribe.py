
from pyon.util.containers import DotDict
from pyon.ion.transforma import TransformStreamListener


def start_ctd_subscriber(container):

    config = DotDict()
    config.process.queue_name = 'output_queue'  #Give the queue a name

    #spawn the process. ExampleDataReceiver will create the queue
    container.spawn_process(name='stream_subscriber', module='examples.stream.subscribe', cls='StreamSubscriber', config=config)

    xn2 = container.ex_manager.create_xn_queue(config.process.queue_name)     #create_xn_queue gets the queue with the given name or creates it if it doesn't exist
    xp2 = container.ex_manager.create_xp('science_data')              #create_xp gets the xp with the given name or creates it
    xn2.bind('ctd_publisher.stream', xp2)                             #bind the queue to the xp




class StreamSubscriber(TransformStreamListener):


    def recv_packet(self, msg, stream_route, stream_id):
        from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
        rdt = RecordDictionaryTool.load_from_granule(msg)
        #print 'Message Received: {0}'.format(rdt.pretty_print())
        flds = ''
        for field in rdt.fields:
            flds += field + ' '
        print '\n' + flds
        for i in xrange(len(rdt)):
            var_tuple = [ float(rdt[field][i]) if rdt[field] is not None else 0.0 for field in rdt.fields]
            print var_tuple

