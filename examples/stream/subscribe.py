

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.ion.stream import StandaloneStreamSubscriber


my_subscriber = ''
my_subscription_id = ''

def start_ctd_listener(container):

    pubsubclient = PubsubManagementServiceClient(node=container.node)
    rrclient = ResourceRegistryServiceClient(node=container.node)

    def message_received(msg, stream_route, stream_id):
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


    stream_id,_ = rrclient.find_resources(name='ctd_publisher', id_only=True)

    my_subscription_id = pubsubclient.create_subscription(name='sub1',exchange_name='ctd_listener', stream_ids= stream_id)

    my_subscriber = StandaloneStreamSubscriber(exchange_name='ctd_listener', callback=message_received)

    my_subscriber.start()

    pubsubclient.activate_subscription(subscription_id=my_subscription_id)

