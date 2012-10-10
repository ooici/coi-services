
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

def start_ctd_publisher(container):


    pubsubclient = PubsubManagementServiceClient(node=container.node)
    stream_id, route = pubsubclient.create_stream('ctd_publisher', exchange_point='science_data')

    pid = container.spawn_process('ctdpublisher', 'ion.processes.data.sinusoidal_stream_publisher','SinusoidalCtdPublisher',{'process':{'stream_id':stream_id}})

    print 'stream_id=' + stream_id
    print 'pid=' + pid