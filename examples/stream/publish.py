
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

def start_ctd_publisher(container):


    pubsubclient = PubsubManagementServiceClient(node=container.node)

    ctd_stream_def_id = pubsubclient.create_stream_definition(name='SBE37_CDM')

    stream_id, route = pubsubclient.create_stream('ctd_publisher', exchange_point='science_data', stream_definition_id=ctd_stream_def_id)

    pid = container.spawn_process('ctdpublisher', 'ion.processes.data.sinusoidal_stream_publisher','SinusoidalCtdPublisher',
        {'process':{'stream_id':stream_id}})

    print 'stream_id=' + stream_id
    print 'pid=' + pid