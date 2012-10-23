from gevent.greenlet import Greenlet
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from pyon.ion.streamproc import StreamProcess
from ion.core.process.transform import TransformDataProcess
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log

class logical_transform(TransformDataProcess):


    def on_start(self):
        super(logical_transform, self).on_start()

        if not self.CFG.process.publish_streams.has_key('logical'):
            raise BadRequest("For the logical transform, please send the stream_id using a special keyword (ex: logical)")

        self.logical_stream = self.CFG.process.publish_streams.logical

    def recv_packet(self, msg, *args, **kwargs):
        '''
        Send the packet out the same way it came in
        '''
        # Uses a publisher that is dynamically created in order to publish the message
        self.logical.publish(msg)



  
