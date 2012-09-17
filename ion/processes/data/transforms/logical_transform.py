import commands
import time
from gevent.greenlet import Greenlet
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from pyon.ion.streamproc import StreamProcess
from pyon.ion.transforma import TransformDataProcess
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log



class logical_transform(TransformDataProcess):


    # send the packet out the same way it came in
    def recv_packet(self, msg, *args, **kwargs):
        self.publish(msg)



  
