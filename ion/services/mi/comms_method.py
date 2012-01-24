#!/usr/bin/env python

"""
@package ion.services.mi.comms_method Communications structures for ION side
@file ion/services/mi/comms_method.py
@author Steve Foley
@brief Communications classes that provide structure towards interaction
with the instrument agent, observatory, or other classes generally upstream of
the instrument driver.
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

from ion.services.mi.exceptions import CommsException

class CommsMethod(object):
    """Communications base class for talking to the observatory classes
    upstream of the instrument driver"""
    
    def send(self, msg):
        """Send the message via the encapsulated method
        
        @param msg The message to send. Should be understandable by whatever
        class is doing the sending. This may require a structured object
        that varies by subclass.
        @retval result Success/fail with error code
        @throws CommsException
        """
        pass
    
class AMQPCommsMethod(CommsMethod):
    """The AMQP specific implementation of the CommsMethod class"""
    
    def send(self, msg):
        pass
    
class ZMQCommsMethod(CommsMethod):
    """The ZeroMQ specific implementation of the CommsMethod class"""
    
    def send(self, msg):
        pass