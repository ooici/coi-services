#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from pyon.core.exception import ServerError
from ion.agents.instrument.exceptions import NotImplementedException
import time
import gevent
import uuid


class DirectAccessTypes:
    # set up range so values line up with attributes of class to get enum names back
    # using 'dir(DirectAccessTypes)[enum_value]
    # NOTE: list names must be alphabetical for this to work
    (ssh, telnet, vsp) = range(2, 5)
    
class SessionCloseReasons:
    # set up range so values line up with attributes of class to get enum names back
    # using 'dir(SessionCloseReasons)[enum_value]
    # NOTE: list names must be alphabetical for this to work
    (client_closed, inactivity_timeout, parent_closed, session_timeout) = range(2, 6)
           

"""
TELNET server class
"""
    
class TcpServer(object):

    server_socket = None
    port = None
    ip_address = None
    parent_input_callback = None
    fileobj = None
    PORT_RANGE_LOWER = 8000
    PORT_RANGE_UPPER = 8010
    close_reason = SessionCloseReasons.client_closed
    activity_seen = False
    already_stopping = False


    def __init__(self, input_callback=None, ip_address=None):
        log.debug("TcpServer.__init__(): IP address = %s" %ip_address)

        # save callback if specified
        if not input_callback:
            log.warning("TcpServer.__init__(): callback not specified")
            raise ServerError("TcpServer.__init__(): callback not specified")
        self.parent_input_callback = input_callback
        
        # save ip address if specified
        if not ip_address:
            log.warning("TcpServer.__init__(): IP address not specified")
            raise ServerError("TcpServer.__init__(): IP address not specified")
        self.ip_address = ip_address
        
        # search for available port
        self.port = self.PORT_RANGE_LOWER
        # create a TCP socket
        self.server_socket = gevent.socket.socket()
        self.server_socket.allow_reuse_address = True
        while True:
            try:
                log.debug("trying to bind to port %s on %s" %(str(self.port), self.ip_address))
                self.server_socket.bind((self.ip_address, self.port))
                break
            except Exception as ex:
                log.debug("exception caught for socket bind:" + str(ex))
                self.port = self.port + 1
                if self.port > self.PORT_RANGE_UPPER:
                    log.warning("TcpServer.__init__(): no available ports for server")
                    raise ServerError("TcpServer.__init__(): no available ports")
                    return

        # create token
        self.token = str(uuid.uuid4()).upper()
        
        log.debug("TcpServer.__init__(): starting server greenlet")
        self.server = gevent.spawn(self._server_greenlet)
        

    # public methods
    
    def get_connection_info(self):
        return self.port, self.token


    def any_activity(self):
        if self.activity_seen:
            # re-arm the activity detector
            self.activity_seen = False
            return True
        return False
    
    
    def stop(self, reason=None):
        if self.already_stopping == True:
            log.debug("TcpServer.stop(): already stopping")
            return
        log.debug("TcpServer.stop(): stopping telnet server - reason = %s", dir(SessionCloseReasons)[reason])
        if (reason):
            self.close_reason = reason
        self.server.kill()
        log.debug("TcpServer.stop(): server killed")
            

    def send(self, data):
        # send data from parent to telnet server process to forward to client
        log.debug("TcpServer.send(): data = " + str(data))
        self._write(data)
        

    # private methods

    def _write(self, text):
        log.debug("TcpServer._write(): text = " + str(text))
        if self.connection_socket:
            self.activity_seen = True;
            MSGLEN = len(text)
            total_sent = 0
            while total_sent < MSGLEN:
                sent = self.connection_socket.send(text[total_sent:])
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                total_sent = total_sent + sent
        else:
            log.warning("TcpServer._write(): no connection yet, can not write text")            

    
    def _writeline(self, text):
        """Send a packet with line ending."""
        self._write(text+chr(10))


    def _authorized(self, token):
        if token == self.token:
            return True
        else:
            log.debug("TcpServer._authorized: entered token =" + token + ", expected token =" + self.token)
            return False
    

    def _exit_handler (self, reason):
        log.debug("TcpServer._exit_handler(): stopping, " + reason)
        self.already_stopping = True
        # indicate to parent that connection seems to have been closed by client
        self.parent_input_callback(self.close_reason)
    

    def _handler(self):
        "The actual server to which the user has connected."
        log.debug("TcpServer._handler(): not implemented")
        raise NotImplementedException('TcpServer._handler() not implemented.')        
            

    def _server_greenlet(self):
        log.debug("TcpServer._server_greenlet(): started")
        self.connection_socket = None
        try:
            self.server_socket.listen(1)
            self.connection_socket, address = self.server_socket.accept()
            self._handler()
        except Exception as ex:
            log.info("TcpServer._server_greenlet(): exception caught <%s>" %str(ex))
            if self.close_reason != SessionCloseReasons.parent_closed:
                # indicate to parent that connection has been closed if it didn't initiate it
                log.debug("TcpServer._server_greenlet(): telling parent to close session")
                self.parent_input_callback(self.close_reason)
        log.debug("TcpServer._server_greenlet(): stopped")
        

class TelnetServer(TcpServer):

    #TELNET_PROMPT = 'ION telnet>'
    TELNET_PROMPT = None


    def _handler(self):
        "The actual telnet server to which the user has connected."
        log.debug("TelnetServer._handler(): starting")
        
        self.fileobj = self.connection_socket.makefile()
        username = None
        token = None
        self._write("Username: ")
        username = self.fileobj.readline().rstrip('\n\r')
        if username == '':
            self._exit_handler("lost connection")
            return
        self.activity_seen = True;
        self._write("token: ")
        token = self.fileobj.readline().rstrip('\n\r')
        if token == '':
            self._exit_handler("lost connection")
            return
        self.activity_seen = True;
        if not self._authorized(token):
            log.debug("login failed")
            self._writeline("login failed")
            self._exit_handler("login failed")
            return
        self._writeline("connected")   # let telnet client user know they are connected
        while True:
            if self.TELNET_PROMPT:
                self._write(self.TELNET_PROMPT)
            input_line = self.connection_socket.recv(1024)
            if input_line == '':
                self._exit_handler("lost connection")
                break
            self.activity_seen = True;
            log.debug("rcvd: " + input_line)
            log.debug("len=" + str(len(input_line)))
            for i in range(len(input_line)):
                log.debug("%d - %x", i, ord(input_line[i])) 
            self.parent_input_callback(input_line)
            

class SerialServer(TcpServer):


    def _handler(self):
        "The actual serial server to which the user has connected."
        log.debug("SerialServer._handler(): starting")
        
        self.fileobj = self.connection_socket.makefile()
        while True:
            input = self.connection_socket.recv(1024)
            if input == '':
                self._exit_handler("lost connection")
                break
            self.activity_seen = True;
            log.debug("rcvd: " + input)
            log.debug("len=" + str(len(input)))
            for i in range(len(input)):
                log.debug("%d - %x", i, ord(input[i])) 
            self.parent_input_callback(input)
            

class DirectAccessServer(object):
    """
    Class for direct access server that interfaces to an IA ResourceAgent
    """
    
    server = None
    already_stopping = False
    
    def __init__(self, 
                 direct_access_type=None, 
                 input_callback=None, 
                 ip_address=None,
                 session_timeout=None,
                 inactivity_timeout=None):
        log.debug("DirectAccessServer.__init__()")

        if not direct_access_type:
            log.warning("DirectAccessServer.__init__(): direct access type not specified")
            raise ServerError("DirectAccessServer.__init__(): direct access type not specified")

        if not input_callback:
            log.warning("DirectAccessServer.__init__(): callback not specified")
            raise ServerError("DirectAccessServer.__init__(): callback not specified")
               
        if not ip_address:
            log.warning("DirectAccessServer.__init__(): IP address not specified")
            raise ServerError("DirectAccessServer.__init__(): IP address not specified")
               
        if not session_timeout:
            log.warning("DirectAccessServer.__init__(): session timeout not specified")
            raise ServerError("DirectAccessServer.__init__(): session timeout not specified")
               
        if not inactivity_timeout:
            log.warning("DirectAccessServer.__init__(): inactivity timeout not specified")
            raise ServerError("DirectAccessServer.__init__(): inactivity timeout not specified")
               
        # start the correct server based on direct_access_type
        if direct_access_type == DirectAccessTypes.telnet:
            self.server = TelnetServer(input_callback, ip_address)
        elif direct_access_type == DirectAccessTypes.vsp:
            self.server = SerialServer(input_callback, ip_address)
        else:
            raise ServerError("DirectAccessServer.__init__(): Unsupported direct access type")

        log.debug("DirectAccessServer.__init__(): starting timer greenlet")
        self.timer = gevent.spawn(self._timer_greenlet, 
                                   session_timeout=session_timeout,
                                   inactivity_timeout=inactivity_timeout)
                

    # public methods
    
    def stop(self, reason=SessionCloseReasons.parent_closed):
        if self.already_stopping == True:
            log.debug("DirectAccessServer.stop(): already stopping")
            return
        log.debug("DirectAccessServer.stop(): stopping DA server - reason = %s", dir(SessionCloseReasons)[reason])
        self.already_stopping = True
        if self.server:
            self.server.stop(reason)
            del self.server
        if self.timer and reason == SessionCloseReasons.parent_closed:
            # timer didn't initiate the stop, so kill it
            try:
                self.timer.kill()
            except Exception as ex:
                # can happen if timer already closed session (race condition)
                log.debug("DirectAccessServer.stop(): exception caught for timer kill: " + str(ex))
        

    def get_connection_info(self):
        if self.server:
            return self.server.get_connection_info()
        else:
            return 0, "no-token"
    

    def send(self, data):
        log.debug("DirectAccessServer.send(): data = " + str(data))
        if self.server:
            self.server.send(data)
            
            
    # private methods
    
    def _timer_greenlet(self, session_timeout, inactivity_timeout):
        log.debug("DirectAccessServer._timer_greenlet(): started - sessionTO=%d, inactivityTO=%d"
                  %(session_timeout, inactivity_timeout))
        session_start_time = inactivity_start_time = time.time()
        try:
            while True:
                gevent.sleep(1)
                timenow = time.time()
                if ((timenow - session_start_time) > session_timeout):
                    log.debug("DirectAccessServer._timer_greenlet(): session exceeded session timeout of %d seconds"
                              %session_timeout)
                    self.stop(SessionCloseReasons.session_timeout)
                    break
                if self.server.any_activity():
                    inactivity_start_time = time.time()
                elif ((timenow - inactivity_start_time) > inactivity_timeout):
                    log.debug("DirectAccessServer._timer_greenlet(): session exceeded inactivity timeout of %d seconds"
                              %inactivity_timeout)
                    self.stop(reason=SessionCloseReasons.inactivity_timeout)
                    break
        except:
            pass
        log.debug("DirectAccessServer._timer_greenlet(): stopped ")
                

        
        
        
