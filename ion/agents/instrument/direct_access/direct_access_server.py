#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from pyon.core.exception import ServerError
from ion.agents.instrument.exceptions import NotImplementedException, \
                                             InstrumentException
import time
import gevent
import uuid
import errno
import sys


class ServerExitException(InstrumentException):
    """Exit from a server"""
    pass

class DirectAccessTypes:
    # set up range so values line up with attributes of class to get enum names back
    # using 'dir(DirectAccessTypes)[enum_value]
    # NOTE: list names must be in alphabetical order for this to work
    (ssh, telnet, vsp) = range(2, 5)
    
class SessionCloseReasons:
    enum_range = range(0, 8)
    
    (client_closed, 
     inactivity_timeout, 
     parent_closed, 
     session_timeout, 
     login_failed,
     telnet_setup_timeout,
     socket_error,
     unspecified_reason) = enum_range
     
    str_rep = ['client closed', 
               'inactivity timeout', 
               'parent closed', 
               'session timeout', 
               'login failed',
               'telnet setup timed out',
               'TCP socket error',
               'unspecified reason']
    
    @staticmethod
    def string(enum_value):
        if not isinstance(enum_value, int):
            return 'SessionCloseReasons.string: ERROR - enum not an int'
        if enum_value not in SessionCloseReasons.enum_range:
            return 'unrecognized reason - %d' %enum_value
        return SessionCloseReasons.str_rep[enum_value]
           

"""
TCP server class
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
    server_ready_to_send = False
    stop_server = False

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
    
    
    def stop(self, reason):
        if self.stop_server == True:
            log.debug("TcpServer.stop(): already stopping")
            return
        self.stop_server = True
        self.server_ready_to_send = False
        # set close reason in case it's not 'parent closed' so server can inform parent via callback
        self.close_reason = reason
        log.debug("TcpServer.stop(): stopping TCP server - reason = %s", SessionCloseReasons.string(self.close_reason))     
            

    def send(self, data):
        # send data from parent to telnet server process to forward to client
        log.debug("TcpServer.send(): data = " + str(data))
        if self.server_ready_to_send:
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
        self._write(text+chr(13)+chr(10))


    def _authorized(self, token):
        if token == self.token:
            return True
        else:
            log.debug("TcpServer._authorized: entered token =" + token + ", expected token =" + self.token)
            return False
    

    def _indicate_server_stopping(self, reason):
        self.stop_server = True
        self.close_reason = reason
        self.server_ready_to_send = False


    def _exit_handler (self, reason):
        log.debug("TcpServer._exit_handler(): stopping, reason = %s" %SessionCloseReasons.string(reason))
        if self.stop_server == False:
            self._indicate_server_stopping(reason)
        raise ServerExitException("TcpServer: exiting from server, reason = %s" %SessionCloseReasons.string(reason))
    

    def _handler(self):
        "The actual server to which the user has connected."
        log.debug("TcpServer._handler(): not implemented")
        raise NotImplementedException('TcpServer._handler() not implemented.')        
            

    def _get_data(self, timeout=None):
        log.debug("TcpServer._get_data(): timeout = %s" %str(timeout))
        start_time = time.time()
        input_data = ''
        
        self.connection_socket.setblocking(0)   # just to be sure!
        
        while True:
            try:
                # this call must be non-blocking to let server check the stop_server flag
                input_data = self.connection_socket.recv(1024)
                if len(input_data) == 0:
                    self._exit_handler(SessionCloseReasons.client_closed)
                self.activity_seen = True;
                return input_data
            except gevent.socket.error, error:
                if error.errno == errno.EAGAIN or error.errno == errno.EWOULDBLOCK:
                    if self.stop_server:
                        self._exit_handler(self.close_reason)
                    if timeout:
                        if ((time.time() - start_time) > timeout):
                            return ''
                    gevent.sleep(.1)
                else:
                    log.debug("TcpServer._get_data(): exception caught <%s>" %str(error))
                    self._exit_handler(SessionCloseReasons.client_closed)
                    return False

                
    def _readline(self, timeout=5):
        start_time = time.time()
        input_data = ''
        
        while True:
            input_data += self._get_data(1)
            if '\r\n' in input_data:
                return input_data.split('\r\n')[0]
            if ((time.time() - start_time) > timeout):
                log.info("TcpServer._readline(): timeout, rcvd <%s>" %input_data)
                self._exit_handler(SessionCloseReasons.telnet_setup_timeout)
            gevent.sleep(.1)

                
            
    def _notify_parent(self):
        if self.close_reason != SessionCloseReasons.parent_closed:
            # indicate to parent that connection has been closed since it didn't initiate it
            log.debug("TcpServer._server_greenlet(): telling parent to close session, reason = %s"
                      %SessionCloseReasons.string(self.close_reason))
            self.parent_input_callback(self.close_reason)
        log.debug("TcpServer._server_greenlet(): stopped")
    

    def _server_greenlet(self):
        log.debug("TcpServer._server_greenlet(): started")
        # set socket to timeout on blocking calls so accept() method will timeout to let server
        # check the stop_server flag
        self.server_socket.settimeout(1.0)
        self.server_socket.listen(1)
        while True:
            try:
                self.connection_socket, address = self.server_socket.accept()
                log.info("TcpServer._server_greenlet(): connection accepted from <%s>" %str(address))
                # set socket to non-blocking so recv() will not block to let server 
                # check the stop_server flag
                self.connection_socket.setblocking(0)
                break
            except Exception as ex:
                log.info("TcpServer._server_greenlet(): exception caught while listening for connection <%s>" %str(ex))
                if self.server_socket.timeout:
                    if self.stop_server:
                        self._notify_parent()
                        return
                else:
                    log.info("TcpServer._server_greenlet(): exception caught while listening for connection <%s>" %str(ex))
                    self._indicate_server_stopping(SessionCloseReasons.socket_error)
                    self._notify_parent()
                    return
        try:
            self._handler()
        except Exception as ex:
            log.info("TcpServer._server_greenlet(): exception caught from handler <%s>" %str(ex))
        finally:
            self._notify_parent()
        

class TelnetServer(TcpServer):

    #TELNET_PROMPT = 'ION telnet>'
    TELNET_PROMPT = None
    # 'will echo' command sequence to be sent from this telnet server
    # see RFCs 854 & 857
    WILL_ECHO_CMD = '\xff\xfd\x03\xff\xfb\x03\xff\xfb\x01'
    # 'do echo' command sequence to be sent back from telnet client
    DO_ECHO_CMD   = '\xff\xfb\x03\xff\xfd\x03\xff\xfd\x01'
    

    def _setup_session(self):
        # negotiate with the telnet client to have server echo characters
        response = input = ''
        start_time = time.time()
        self._write(self.WILL_ECHO_CMD)
        while True:
            input = self._get_data()
            if len(input) > 0:
                response += input
            if self.DO_ECHO_CMD in response:
                return True
            elif time.time() - start_time > 5:
                self._writeline("session negotiation with telnet client failed, closing connection")
                self._exit_handler(SessionCloseReasons.telnet_setup_timeout)
            

    def _handler(self):
        "The actual telnet server to which the user has connected."
        log.debug("TelnetServer._handler(): starting")
        
        username = None
        token = None

        self._write("Username: ")
        username = self._readline()
        
        self._write("token: ")
        token = self._readline()

        if not self._authorized(token):
            log.debug("login failed")
            self._writeline("login failed")
            self._exit_handler(SessionCloseReasons.login_failed)
            return

        if not self._setup_session():
            return       

        self._writeline("connected")   # let telnet client user know they are connected
        self.server_ready_to_send = True
        
        while True:
            if self.TELNET_PROMPT:
                self._write(self.TELNET_PROMPT)
            input_data = self._get_data()
            log.debug("rcvd: " + input_data)
            log.debug("len=" + str(len(input_data)))
            for i in range(len(input_data)):
                log.debug("%d - %x", i, ord(input_data[i])) 
            self.parent_input_callback(input_data)
            

class SerialServer(TcpServer):


    def _handler(self):
        "The actual serial server to which the user has connected."
        log.debug("SerialServer._handler(): starting")
        
        self.server_ready_to_send = True
        while True:
            input_data = self._get_data()
            log.debug("SerialServer._handler: rcvd: [" + input_data + "]\nlen=" + str(len(input_data)))
            for i in range(len(input_data)):
                log.debug("SerialServer._handler: char @ %d = %x", i, ord(input_data[i])) 
            self.parent_input_callback(input_data)
            

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
    
    def stop(self):
        self._stop(SessionCloseReasons.parent_closed)


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
    
    def _stop(self, reason):
        if self.already_stopping == True:
            log.debug("DirectAccessServer.stop(): already stopping")
            return
        self.already_stopping = True
        log.debug("DirectAccessServer.stop(): stopping DA server - reason = %s (%d)", 
                  SessionCloseReasons.string(reason), reason)
        if self.server:
            log.debug("DirectAccessServer.stop(): stopping TCP server")
            # pass in reason so server can tell parent via callback
            self.server.stop(reason)
            del self.server
        if self.timer and reason == SessionCloseReasons.parent_closed:
            # timer didn't initiate the stop, so kill it
            log.debug("DirectAccessServer.stop(): stopping timer")
            try:
                self.timer.kill()
            except Exception as ex:
                # can happen if timer already closed session (race condition)
                log.debug("DirectAccessServer.stop(): exception caught for timer kill: " + str(ex))
        

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
                    self._stop(SessionCloseReasons.session_timeout)
                    break

                if self.server.any_activity():
                    inactivity_start_time = time.time() 
                elif ((timenow - inactivity_start_time) > inactivity_timeout):
                    log.debug("DirectAccessServer._timer_greenlet(): session exceeded inactivity timeout of %d seconds"
                              %inactivity_timeout)
                    self._stop(reason=SessionCloseReasons.inactivity_timeout)
                    break
        except:
            pass
        log.debug("DirectAccessServer._timer_greenlet(): stopped ")
                     