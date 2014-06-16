#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'


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
    """
    Exit from the server
    used by handlers to indicate to server greenlet that it should exit
    """
    pass

class DirectAccessTypes:
    # set up range so values line up with attributes of class to get enum names back
    # using 'dir(DirectAccessTypes)[enum_value]
    # NOTE: list names must be in alphabetical order for this to work
    # telnet - supports telnet
    # vsp    - supports virtual serial port via com0com on the client end
    # ssh    - not implemented yeet
    (ssh, telnet, vsp) = range(2, 5)
    
class SessionCloseReasons:
    enum_range = range(0, 9)
    
    (client_closed, 
     inactivity_timeout, 
     parent_closed, 
     session_timeout, 
     login_failed,
     telnet_setup_timeout,
     socket_error,
     instrument_agent_exception,
     unspecified_reason) = enum_range
     
    str_rep = ['client closed', 
               'inactivity timeout', 
               'parent closed', 
               'session timeout', 
               'login failed',
               'telnet setup timed out',
               'TCP socket error',
               'instrument agent exception',
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
        
        # search for an available port
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

        # create token for login verification of telnet 
        self.token = str(uuid.uuid4()).upper()
        
        log.debug("TcpServer.__init__(): starting server greenlet")
        self.server = gevent.spawn(self._server_greenlet)
        

    # public methods
    
    def get_connection_info(self):
        return self.port, self.token


    def any_activity(self):
        # used by the timer greenlet to support the inactivity timeout detection
        if self.activity_seen:
            # re-arm the activity detector
            self.activity_seen = False
            return True
        return False
    
    
    def stop(self, reason):
        # called by DA server parent to shut down the server
        if self.stop_server == True:
            log.debug("TcpServer.stop(): already stopping")
            return
        self.stop_server = True
        self.server_ready_to_send = False
        # set close reason in case it's not 'parent closed' so server can inform parent via callback
        self.close_reason = reason
        log.debug("TcpServer.stop(): stopping TCP server - reason = %s", SessionCloseReasons.string(self.close_reason))     
            

    def send(self, data):
        # send data from parent to tcp client
        log.debug("TcpServer.send(): data = " + str(data))
        if self.server_ready_to_send:
            self._write(data)
        

    # private methods
    
    def _write(self, data):
        # write data to tcp client
        log.debug("TcpServer._write(): data = " + str(data))
        if self.connection_socket:
            self.activity_seen = True;
            MSGLEN = len(data)
            total_sent = 0
            while total_sent < MSGLEN:
                sent = self.connection_socket.send(data[total_sent:])
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                total_sent = total_sent + sent
        else:
            log.warning("TcpServer._write(): no connection yet, can not write data")            

    
    def _writeline(self, data):
        """Send data with line ending."""
        self._write(data+chr(13)+chr(10))


    def _authorized(self, token):
        # check for correct token to authorize this connection
        if token == self.token:
            return True
        else:
            log.debug("TcpServer._authorized: entered token =" + token + ", not equal to expected token =" + self.token)
            return False
    

    def _indicate_server_stopping(self, reason):
        # set flags and reason for shut down
        self.stop_server = True    # indicate to greenlet server that shut down is required
        self.close_reason = reason
        self.server_ready_to_send = False


    def _exit_handler (self, reason):
        # used by handlers to indicate to the server greenlet that it should shut down
        log.debug("TcpServer._exit_handler(): stopping, reason = %s" %SessionCloseReasons.string(reason))
        if self.stop_server == False:
            # not already stopping for another reason so indicate the handler's reason for shut down
            self._indicate_server_stopping(reason)
        # raise the server exit exception to the server greenlet
        raise ServerExitException("TcpServer: exiting from server, reason = %s" %SessionCloseReasons.string(reason))
    

    def _handler(self):
        "The actual handler to which the client is connected.  Must be implemented in any server that inherits from this class"
        log.debug("TcpServer._handler(): not implemented")
        raise NotImplementedException('TcpServer._handler() not implemented.')        
            

    def _get_data(self, timeout=None):
        # must be used by all servers that inherit this class to allow the server greenlet to detect that
        # server shut down is requested
        log.debug("TcpServer._get_data(): timeout = %s" %str(timeout))
        start_time = time.time()
        input_data = ''
        
        self.connection_socket.setblocking(0)   # just to be sure!
        
        while True:
            try:
                # this call must be non-blocking to let server check the stop_server flag
                input_data = self.connection_socket.recv(1024)
                if len(input_data) == 0:
                    # one way that the socket can indicate that the client has closed the connection
                    self._exit_handler(SessionCloseReasons.client_closed)
                self.activity_seen = True;
                return input_data
            except gevent.socket.error, error:
                if error.errno == errno.EAGAIN or error.errno == errno.EWOULDBLOCK:
                    # exceptions that indicate that nothing is available to read
                    if self.stop_server:
                        self._exit_handler(self.close_reason)
                    if timeout:
                        # if a timeout was specified then check for if it has elapsed
                        if ((time.time() - start_time) > timeout):
                            return ''
                    gevent.sleep(.1)
                else:
                    # some socket error condition other than 'nothing to read' so shut down server
                    log.debug("TcpServer._get_data(): exception caught <%s>" %str(error))
                    self._exit_handler(SessionCloseReasons.client_closed)

                
    def _readline(self, timeout=None):
        # must be used by all servers that inherit this class to allow the server greenlet to detect that
        # server shut down is requested
        # used to support the 'login' dialog for the telnet client
        start_time = time.time()
        input_data = ''
        
        while True:
            input_data += self._get_data(1)
            if '\r\n' in input_data:
                return input_data.split('\r\n')[0]   # don't pass back the line terminators
            if timeout:
                # if a timeout was specified then check for if it has elapsed
                if ((time.time() - start_time) > timeout):
                    log.info("TcpServer._readline(): timeout, rcvd <%s>" %input_data)
                    self._exit_handler(SessionCloseReasons.telnet_setup_timeout)

    
    def _forward_data_to_parent(self, data):
        if self.stop_server:
            self._exit_handler(self.close_reason)
        try:
            self.parent_input_callback(data)
        except Exception as ex:
            log.debug("TcpServer._forward_data_to_parent(): exception caught while trying to forward data to IA, exception = <%s>" 
                      %str(ex))
            self._exit_handler(SessionCloseReasons.instrument_agent_exception)

    
    def _notify_parent(self):
        # used by server greenlet to inform DA server parent that shut down is occurring if the parent
        # didn't request it
        # this is the only place that the parent callback should be called outside of the handlers to
        # avoid race conditions and duplicate callbacks
        if self.close_reason != SessionCloseReasons.parent_closed:
            # indicate to parent that the server is shutting down since it didn't initiate it
            log.debug("TcpServer._server_greenlet(): telling parent to close session, reason = %s"
                      %SessionCloseReasons.string(self.close_reason))
            try:
                self.parent_input_callback(self.close_reason)
            except Exception as ex:
                log.debug("TcpServer._notify_parent(): exception caught while trying to tell parent to close session, close reason = %s, exception = <%s>" 
                          %(SessionCloseReasons.string(self.close_reason), str(ex)))
        log.debug("TcpServer._server_greenlet(): stopped")
    

    def _server_greenlet(self):
        # the main DA server thread that listens for a tcp client connection and passes it to the 
        # handler for processing.  It expects the handler to raise an exception when it needs to quit,
        # but it will work correctly if it simply returns when done
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
                log.debug("TcpServer._server_greenlet(): exception caught while listening for connection <%s>" %str(ex))
                if self.server_socket.timeout:
                    # check to see if server shut down is requested
                    if self.stop_server:
                        self._notify_parent()
                        return
                else:
                    # something is wrong with the tcp socket so shut down
                    log.info("TcpServer._server_greenlet(): exception caught while listening for connection <%s>" %str(ex))
                    self._indicate_server_stopping(SessionCloseReasons.socket_error)
                    self._notify_parent()
                    return
        # got a client connection so call handler to process it
        try:
            self._handler()
        except Exception as ex:
            log.info("TcpServer._server_greenlet(): exception caught from handler <%s>" %str(ex))
        finally:
            # handler has exited so indicate to parent the server is shutting down
            self._notify_parent()
        

class TelnetServer(TcpServer):
    # this server supports telnet clients to the DA server

    #TELNET_PROMPT = 'ION telnet>'
    TELNET_PROMPT = None
    # 'will echo' command sequence to be sent from this telnet server
    # see RFCs 854 & 857
    WILL_ECHO_CMD = '\xff\xfd\x03\xff\xfb\x03\xff\xfb\x01'
    # 'do echo' command sequence expected to be sent back from telnet client
    DO_ECHO_CMD   = '\xff\xfb\x03\xff\xfd\x03\xff\xfd\x01'
    

    def _setup_session(self):
        # negotiate with the telnet client to have server echo characters
        response = input_data = ''
        start_time = time.time()
        self._write(self.WILL_ECHO_CMD)
        while True:
            input_data = self._get_data(1)
            if len(input_data) > 0:
                response += input_data
            if self.DO_ECHO_CMD in response:
                return True
            elif time.time() - start_time > 5:
                self._writeline("session negotiation with telnet client failed, closing connection")
                self._exit_handler(SessionCloseReasons.telnet_setup_timeout)
            

    def _handler(self):
        "The actual telnet server to which the telnet client is connected."
        log.debug("TelnetServer._handler(): starting")
        
        username = None
        token = None

        # prompt user for username; the response is ignored and not used
        self._write("Username: ")
        self._readline()
        
        # prompt user for token
        self._write("token: ")
        token = self._readline()

        # check token for correctness
        if not self._authorized(token):
            log.debug("login failed")
            self._writeline("login failed")
            self._exit_handler(SessionCloseReasons.login_failed)
            return

        # setup the telnet session with the telnet client
        if not self._setup_session():
            return       

        self._writeline("connected")   # let telnet client user know they are connected
        self.server_ready_to_send = True
        
        while True:
            if self.TELNET_PROMPT:
                self._write(self.TELNET_PROMPT)
            # must use _get_data() method from the TcpServer class to ensure that requested shut downs are detected
            input_data = self._get_data()
            log.debug("rcvd: " + input_data)
            log.debug("len=" + str(len(input_data)))
            for i in range(len(input_data)):
                log.debug("%d - %x", i, ord(input_data[i])) 
            # ship the data read from tcp client up to parent to forward on to instrument
            self._forward_data_to_parent(input_data)
            

class SerialServer(TcpServer):
    # this server supports virtual serial port (com0com) clients to the DA server


    def _handler(self):
        "The actual virtual serial port server to which the com0com client is connected."
        log.debug("SerialServer._handler(): starting")
        
        self.server_ready_to_send = True
        while True:
            # must use _get_data() method from the TcpServer class to ensure that requested shut downs are detected
            input_data = self._get_data()
            log.debug("SerialServer._handler: rcvd: [" + input_data + "]\nlen=" + str(len(input_data)))
            for i in range(len(input_data)):
                log.debug("SerialServer._handler: char @ %d = %x", i, ord(input_data[i])) 
            # ship the data read from virtual serial port client up to parent to forward on to instrument
            self._forward_data_to_parent(input_data)
            

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
        # called by the parent to indicate that server should shut down
        self._stop(SessionCloseReasons.parent_closed)


    def get_connection_info(self):
        # return tcp connection port and token
        if self.server:
            return self.server.get_connection_info()
        else:
            return 0, "no-token"
    

    def send(self, data):
        # send data from parent (coming from the instrumment) to tcp client
        log.debug("DirectAccessServer.send(): data = " + str(data))
        if self.server:
            self.server.send(data)
            
            
    # private methods
    
    def _stop(self, reason):
        # can be called by timer greenlet or by parent interface stop() method
        if self.already_stopping == True:
            log.debug("DirectAccessServer.stop(): already stopping")
            return
        self.already_stopping = True
        log.debug("DirectAccessServer.stop(): stopping DA server - reason = %s (%d)", 
                  SessionCloseReasons.string(reason), reason)
        if self.server:
            log.debug("DirectAccessServer.stop(): stopping TCP server")
            # pass in reason so server can tell parent via callback if necessary
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
        # implements session and inactivity timeouts
        # do NOT add parent callback to this greenlet
        # ALL callbacks to the parent should be handled by the server greenlet
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
                    # indicate to the server that it should shut down; the server will inform the parent of the shutdown
                    self._stop(SessionCloseReasons.session_timeout)
                    break

                if self.server.any_activity():
                    # activity detected so reset the inactivity start time
                    inactivity_start_time = time.time() 
                elif ((timenow - inactivity_start_time) > inactivity_timeout):
                    log.debug("DirectAccessServer._timer_greenlet(): session exceeded inactivity timeout of %d seconds"
                              %inactivity_timeout)
                    # indicate to the server that it should shut down; the server will inform the parent of the shutdown
                    self._stop(reason=SessionCloseReasons.inactivity_timeout)
                    break
        except:
            # to detect a kill() from the DA server
            pass
        log.debug("DirectAccessServer._timer_greenlet(): stopped ")
                     