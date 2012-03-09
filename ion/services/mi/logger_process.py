#!/usr/bin/env python

"""
@package ion.services.mi.logger_process
@file ion/services/mi/logger_process.py
@author Edward Hunter
@brief Daemon processes providing hardware specific device connections
and logging.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import socket
import threading
import time
import datetime
import atexit
import errno
from subprocess import Popen
from subprocess import PIPE
import logging
import os

from ion.services.mi.daemon_process import DaemonProcess

mi_logger = logging.getLogger('mi_logger')

"""
import ion.services.mi.mi_logger
import ion.services.mi.logger_process as lp
l = lp.EthernetDeviceLogger('137.110.112.119', 4001, 8888)
c = lp.LoggerClient('localhost', 8888, '\r\n')
"""

class BaseLoggerProcess(DaemonProcess):
    """
    Base class for device loggers. Device loggers are communication
    management processes that are launched by, but have independent lifecycles
    from drivers, allowing them to persist even when drivers are shut down
    or exit abnormally. Inherets fromo DaemonProcess and provides
    a run loop that forwards traffic between driver and device hardware,
    and read/write logic for driver and sniffer client objects.
    Derived subclasses provide read/write logic for TCP/IP, serial or other
    device hardware.
    """
    
    @staticmethod
    def launch_process(cmd_str):
        """
        """
        # Launch a separate python interpreter, executing the calling
        # class command string.
        spawnargs = ['bin/python', '-c', cmd_str]
        #print str(spawnargs)
        return Popen(spawnargs)    
    
    def __init__(self, server_port, pidfname, logfname, statusfname,
                 workdir='/tmp/', delim=['<<','>>'], sniffer_port=None):
        """
        @param server_port The port to listen on for driver connections.
        @param pidfname The file name of the process ID file, used by
                DaemonProcess.
        @param logfname The file name of the logger logfile where comms
                traffic is logged, used by DaemonProcess.
        @param statsfname The name of the logger status file, where logger
                status info and errors is kept.
        @param workdir The path of the working directory where logger files
                are written, used by DaemonProcess.
        @param delim A 2-element delimter list used to delimit traffic from
                the driver in the logfile, thus demarking it from the device
                output.
        @param sniffer_port The port to listen on for sniffer connections.
        """
        DaemonProcess.__init__(self, pidfname, logfname, workdir)
        self.server_port = server_port
        self.sniffer_port = sniffer_port
        self.driver_server_sock = None
        self.driver_sock = None
        self.driver_addr = None
        self.sniffer_server_sock = None
        self.sniffer_sock = None
        self.sniffer_addr = None
        self.delim = delim
        self.statusfname = workdir + statusfname

    def _init_driver_comms(self):
        """
        Initialize driver comms. Create, bind and listen on the driver
        conneciton server port. Make server socket nonblocking so
        accepts in the run loop return immediately.
        Log success and errors to status file. Handles address in use and
        unspecified socket errors.
        @retval True on success, False otherwise.
        """
        if not self.driver_server_sock:
            try:
                self.driver_server_sock = socket.socket(socket.AF_INET,
                                                socket.SOCK_STREAM)
                self.driver_server_sock.setsockopt(socket.SOL_SOCKET,
                                                   socket.SO_REUSEADDR, 1)
                self.driver_server_sock.bind(('',self.server_port))
                self.driver_server_sock.listen(1)
                self.driver_server_sock.setblocking(0)
                self.statusfile.write('_init_driver_comms: Listening for driver on port %i.\n' % self.server_port)
                self.statusfile.flush()
                return True
            
            except socket.error as e:
                # [Errno 48] Address already in use.
                # Report and fail.
                if e.errno == errno.EADDRINUSE:
                    self.statusfile.write('_init_driver_comms: raised errno %i, %s.\n' % (e.errno, str(e)))
                    self.statusfile.flush()
                    return False
                
                else:
                    # TBD. Report and fail.
                    self.statusfile.write('_init_driver_comms: raised errno %i, %s.\n' % (e.errno, str(e)))
                    self.statusfile.flush()
                    return False
            
    def _accept_driver_comms(self):
        """
        Accept a driver connection request from nonblocking driver server
        socket. If nothing available, proceed. If a connection is accepted,
        log with status file. Handles resource unavailable and unspecified
        socket errors.
        """
        sock = None
        addr = None
        try:
            sock, addr = self.driver_server_sock.accept()

        except socket.error as e:
            # [Errno 35] Resource temporarily unavailable.
            if e.errno == errno.EAGAIN:
                # Wating for a driver connection, proceed out of function.
                pass
            
            else:
                # TBD. Report and proceed.
                self.statusfile.write('_accept_driver_comms: raised errno %i, %s.\n' % (e.errno, str(e)))
                self.statusfile.flush()
            
        if sock:        
            self.driver_sock = sock
            self.driver_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)                            
            self.driver_addr = addr
            self.statusfile.write('_accept_driver_comms: driver connected at %s, %i.\n' % self.driver_addr)
            self.statusfile.flush()
        
    def _close_driver_comms(self):
        """
        Close driver communications. Close driver and driver server sockets
        if they exist. Log with status file.
        """
        if self.driver_sock:
            #-self.driver_sock.shutdown(socket.SHUT_RDWR)
            self.driver_sock.close()
            self.driver_sock = None
            self.driver_addr = None
            self.statusfile.write('_close_driver_comms: closed driver connection.\n')
            self.statusfile.flush()

        if self.driver_server_sock:
            self.driver_server_sock.close()
            self.driver_server_sock = None
            self.statusfile.write('_close_driver_comms: closed driver server.\n')
            self.statusfile.flush()


    def _init_device_comms(self):
        """
        Initialize device communications. Overridden by hardware specific
        subclasses.
        """
        pass

    def _close_device_comms(self):
        """
        Close communiations with the device. Overridden in hardware specific
        subclasses.
        """
        pass

    def _device_connected(self):
        """
        Device connected status function. Overridden in hardware specific
        subclasses.
        """
        return False

    def read_driver(self):
        """
        Read data from driver, if available. Log errors to status file.
        Handles resource unavailable, connection reset by peer, broken pipe
        and unspecified socket errors.
        @retval The string of data read from the driver or None.
        """
        data = None
        if self.driver_sock:
            try:
                data = self.driver_sock.recv(4096)

            except socket.error as e:                
                # [Errno 35] Resource temporarily unavailable.
                if e.errno == errno.EAGAIN:
                    # Nothing to read, proceed out of the function.
                    pass
                
                # [Errno 54] Connection reset by peer.
                elif e.errno == errno.ECONNRESET:
                    # The client side has disconnected, report and close socket.
                    self.statusfile.write('read_driver: raised errno %i, %s.\n'
                                          % (e.errno, str(e)))
                    self.statusfile.flush()
                    self.driver_sock.close()
                    self.driver_sock = None
                    self.driver_addr = None
                
                # [Errno 32] Broken pipe.
                elif e.errno == errno.EPIPE:
                    # Broken pipe, report and close socket.
                    self.statusfile.write('read_driver: raised errno %i, %s.\n'
                                          % (e.errno, str(e)))
                    self.statusfile.flush()
                    self.driver_sock.close()
                    self.driver_sock = None
                    self.driver_addr = None
                
                # Unspecified socket error, report and close socket.
                else:
                    # TBD. Report and close socket.
                    self.statusfile.write('read_driver: raised errno %i, %s.\n'
                                          % (e.errno, str(e)))
                    self.statusfile.flush()
                    self.driver_sock.close()
                    self.driver_sock = None
                    self.driver_addr = None

        return data
    
    def write_driver(self, data):
        """
        Write data to driver, retrying until all has been sent. Log errors
        to status file. Handles resource unavailable, connection reset by peer,
        broken pipe and unspecified socket errors.
        @param data The data string to write to the driver.
        """
        if self.driver_sock:
            sent = 0
            while len(data)>0:
                try:
                    sent = self.driver_sock.send(data)
                    data = data[sent:]

                except socket.error as e:                
                    # [Errno 35] Resource temporarily unavailable.
                    if e.errno == errno.EAGAIN:
                        # Occurs when the network write buffer is full.
                        # Sleep a short period of time and retry.
                        time.sleep(.1)
                    
                    # [Errno 54] Connection reset by peer.
                    elif e.errno == errno.ECONNRESET:
                        # The client side has disconnected, report and close socket.
                        self.statusfile.write('read_driver: raised errno %i, %s.\n'
                                              % (e.errno, str(e)))
                        self.statusfile.flush()
                        self.driver_sock.close()
                        self.driver_sock = None
                        self.driver_addr = None
                        break
                    
                    # [Errno 32] Broken pipe.
                    elif e.errno == errno.EPIPE:
                        # Broken pipe, report and close socket.
                        self.statusfile.write('read_driver: raised errno %i, %s.\n'
                                              % (e.errno, str(e)))
                        self.statusfile.flush()
                        self.driver_sock.close()
                        self.driver_sock = None
                        self.driver_addr = None
                        break
                    
                    # Unspecified socket error, report and close socket.
                    else:
                        # TBD. Report and close socket.
                        self.statusfile.write('read_driver: raised errno %i, %s.\n' % (e.errno, str(e)))
                        self.statusfile.flush()
                        self.driver_sock.close()
                        self.driver_sock = None
                        self.driver_addr = None
                        break
                    
    def read_device(self):
        """
        Read from device, if available. Overridden by hardware
        specific subclass.
        @retval The data string read from the device, or None.
        """
        pass
    
    def write_device(self, data):
        """
        Write to device, retrying until all has been sent. Overridden
        by hardware specific subclass.
        @param data The data string to write to the device.
        """
        pass

    def _cleanup(self):
        """
        Cleanup function prior to logger exit. Close comms, status file and
        call DaemonProcess cleanup. This is called by the DaemonProcess
        SIGTERM handler if termination occurs due to signal, or by
        atexit handler if the run loop concludes normally.
        """
        self._close_device_comms()
        self._close_driver_comms()
        if self.statusfile:
            self.statusfile.write('_cleanup: logger stopping.\n')
            self.statusfile.flush()
            self.statusfile.close()
            self.statusfile = None
        DaemonProcess._cleanup(self)
     
    def _run(self):
        """
        Logger run loop. Create and initialize status file, initialize
        device and driver comms and loop while device connected. Loop
        accepts driver connections, reads driver, writes to device and
        sniffer, reads device, writes to driver and sniffer and repeats.
        Logger is stopped by calling DaemonProcess.stop() resulting in
        SIGTERM signal sent to the logger, or if the device hardware connection
        is lost, whereby the run loop and logger process will terminate.
        """

        atexit.register(self._cleanup)

        self.statusfile = file(self.statusfname, 'w+')
        self.statusfile.write('_run: logger starting.\n')
        self.statusfile.flush()
        
        if not self._init_device_comms():
            self.statusfile.write('_run: could not connect to device.\n')
            self.statusfile.flush()
            return
        
        if not self._init_driver_comms():
            self.statusfile.write('_run: could not listen for drivers.\n')
            self.statusfile.flush()
            return
        
        while self._device_connected():
            self._accept_driver_comms()
            driver_data = self.read_driver()
            if driver_data:
                self.write_device(driver_data)
                self.logfile.write(self.delim[0]+repr(driver_data)+self.delim[1])
                self.logfile.write('\n')
                self.logfile.flush()
            device_data = self.read_device()
            if device_data:
                #ddlen = len(device_data)
                #if ddlen < 1024:
                #    device_data += '\x00'*(1024-ddlen)
                self.write_driver(device_data)
                self.logfile.write(repr(device_data))
                self.logfile.write('\n')
                self.logfile.flush()
            if not driver_data and not device_data:
                time.sleep(.1)

class EthernetDeviceLogger(BaseLoggerProcess):
    """
    A device logger process specialized to read/write to TCP/IP devices.
    Provides functionality opening, closing, reading, writing and checking
    connection status of device.
    """    
    def __init__(self, device_host, device_port, server_port, workdir='/tmp/',
                 delim=['<<','>>'], sniffer_port=None):
        """
        @param server_port The port to listen on for driver connections.
        @param pidfname The file name of the process ID file, used by
                DaemonProcess.
        @param logfname The file name of the logger logfile where comms
                traffic is logged, used by DaemonProcess.
        @param statsfname The name of the logger status file, where logger
                status info and errors is kept.
        @param workdir The path of the working directory where logger files
                are written, used by DaemonProcess.
        @param delim A 2-element delimter list used to delimit traffic from
                the driver in the logfile, thus demarking it from the device
                output.
        @param sniffer_port The port to listen on for sniffer connections.        
        """
        
        start_time = datetime.datetime.now()
        self.start_time = start_time
        dt_string = '%s_%i__%i_%i_%i_%i_%i_%i' % \
                (device_host, device_port, start_time.year, start_time.month,
                start_time.day, start_time.hour, start_time.minute,
                start_time.second)
        
        pidfname = '%s_%i.pid.txt' % (device_host, device_port)
        logfname = '%s.log.txt' % dt_string
        statusfname = '%s.status.txt' % dt_string
        self.device_host = device_host
        self.device_port = device_port
        self.device_sock = None
        
        BaseLoggerProcess.__init__(self, server_port, pidfname, logfname,
                            statusfname, workdir, delim=['<<','>>'],
                            sniffer_port=None)

    def launch_process(self):
        
        
        import_str = 'import ion.services.mi.logger_process as lp; '
        ctor_str = 'l = lp.EthernetDeviceLogger'
        if not self.sniffer_port:
            ctor_str += '("%s", %i, %i, "%s", ["%s","%s"]); ' \
                        % (self.device_host, self.device_port, self.server_port,
                           self.workdir, self.delim[0], self.delim[1])
        else:
            ctor_str += '("%s", %i, %i, "%s", ["%s","%s"], %i); ' \
                        % (device_host, device_port, server_port,
                           workdir, delim[0], delim[1], self.sniffer_port)


        cmd_str = import_str + ctor_str + 'l.start()'            
            
        return BaseLoggerProcess.launch_process(cmd_str)

    def _init_device_comms(self):
        """
        Initialize ethernet device comms. Attempt to connect to an IP
        device with timeout, setting socket to nonblocking and returning
        Log success or error with statusfile.
        @retval True on success, False otherwise.
        """

        self.device_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.device_sock.settimeout(10)
        
        try:
            self.device_sock.connect((self.device_host, self.device_port))
            self.device_sock.setblocking(0)
            self.device_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)                            
            
        except Exception as e:
            # This could be a timeout.
            self.statusfile.write('_init_device_comms: raised %s.\n' % str(e))
            self.statusfile.flush()
            self.device_sock = None
            return False
        
        else:
            self.statusfile.write('_init_device_comms: device connected.\n')
            self.statusfile.flush()
            return True
        
    def _close_device_comms(self):
        """
        Close ethernet device comms and log with status file.
        """
        if self.device_sock:
            #-self.device_sock.shutdown(socket.SHUT_RDWR)
            self.device_sock.close()
            self.device_sock = None
            time.wait(1)
            self.statusfile.write('_close_device_comms: device connection closed.\n')
            self.statusfile.flush()                            

    def _device_connected(self):
        """
        Determine if device still connected.
        @retval True on success, False otherwise.
        """
        return self.device_sock != None
                            
    def read_device(self):
        """
        Read from an ethernet device, if available. Log errors (except
        resource temporarily unavailable, if they occur.) Handles resource
        temporarily unavailable, connection reset by peer, broken pipe,
        and unspecified socket errors.
        @retval A data string read from the device, or None.
        """
        data = None
        if self.device_sock:
            try:
                data = self.device_sock.recv(4096)

            except socket.error as e:                
                # [Errno 35] Resource temporarily unavailable.
                if e.errno == errno.EAGAIN:
                    # No data to read from device.
                    # Proceed out of the read function.
                    pass
                
                # [Errno 54] Connection reset by peer.
                elif e.errno == errno.ECONNRESET:
                    # TBD. Report and close socket (end logger).
                    self.statusfile.write('read_device: raised errno %i, %s.\n' % (e.errno, str(e)))
                    self.statusfile.flush()
                    self.device_sock.close()
                    self.device_sock = None
                    
                # [Errno 32] Broken pipe.
                elif e.errno == errno.EPIPE:
                    # TBD. Report and close socket (end logger).
                    self.statusfile.write('read_device: raised errno %i, %s.\n' % (e.errno, str(e)))
                    self.statusfile.flush()
                    self.device_sock.close()
                    self.device_sock = None
                    
                # Unspecified socket error.
                else:
                    # TBD. Report and close socket (end logger).                    
                    self.statusfile.write('read_device: raised errno %i, %s.\n' % (e.errno, str(e)))
                    self.statusfile.flush()
                    self.device_sock.close()
                    self.device_sock = None
            
        return data
    
    def write_device(self, data):
        """
        Write to an ethernet device, retrying until all sent. Log errors (except
        resource temporarily unavailable, if they occur.) Handles resource
        temporarily unavailable, connection reset by peer, broken pipe,
        and unspecified socket errors.
        @param data The data string to write to the device.
        """
        if self.device_sock:
            sent = 0
            while len(data)>0:
                try:
                    sent = self.device_sock.send(data)
                    data = data[sent:]                    

                except socket.error as e:                
                    # [Errno 35] Resource temporarily unavailable.
                    if e.errno == errno.EAGAIN:
                        # Occurs when the network write buffer is full.
                        # Sleep a short period of time and retry.
                        time.sleep(.1)
                    
                    # [Errno 54] Connection reset by peer.
                    elif e.errno == errno.ECONNRESET:
                        # TBD. Report and close socket (end logger). 
                        self.statusfile.write('write_device: raised errno %i, %s.\n' % (e.errno, str(e)))
                        self.statusfile.flush()
                        self.device_sock.close()
                        self.device_sock = None
                        break
                    
                    # [Errno 32] Broken pipe.
                    elif e.errno == errno.EPIPE:
                        # TBD. Report and close socket (end logger). 
                        self.statusfile.write('write_device: raised errno %i, %s.\n' % (e.errno, str(e)))
                        self.statusfile.flush() 
                        self.device_sock.close()
                        self.device_sock = None
                        break
                    
                    # Unspecified socket error, report and close socket.
                    else:
                        # TBD. Report and close socket (end logger).
                        self.statusfile.write('write_device: raised errno %i, %s.\n' % (e.errno, str(e)))
                        self.statusfile.flush()
                        self.device_sock.close()
                        self.device_sock = None
                        break
                    
class SerialDeviceLogger(BaseLoggerProcess):
    """
    A device logger process specialized to read/write to serial devices.
    Provides functionality opening, closing, reading, writing and checking
    connection status of device.    
    """
    def __init__(self, server_port, device_host, device_port,
                 dir=None, delim=['<<','>>'], sniffer_port=None):
        """
        @param server_port The port to listen on for driver connections.
        @param pidfname The file name of the process ID file, used by
                DaemonProcess.
        @param logfname The file name of the logger logfile where comms
                traffic is logged, used by DaemonProcess.
        @param statsfname The name of the logger status file, where logger
                status info and errors is kept.
        @param workdir The path of the working directory where logger files
                are written, used by DaemonProcess.
        @param delim A 2-element delimter list used to delimit traffic from
                the driver in the logfile, thus demarking it from the device
                output.
        @param sniffer_port The port to listen on for sniffer connections.        
        """        
        pass

class LoggerClient(object):
    """
    A logger process client class to test and demonstrate the correct use
    of device logger processes. The client object starts and stops
    comms with the logger. Data is sent to the logger with the send function,
    and data is retrieved from the logger with a listener thread.
    """
    
    def __init__(self, host, port, delim=None):
        """
        Logger client constructor.
        """
        self.host = host
        self.port = port
        self.sock = None
        self.listener_thread = None
        self.stop_event = None
        self.delim = delim
        
    def init_comms(self, callback=None):
        """
        Initialize client comms with the logger process and start a
        listener thread.
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # This can be thrown here.
        # error: [Errno 61] Connection refused
        self.sock.connect((self.host, self.port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)                        
        self.sock.setblocking(0)        
        self.listener_thread = Listener(self.sock, self.delim, callback)
        self.listener_thread.start()
        mi_logger.info('Logger client comms initialized.')
        #print 'init client comms done'
        #logging.info('init client comms done')        
        
    def stop_comms(self):
        """
        Stop the listener thread and close client comms with the device
        logger. This is called by the done function.
        """
        mi_logger.info('Got Stop COMMS')
        self.listener_thread.done()
        self.listener_thread.join()
        #-self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        self.sock = None
        mi_logger.info('Loggerr client comms stopped.')
        #print 'stopped client comms'
        #logging.info('stopped client comms')

    def done(self):
        """
        Send a stop message to the logger process, causeing it to
        close comms and shutdown, then close client comms.
        """
        self.stop_comms()

    def send(self, data):
        """
        Send data to the device logger, retrying until all is sent.
        """
        
        if self.sock:
            while len(data)>0:
                try:
                    sent = self.sock.send(data)
                    gone = data[:sent]
                    data = data[sent:]
                    mi_logger.info('logger sent: %s',repr(gone))   
                except socket.error:
                    time.sleep(.1)
                
class Listener(threading.Thread):
    """
    A listener thread to monitor the client socket data incomming from
    the logger process. A similar construct will be used in drivers
    to catch and act upon the incomming data, so the pattern is presented here.
    """
    
    def __init__(self, sock, delim, callback=None):
        """
        Listener thread constructor.
        @param sock The socket to listen on.
        @param delim The line delimiter to split incomming lines on.
        @param callback The callback on data arrival.
        """
        threading.Thread.__init__(self)
        self.sock = sock
        self._done = False
        self.linebuf = ''
        self.delim = delim
        
        if callback:
            def fn_callback(data):
                callback(data)            
            self.callback = fn_callback
        else:
            self.callback = None


    def done(self):
        """
        Signal to the listener thread to end its processing loop and
        conclude.
        """
        self._done = True
        
    def run(self):
        """
        Listener thread processing loop. Read incomming data when
        available and report it to the logger.
        """
        mi_logger.info('Logger client listener started.')
        #logging.info('listener started')
        oldtime = 0
        while not self._done:
            try:
                #newtime = time.time()
                #if newtime > oldtime:
                    #mi_logger('logger client listening')
                    #oldtime = newtime
                data = self.sock.recv(4069)
                if self.callback:
                    #mi_logger.debug('logger got data: %s', repr(data))
                    self.callback(data)
                else:
                    if not self.delim:
                        #logging.info('from device:%s' % repr(data))
                        print 'from device:%s' % repr(data)
                    else:
                        self.linebuf += data
                        lines = str.split(self.linebuf, self.delim)
                        self.linebuf = lines[-1]
                        lines = lines[:-1]
                        #[logging.info('from device:%s' % item) for item in lines]
                        for item in lines:
                            print 'from device:%s' % item
                
            except socket.error:
                time.sleep(.1)
        #logging.info('listener done')
        mi_logger.info('Logger client done listening.')
