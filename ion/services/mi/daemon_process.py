#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import os
import sys
import time
import signal
import atexit

class DaemonProcess(object):
    """
    A unix daemon process that runs in the background and has no
    environment or standard io. Objects of this class will persist even
    if the processes that create them disappear. They can be started, stopped
    and restarted, and log their outputs to a logfile.
    """
    
    def __init__(self, pidfname='daemon.pid.txt', logfname='daemon.log.txt',
                 workdir='/'):
        """
        DaemonProcess constructor.
        @param pidfname the filename of the dameon process id file.
        @param logfname the filename of the daemon process log file.
        @param workdir the working directory where daemon files are written.
        """
        self.pidfname = workdir + pidfname
        self.logfname = workdir + logfname
        self.logfile = None

        # Register a handler for the SIGCHLD signal. This will
        # call os.wait() to retrieve the child process return value and
        # prevent it from becomming a zombie process.
        def wait_on_child(signum=None, frame=None):
            retval = os.wait()
        signal.signal(signal.SIGCHLD, wait_on_child)

    def start(self):
        """
        Start the daemon process. Checks if the pidfile is not present,
        then calls _daemonize and routes the return values differently
        for the parent and child processes.
        """
        try:
            pf = file(self.pidfname, 'r')
            pid = int(pf.read().strip())
            pf.close()
            
        except IOError:
            pid = None
            
        if pid:
            msg = 'pidfile %s exists. Daemon already running.\n'
            sys.stderr.write(msg % self.pidfname)
            return -1
        
        # Start the daemon.
        # Route parent and child forks differently.
        pid = self._daemonize()

        if pid > 0:
            # This is the child fork. Run the process.
            # The run logic may exit without returning, or
            # will exit here upon completion.
            self._run()
            sys.exit(0)

        elif pid == 0:
            # This is the parent fork. Return success.
            return 0

        else:
            # An error in the inital fork occurred, return error.
            return -1

    def stop(self):
        """
        Stop the daemon process. Check that the pidfile is present, then
        send SIGTERM to the daemon process to trigger closedown.
        """
        try:
            pf = file(self.pidfname, 'r')
            pid = int(pf.read().strip())
            pf.close()

        except IOError:
            pid = None

        if not pid:
            msg = 'pidfile %s does not exist. Daemon not running.\n'
            sys.stderr.write(msg % self.pidfname)
            return

        while True:
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(.1)
                
            except Exception as e:
                if str(e).find('No such process') > 0:
                    break
                else:
                    raise e

    def restart(self):
        """
        Restart the daemon process.
        """
        self.stop()
        self.start()
    
    def _daemonize(self):
        """
        Daemonize the process.
        """
        try:
            # Fork a child process.
            # Child returns pid==0, parent pid>0.
            # Retrun and conclude parent logic.
            pid = os.fork()
            if pid > 0:
                return 0
            
        except OSError as e:
            # If fork fails, return -1 to start function.
            sys.stderr.write('Failed to fork child #1.')
            return -1
    
        # Decouple the child process from parent environment.
        os.chdir('/')
        os.setsid()
        os.umask(0)

        try:
            # Fork a second child process.
            # Exit parent process, with error if fork fails.
            pid = os.fork()
            if pid > 0:
                sys.exit(0)

        except OSError as e:
            sys.stderr.write('Failed to fork child #2.')
            sys.exit(1)

        # Setup the cleanup handler.
        signal.signal(signal.SIGTERM, self._cleanup_and_exit)

        # Write pid file.
        pid = os.getpid()
        try:
            file(self.pidfname, 'w+').write('%i\n' % pid)
            self.logfile = file(self.logfname,'w+')

        except IOError:
            sys.stderr.write('Could not create pid or logfile.')
            exit(1)

        # Redirect std file descriptors.
        sys.stdout.flush()
        sys.stderr.flush()        
        si = file(os.devnull, 'r')
        so = file(os.devnull, 'a+')
        se = file(os.devnull, 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        
        return pid
    
    def _cleanup(self):
        """
        Cleanup function prior to daemon exit. Remove pidfile, close
        logfile.
        """
        if os.path.exists(self.pidfname):
            os.remove(self.pidfname)
        if self.logfile:
            self.logfile.close()
            self.logfile = None

    def _cleanup_and_exit(self, signum=None, frame=None)   :
        """
        SIGTERM handler that performs cleanup and exits daemon process.
        """
        self._cleanup()
        sys.exit(0)
        
    def _run(self):
        """
        The daemon process run loop. Override this function in subclasses.
        """
        count = 0
        while True:
            time.sleep(1)
            if self.logfile:
                self.logfile.write('hello from daemon %i\n' % count)
                self.logfile.flush()
            count += 1

