#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/utilities/SBE37_util.py 
@author Edward Hunter
@brief Command line instrument controls utility.
"""

"""
Use:

bin/python ion/services/mi/drivers/sbe37/SBE37_util.py [options] ipaddress port command
bin/python ion/services/mi/drivers/sbe37/SBE37_util.py -n 20 -f ion/services/mi/drivers/sbe37/SBE37_3464_config_short.txt 137.110.112.119 4001 reset 
bin/python ion/services/mi/drivers/sbe37/SBE37_util.py -n 20 -f ion/services/mi/drivers/sbe37/SBE37_3464_config.txt 137.110.112.119 4001 reset 

commands:
reset               reset the instrument to nonsampling mode and
                    reset parameters to config file
options:
-f --config_file    config file path
-n --no_trys        number of wakeup trys
"""

import socket
import time

from ion.services.mi.drivers.sbe37.sbe37_driver import SBE37Prompt, SBE37_NEWLINE

commands = ['reset']

def wakeup(sock,no_tries):
    """
    Attempt to wake the device in standby or autosample mode.
    @param sock A socket connected to the device.
    @param no_tries The number of wakeup attempts.
    @retval buffer Line buffer of interaction during the wakeup.
    """
    
    # Timeout to wait for data.
    sock.settimeout(2.0)
    buffer = ''
    count = 0
    
    while count < no_tries:
        # Send a device wakeup
        print 'sending wakeup'
        sock.send(SBE37_NEWLINE)
        time.sleep(1)

        # Receive data.
        # Break out if nothing available within a few seconds.
        while True:
            try:
                data = ''
                data = sock.recv(1024)
                buffer += data
            except socket.timeout:
                #print 'recv timed out'
                break        
            #else:
            #    print 'received '+ str(len(data)) + ' bytes'

        # If the data buffer includes the prompt exit
        # otherwise send another wakeup and continue.
        if buffer.endswith(SBE37Prompt.COMMAND) \
            or buffer.endswith(SBE37Prompt.AUTOSAMPLE):
            break
        else:
            count += 1

    # Reset the socket to infinite blocking mode.
    sock.settimeout(None)
    
    return buffer

def do_cmd(cmd,sock):
    """
    Issue a command to the alert device.
    @param cmd The command to write.
    @param sock A socket connected to the device.
    @retval buffer Line buffer of interaction during the command write.
    """

    buffer = ''
    
    # Write the command and wait one second.
    print 'writing command '+cmd    
    sock.send(cmd+SBE37_NEWLINE)
    time.sleep(1)
    
    # Block to receive all data.
    # Continue reading if the received data does not include a prompt.
    # Break out when the received data ends in a prompt.
    while True:
        try:
            data = ''
            data = sock.recv(1024)
            buffer += data
        except:
            raise
        else:
            #print 'received '+str(len(data))+' bytes'    
            if buffer.endswith(SBE37Prompt.COMMAND):
                break
            elif buffer.endswith(SBE37Prompt.AUTOSAMPLE):
                break
            elif buffer.endswith(SBE37Prompt.BAD_COMMAND):
                break

    return buffer

def reset(s, no_tries, config_file):
    """
    Reset the device to idle mode, preset configuration.
    @param s A socket connected to the device.
    @param no_tries Number of tries to wake the device.
    @param config_file Path to a configuration file containing set commands
        to reset the device.
    """
    
    data_lines = []    
    reset_autosample = False
 
    # Wakeup the device.   
    buffer = wakeup(s, no_tries)    
    data_lines += buffer.splitlines()
    
    if not buffer.endswith(SBE37Prompt.COMMAND) \
        and not buffer.endswith(SBE37Prompt.AUTOSAMPLE):
        raise Exception('Could not wake the device.')
        
    # Send a newline. This will return stop prompt if in autosample mode.
    buffer = do_cmd('',s)
    data_lines += buffer.splitlines()

    # If in autosample, send stop command.
    if buffer.endswith(SBE37Prompt.AUTOSAMPLE):
    
        # Send stop command. This returns stop prompt again.
        buffer = do_cmd('stop',s)
        data_lines += buffer.splitlines()

        # Send a newline. This returns normal prompt.
        buffer = do_cmd('',s)
        data_lines += buffer.splitlines()

        # Error if we didn't get the regular prompt.
        if not buffer.endswith(SBE37Prompt.COMMAND):
            raise Exception('Could not stop autosample mode.')

        # Set flag to true for report.
        reset_autosample = True
        
    # Open and read config file. Strip newlines.
    try:
        fh = open(config_file)
    
    except IOError:
        print 'Could not open config file ' + config_file
        flines = []
        
    else:
        flines = fh.readlines()
        flines = [item.rstrip() for item in flines]
        fh.close()
        
    finally:    
        # Add time and date set commands.
        ltime = time.localtime()
        date_str = 'MMDDYY=%02i%02i%s' % (ltime[1],ltime[2],str(ltime[0])[-2:])
        time_str = 'HHMMSS=%02i%02i%02i' % (ltime[3],ltime[4],ltime[5])
        flines.append(date_str)
        flines.append(time_str)

    # Set count and errors count.
    count = 0
    errors = 0
    
    # Issue set commands.
    # Count errors with bad prompts, raise if the prompt is not normal or
    # unknown command.
    for item in flines:
        buffer = do_cmd(item.rstrip(),s)
        data_lines += buffer.splitlines()
        if buffer.endswith(SBE37Prompt.COMMAND):
            pass
        elif buffer.endswith(SBE37Prompt.BAD_COMMAND):
            errors += 1
        else:
            raise Exception('Unknown prompt resulted from set command.')
        count += 1

    # Report results
    #if reset_autosample:
    #    print 'reset from autosample mode'
    #print 'reset %i parameters with %i errors' % (count, errors)
    return (reset_autosample, count, errors)
    

def opensock(ipaddr,port):
    """
    Open a socket for communications to the device.
    @param ipaddr The device ip address.
    @param port The device port.
    @retval A socket connected to the device.
    """
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.connect((ipaddr,port))
    
    return s
    
    
def closesock(s):
    """
    Close the device communication socket.
    @param s An open socket connected to the device.
    """
    s.close()
    s = None
    
if __name__ == '__main__':


    import sys
    import optparse
    usage = 'usage: %prog [options] host port command'
    parser = optparse.OptionParser(usage)
    
    parser.add_option('-f','--file',dest='file',
                      type='string',action='store',
                      default='',
                      help='configuration file used with reset')
    
    parser.add_option('-n','--no_trys',dest='no_trys',
                      type='int',action='store',
                      default=25,
                      help='number to times to try to wakeup instrument')

    (options, args) = parser.parse_args()
    
    if len(args) != 3:
        parser.error('Incorrect number of arguments.')

    ipaddr = args[0]
    if not args[1].isdigit():
        parser.error('port must be an integer')
    else:
        port = int(args[1])
    cmd = args[2]

    if cmd not in commands:
        parser.error('Command must be one of %s' % str(commands))

    if cmd == 'reset':
        fpath = options.file
        no_trys = options.no_trys
        print 'resetting instrument with these parameters:'
        print 'ip address: ' + ipaddr
        print 'port: ' + str(port)
        print 'config file: ' + fpath
        print 'number of wakeup trys: ' + str(no_trys)
        sock = opensock(ipaddr, port)
        (reset_autosample, count, errors) = reset(sock,no_trys, fpath)
        if reset_autosample:
            print 'stopped autosample mode successfully'
        print 'reset %i parameters with %i errors' % (count, errors)
        closesock(sock)

    

