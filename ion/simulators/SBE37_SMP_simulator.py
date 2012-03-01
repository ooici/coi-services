#!/usr/bin/env python
__author__ = 'roger unwin'
__license__ = 'Apache 2.0'

import socket
import time
import datetime
import string
import sys
import random
import asyncore
import thread
import getopt

port = 4001  # Default port to run on.
connection_count = 0

class sbe37(asyncore.dispatcher_with_send):
    buf = ""
    count = 8
    time_set_at = time.time() 
    out_buffer = ""
    allowable_baud_rates = ['600', '1200', '2400', '4800', '9600', '19200', '38400']
    baud_rate = '9600'
    date = "010201" # MMDDYY
    time = "010100" # HHMMSS
    output_salinity = False
    output_sound_velocity = False
    format = 1
    reference_preassure = 0.0
    pump_installed = True
    sample_number = 0
    sleep_state = True
    interval = random.randrange(5, 32767)
    navg = 0
    store_time = False
    tx_real_time = False
    start_mmddyy = "010201"
    start_time = "010101"
    sync_wait = 0
    serial_sync_mode = False

    logging = False
    locked = False
    start_later = False
  
    tcaldate = "08-nov-05"
    ta0 = -2.572242e-04
    ta1 = 3.138936e-04
    ta2 = -9.717158e-06
    ta3 = 2.138735e-07
    caldate = "08-nov-05"
    cg = -9.870930e-01
    ch = 1.417895e-01
    ci = 1.334915e-04
    cj = 3.339261e-05
    wbotc = 1.202400e-05
    ctcor = 3.250000e-06
    cpcor = 9.570000e-08
    pcaldate = "12-aug-05"
    pa0 = 5.916199e+00
    pa1 = 4.851819e-01
    pa2 = 4.596432e-07
    ptca0 = 2.762492e+02
    ptca1 = 6.603433e-01
    ptca2 = 5.756490e-03
    ptcb0 = 2.461450e+01
    ptcb1 = -9.000000e-04
    ptcb2 = 0.000000e+00
    poffset = 0.000000e+00
    rcaldate = "08-nov-05"
    rtca0 = 9.999862e-01
    rtca1 = 1.686132e-06
    rtca2 = -3.022745e-08

    months = ['BAD PROGRAMMER MONTH', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    def __init__(self, socket, thread):
        self.socket = socket
        self.socket.settimeout(0.0)
        self.thread = thread
        try:
            self.socket.send("S>")
        except:
            print "exception could not send 'S>' for some reason...\n"
        self.handle_read()

    def handle_error(self, request, client_address):
        print str(request) + str(client_address) + " Do I ever get called by anything?"

    def get_current_time_startlater(self):
        current_time = datetime.datetime.strptime(self.date + " " + self.time, "%m%d%y %H%M%S") + datetime.timedelta( seconds=( int(time.time()) - self.time_set_at) )
        format = "%d %b %Y, %H:%M:%S"
        return current_time.strftime(format)

    def get_current_time_startnow(self):
        current_time = datetime.datetime.strptime(self.date + " " + self.time, "%m%d%y %H%M%S") + datetime.timedelta( seconds=( int(time.time()) - self.time_set_at) )
        format = "%m-%d-%Y, %H:%M:%S"
        return current_time.strftime(format)



    def read_a_char(self):
            c = None
            if len(self.buf) > 0:
                c = self.buf[0:1]
                self.buf = self.buf[1:]
            else:
                self.buf = self.recv(8192)

            return c
    def get_data(self):
        try:
            ret = ''

            while True:
                c = self.read_a_char()
                print "read " + c
                if c == None:
                    break
                if c == '\n' or c == '':
                    break
                else:
                    ret += c

            data = ret
        except:
            data = None

        if data:
            data = data.lower()
            print "\nIN  [" + str(data.replace("\r", "\\r").replace("\n", "\\n")) + "]\n"

        return data
 
    def send_data(self, data, debug):

        try:
            print "OUT [" + str(data.replace("\r", "\\r").replace("\n", "\\n")) + "]"
            self.socket.send(data)
        except:
            print "*** send_data FAILED [" + debug + "] had an exception sending [" + data + "]"

    def handle_read(self):
        while True:
            time.sleep(0.5)
            start_time = datetime.datetime.strptime(self.start_mmddyy + " " + self.start_time, "%m%d%y %H%M%S")
            current_time = datetime.datetime.strptime(self.date + " " + self.time, "%m%d%y %H%M%S") + \
                           datetime.timedelta( seconds=( int(time.time()) - self.time_set_at) )
            if self.start_later == True:
                if current_time > start_time:
                    self.start_later = False   # only trigger once
                    self.logging = True

            #------------------------------------------------------------------#
            data = None
            data = self.get_data()
            if data == "":
                print "closing"
                self.socket.close()
                self.thread.exit()

            if self.logging == True:
                self.count += 1
                time.sleep(1)
                if self.count > 8:
                    self.count = 1
                    self.send_data('\r\n#{:8.4f},{:8.5f},{:9.3f},{:9.4f},{:9.3f}'.format(random.uniform(10,30), random.uniform(0.03, 0.07), random.uniform(-5, -9), random.uniform(0.18, 0.36), random.uniform(1400, 1500)) + ', ' + self.get_current_time_startlater() + '\r\n', 'MAIN LOGGING LOOP')

                # Need to handle commands that are not in the blessed list #
                if data:
                    command_args = string.splitfields(data.rstrip('\r\n'), "=")
                    if data[0] == '\r' or data[0] == '\n':
                        locked = False
                        self.send_data('S>', 'is this what it wants')
                        self.send_data('S>\r\n', 'is this what it wants')
                        time.sleep(1)
                        self.send_data('S>\r\n', 'is this what it wants')

                    if command_args[0] in ['ds', 'dc', 'ts', 'tsr', 'slt', 'sltr', 'qs', 'stop', '\r\n', '\n\r']:
                        """
                        print "GOT A PERMITTED COMMAND " + command_args[0] + "\n"
                        """
                    else:
                        """
                        print "SILENTLY GOBBLING COMMAND " + data
                        """
                        data = None

            if data:
                self.send_data(data.rstrip('\r').rstrip('\n') + "\r\n", 'ECHO COMMAND BACK TO SENDER')
                command_args = string.splitfields(data.rstrip('\r\n'), "=")

                if command_args[0] == 'baud':
                    if command_args[1] in self.allowable_baud_rates:
                        self.baud_rate = command_args[1]
                    else:
                        self.send_data("***BAUD ERROR MESSAGE***", 'BAUD ERROR MESSAGE')

                elif command_args[0] == 'ds':
                    self.send_data("SBE37-SMP V 2.6 SERIAL NO. 2165   " + self.date[0:2] + ' ' + self.months[int(self.date[2:4])] + ' 20' + self.date[4:6] + '  ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'DS line 1')


                    if self.logging:
                        self.send_data("logging data\r\n", 'DS line 2')
                    else:
                        self.send_data("not logging: received stop command\r\n", 'DS line 2')

                    self.send_data("sample interval = " + str(self.interval) + " seconds\r\n", 'DS line 3')

                    self.send_data("samplenumber = " + str(self.sample_number) + ", free = " + str(200000 - self.sample_number * 8) + "\r\n", 'DS line 4') # likely more complex than i have...

                    if self.tx_real_time:
                        self.send_data("transmit real-time data\r\n", 'DS line 5')
                    else:
                        self.send_data("do not transmit real-time data\r\n", 'DS line 5')

                    if self.output_salinity:
                        self.send_data("output salinity with each sample\r\n", 'DS line 6')
                    else:
                        self.send_data("do not output salinity with each sample\r\n", 'DS line 6')

                    if self.output_sound_velocity:
                        self.send_data("output sound velocity with each sample\r\n", 'DS line 7')
                    else:
                        self.send_data("do not output sound velocity with each sample\r\n", 'DS line 7')

                    if self.store_time:
                        self.send_data("store time with each sample\r\n", 'DS line 8')
                    else:
                        self.send_data("do not store time with each sample\r\n", 'DS line 8')

                    self.send_data("number of samples to average = " + str(self.navg) + "\r\n", 'DS line 9')

                    self.send_data("reference pressure = " + str(self.reference_preassure) + " db\r\n", 'DS line 10')

                    if self.serial_sync_mode:
                        self.send_data("serial sync mode enabled\r\n", 'DS line 11')
                    else:
                        self.send_data("serial sync mode disabled\r\n", 'DS line 11')

                    self.send_data("wait time after serial sync sampling = " + str(self.sync_wait) + " seconds\r\n", 'DS line 12')

                    if self.pump_installed:
                        self.send_data("internal pump is installed\r\n", 'DS line 13')
                    else:
                        self.send_data("internal pump is not installed\r\n", 'DS line 13')

                    self.send_data("temperature = " + str(7.54) + " deg C\r\n", 'DS line 14')

                    self.send_data("WARNING: LOW BATTERY VOLTAGE!!\r\n", 'DS line 15')
               
                elif command_args[0] == 'mmddyy':
                    try:
                        if ((int(command_args[1][0:2]) > 0) and 
                            (int(command_args[1][0:2]) < 13) and
                            (int(command_args[1][2:4]) > 0) and
                            (int(command_args[1][2:4]) < 32)):
                            self.date=command_args[1][0:6]
                        else:
                            self.send_data("***DATE RANGE ERROR***" + command_args[1] + "\r\n", 'mmddyy line 1')
                    except ValueError:
                        self.send_data("ERROR expected NUMERIC INPUT", 'mmddyy line 2')

                elif command_args[0] == 'ddmmyy':
                    try:
                        if ((int(command_args[1][2:4]) > 0) and 
                            (int(command_args[1][2:4]) < 13) and
                            (int(command_args[1][0:2]) > 0) and
                            (int(command_args[1][0:2]) < 32)):
                            self.date=command_args[1][2:4] + command_args[1][0:2] + command_args[1][4:6]
                        else:
                            self.send_data("***DATE RANGE ERROR***" + command_args[1] + "\r\n", 'ddmmyy line 1')
                    except ValueError:
                        self.send_data("ERROR expected NUMERIC INPUT", 'ddmmyy line 2')

                elif command_args[0] == 'hhmmss':
                    try:
                        if ((int(command_args[1][0:2]) >= 0) and 
                            (int(command_args[1][0:2]) < 24) and
                            (int(command_args[1][2:4]) >= 0) and
                            (int(command_args[1][2:4]) < 60) and
                            (int(command_args[1][4:6]) >= 0) and
                            (int(command_args[1][4:6]) < 60)):
                            self.time=command_args[1][0:6]
                            self.time_set_at = int(time.time())
                        else:
                            self.send_data("***TIME RANGE ERROR***" + command_args[1] + "\r\n", 'hhmmss line 1')
                    except ValueError:
                        self.send_data("ERROR expected NUMERIC INPUT", 'hhmmss line 2')

                elif command_args[0] == 'outputsal':
                    if command_args[1] == 'y':
                        self.output_salinity = True
                    elif command_args[1] == 'n':
                        self.output_salinity = False
                    else:
                        self.send_data("***ERROR IT WAS A Y/N QUESTION*** " + command_args[1] + "\r\n", 'outputsal line 1')

                elif command_args[0] == 'outputsv':
                    if command_args[1] == 'y':
                        self.output_sound_velocity = True
                    elif command_args[1] == 'n':
                        self.output_sound_velocity = False
                    else:
                        self.send_data("***ERROR IT WAS A Y/N QUESTION*** " + command_args[1] + "\r\n", 'outputsv line 1')

                elif command_args[0] == 'format':
                    if command_args[1] == '0':
                        self.format = 0;
                    elif command_args[1] == '1':
                        self.format = 1;
                    elif command_args[1] == '2':
                        self.format = 2;
                    else:
                        self.send_data("***ERROR VALID SETTINGS ARE 0,1,2*** " + command_args[1] + "\r\n", 'format line 1')

                elif command_args[0] == 'refpress':
                    self.reference_preassure = command_args[1] 
              
                elif command_args[0] == 'pumpinstalled':
                    if command_args[1] == 'y':
                        self.pump_installed = True
                    elif command_args[1] == 'n':
                        self.pump_installed = False
                    else:
                        self.send_data("***ERROR IT WAS A Y/N QUESTION*** " + command_args[1] + "\r\n", 'pumpinstalled line 1')

                elif command_args[0] == 'samplenum':
                    try:
                        self.sample_number = int(command_args[1])
                    except ValueError:
                        self.send_data("ERROR expected INTEGER", 'samplenum line 1')

                elif command_args[0] == 'qs':
                    self.sleep_state = True                 # will need to work out how to get out of sleep state later.

                elif command_args[0] == 'interval':
                    try:
                        self.interval = int(command_args[1])
                    except ValueError:
                        self.send_data("ERROR expected INTEGER", 'interval line 1')
               
                elif command_args[0] == 'navg':
                    try:
                        self.navg = int(command_args[1])
                    except ValueError:
                        self.send_data("ERROR expected INTEGER", 'navg line 1')
               
                elif command_args[0] == 'storetime':
                    if command_args[1] == 'y':
                        self.store_time = True
                    elif command_args[1] == 'n':
                        self.store_time = False
                    else:
                        self.send_data("***ERROR IT WAS A Y/N QUESTION*** " + command_args[1] + "\r\n", 'storetime line 1')

                elif command_args[0] == 'txrealtime':
                    if command_args[1] == 'y':
                        self.tx_real_time = True
                    elif command_args[1] == 'n':
                        self.tx_real_time = False
                    else:
                        self.send_data("***ERROR IT WAS A Y/N QUESTION*** " + command_args[1] + "\r\n", 'txrealtime line 1')

                elif command_args[0] == 'startnow':
                    self.send_data('start now\r\n', 'startnow line 1')
                    self.logging = True
                    self.locked = True

                elif data[0] == '\r':
                    self.send_data('SBE 37-SMP\r\n', '\\ x1b line 1')
                    self.locked = False
                elif data[1] == '\r\n':
                    self.send_data('SBE 37-SMP\r\n', '\\ x1b line 1')
                    self.locked = False
                elif command_args[0] == '\x1b':
                    self.send_data('SBE 37-SMP\r\n', '\\ x1b line 1')
                    self.locked = False

                elif command_args[0] == 'startmmddyy':
                    try:
                        if ((int(command_args[1][0:2]) > 0) and 
                            (int(command_args[1][0:2]) < 13) and
                            (int(command_args[1][2:4]) > 0) and
                            (int(command_args[1][2:4]) < 32)):
                            self.start_mmddyy=command_args[1][0:6]
                        else:
                            self.send_data("***DATE RANGE ERROR***" + command_args[1] + "\r\n", 'startmmddyy line 1')
                    except ValueError:
                        self.send_data("ERROR expected NUMERIC INPUT", 'startmmddyy line 2')

                elif command_args[0] == 'startddmmyy':
                    try:
                        if ((int(command_args[1][2:4]) > 0) and
                            (int(command_args[1][2:4]) < 13) and
                            (int(command_args[1][0:2]) > 0) and
                            (int(command_args[1][0:2]) < 32)):
                            self.start_mmddyy=command_args[1][2:4] + command_args[1][0:2] + command_args[1][4:6]
                        else:
                            self.send_data("***DATE RANGE ERROR***" + command_args[1] + "\r\n", 'startddmmyy line 1')
                    except ValueError:
                        self.send_data("ERROR expected NUMERIC INPUT", 'startddmmyy line 2')

                elif command_args[0] == 'starthhmmss':
                    try:
                        if ((int(command_args[1][0:2]) > 0) and 
                            (int(command_args[1][0:2]) < 24) and
                            (int(command_args[1][2:4]) > 0) and
                            (int(command_args[1][2:4]) < 60) and
                            (int(command_args[1][4:6]) >= 0) and
                            (int(command_args[1][4:6]) < 60)):
                            self.start_time=command_args[1][0:6]
                        else:
                            self.send_data("***START TIME RANGE ERROR***" + command_args[1] + "\r\n", 'starthhmmss line 1')
                    except ValueError:
                        self.send_data("ERROR expected NUMERIC INPUT", 'starthhmmss line 2')

                elif command_args[0] == 'startlater':
                    self.start_later = True
                    self.send_data('start time = ' + self.date[0:2] + ' ' + self.months[int(self.date[2:4])] + ' 20' + self.date[4:6] + ', ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'startlater line 1')

                elif command_args[0] == 'stop':
                    self.start_later = False
                    self.logging = False
                    #print "really got stop command\r\n"

                elif command_args[0] == 'ts':
                    self.send_data('\r\n{:.4f},{:.5f}, {:.3f},   {:.4f}, {:.3f}'.format(random.uniform(680, 710), random.uniform(-0.001, -0.01), random.uniform(-300, -350), random.uniform(0.01, 0.02), random.uniform(268000, 270000)) + ', ' + self.date[0:2] + ' ' + self.months[int(self.date[2:4])] + ' 20' + self.date[4:6] + ', ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'ts line 1') 

                elif command_args[0] == 'tsr':
                    self.send_data('{:9.1f}, {:9.3f}, {:7.1f}\r\n'.format(random.uniform(200000, 500000), random.uniform(2000, 3000), random.uniform(-200, -300)), 'tsr line 1')

                elif command_args[0] == 'tss':
                    self.send_data('{:8.4f},{:8.5f},{:9.3f},{:9.4f},{:9.3f}'.format(random.uniform(15, 25), random.uniform(0.001, 0.01), random.uniform(0.2, 0.9), random.uniform(0.01, 0.02), random.uniform(1000, 2000)) + ', ' + self.date[0:2] + ' ' + self.months[int(self.date[2:4])] + ' 20' + self.date[4:6] + ', ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'tss line 1') 

                elif command_args[0] == 'tsson':
                    self.send_data('{:8.4f},{:8.5f},{:9.3f},{:9.4f},{:9.3f}'.format(random.uniform(15, 25), random.uniform(0.001, 0.01), random.uniform(0.2, 0.9), random.uniform(0.01, 0.02), random.uniform(1000, 2000)) + ', ' + self.date[0:2] + ' ' + self.months[int(self.date[2:4])] + ' 20' + self.date[4:6] + ', ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'tsson line 1')

                elif command_args[0] == 'slt':
                    self.send_data('{:8.4f},{:8.5f},{:9.3f},{:9.4f},{:9.3f}'.format(random.uniform(15, 25), random.uniform(0.001, 0.01), random.uniform(0.2, 0.9), random.uniform(0.01, 0.02), random.uniform(1000, 2000)) + ', ' + self.date[0:2] + ' ' + self.months[int(self.date[2:4])] + ' 20' + self.date[4:6] + ', ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'slt line 1')

                elif command_args[0] == 'sltr':
                    self.send_data('{:9.1f}, {:9.3f}, {:7.1f}\r\n'.format(random.uniform(200000, 500000), random.uniform(2000, 3000), random.uniform(-200, -300)), 'sltr line 1')

                elif command_args[0] == 'sl':
                    self.send_data('{:8.4f},{:8.5f},{:9.3f},{:9.4f},{:9.3f}'.format(random.uniform(15, 25), random.uniform(0.001, 0.01), random.uniform(0.2, 0.9), random.uniform(0.01, 0.02), random.uniform(1000, 2000)) + ', ' + self.date[0:2] + ' ' + self.months[int(self.date[2:4])] + ' 20' + self.date[4:6] + ', ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'sl line 1') 

                elif command_args[0] == 'syncmode':
                    if command_args[1] == 'y':
                        self.serial_sync_mode = True
                    elif command_args[1] == 'n':
                        self.serial_sync_mode = False
                    else:
                        self.send_data("***ERROR IT WAS A Y/N QUESTION*** " + command_args[1] + "\r\n", 'syncmode line 1')

                elif command_args[0] == 'syncwait':
                    try:
                        if int(command_args[1]) >= 0 and int(command_args[1]) < 121:
                            self.sync_wait = int(command_args[1])
                        else:
                            self.send_data("*** ERROR INTEGER OUT OF RANGE (0 - 120)", 'syncwait line 1')
                    except ValueError:
                        self.send_data("*** ERROR expected INTEGER", 'syncwait line 2')

                elif data[0:2] == "dd":
                    data = data[2:].rstrip('\r\n\r')
                    command_args = string.splitfields(data, ",")
                    try:
                        begin = int(command_args[0])
                    except ValueError:
                        self.send_data("*** begin ERROR expected INTEGER", 'dd line 1')

                    try:
                        end = int(command_args[1])
                    except ValueError:
                        self.send_data("*** end ERROR expected INTEGER", 'dd line 2')
                    self.send_data('start time =  ' + self.date[0:2] + ' ' + self.months[int(self.date[2:4])] + ' 20' + self.date[4:6] + '  ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'dd line 3') 
                    self.send_data('sample interval = ' + str(self.interval) + ' seconds\r\n', 'dd line 4')  
                    self.send_data('start sample number = ' + str(self.sample_number) + '\r\n\r\n', 'dd line 5')  
                    for sample in range(begin, end):
                        self.send_data('{:8.4f},{:8.5f},{:9.3f},{:9.4f},{:9.3f}'.format(random.uniform(15, 25), random.uniform(0.001, 0.01), random.uniform(0.2, 0.9), random.uniform(0.01, 0.02), random.uniform(1000, 2000)) + ', ' + self.date[0:2] + ' ' + self.months[int(self.date[2:4])] + ' 20' + self.date[4:6] + ', ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'dd line 6')

                elif command_args[0] == "tt":
                    count = 100
                    while count > 0:
                        count -= 1
                        self.send_data('{:8.4f}\r\n'.format(random.uniform(15, 25)), 'tt line 1')
                        time.sleep(1)

                        data = self.get_data()

                        if data:
                            if data[0] == '\x1b':
                                count = 0

                elif command_args[0] == "tc":
                    count = 100
                    while count > 0:
                        count -= 1
                        self.send_data('{:8.5f}\r\n'.format(random.uniform(0.001, 0.1)), 'tc line 1')
                        time.sleep(1)

                        data = self.get_data()

                        if data:
                            if data[0] == '\x1b':
                                count = 0

                elif command_args[0] == "tp":
                    count = 100
                    while count > 0:
                        count -= 1
                        self.send_data('{:8.3f}\r\n'.format(random.uniform(-6.5, -8.2)), 'tp line 1')
                        time.sleep(1)

                        data = self.get_data()

                        if data:
                            if data[0] == '\x1b':
                                count = 0

                elif command_args[0] == "ttr":
                    count = 100
                    while count > 0:
                        count -= 1
                        self.send_data('{:9.1f}\r\n'.format(random.uniform(361215, 361219)), 'ttr line 1')
                        time.sleep(1)

                        data = self.get_data()

                        if data:
                            if data[0] == '\x1b':
                                count = 0

                elif command_args[0] == "tcr":
                    count = 100
                    while count > 0:
                        count -= 1
                        self.send_data('{:9.3f}\r\n'.format(random.uniform(2600, 2700)), 'tcr line 1')
                        time.sleep(1)

                        data = self.get_data()

                        if data:
                            if data[0] == '\x1b':
                                count = 0

                elif command_args[0] == "tpr":
                    count = 100
                    while count > 0:
                        count -= 1
                        self.send_data('{:7.1f},{:6.1f}\r\n'.format(random.uniform(-250, -290),random.uniform(18.1,20.2)), 'tpr line 1')
                        time.sleep(1)

                        data = self.get_data()

                        if data:
                            if data[0] == '\x1b':
                                count = 0

                elif command_args[0] == "tr":
                    count = 30
                    while count > 0:
                        count -= 1
                        self.send_data('rtcf = {:9.7f}\r\n'.format(random.uniform(1.0, 1.1)), 'tr line 1')
                        time.sleep(1)

                        data = self.get_data()

                        if data:
                            if data[0] == '\x1b':
                                count = 0

                elif command_args[0] == "pumpon":
                    """
                    NOP
                    """

                elif command_args[0] == "pumpoff":
                    """
                    NOP
                    """
             
                elif command_args[0] == 'dc':
                    self.send_data("SBE37-SM V 2.6b  3464\r\n", 'dc line 1')
                    self.send_data("temperature:  " + self.tcaldate + "\r\n", 'dc line 2')
                    self.send_data("    TA0 = " + '{0:.6e}'.format(self.ta0) + "\r\n", 'dc line 3')
                    self.send_data("    TA1 = " + '{0:.6e}'.format(self.ta1) + "\r\n", 'dc line 4')
                    self.send_data("    TA2 = " + '{0:.6e}'.format(self.ta2) + "\r\n", 'dc line 5')
                    self.send_data("    TA3 = " + '{0:.6e}'.format(self.ta3) + "\r\n", 'dc line 6')
                    self.send_data("conductivity:  " + self.caldate + "\r\n", 'dc line 7')
                    self.send_data("    G = " + '{0:.6e}'.format(self.cg) + "\r\n", 'dc line 8')
                    self.send_data("    H = " + '{0:.6e}'.format(self.ch) + "\r\n", 'dc line 9')
                    self.send_data("    I = " + '{0:.6e}'.format(self.ci) + "\r\n", 'dc line 10')
                    self.send_data("    J = " + '{0:.6e}'.format(self.cj) + "\r\n", 'dc line 11')
                    self.send_data("    CPCOR = " + '{0:.6e}'.format(self.cpcor) + "\r\n", 'dc line 12')
                    self.send_data("    CTCOR = " + '{0:.6e}'.format(self.ctcor) + "\r\n", 'dc line 13')
                    self.send_data("    WBOTC = " + '{0:.6e}'.format(self.wbotc) + "\r\n", 'dc line 14')
                    self.send_data("pressure S/N 4955, range = " + str(random.uniform(10000, 11000)) + " psia:  " + self.pcaldate + "\r\n", 'dc line 15')
                    self.send_data("    PA0 = " + '{0:.6e}'.format(self.pa0) + "\r\n", 'dc line 16')
                    self.send_data("    PA1 = " + '{0:.6e}'.format(self.pa1) + "\r\n", 'dc line 17')
                    self.send_data("    PA2 = " + '{0:.6e}'.format(self.pa2) + "\r\n", 'dc line 18')
                    self.send_data("    PTCA0 = " + '{0:.6e}'.format(self.ptca0) + "\r\n", 'dc line 19')
                    self.send_data("    PTCA1 = " + '{0:.6e}'.format(self.ptca1) + "\r\n", 'dc line 20')
                    self.send_data("    PTCA2 = " + '{0:.6e}'.format(self.ptca2) + "\r\n", 'dc line 21')
                    self.send_data("    PTCSB0 = " + '{0:.6e}'.format(self.ptcb0) + "\r\n", 'dc line 22')
                    self.send_data("    PTCSB1 = " + '{0:.6e}'.format(self.ptcb1) + "\r\n", 'dc line 23')
                    self.send_data("    PTCSB2 = " + '{0:.6e}'.format(self.ptcb2) + "\r\n", 'dc line 24')
                    self.send_data("    POFFSET = " + '{0:.6e}'.format(self.poffset) + "\r\n", 'dc line 25')
                    self.send_data("rtc:  " + self.rcaldate + "\r\n", 'dc line 26')
                    self.send_data("    RTCA0 = " + '{0:.6e}'.format(self.rtca0) + "\r\n", 'dc line 27')
                    self.send_data("    RTCA1 = " + '{0:.6e}'.format(self.rtca1) + "\r\n", 'dc line 28')
                    self.send_data("    RTCA2 = " + '{0:.6e}'.format(self.rtca2) + "\r\n", 'dc line 29')
     
                ################################
                # now the coefficient Commands #
                ################################
                elif command_args[0] == 'tcaldate':
                    self.tcaldate=command_args[1]                 #take it on faith

                elif command_args[0] == 'ta0':
                    try:
                        self.ta0 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ta0 line 1')

                elif command_args[0] == 'ta1':
                    try:
                        self.ta1 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ta1 line 1')

                elif command_args[0] == 'ta2':
                    try:
                        self.ta2 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ta2 line 1')

                elif command_args[0] == 'ta3':
                    try:
                        self.ta3 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ta3 line 1')

                elif command_args[0] == 'caldate':
                    self.caldate=command_args[1]                 #take it on faith

                elif command_args[0] == 'cg':
                    try:
                        self.cg = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'cg line 1')

                elif command_args[0] == 'ch':
                    try:
                        self.ch = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ch line 1')

                elif command_args[0] == 'ci':
                    try:
                        self.ci = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ci line 1')

                elif command_args[0] == 'cj':
                    try:
                        self.cj = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'cj line 1')

                elif command_args[0] == 'wbotc':
                    try:
                        self.wbotc = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'wbotc line 1')

                elif command_args[0] == 'ctcor':
                    try:
                        self.ctcor = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ctcor line 1')

                elif command_args[0] == 'cpcor':
                    try:
                        self.cpcor = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'cpcor line 1')

                elif command_args[0] == 'pcaldate':
                    self.pcaldate=command_args[1]                 #take it on faith

                elif command_args[0] == 'pa0':
                    try:
                        self.pa0 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'pa0 line 1')

                elif command_args[0] == 'pa1':
                    try:
                        self.pa1 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'pa1 line 1')

                elif command_args[0] == 'pa2':
                    try:
                        self.pa2 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'pa2 line 1')

                elif command_args[0] == 'ptca0':
                    try:
                        self.ptca0 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ptca0 line 1')

                elif command_args[0] == 'ptca1':
                    try:
                        self.ptca1 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ptca1 line 1')

                elif command_args[0] == 'ptca2':
                    try:
                        self.ptca2 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ptca2 line 1')

                elif command_args[0] == 'ptcb0':
                    try:
                        self.ptcb0 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ptcb0 line 1')

                elif command_args[0] == 'ptcb1':
                    try:
                        self.ptcb1 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ptcb1 line 1')

                elif command_args[0] == 'ptcb2':
                    try:
                        self.ptcb2 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'ptcb2 line 1')

                elif command_args[0] == 'poffset':
                    try:
                        self.poffset = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'poffset line 1')

                elif command_args[0] == 'rcaldate':
                    self.rcaldate=command_args[1]                 #take it on faith

                elif command_args[0] == 'rtca0':
                    try:
                        self.rtca0 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'rtca0 line 1')

                elif command_args[0] == 'rtca1':
                    try:
                        self.rtca1 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'rtca1 line 1')

                elif command_args[0] == 'rtca2':
                    try:
                        self.rtca2 = float(command_args[1])
                    except:
                        self.send_data("? CMD\r\n", 'rtca2 line 1')

                else:
                    self.send_data("? CMD\r\n", 'else line 1 RESPONSE TO ' + data)
             
                self.send_data("\r\nS>", 'default command prompt')
                #------------------------------------------------------------------#

 
class sbe37_server(asyncore.dispatcher):

    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)

    def handle_accept(self):
        pair = self.accept()
        if pair is None:
            pass
        else:
            sock, addr = pair
            global connection_count
            connection_count += 1
            print str(connection_count) + ' Incoming connection from %s' % repr(addr)
            try:
                thread.start_new_thread(sbe37, (sock, thread))
            except:
                print "exception starting new thread\r\n"

def port_usage():
    print "The port flag takes a int value > 1000\n"

def usage():
    print "SBE37-SMP Simulator:\n"
    print "This program simulates a SBE37-SMP sensor deployed by \nbeing connected to a MOXA NPort 5410 Serial Device Server."
    print "Available options are:"
    print "  -h, --help    : Displays this message"
    print "  -p, --port=   : Sets the port to listen on (default = " + str(port) + ")."
    print

def get_opts():

    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:h", ["help", "port="])
    except getopt.GetoptError, err:
        print str(err)
        sys.exit()
    
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-p", "--port"):
            try: 
                port = int(a)
            except:
                port_usage()
                sys.exit()
            if port < 1000:
                port_usage()
                sys.exit()
        else:
            assert False, "unhandled option"



if __name__ == '__main__':
    get_opts()

    print "\nStarting simulator on port " + str(port) + ".\n"

    server = sbe37_server('', port)

    try:
        asyncore.loop()
    except:
        sys.exit() # Be silent when ^c pressed



