#!/usr/bin/env python
__author__ = 'Roger Unwin'
__license__ = 'Apache 2.0'

import socket
import time
from time import gmtime, strftime
import datetime
import string
import sys
import random
import asyncore
import thread
import getopt
import select
import os

### default values defined below (b/c class is not yet defined)
#default_port = 4001       # TCP port to run on.
#default_message_rate = 5  # 5 sec between messages when streaming
#default_sim=SBE37_random

########### BASE class here handles SBE37 behaviors
########### see below for subclasses that provide different data values

class SBE37(asyncore.dispatcher_with_send):
    buf = ""
    next_send = None
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
    tx_real_time = True
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
    ccaldate = "08-nov-05"
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
    knock_count = 0

    months = ['BAD PROGRAMMER MONTH', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    save = ""

    def __init__(self, socket, thread, streaming_rate, connection_id):
        self.socket = socket
        self.socket.settimeout(0.0)
        self.thread = thread
        self.streaming_rate = streaming_rate
        self.max_sleep = streaming_rate/2. if streaming_rate<4 else 2.
        self.connection_id = connection_id
        self.handle_read()

    def handle_error(self, request, client_address):
        print "%3d *** dispatcher reports error: %s %s" % (self.connection_id,client_address,request)

    def get_current_time_startlater(self):
        #current_time = datetime.datetime.strptime(self.date + " " + self.time, "%m%d%y %H%M%S") + datetime.timedelta( seconds=( int(time.time()) - self.time_set_at) )
        format = "%d %b %Y, %H:%M:%S"
        return strftime(format, gmtime())
        #return current_time.strftime(format)

    def get_current_time_startnow(self):
        current_time = datetime.datetime.strptime(self.date + " " + self.time, "%m%d%y %H%M%S") + datetime.timedelta( seconds=( int(time.time()) - self.time_set_at) )
        format = "%m-%d-%Y, %H:%M:%S"
        return strftime(format, gmtime())
        #return current_time.strftime(format)



    def read_a_char(self):
            c = None
            if len(self.buf) > 0:
                c = self.buf[0:1]
                self.buf = self.buf[1:]
            else:
                self.buf = self.recv(8192)
                for x in self.buf:
                    self.socket.send(x + '\0')

            return c

    def get_data(self):
        data = ""
        ret = self.save
        try:

            while True:
                c = self.read_a_char()
                if c == None:
                    break
                if c == '\n' or c == '':
                    self.save = ""
                    ret += c
                    data = ret
                    break
                else:
                    ret += c

        except AttributeError:
            print "%3d *** closing connection" % self.connection_id
#            log_file.close()
            self.socket.close()
            self.thread.exit()
        except:
            self.save = ret
            data = ""

        if data:
            data = data.lower()
            print "%3d <-- %s"%(self.connection_id,data.strip())
#            if log_file.closed == False:
#                log_file.write("IN  [" + repr(data) + "]\n")
        return data
 
    def send_data(self, data, debug):

        try:
            print "%3d --> %s"%(self.connection_id,data.strip())
            self.socket.send(data)
#            if log_file.closed == False:
#                log_file.write("OUT  [" + repr(data) + "]\n")
        except Exception,e:
            print "%3d *** send_data FAILED [%s] had an exception sending [%s]: %s" % (self.connection_id,debug,data,e)

    def handle_read(self):
        while True:
            self.date = strftime("%m%d%y", gmtime())
            self.time = strftime("%H%M%S", gmtime())
            time.sleep(0.01)
            start_time = datetime.datetime.strptime(self.start_mmddyy + " " + self.start_time, "%m%d%y %H%M%S")
            current_time = datetime.datetime.strptime(self.date + " " + self.time, "%m%d%y %H%M%S") + \
                           datetime.timedelta( seconds=( int(time.time()) - self.time_set_at) )
            if self.start_later == True:
                if current_time > start_time:
                    self.start_later = False   # only trigger once
                    self.logging = True

            #------------------------------------------------------------------#
            data = self.get_data()

            if self.logging == True:
                if not self.next_send:
                    time.sleep(0.1)
                else:
                    # sleep longer to use less CPU time when multiple simulators are running until it is about time to transmit
                    remaining = self.next_send - time.time()
                    send_now = False
                    if remaining>self.max_sleep:
                        time.sleep(self.max_sleep) # worst case: 2sec latency handling command while in streaming mode
                    elif remaining>0.1:
                        time.sleep(remaining - 0.1) # sleep off most of remaining time (< max_sleep)
                    else:
                        if remaining>0:
                            time.sleep(remaining)
                        self.next_send += self.streaming_rate
                        send_now = True

                    if send_now and self.tx_real_time:
                        a,b,c,d,e = self.generate_data_values()
                        t = self.get_current_time_startlater()
                        msg = '\r\n#{:.4f},{:.5f}, {:.3f},   {:.4f}, {:.3f}, {}\r\n'.format(a,b,c,d,e,t)
                        self.send_data(msg, 'MAIN LOGGING LOOP')

                # Need to handle commands that are not in the blessed list #
                if data:
                    command_args = string.splitfields(data.rstrip('\r\n'), "=")
                    if data[0] == '\r' or data[0] == '\n':
                        locked = False

                        self.knock_count += 1

                        if self.knock_count >= 5:
                            self.send_data('\r\nS>\r\n', 'NEW')

                        if self.knock_count == 4:
                            self.send_data('\r\nS>\r\n', 'NEW')

                        if self.knock_count == 3:
                            self.send_data('\x00SBE 37-SM\r\n', 'NEW')
                            self.send_data('S>', 'NEW')

                    elif command_args[0] not in ['ds', 'dc', 'ts', 'tsr', 'slt', 'sltr', 'qs', 'stop', '\r\n', '\n\r']:
                        self.send_data('cmd not allowed while logging\n', 'non-permitted command')
                        data = None

            if data:
                handled = True
                command_args = string.splitfields(data.rstrip('\r\n'), "=")

                if command_args[0] == 'baud':
                    if command_args[1] in self.allowable_baud_rates:
                        self.baud_rate = command_args[1]
                    else:
                        self.send_data("***BAUD ERROR MESSAGE***", 'BAUD ERROR MESSAGE')

                elif command_args[0] == 'ds':
                    self.send_data("SBE37-SMP V 2.6 SERIAL NO. 2165   " + self.date[2:4] + ' ' + self.months[int(self.date[0:2])] + ' 20' + self.date[4:6] + '  ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6] + '\r\n', 'DS line 1')


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
#                        self.next_send = time.time() + self.streaming_rate
                    elif command_args[1] == 'n':
                        self.tx_real_time = False
                        self.next_send = None
                    else:
                        self.send_data("***ERROR IT WAS A Y/N QUESTION*** " + command_args[1] + "\r\n", 'txrealtime line 1')

                elif command_args[0] == 'startnow':
                    self.send_data('start now\r\n', 'startnow line 1')
                    self.logging = True
                    self.locked = True
                    self.knock_count = 0
                    handled = False
                    self.next_send = time.time() + self.streaming_rate

                elif data[0] == '\r':
                    #self.send_data('SBE 37-SMP\r\n', '\\ x1b line 1')
                    handled = False
                    if self.logging == False:
                        self.send_data('\r\nS>', '\\ r line 1')
                    self.locked = False
                    data = ""
                elif data[:1] == '\r\n':
                    #self.send_data('SBE 37-SMP\r\n', '\\ x1b line 1')
                    handled = False
                    self.send_data('S>  ', '\\ r \\ n line 1')
                    self.locked = False
                elif command_args[0] == '\x1b':
                    #self.send_data('SBE 37-SMP\r\n', '\\ x1b line 1')
                    handled = False
                    self.send_data('S>  ', '\\ x1b line 1')
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
                    self.send_data('S>\r\n', 'SPECIAL STOP PROMPT')
                    handled = False

                elif command_args[0] in ('ts', 'tss', 'tsson', 'slt', 'sl'):
                    a,b,c,d,e = self.generate_data_values()
                    t = self.date[2:4] + ' ' + self.months[int(self.date[0:2])] + ' 20' + self.date[4:6] + ', ' + self.time[0:2] + ':' + self.time[2:4] + ':' + self.time[4:6]
                    self.send_data('\r\n{:.4f},{:.5f}, {:.3f},   {:.4f}, {:.3f}, %s\r\n'.format(a,b,c,d,e,t), command_args[0] + ' line 1')

                elif command_args[0] in ('tsr','stlr'):
                    self.send_data('{:9.1f}, {:9.3f}, {:7.1f}\r\n'.format(random.uniform(200000, 500000), random.uniform(2000, 3000), random.uniform(-200, -300)), command_args[0] + ' line 1')

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
                    self.send_data("conductivity:  " + self.ccaldate + "\r\n", 'dc line 7')
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

                elif command_args[0] == 'ccaldate':
                    self.ccaldate=command_args[1]                 #take it on faith

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
                    handled = False
                    self.send_data("? CMD\r\n", 'else line 1 RESPONSE TO ' + data)
             
                if handled == True:
                    self.send_data("\r\nS>", 'default command prompt')
                #------------------------------------------------------------------#

class SBE37_server(asyncore.dispatcher):
    def __init__(self, sim_class, host, port, rate):
        asyncore.dispatcher.__init__(self)
        self.connection_count = 0
        self.sim_class = sim_class
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)
        self.message_rate = rate

    def handle_accept(self):
        pair = self.accept()
        if pair is None:
            pass
        else:
            sock, addr = pair
            self.connection_count += 1 # not threadsafe -- could wind up with two threads and same count value
            print '%3d *** new connection from %r' % (self.connection_count,addr)
            try:
                thread.start_new_thread(self.sim_class, (sock, thread, self.message_rate, self.connection_count))
            except Exception, e:
                print "%3d *** exception starting thread: %s"%(self.connection_count,e)

def usage():
    print "SBE37-SMP Simulator:\n"
    print "This program simulates a SBE37-SMP sensor deployed by \nbeing connected to a MOXA NPort 5410 Serial Device Server."
    print "Available options are:"
    print "  -h, --help    : Displays this message"
    print "  -p, --port=   : Sets the port to listen on (>1024, default = %s)." % default_port

def get_opts():
    opts, args = getopt.getopt(sys.argv[1:], "c:p:h", ["class=", "port=", "rate="])

    out={'rate':default_message_rate,'port':default_port,'simulator':SBE37_random}
    for o, a in opts:
        if o in ("-c", "--class"):
            out['simulator'] = getattr(sys.modules[__name__],a)
        if o in ("-r", "--rate"):
            out['message_rate'] = int(a)
        elif o in ("-p", "--port"):
            out['port'] = int(a)
        else:
            print 'unknown option: '+o
    return out

def main():
    try:
        args = get_opts()
    except Exception as e:
        print 'Exception: %s'%e
        usage()
        sys.exit()
    print 'using args: %r'%args
    SBE37_server(sim_class=args['simulator'], host='', port=args['port'], rate=args['rate'])
    try:
        asyncore.loop()
    except:
        sys.exit() # Be silent when ^c pressed

################################################################################################
##
## THESE CLASSES generate different sample values for the simulator
#
# return tuple of: temperature, conductivity, pressure, salinity, sound velocity

class SBE37_random(SBE37):
    def generate_data_values(self):
        return ( random.uniform(-10.0, 100.0), random.uniform(0.0, 100.0), random.uniform(0.0, 1000.0),
                 random.uniform(0.1, 40.0), random.uniform(1505, 1507))

class SBE37_High(SBE37):
    def generate_data_values(self):
        return ( random.uniform(45.0, 100.0), random.uniform(50.0, 100.0), random.uniform(500.0, 1000.0), random.uniform(20.05, 40.0), random.uniform(1506.0, 1507.0))

class SBE37_Low(SBE37):
    def generate_data_values(self):
        return ( random.uniform(-10.0, 45.0), random.uniform(0.0, 50.0), random.uniform(0.0, 500.0), random.uniform(0.1, 20.05), random.uniform(1505.0, 1506.0))

import math

def my_sin(time, Amin, Amax):
    sin_val = math.sin(time)
    range = Amax - Amin
    adj_sin = (sin_val + 1.0) * range/2.0 + Amin
    return adj_sin

# vary as sine wave over time
class SBE37_sine(SBE37):
    sinwave_time = 0.0
    def generate_data_values(self):
        self.sinwave_time += 0.2
        return ( my_sin(self.sinwave_time, -10.0, 100.0), my_sin(self.sinwave_time, 0.0, 100.0), my_sin(self.sinwave_time, 0.0, 1000.0), my_sin(self.sinwave_time, 0.1, 40.0), my_sin(self.sinwave_time, 1505, 1507))

# narrower, valid range to help ensure density can be calculated
class SBE37_midrange(SBE37):
    sinwave_time = 0.0
    def generate_data_values(self):
        self.sinwave_time += 0.2
        return ( my_sin(self.sinwave_time, 5.0, 15.0), my_sin(self.sinwave_time, 2.5, 4.5), my_sin(self.sinwave_time, 2000.0, 4000.0), my_sin(self.sinwave_time, 0.1, 40.0), my_sin(self.sinwave_time, 1505, 1507))

#> Valid ranges for conductivity are 0-7 S/m. Typical values we've seen off the Oregon coast are ~35 mS/cm, which converts to ~3.5 S/m.
#>
#> Valid ranges for temperature are -2-40 deg_C. Typical values we've seen off the Oregon coast are between 5 and 20 deg_C. 12 deg_C would be absolutely reasonable.
#>
#> Valid ranges for pressure are 0-7000 dbar. Really, just choose a depth.
#>
#> I would recommend the simulator produce at C of 3.5 S/m, a T of 12 deg_C and a depth of 10 dbar. Apply sine wave functions with some small fraction of random white noise and let it rip.
#>

################################################################################################

default_port = 4001       # TCP port to run on.
default_message_rate = 5  # 5 sec between messages when streaming
default_sim=SBE37_random

if __name__ == '__main__':
    main()



