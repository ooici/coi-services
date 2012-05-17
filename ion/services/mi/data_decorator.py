#!/usr/bin/env python

'''
@package ion.services.mi.data_decorator Implementation of data decoration
   classes
@file ion/services/mi/data_decorator.py
@author Steve Foley
@brief Decorators that operate on the data returning from the driver
These decorators can be chained together to provide a series of actions on
the data that flows back from the driver. They may be located in different
modules as appropriate.
'''

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

import re
from ion.services.mi.exceptions import InstrumentDataException

class DataDecorator(object):
    '''The base decorator class that all data decorators should extend
    
    Data flowing back from a driver should pass through a chain of decorator
    classes that operate on the data in some way. They should work with their
    input, then call off to the next decorator. When the end of the chain is
    reached, operation should stop.
    
    The chain of decorators is expected to be built on its own, then passed
    into the agent that is operating on the data.
    '''
    
    def __init__(self):
        
        self.next_decorator = None
        '''A link to the next decorator in the chain, None at the end'''
    
    def handle_incoming_data(self, original_data=None, chained_data=None):
        '''Operate on the data being passed in
        
        This method is intended to be overriden by the extending classes. The
        final step in this method should be to call handle_incoming_data() on
        the self.nextDecorator member. Decorators may choose to modify the data
        in some way for decorators further down the chain. Decorators that
        purely provide side effects should not affect either data argument.
        
        @param original_data The unadulterated data stream that is to be
        operated on. This should not be modified along the way!
        @param chained_data The data that may have been modified along the
        chain.
        @throws InstrumentDataException Problem handling data from the device
        '''
        self.next_decorator.handle_incoming_data(original_data, chained_data)
        
class DataEventPublisherDecorator(DataDecorator):
    '''A decorator that publishes data events'''
    
    def handle_incoming_data(self, original_data=None, chained_data=None):
        '''Publish the chained data to the data topic'''
        # execute a publish on the chained_data argument
        
class TimestampDecorator(DataDecorator):
    '''A decorator that operates on timestamps in the data stream
    
    The decorator decodes timestamps from the data stream and does the
    appropriate thing with them down the line. This may involve modifying
    the chained_data parameter.
    '''

class RSNTimestampDecorator(TimestampDecorator):
    '''A decorator that decodes RSN timestamps in the data stream
    
    The decorator decodes timestamps from the data stream and does the
    appropriate thing with them down the line. This may involve modifying
    the chained_data parameter.
    '''
    
    TS_PATTERN = r'<OOI-TS (?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d*) TS>(?P<data>.*)<\00I-TS>'
    '''Pattern of timestamp from RSN. EX:
    <OOI-TS 2012-04-11T23:40:04.956497 TS>
    data<\00I-TS>
    '''
    TS_REGEX = re.compile(TS_PATTERN)
    
    def handle_incoming_data(self, original_data=None, chained_data=None):
        '''Pulls timestamp out of the original_data argument'''
        (timestamp, instrument_string) = self._parse_timestamp(original_data)
        
        if timestamp:
            if self.next_decorator == None:
                return (original_data, chained_data)
            else:
                self.next_decorator.handle_incoming_data(original_data, chained_data)
        else:
            raise InstrumentDataException(error_code=InstErrorCode.HARDWARE_ERROR,
                                          msg="Checksum failure!")
    
    def _parse_timestamp(self, s):
        '''Parse a string to see if it matches the given regex. If so, get
        the timestamp out and return the string and the data.
        @param s The string to run through the regex
        @retval 2-tuple of timestamp string and data string
        '''
        ts = None
        data = None
        if self.TS_REGEX.matches(s):
            # get the timestamp and data string
            pass
        return (ts, data)
        
class CGSNTimestampDecorator(TimestampDecorator):
    '''A decorator that attaches timestamps to the data stream
    
    The decorator obtains timestamps from the best time source and applies
    them to the data stream.
    '''
    
    def handle_incoming_data(self, original_data=None, chained_data=None):
        '''Pulls timestamp out of the original_data argument'''
        # execute a publish on the chained_data argument
        
class DataParserDecorator(DataDecorator):
    '''A decorator that parses the native format into something else
    
    The decorator parses the native format and sends on the chained_data as
    a new format. This class may include a description of the format in some
    regex or other class variable.
    '''
    
    def handle_incoming_data(self, original_data=None, chained_data=None):
        '''Pulls translates the original data into a new chained data format'''

class ChecksumDecorator(DataDecorator):
    '''A decorator that confirms a checksum is good or not. Must be instrument
    specific (or generic enough) to handle the particular type of checksum the
    instrument uses for its data.
    
    The decorator parses the native format and sends on the chained_data as
    a new format. This class may include a description of the format in some
    regex or other class variable.
    '''
    
    def handle_incoming_data(self, original_data=None, chained_data=None):
        '''Pulls translates the original data into a new chained data format'''
