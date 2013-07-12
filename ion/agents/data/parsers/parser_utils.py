#!/usr/bin/env python

__author__ = 'Michael Meisinger'

import time
import ntplib
import json

from ooi.logging import log


DEFAULT_STREAM = "parsed"

# TODO: For now, copied the MI definitions to avoid import from mi.*
# Move to MI submodule when ready to move all parsers


class BaseEnum(object):
    """Base class for enums.

    Used to code agent and instrument states, events, commands and errors.
    To use, derive a class from this subclass and set values equal to it
    such as:
    @code
    class FooEnum(BaseEnum):
       VALUE1 = "Value 1"
       VALUE2 = "Value 2"
    @endcode
    and address the values as FooEnum.VALUE1 after you import the
    class/package.

    Enumerations are part of the code in the MI modules since they are tightly
    coupled with what the drivers can do. By putting the values here, they
    are quicker to execute and more compartmentalized so that code can be
    re-used more easily outside of a capability container as needed.
    """

    @classmethod
    def list(cls):
        """List the values of this enum."""
        return [getattr(cls,attr) for attr in dir(cls) if\
                not callable(getattr(cls,attr)) and not attr.startswith('__')]

    @classmethod
    def dict(cls):
        """Return a dict representation of this enum."""
        result = {}
        for attr in dir(cls):
            if not callable(getattr(cls,attr)) and not attr.startswith('__'):
                result[attr] = getattr(cls,attr)
        return result

    @classmethod
    def has(cls, item):
        """Is the object defined in the class?

        Use this function to test
        a variable for enum membership. For example,
        @code
        if not FooEnum.has(possible_value)
        @endcode
        @param item The attribute value to test for.
        @retval True if one of the class attributes has value item, false
        otherwise.
        """
        return item in cls.list()

class DataParticleKey(BaseEnum):
    PKT_FORMAT_ID = "pkt_format_id"
    PKT_VERSION = "pkt_version"
    STREAM_NAME = "stream_name"
    INTERNAL_TIMESTAMP = "internal_timestamp"
    PORT_TIMESTAMP = "port_timestamp"
    DRIVER_TIMESTAMP = "driver_timestamp"
    PREFERRED_TIMESTAMP = "preferred_timestamp"
    QUALITY_FLAG = "quality_flag"
    VALUES = "values"
    VALUE_ID = "value_id"
    VALUE = "value"
    BINARY = "binary"

class DataParticleValue(BaseEnum):
    JSON_DATA = "JSON_Data"
    ENG = "eng"
    OK = "ok"
    CHECKSUM_FAILED = "checksum_failed"
    OUT_OF_RANGE = "out_of_range"
    INVALID = "invalid"
    QUESTIONABLE = "questionable"


class FlexDataParticle(object):
    # data particle type is intended to be defined in each derived data particle class.  This value should be unique
    # for all data particles.  Best practice is to access this variable using the accessor method:
    # data_particle_type()
    _data_particle_type = DEFAULT_STREAM

    def __init__(self, raw_data=None,
                 port_timestamp=None,
                 internal_timestamp=None,
                 driver_timestamp=None,
                 preferred_timestamp=DataParticleKey.DRIVER_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 stream_name=DEFAULT_STREAM):
        """ Build a particle seeded with appropriate information

        @param raw_data The raw data used in the particle
        @param driver_timestamp Timestamp in
        """
        self.contents = {
            DataParticleKey.PKT_FORMAT_ID: DataParticleValue.JSON_DATA,
            DataParticleKey.PKT_VERSION: 1,
            DataParticleKey.PORT_TIMESTAMP: port_timestamp,
            DataParticleKey.INTERNAL_TIMESTAMP: internal_timestamp,
            DataParticleKey.DRIVER_TIMESTAMP: driver_timestamp or ntplib.system_to_ntp_time(time.time()),
            DataParticleKey.PREFERRED_TIMESTAMP: preferred_timestamp,
            DataParticleKey.QUALITY_FLAG: quality_flag
        }
        self.values = []
        self.raw_data = raw_data
        self.stream_name = stream_name

    def set_internal_timestamp(self, timestamp=None, unix_time=None):
        """
        Set the internal timestamp
        @param timestamp: NTP timestamp to set
        @param unit_time: Unix time as returned from time.time()
        @raise InstrumentParameterException if timestamp or unix_time not supplied
        """
        if (timestamp == None and unix_time is None):
            raise Exception("timestamp or unix_time required")

        if (unix_time != None):
            timestamp = ntplib.system_to_ntp_time(unix_time)

        # Do we want this to happen here or in down stream processes?
        #if(not self._check_timestamp(timestamp)):
        #    raise InstrumentParameterException("invalid timestamp")

        self.contents[DataParticleKey.INTERNAL_TIMESTAMP] = float(timestamp)

    def set_value(self, id, value):
        """
        Set a content value, restricted as necessary

        @param id The ID of the value to set, should be from DataParticleKey
        @param value The value to set
        @raises ReadOnlyException If the parameter cannot be set
        """
        if (id == DataParticleKey.INTERNAL_TIMESTAMP) and (self._check_timestamp(value)):
            self.contents[DataParticleKey.INTERNAL_TIMESTAMP] = value
        else:
            raise Exception("Parameter %s not able to be set to %s after object creation!" %
                                    (id, value))

    def set_data_values(self, values):
        for val_id, value in values.iteritems():
            self.set_data_value(val_id, value)

    def set_data_value(self, id, value):
        """
        Set a data content value, restricted as necessary

        @param id The ID of the value to set, should be from DataParticleKey
        @param value The value to set
        @raises ReadOnlyException If the parameter cannot be set
        """
        for v in self.values:
            if v[DataParticleKey.VALUE_ID] == id:
                v[DataParticleKey.VALUE_ID] = value
                return

        val_entry = {DataParticleKey.VALUE_ID: id, DataParticleKey.VALUE: value}
        self.values.append(val_entry)

    def get_value(self, id):
        """ Return a stored value

        @param id The ID (from DataParticleKey) for the parameter to return
        @raises NotImplementedException If there is an invalid id
        """
        if DataParticleKey.has(id):
            return self.contents[id]
        else:
            raise Exception("Value %s not available in particle!", id)


    def data_particle_type(self):
        """
        Return the data particle type (aka stream name)
        @raise: NotImplementedException if _data_particle_type is not set
        """
        if(self._data_particle_type is None):
            raise Exception("_data_particle_type not initialized")

        return self._data_particle_type

    def generate(self, sorted=False, encode=True):
        """
        Generates a JSON_parsed packet from a sample dictionary of sensor data and
        associates a timestamp with it

        @param portagent_time The timestamp from the instrument in NTP binary format
        @param data The actual data being sent in raw byte[] format
        @param sorted Returned sorted json dict, useful for testing
        @return A JSON_raw string, properly structured with port agent time stamp
           and driver timestamp
        @throws InstrumentDriverException If there is a problem with the inputs
        """
        # Do we wan't downstream processes to check this?
        #for time in [DataParticleKey.INTERNAL_TIMESTAMP,
        #             DataParticleKey.DRIVER_TIMESTAMP,
        #             DataParticleKey.PORT_TIMESTAMP]:
        #    if  not self._check_timestamp(self.contents[time]):
        #        raise SampleException("Invalid port agent timestamp in raw packet")

        # verify preferred timestamp exists in the structure...
        if not self._check_preferred_timestamps():
            raise Exception("Preferred timestamp not in particle!")

        # build response structure
        values = self._build_parsed_values()
        result = self._build_base_structure()
        result[DataParticleKey.STREAM_NAME] = self.stream_name or self.data_particle_type()
        result[DataParticleKey.VALUES] = values

        log.debug("Serialize result: %s", result)

        if not encode:
            return result

        # JSONify response, sorting is nice for testing
        # But sorting is awfully slow
        json_result = json.dumps(result, sort_keys=sorted)

        # return result
        return json_result

    def _build_parsed_values(self):
        """
        Build values of a parsed structure. Just the values are built so
        so that a child class can override this class, but call it with
        super() to get the base structure before modification

        @return the values tag for this data structure ready to JSONify
        @raises SampleException when parsed values can not be properly returned
        """
        return self.values


    def _build_base_structure(self):
        """
        Build the base/header information for an output structure.
        Follow on methods can then modify it by adding or editing values.

        @return A fresh copy of a core structure to be exported
        """
        result = dict(self.contents)
        # clean out optional fields that were missing
        if not self.contents[DataParticleKey.PORT_TIMESTAMP]:
            del result[DataParticleKey.PORT_TIMESTAMP]
        if not self.contents[DataParticleKey.INTERNAL_TIMESTAMP]:
            del result[DataParticleKey.INTERNAL_TIMESTAMP]
        return result

    def _check_timestamp(self, timestamp):
        """
        Check to make sure the timestamp is reasonable

        @param timestamp An NTP4 formatted timestamp (64bit)
        @return True if timestamp is okay or None, False otherwise
        """
        if timestamp == None:
            return True
        if not isinstance(timestamp, float):
            return False

        # is it sufficiently in the future to be unreasonable?
        if timestamp > ntplib.system_to_ntp_time(time.time()+(86400*365)):
            return False
        else:
            return True

    def _check_preferred_timestamps(self):
        """
        Check to make sure the preferred timestamp indicated in the
        particle is actually listed, possibly adjusting to 2nd best
        if not there.

        @throws SampleException When there is a problem with the preferred
            timestamp in the sample.
        """
        if self.contents[DataParticleKey.PREFERRED_TIMESTAMP] is None:
            raise Exception("Missing preferred timestamp, %s, in particle" %
                                  self.contents[DataParticleKey.PREFERRED_TIMESTAMP])

        # This should be handled downstream.  Don't want to not publish data because
        # the port agent stopped putting out timestamps
        #if self.contents[self.contents[DataParticleKey.PREFERRED_TIMESTAMP]] == None:
        #    raise SampleException("Preferred timestamp, %s, is not defined" %
        #                          self.contents[DataParticleKey.PREFERRED_TIMESTAMP])

        return True
