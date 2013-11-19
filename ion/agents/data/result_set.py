#!/usr/bin/env python
"""
@file ion/agents/data/result_set.py
@author Bill French
@brief Read a result set file and use the data to verify
data granules.

Usage:

rs = ResultSet(result_set_file_path)
if not rs.verify(granules):
    log.info("Granule verified")
else:
    log.error("Granule validate failed")
    log.error(rs.report())

Result Set File Format:
  result files are yml formatted files with a header and data section.
  the data is stored in record elements with the key being the parameter name.
     - two special fields are internal_timestamp and _index.

eg.

# Result data for verifying granules. Comments are ignored.

header:
  stream_name: ctdpf_parsed

data:
  -  _index: 1
     _new_sequence: True
     internal_timestamp: 07/26/2013 21:01:03
     temperature: 4.1870
     conductivity: 10.5914
     pressure: 161.06
     oxygen: 2693.0
  -  _index: 2
     internal_timestamp: 07/26/2013 21:01:04
     temperature: 4.1872
     conductivity: 10.5414
     pressure: 161.16
     oxygen: 2693.1

New sequence flag indicates that we are at the beginning of a new sequence of
contiguous records.
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import re
import yaml
import ntplib
from dateutil import parser
import pprint

from pyon.public import log
from ion.services.dm.utility.granule_utils import RecordDictionaryTool

DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$'
DATE_MATCHER = re.compile(DATE_PATTERN)

class ResultSet(object):
    """
    Result Set object
    Read result set files and compare to parsed granules.
    """
    def __init__(self, result_file_path):
        self.yaml = dict()

        log.debug("read result file: %s" % result_file_path)
        stream = file(result_file_path, 'r')
        result_set = yaml.load(stream)

        self._set_result_set(result_set)

        self._clear_report()

    def verify(self, granules):
        """
        Verify granules passed in against result set read
        in the ctor.

        Ensure:
          - Verify granules as a set
          - Verify individual granule data

        store verification result in the object and
        return success or failure.
        @param particls: list of granules to verify.
        @return True if verification successful, False otherwise
        """
        self._clear_report()
        result = True

        granule_data = self._extract_granule_data(granules)

        if self._verify_set(granule_data):
            result = self._verify_granules(granule_data)
        else:
            result = False

        if not result:
            log.error("Failed verification: \n%s", self.report())

        return result

    def report(self):
        """
        Return an ascii formatted verification failure report.
        @return string report
        """
        if len(self._report):
            return "\n".join(self._report)
        else:
            return None

    ###
    #   Helpers
    ###
    def _add_to_report(self, messages, indent = 0):
        """
        Add a message to the report buffer, pass an indent factor to
        indent message in the ascii report.
        """
        if not isinstance(messages, list): messages = [messages]

        for message in messages:
            ind = ""
            for i in range(0, indent):
                ind += "    "
            self._report.append("%s%s" %(ind, message))
            log.warn(message)

    def _clear_report(self):
        """
        Add a message to the report buffer, pass an indent factor to
        indent message in the ascii report.
        """
        self._report = []

    def _set_result_set(self, result_set):
        """
        Take data from yaml file and store it in internal objects for
        verifying data.  Raise an exception on error.
        """
        log.trace("Parsing result set header: %s", result_set)

        self._result_set_header = result_set.get("header")
        if not self._result_set_header: raise IOError("Missing result set header")
        log.trace("Header: %s", self._result_set_header)

        if self._result_set_header.get("stream)name") is None:
            IOError("header.stream_name not defined")

        self._result_set_data = {}
        data = result_set.get("data")
        if not data: raise IOError("Missing result set data")

        for granule in data:
            index = granule.get("_index")
            if index is None:
                log.error("Granule definition missing _index: %s", granule)
                raise IOError("Granule definition missing _index")

            if self._result_set_data.get(index) is not None:
                log.error("Duplicate granule definition for _index %s: %s", index, granule)
                raise IOError("Duplicate definition found for index: %s"% index)

            self._result_set_data[index] = granule
            log.trace("Result set data: %s", self._result_set_data)

    def _verify_set(self, granules):
        """
        Verify the granules as a set match what we expect.
        - All granules are of the expected type
        - Check granule count
        - Connection IDs change as expected
        """
        errors = []

        if len(self._result_set_data) != len(granules):
            errors.append("result set records != granules to verify (%d != %d)" %
                          (len(self._result_set_data), len(granules)))

        for granule in granules:
            if not self._verify_granule_type(granule):
                log.error("granule type mismatch: %s", granule)
                errors.append('granule type mismatch')

        if len(errors):
            self._add_to_report("Header verification failure")
            self._add_to_report(errors, 1)
            return False

        return True

    def _verify_granules(self, granules):
        """
        Verify data in the granules individually.
        - Verify order based on _index
        - Verify parameter data values
        - Verify there are extra or missing parameters
        """
        result = True
        index = 1
        last_connection_id = None

        for granule in granules:
            granule_def = self._result_set_data.get(index)
            errors = []

            # No granule definition, we fail
            if granule_def is None:
                errors.append("no granule result defined for index %d" % index)

            # Otherwise lets do some validation
            else:
                errors += self._get_granule_header_errors(granule, granule_def)
                errors += self._get_granule_data_errors(granule, granule_def, last_connection_id)
                last_connection_id = granule['connection_id']

            if len(errors):
                self._add_to_report("Failed granule validation for index %d" % index)
                self._add_to_report(errors, 1)
                result = False

            index += 1

        return result

    def _verify_granule_type(self, granule):
        """
        Verify that the object is a DataGranule and is the
        correct type.
        """
        if isinstance(granule, dict):
            return True

        else:
            log.error("granule is not a dict")
            return False

    def _get_granule_header_errors(self, granule, granule_def):
        """
        Verify all parameters defined in the header:
        - Stream type
        - Internal timestamp
        """
        errors = []
        granule_dict = self._granule_as_dict(granule)
        granule_timestamp = granule_dict.get('internal_timestamp')
        expected_time = granule_def.get('internal_timestamp')

        # Verify the timestamp
        if granule_timestamp and not expected_time:
            errors.append("granule_timestamp defined in granule, but not expected")
        elif not granule_timestamp and expected_time:
            errors.append("granule_timestamp expected, but not defined in granule")

        # If we have a timestamp AND expect one then compare values
        elif (granule_timestamp and
              granule_timestamp != self._string_to_ntp_date_time(expected_time)):
            errors.append("expected internal_timestamp mismatch, %f != %f (%f)" %
                (self._string_to_ntp_date_time(expected_time), granule_timestamp,
                 self._string_to_ntp_date_time(expected_time)- granule_timestamp))

        # verify the stream name
        granule_stream = granule_dict.get('stream_name')
        if granule_stream:
            expected_stream =  self._result_set_header['granule_type']
            if granule_stream != expected_stream:
                errors.append("expected stream name mismatch: %s != %s" %
                              (expected_stream, granule_stream))
        else:
            log.warn("No stream defined in granule.  Not verifying stream name.")

        return errors

    def _get_granule_data_errors(self, granule, granule_def, last_connection_id):
        """
        Verify that all data parameters are present and have the
        expected value
        """
        errors = []
        log.debug("Granule to test: %s", granule)
        log.debug("Granule definition: %s", granule_def)

        expected_keys = []
        for (key, value) in granule_def.items():
            if(key not in ['_index', '_new_sequence', 'internal_timestamp']):
                expected_keys.append(key)

        log.debug("Expected keys: %s", sorted(expected_keys))
        log.debug("Granule keys: %s", sorted(granule.keys()))

        for key in expected_keys:
            expected_value = granule_def[key]
            granule_value = None

            try:
               granule_value = granule[key]
            except:
                errors.append("Missing granule parameter. %s" % key)

            e = self._verify_value(expected_value, granule_value)
            if e:
                errors.append("'%s' %s"  % (key, e))

        # Verify connection ID has or has not changed.  Ignore the first record
        current_id = granule['connection_id']
        new_session = granule_def.get('_new_sequence', False)
        if last_connection_id:
            if current_id is None:
                errors.append("connection id missing")

            # We expect a new id, but didn't get one.
            elif new_session and current_id == last_connection_id:
                errors.append("Expected a new connection id, but still have old id")

            # We don't expect a new id, but get one.
            elif not new_session and current_id != last_connection_id:
                errors.append("New connection id detected, but didn't expect one")

        return errors

    def _verify_value(self, expected_value, granule_value):
        """
        Verify a value matches what we expect.  If the expected value (from the yaml)
        is a dict then we expect the value to be in a 'value' field.  Otherwise just
        use the parameter as a raw value.

        when passing a dict you can specify a 'round' factor.
        """
        if isinstance(expected_value, dict):
            ex_value = expected_value['value']
            round_factor = expected_value.get('round')
        else:
            ex_value = expected_value
            round_factor = None

        if ex_value is None:
            log.debug("No value to compare, ignoring")
            return None

        log.debug("Test %s == %s", ex_value, granule_value)

        if round_factor is not None and granule_value is not None:
            granule_value = round(granule_value, round_factor)
            log.debug("rounded value to %s", granule_value)

        if ex_value != granule_value:
            return "value mismatch, %s != %s (decimals may be rounded)" % (ex_value, granule_value)

        return None

    def _string_to_ntp_date_time(self, datestr):
        """
        Extract a date tuple from a formatted date string.
        @param str a string containing date information
        @retval a date tuple.
        @throws InstrumentParameterException if datestr cannot be formatted to
        a date.
        """
        if not isinstance(datestr, str):
            raise IOError('Value %s is not a string.' % str(datestr))
        try:
            localtime_offset = self._parse_time("1970-01-01T00:00:00.00")
            converted_time = self._parse_time(datestr)
            adjusted_time = converted_time - localtime_offset
            timestamp = ntplib.system_to_ntp_time(adjusted_time)

        except ValueError as e:
            raise ValueError('Value %s could not be formatted to a date. %s' % (str(datestr), e))

        log.debug("converting time string '%s', unix_ts: %s ntp: %s", datestr, adjusted_time, timestamp)

        return timestamp

    def _parse_time(self, datestr):
        if not DATE_MATCHER.match(datestr):
            raise ValueError("date string not in ISO8601 format YYYY-MM-DDTHH:MM:SS.SSSSZ")
        else:
            log.debug("Match: %s", datestr)

        if datestr[-1:] != 'Z':
            datestr += 'Z'

        log.debug("Converting time string: %s", datestr)
        dt = parser.parse(datestr)
        elapse = float(dt.strftime("%s.%f"))
        return elapse

    def _granule_as_dict(self, granule):
        if isinstance(granule, dict):
            return granule

        raise IOError("Granule must be a dict.")


    def _extract_granule_data(self, granules):
        """
        Pull all data out of all granules and return a dict of values
        """
        result = []
        for granule in granules:
            group = []
            log.debug("Granule: %s", granule)
            rdt = RecordDictionaryTool.load_from_granule(granule)

            # Store the data from each record
            for key, value in rdt.iteritems():
                for i in range(0, len(value)):
                    if len(group) <= i:
                        group.append({})
                    group[i][key] = value[i]

                    # Store the connection information for each record
                    if not 'connection_index' in group[i]:
                        group[i]['connection_index'] = granule.connection_index

                    if not 'connection_id' in group[i]:
                        group[i]['connection_id'] = granule.connection_id



            result += group

        log.debug("extracted granules: %s", pprint.pformat(result))

        return result
