#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.trhph
@file ion/services/mi/drivers/uw_trhph/trhph.py
@author Carlos Rueda
@brief UW TRHPH definitions and misc utilities
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import re

NEWLINE = '\r\n'


def _newline(string):
    return string.replace('\n', NEWLINE)


###########################################################################
#
# Menus, typical responses, and associated regexs and utilities.
#
# Leading blank lines in the actual responses are ignored in these strings.
#

MAIN_MENU_FORMAT = _newline("""
      *********************************************************************
      *                                                                   *
      *  Welcome to the Consortium for Ocean Leadership Program Main Menu *
      *              (TRHPH - Temperature Resistivity Probe)              *
      *                      (Serial Number 001)                          *
      *                                                                   *
      *********************************************************************

                 Version 1.2 - Last Revision: Mar. 27, 2012

                               Written by:

                               Rex Johnson
                               Ocean Engineering Services
                               School of Oceanography
                               University of Washington
                               Seattle, WA 98195


                  Select one of the following functions:

                      0).  Reprint this Menu.
                      1).  Restart Data Collection.
                      2).  Change Data Collection Parameters.
                      3).  System Diagnostics.
                      4).  Control Power to Sensors.
                      5).  Provide Information on this System.
                      6).  Exit this Program.

                    Enter 0, 1, 2, 3, 4, 5, or 6 here  --> """)

MAIN_MENU = MAIN_MENU_FORMAT

#----------------------------------------------
# 0).  Reprint Time & this Menu.
# Also \r can be entered here to re-generate the main menu.

#----------------------------------------------
# 1).  Restart Data Collection.
# A typical line when data streaming is resumed:
# 0.197  0.992  4.095  0.135  0.687  3.441  1.990  1.038  24.83  0.000   24.8   9.1

# the names of the corresponding channels are (in order):
RESISTIVITY_5 = "Resistivity/5"
RESISTIVITY_X1 = "Resistivity X1"
RESISTIVITY_X5 = "Resistivity X5"
HYDROGEN_5 = "Hydrogen/5"
HYDROGEN_X1 = "Hydrogen X1"
HYDROGEN_X5 = "Hydrogen X5"
EH_SENSOR = "Eh Sensor"
REFERENCE_TEMP_VOLTS = "Reference Temp Volts"
REFERENCE_TEMP_DEG_C = "Reference Temp Deg C"
RESISTIVITY_TEMP_VOLTS = "Resistivity Temp Volts"
RESISTIVITY_TEMP_DEG_C = "Resistivity Temp Deg C"
BATTERY_VOLTAGE = "Battery Voltage"

# all the above in an array to preserve the order they are reported.
CHANNEL_NAMES = [
        RESISTIVITY_5,
        RESISTIVITY_X1,
        RESISTIVITY_X5,
        HYDROGEN_5,
        HYDROGEN_X1,
        HYDROGEN_X5,
        EH_SENSOR,
        REFERENCE_TEMP_VOLTS,
        REFERENCE_TEMP_DEG_C,
        RESISTIVITY_TEMP_VOLTS,
        RESISTIVITY_TEMP_DEG_C,
        BATTERY_VOLTAGE]
"""
The names of the channels contained in a single data sample.
"""


#----------------------------------------------
# 2).  Change Data Collection Parameters.

DATA_ONLY = "Data Only"

VERBOSE = "Verbose Printing of Status Information with the Data"

PRINT_METADATA_POWER_UP_ENABLED = "enabled"
PRINT_METADATA_POWER_UP_DISNALED = "inhibited"

PRINT_METADATA_RESTART_ENABLED = "enabled"
PRINT_METADATA_RESTART_DISABLED = "inhibited"

SYSTEM_PARAMETER_MENU_FORMAT = _newline("""
                               System Parameter Menu

*****************************************************************************

                       The present value for the Cycle Time is
                                 %s %s.

                  The present setting for Verbose versus Data only is
                                     %s.

                  Printing of the Metadata Printing on Power up is %s.

                  Printing of the Metadata Printing on Restart of Data is %s.

*****************************************************************************


                Select one of the following functions:

                      0).  Reprint this Menu.
                      1).  Change the Cycle Time.
                      2).  Change the Verbose Setting.
                      3).  Change Print Status of Metadata on Powerup.
                      4).  Change Print Status of Metadata on Restart of Data.
                      5).  Reset all Parameters back to Default Settings.
                      6).  Print out all Parameters.
                 7 to 9).  Return to the Main Menu.

                    Enter 0 through 9 here  --> """)

SYSTEM_PARAMETER_MENU = SYSTEM_PARAMETER_MENU_FORMAT % (
        "20", "Seconds", DATA_ONLY,
        PRINT_METADATA_POWER_UP_ENABLED,
        PRINT_METADATA_RESTART_ENABLED)

# to extract cycle time from system parameter menu:
CYCLE_TIME_PATTERN = re.compile(
        r'present value for the Cycle Time is\s+([^.]*)\.')

# to extract "Verbose versus Data only" from system parameter menu:
VERBOSE_VS_DATA_ONLY_PATTERN = re.compile(
        r'present setting for Verbose versus Data only is\s+([^.]*)\.')


def _search_in_pattern(string, pattern, *groups):
    """
    Extracts the groups corresponding to the given pattern in the string.
    Return None if pattern not found.
    """
    mo = re.search(pattern, string)
    return None if mo is None else mo.group(*groups)


def get_cycle_time(string):
    """
    Extract the cycle time text from the given string. None if not found.
    """
    return _search_in_pattern(string, CYCLE_TIME_PATTERN, 1)


def get_cycle_time_seconds(string):
    """
    Parses the string to get the corresponding number of seconds, for example,
    if string="20 Seconds" then returns 20,
    if string="2 Minutes" then returns 120.
    @param string the text presumably obtained by get_cycle_time.
    @return the number of seconds. None if the string cannot be parsed.
    """
    mo = re.match(r"(\d+)\s+(Seconds|Minutes)", string)
    if mo is not None:
        value = int(mo.group(1))
        units = mo.group(2)
        if units == "Seconds":
            seconds = value
        else:
            seconds = 60 * value
        return seconds
    else:
        return None


def get_verbose_vs_data_only(string):
    """
    Extract the verbose vs. data only mode text from the given string. None
    if not found.
    """
    return _search_in_pattern(string, VERBOSE_VS_DATA_ONLY_PATTERN, 1)


CHANGE_CYCLE_TIME = _newline("""
                    Do you want to specify a Cycle Time in
                           in Seconds or Minutes?
                    Enter 1 for Seconds, 2 for Minutes --> """)

# if 0 is entered:
CHANGE_CYCLE_TIME_IN_SECONDS = "Enter a new value between 15 and 59 here --> "

# TODO determine prompt when 1 is entered
CHANGE_CYCLE_TIME_IN_MINUTES = "Enter a new value between _ and _ here --> "

CHANGE_VERBOSE_MODE = _newline("""
 Do you want Verbose Status Information during Logging
               or just the Data?
    Enter 1 for Verbose, 0 for just Data. --> """)

#----------------------------------------------
# 3).  System Diagnostics.

HOW_MANY_SCANS_FOR_SYSTEM_DIAGNOSTICS = _newline("""
             This Test Routine Prints out the actual Analog Voltages
                          from all the Analog Inputs.
                   This should be helpful for system testing.


                       How Many Scans do you want? --> """)

SYSTEM_DIAGNOSTICS_HEADER = "-Res/5-  -ResX1-  -ResX5-  -*H2/5-  -*H2X1-  -*H2X5-  -*Eh**- RefTemp  ResTemp  -VBatt-"
SYSTEM_DIAGNOSTICS_RETURN_PROMPT = "Press Enter to return to Main Menu."

# sample of response:
SYSTEM_DIAGNOSTICS_RESPONSE = _newline("""
%s
0.000    0.000    0.000    4.096    4.096    4.096    2.007    1.151    0.011    9.292
0.000    0.000    0.000    4.096    4.096    4.096    2.007    1.154    0.010    9.242
0.000    0.000    0.000    4.096    4.096    4.096    2.008    1.152    0.006    9.242
0.000    0.000    0.000    4.096    4.096    4.096    2.010    1.157    0.010    9.252
%s""" % (SYSTEM_DIAGNOSTICS_HEADER, SYSTEM_DIAGNOSTICS_RETURN_PROMPT))


#----------------------------------------------
# 5).  Control Power to Sensors.

SENSOR_POWER_CONTROL_MENU_FORMAT = _newline("""
                             Sensor Power Control Menu

*****************************************************************************

                 Here is the current status of power to each sensor

                      Res Sensor Power is ............. %s
                      Instrumentation Amp Power is .... %s
                      eH Isolation Amp Power is ....... %s
                      Hydrogen Power .................. %s
                      Reference Temperature Power ..... %s

*****************************************************************************


                Select one of the following functions:

                      0).  Reprint this Menu.
                      1).  Toggle Power to Res Sensor.
                      2).  Toggle Power to the Instrumentation Amp.
                      3).  Toggle Power to the eH Isolation Amp.
                      4).  Toggle Power to the Hydrogen Sensor.
                      5).  Toggle Power to the Reference Temperature Sensor.
                 6 to 9).  Return to the Main Menu.

                    Enter 0 through 9 here  --> """)

SENSOR_POWER_CONTROL_MENU = SENSOR_POWER_CONTROL_MENU_FORMAT % (
    'On', 'On', 'On', 'On', 'On')


# To extract power statuses from sensor power control menu
CURRENT_POWER_STATUS_PATTERN = re.compile(
        r'Res Sensor Power is\s*\.*\s*([^\s]*)\s+' +
        r'Instrumentation Amp Power is\s*\.*\s*([^\s]*)\s+' +
        r'eH Isolation Amp Power is\s*\.*\s*([^\s]*)\s+' +
        r'Hydrogen Power\s*\.*\s*([^\s]*)\s+' +
        r'Reference Temperature Power\s*\.*\s*([^\s]*)\s+')


def get_power_statuses(string):
    """
    Returns (rs, ia, eh, hy, rt), the current power status of:
    - Res Sensor Power
    - Instrumentation Amp Power
    - eH Isolation Amp Power
    - Hydrogen Power
    - Reference Temperature Power.
    None if not found.
    """
    return _search_in_pattern(string, CURRENT_POWER_STATUS_PATTERN,
                              1, 2, 3, 4, 5)

#----------------------------------------------
# 6).  Provide Information on this System.

SYSTEM_INFO_FORMAT = _newline("""
  System Name: %s
  System Owner: %s
  Contact Info: %s
  System Serial #: %s

  Press Enter to return to the Main Menu. --> """)

SYSTEM_INFO = SYSTEM_INFO_FORMAT % (
  "Temperature Resistivity Probe - TRHPH",
  "Consortium for Ocean Leadership",
  "Giora Proskurowski, 206-685-3507",
  "001")

SYSTEM_INFO_PATTERN = re.compile(
  r'System Name:\s*([^\r\n]*)\s+' +
  r'System Owner:\s*([^\r\n]*)\s+' +
  r'Contact Info:\s*([^\r\n]*)\s+' +
  r'System Serial #:\s*([^\r\n]*)\s+')


def get_system_info_metadata(string):
    """
    Returns (sn, so, ph, ss), the system info metadata:
    - system name
    - system owner
    - owner contact phone
    - system serial number
    None if not found.
    """
    return _search_in_pattern(string, SYSTEM_INFO_PATTERN,
                              1, 2, 3, 4)
