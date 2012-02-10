#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_bars.bars
@file ion/services/mi/drivers/uw_bars/bars.py
@author Carlos Rueda
@brief UW TRHPH BARS definitions and misc utilities
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import re


###########################################################################
#
# Menus, typical responses, and associated regexs and utilities.
#
# Leading blank lines in the actual responses are ignored in these strings.
# The lines here are '\n'-terminated while the instrument actually uses '\r\n.'
#

MAIN_MENU = """
      ***************************************************************
      *                                                             *
      *            Welcome to the BARS Program Main Menu            *
      *              (Benthic And Resistivity Sensors)              *
      *                    (Serial Number 002)                      *
      *                                                             *
      ***************************************************************

                 Version 1.7 - Last Revision: July 11, 2011

                               Written by:

                               Rex Johnson
                               Engineering Services
                               School of Oceanography
                               University of Washington
                               Seattle, WA 98195


                  The System Clock has not been set.
                    Use option 4 to Set the Clock.

                  Select one of the following functions:

                      0).  Reprint Time & this Menu.
                      1).  Restart Data Collection.
                      2).  Change Data Collection Parameters.
                      3).  System Diagnostics.
                      4).  Set the System Clock.
                      5).  Control Power to Sensors.
                      6).  Provide Information on this System.
                      7).  Exit this Program.

                    Enter 0, 1, 2, 3, 4, 5, 6 or 7 here  --> """

#----------------------------------------------
# 0).  Reprint Time & this Menu.
# Also \r can be entered here to re-generate the main menu.

#----------------------------------------------
# 1).  Restart Data Collection.
# A typical line when data streaming is resumed:
# 0.000  0.000  0.000  4.095  4.095  4.095  2.007  1.165  20.98  0.008   22.9  9.2

#----------------------------------------------
# 2).  Change Data Collection Parameters.
SYSTEM_PARAMETER_MENU = """
                               System Parameter Menu

*****************************************************************************

                       The present value for the Cycle Time is
                                 20 Seconds.

                  The present setting for Verbose versus Data only is
                                     Data Only.

*****************************************************************************


                Select one of the following functions:

                      0).  Reprint this Menu.
                      1).  Change the Cycle Time.
                      2).  Change the Verbose Setting.
                      3).  Return to the Main Menu.

                    Enter 0, 1, 2, or 3 here  --> """


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


def get_verbose_vs_data_only(string):
    """
    Extract the verbose vs. data only mode text from the given string. None
    if not found.
    """
    return _search_in_pattern(string, VERBOSE_VS_DATA_ONLY_PATTERN, 1)


#----------------------------------------------
# 3).  System Diagnostics.

HOW_MANY_SCANS_FOR_SYSTEM_DIAGNOSTICS = """
             This Test Routine Prints out the actual Analog Voltages
                          from all the Analog Inputs.
                   This should be helpful for system testing.


                       How Many Scans do you want? --> """

# sample of response:
SYSTEM_DIAGNOSTICS_RESPONSE = """
-Res/5-  -ResX1-  -ResX5-  -*H2/5-  -*H2X1-  -*H2X5-  -*Eh**- RefTemp  ResTemp  -VBatt-
0.000    0.000    0.000    4.096    4.096    4.096    2.007    1.151    0.011    9.292
0.000    0.000    0.000    4.096    4.096    4.096    2.007    1.154    0.010    9.242
0.000    0.000    0.000    4.096    4.096    4.096    2.008    1.152    0.006    9.242
0.000    0.000    0.000    4.096    4.096    4.096    2.010    1.157    0.010    9.252
Press Enter to return to Main Menu."""




#----------------------------------------------
# 4).  Set the System Clock.

ADJUST_SYSTEM_CLOCK_MENU = """
             This routine is to set or adjust the System Clock.

                  The Current System Date = 02/09/12
                  The Current System Time = 14:21:06
     Do you want to Change the Current Time? (0 = No, 1 = Yes) --> """

# also \r can be entered to return to main menu.

# To extract date and time from adjust system clock menu:
CURRENT_SYSTEM_DATE_AND_TIME_PATTERN = re.compile(
        r'The Current System Date =\s*([^\s]*)\s+' +
         'The Current System Time =\s*([^\s]*)')

def get_system_date_and_time(string):
    """
    Returns (date,time), the system date and time from the given string.
    None if not found.
    """
    return _search_in_pattern(string, CURRENT_SYSTEM_DATE_AND_TIME_PATTERN,
                              1, 2)

# prompts to set system clock:
ENTER_MONTH = """Enter the Month (1-12)     : """
ENTER_DAY = """Enter the Day (1-31)       : """
ENTER_YEAR = """Enter the Year (Two Digits): """
ENTER_HOUR = """Enter the Hour (0-23)      : """
ENTER_MINUTE = """Enter the Minute (0-59)    : """
ENTER_SECOND = """Enter the Second (0-59)    : """



#----------------------------------------------
# 5).  Control Power to Sensors.

SENSOR_POWER_CONTROL_MENU = """
                             Sensor Power Control Menu

*****************************************************************************

                 Here is the current status of power to each sensor

                      Res Sensor Power is ............. On
                      Instrumentation Amp Power is .... On
                      eH Isolation Amp Power is ....... On
                      Hydrogen Power .................. On
                      Reference Temperature Power ..... On

*****************************************************************************


                Select one of the following functions:

                      0).  Reprint this Menu.
                      1).  Toggle Power to Res Sensor.
                      2).  Toggle Power to the Instrumentation Amp.
                      3).  Toggle Power to the eH Isolation Amp.
                      4).  Toggle Power to the Hydrogen Sensor.
                      5).  Toggle Power to the Reference Temperature Sensor.
                      6).  Return to the Main Menu.

                    Enter 0, 1, 2, 3, 4, 5, or 6 here  --> """

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

SYSTEM_INFO = """
  System Name: BARS (Benthic And Resistivity Sensors)
  System Owner: Marv Lilley, University of Washington
  Owner Contact Phone #: 206-543-0859
  System Serial #: 002

  Press Enter to return to the Main Menu. --> """
