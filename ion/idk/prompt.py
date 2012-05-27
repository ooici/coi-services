#!/usr/bin/env python

"""
@file coi-services/ion/idk/prompt.py
@author Bill French
@brief Collection of functions for prompting user input from the console.
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import sys


def text(label, default=None):
    """
    @brief prompt and read a single line of input from a user
    @retval string input from user
    """
    display = label
    if( default ):
        display += " (default: " + str(default) + ")"

    input = raw_input( display + ": " )

    if input == '' and default:
        input = default

    if input == '':
        print "%s is a required field" % label
        return text(label, default)

    return input


def multiline(label, default):
    """
    @brief prompt and read multi-line text from a user.  User types 'done' to end input.  I'm sure there is a better
        way to do this
    @retval multiline string input from user
    """
    if( default ):
        print label + " is currently:\n" + default

    user_input = []
    entry = raw_input( label + " (type 'done' on its own line end input. enter to keep current): \n" )

    if entry:
        while entry != 'done':
            user_input.append(entry)
            entry = raw_input("")

    user_input = "\n" . join(user_input)

    if user_input == '' and default:
        user_input = default

    if user_input == '':
        print "%s is required" % label
        return multiline(label, default)

    return user_input


def yes_no(prompt):
    """
    @brief prompt and read a yes or no answer from the user
    @retval True if yes False if no
    """
    answer = raw_input( prompt + ": " )
    answer = answer.lower();

    if( answer == 'y' or answer == 'yes' ):
        return True

    elif( answer == 'n' or answer == 'no'):
        return False

    else:
        print( "Invalid response." )
        return promptYesNo( prompt )


