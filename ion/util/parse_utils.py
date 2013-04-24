#!/usr/bin/env python

"""Common utilities to parse external files, e.g. for preload"""

__author__ = 'Michael Meisinger, Ian Katz'

import ast

from pyon.public import iex, IonObject, log

from interface import objects


def get_typed_value(value, schema_entry=None, targettype=None):
    """
    Performs a value type conversion according to a schema specified target type.
    """
    targettype = targettype or schema_entry["type"]
    if schema_entry and 'enum_type' in schema_entry:
        enum_clzz = getattr(objects, schema_entry['enum_type'])
        return enum_clzz._value_map[value]
    elif targettype == 'str':
        return str(value)
    elif targettype == 'bool':
        if value in ('TRUE', 'True', 'true', '1', 1, True):
            return True
        if value in ('FALSE', 'False', 'false', '0', 0, '', None, False):
            return False
        raise iex.BadRequest("Value %s is no bool" % value)
    elif targettype == 'int':
        try:
            return int(value)
        except Exception:
            log.warn("Value %s is type %s not type %s" % (value, type(value), targettype))
            return ast.literal_eval(value)
    elif targettype == 'float':
        try:
            return float(value)
        except Exception:
            log.warn("Value %s is type %s not type %s" % (value, type(value), targettype))
            return ast.literal_eval(value)
    elif targettype == 'simplelist':
        if value.startswith('[') and value.endswith(']'):
            value = value[1:len(value)-1].strip()
        elif not value.strip():
            return []
        return list(value.split(','))
    else:
        log.trace('parsing value as %s: %s', targettype, value)
        return ast.literal_eval(value)

def parse_dict(text):
    """
    parse a "simple" dictionary of unquoted string keys and values. -- no nested values, no complex characters

    But is it really simple?  Not quite.  The following substitutions are made:

    keys with dots ('.') will be split into dictionaries.
    booleans "True", "False" will be parsed
    numbers will be parsed as floats unless they begin with "0" or include one "." and end with "0"
    "{}" will be converted to {}
    "[]" will be converted to [] (that's "[ ]" with no space)

    For example, an entry in preload would be this:

    PARAMETERS.TXWAVESTATS: False,
    PARAMETERS.TXREALTIME: True,
    PARAMETERS.TXWAVEBURST: false,
    SCHEDULER.ACQUIRE_STATUS: {},
    SCHEDULER.CLOCK_SYNC: 48.2
    SCHEDULER.VERSION.number: 3.0

    which would translate back to
    { "PARAMETERS": { "TXWAVESTATS": False, "TXREALTIME": True, "TXWAVEBURST": "false" },
      "SCHEDULER": { "ACQUIRE_STATUS": { }, "CLOCK_SYNC", 48.2, "VERSION": {"number": "3.0"}}
    }

    """

    substitutions = {"{}": {}, "[]": [], "True": True, "False": False}

    def parse_value(some_val):
        if some_val in substitutions:
            return substitutions[some_val]

        try:
            int_val = int(some_val)
            if str(int_val) == some_val:
                return int_val
        except ValueError:
            pass

        try:
            float_val = float(some_val)
            if str(float_val) == some_val:
                return float_val
        except ValueError:
            pass

        return some_val


    def chomp_key_list(out_dict, keys, value):
        """
        turn keys like ['a', 'b', 'c', 'd'] and a value into
        out_dict['a']['b']['c']['d'] = value
        """
        dict_ptr = out_dict
        last_ptr = out_dict
        for i, key in enumerate(keys):
            last_ptr = dict_ptr
            if not key in dict_ptr:
                dict_ptr[key] = {}
            else:
                if type(dict_ptr[key]) != type({}):
                    raise iex.BadRequest("Building a dict in %s field, but it exists as %s already" %
                                         (key, type(dict_ptr[key])))
            dict_ptr = dict_ptr[key]
        last_ptr[keys[-1]] = value


    out = { }
    pairs = text.split(',') # pairs separated by commas
    for pair in pairs:
        if 0 == pair.count(':'):
            continue
        fields = pair.split(':') # pair separated by colon
        key = fields[0].strip()
        value = fields[1].strip()

        keyparts = key.split(".")
        chomp_key_list(out, keyparts, parse_value(value))


    return out


def parse_phones(text):
    if ':' in text:
        out = []
        for type,number in parse_dict(text).iteritems():
            out.append(IonObject("Phone", phone_number=number, phone_type=type))
        return out
    elif text:
        return [ IonObject("Phone", phone_number=text.strip(), phone_type='office') ]
    else:
        return []
