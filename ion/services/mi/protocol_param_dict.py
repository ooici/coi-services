#!/usr/bin/env python

"""
@package 
@file 
@author 
@brief 
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'


class ParameterDictVal(object):
    """
    """
    def __init__(self, name, pattern, f_getval, f_format, value=None):
        """
        """
        self.name = name
        self.pattern = pattern
        self.regex = re.compile(pattern)
        self.f_getval = f_getval
        self.f_format = f_format
        self.value = value

    def update(self, input):
        """
        """
        match = self.regex.match(input)
        if match:
            self.value = self.f_getval(match)
            mi_logger.debug('Updated parameter %s=%s', self.name, str(self.value))
            return True
        else: return False


class ProtocolParameterDict(object):
    """
    """
    def __init__(self):
        """
        """
        self._param_dict= {}
        
    def add(self, name, pattern, f_getval, f_format, value=None):
        """
        """
        val = ParameterDictVal(name, pattern, f_getval, f_format, value)
        self._param_dict[name] = val
        
    def get(self, name):
        """
        """
        return self._param_dict[name].value
        
    def set(self, name, value):
        """
        """
        self._param_dict[name] = value
        
    def update(self, input):
        """
        """
        for (name, val) in self._param_dict.iteritems():
            if val.update(input):
                break
    
    def get_config(self):
        """
        """
        config = {}
        for (key, val) in self._param_dict.iteritems():
            config[key] = val.value
        return config
    
    def format(self, name, val):
        """
        """
        return self._param_dict[name].f_format(val)
        
    def get_keys(self):
        """
        """
        return self._param_dict.keys()

        
