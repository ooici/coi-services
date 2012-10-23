#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/util/parameter_loader.py
@date Tue Oct 23 16:12:58 EDT 2012
@brief Utility for loading parameter definitions
'''

from coverage_model.parameter import ParameterContext
from ooi.logging import log
import sys
import imp
import pkgutil
import requests
import gevent

class ParameterLoader(object):
    @classmethod 
    def build_contexts(cls, definitions_path):
        '''
        Builds a set of parameter definitions by loading the parameter definition plugins in definitions_path
        '''
        contexts = __import__(definitions_path)

        package = contexts
        contexts = {}
        for importer, modname, ispkg in pkgutil.walk_packages(package.__path__, package.__name__ + '.'):
            module = __import__(modname, fromlist='dummy')
            if not ispkg:
                cls.load_module(module, contexts)

        return contexts
    @classmethod
    def load_module(cls,module,contexts):
        '''
        Load parameter definition plugin using a module
        '''
        if hasattr(module,'load'):
            print 'Loading %s' % module.__name__
            ctxt_list = module.load()
            if isinstance(ctxt_list, list):
                ctxt_list = [i for i in ctxt_list if isinstance(i, ParameterContext)]
                for ctxt in ctxt_list:
                    if ctxt.name in contexts:
                        log.warn('Duplicate context %s found in %s',(ctxt.name, module.__name__))
                    contexts[ctxt.name] = ctxt

        else:
            print 'Module %s could not be loaded' % module.__name__

    @classmethod
    def load_url(cls, url, contexts,timeout=5):
        '''
        Load a parameter definition plugin using a URL
        '''

        # Use gevent instaead of requests socket based timeout.
        with gevent.timeout.Timeout(timeout):
            r = requests.get(url)
        if r.status_code != 200:
            log.error('Failed to load module from %s', url)
            return []
        code = r.text
        if 'context_url' in sys.modules: sys.modules.pop('context_url')
        mixin_module = imp.new_module('context_url')
        exec code in mixin_module.__dict__
        return cls.load_module(mixin_module, contexts)
        


