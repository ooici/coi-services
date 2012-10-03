#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Wed Oct  3 09:04:12 EDT 2012
@file ion/processes/bootstrap/plugins/bootstrap_parameter_definitions.py
@description Bootstrapping for parameter definitions.
'''

from ion.core.bootstrap_process import BootstrapPlugin
from interface.services.dm.idataset_management_service import DatasetManagementServiceProcessClient
from ion.util.parameter_yaml_IO import build_contexts
from pyon.util.log import log

import yaml

class BootstrapParameterDefinitions(BootstrapPlugin):
    '''
    Bootstrap plugin for preloading standard parameter definitions
    '''
    def on_initial_bootstrap(self, process, config, **kwargs):
        self.dataset_management = DatasetManagementServiceProcessClient(process=process)
        self.dict_defs = config.get_safe('process.bootstrap.dict_defs','res/config/param_dict_defs.yml')

        contexts = self.load_contexts()
        self.load_dictionaries(self.dict_defs, contexts)


    def load_contexts(self):

        contexts = {i.name : i for i in build_contexts()} # Thanks to DAF for this pattern
        for name, context in contexts.iteritems():
            context_id = self.dataset_management.create_parameter_context(name=name, parameter_context=context.dump())
            contexts[name] = context_id

        print 'Loaded %s Parameter Contexts' % len(contexts)
        return contexts

    def load_dictionaries(self, path, contexts):
        try:
            body = None
            with open(path) as f:
                body = yaml.load(f)
            print body
            for name,context_names in body.iteritems():
                context_ids = [contexts[i] for i in context_names if i in contexts]
                self.dataset_management.create_parameter_dictionary(name=name, 
                                            parameter_context_ids=context_ids)
        except Exception as e:
            log.error('Problem loading dictionaries, stopping: %s', e.message)
            




        
        

        

        

