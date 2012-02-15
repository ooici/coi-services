#!/usr/bin/env python

"""Process that loads the datastore"""

__author__ = 'Michael Meisinger'

"""
Work with ION directory.
Possible features:
- Load object definitions into directory
- Load service definitions into directory
"""

import yaml
import datetime
import os
import os.path
import inspect

from pyon.core.object import IonObjectBase
from pyon.public import CFG, log, ImmediateProcess, iex, Container
from pyon.datastore.datastore import DatastoreManager

class DirectoryAdmin(ImmediateProcess):
    def on_init(self):
        pass

    def on_start(self):
        op = self.CFG.get("op", None)
        log.info("DirectoryAdmin: {}" % ())
        if op:
            if op == "register":
                self.op_register_definitions()
        else:
            raise iex.BadRequest("No operation specified")

    def on_quit(self):
        pass

    @classmethod
    def op_register_definitions(cls):
        directory = Container.instance.directory
        cls._register_service_definitions(directory)
        cls._register_objects(directory)


    @classmethod
    def _register_service_definitions(cls, directory):
        from pyon.core.bootstrap import service_registry
        svc_list = []
        for svcname, svc in service_registry.services.iteritems():
            svc_list.append(("/ServiceInterfaces", svcname, {}))
            #log.debug("Register service: %s" % svcname)
        directory.register_mult(svc_list)
        log.info("Registered %d services" % len(svc_list))

    @classmethod
    def _register_objects(cls, directory):
        from interface import objects
        delist = []
        for cname, cobj in inspect.getmembers(objects, inspect.isclass):
            if issubclass(cobj, IonObjectBase) and cobj != IonObjectBase:
                parentlist = [parent.__name__ for parent in cobj.__mro__ if parent.__name__ not in ['IonObjectBase','object']]
                delist.append(("/ObjectTypes", cname, dict(schema=cobj._schema, extends=parentlist)))
                #log.debug("Register object: %s" % cname)
        directory.register_mult(delist)
        log.info("Registered %d objects" % len(delist))
