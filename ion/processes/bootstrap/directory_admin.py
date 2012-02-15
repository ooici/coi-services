#!/usr/bin/env python

"""Process that loads the datastore"""

__author__ = 'Michael Meisinger'

"""
Work with ION directory from a process.
"""

from pyon.public import CFG, log, ImmediateProcess, iex, Container

class DirectoryAdmin(ImmediateProcess):
    """
    bin/pycc -x ion.processes.bootstrap.directory_admin.DirectoryAdmin op=load
    """
    def on_init(self):
        pass

    def on_start(self):
        op = self.CFG.get("op", None)
        log.info("DirectoryAdmin: {op=%s}" % (op))
        if op:
            if op == "register":
                self.op_register_definitions()
            elif op == "load":
                pass
            else:
                raise iex.BadRequest("Operation unknown")
        else:
            raise iex.BadRequest("No operation specified")

    def on_quit(self):
        pass

    @classmethod
    def op_register_definitions(cls):
        directory = Container.instance.directory
        directory._register_service_definitions()
        directory._register_object_types()

    @classmethod
    def op_load_definitions(cls):
        directory = Container.instance.directory
        directory.load_definitions()
