#!/usr/bin/env python

""" call system_management_service to invoke a logging change.  use as a proof of concept or the world's worst UI.

    invoke with command like this:

    bin/pycc -x ion.processes.test.logging_change.ChangeLogLevel logger=ion.processes.bootstrap.ion_logger level=DEBUG
"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan'


from pyon.core.bootstrap import get_service_registry
from pyon.public import ImmediateProcess

class ChangeLogLevel(ImmediateProcess):

    def on_start(self):
        logger = self.CFG.get("logger")
        level = self.CFG.get("level")
        recursive = self.CFG.get("recursive", False)

        svc = get_service_registry().services['system_management'].client(process=self)
        svc.set_log_level(logger=logger, level=level, recursive=recursive)
