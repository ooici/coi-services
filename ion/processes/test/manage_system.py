#!/usr/bin/env python

""" call system_management_service actions.  use as a proof of concept or the world's worst UI.

    invoke with commands like this:

    bin/pycc -x ion.processes.test.manage_system.ChangeLogLevel logger=ion.processes.bootstrap.ion_logger level=DEBUG
    bin/pycc -x ion.processes.test.manage_system.ReportStats
    bin/pycc -x ion.processes.test.manage_system.ClearStats
"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan'


from pyon.core.bootstrap import get_service_registry
from pyon.public import ImmediateProcess
from interface.objects import AllContainers, ReportStatistics, ClearStatistics

class ChangeLogLevel(ImmediateProcess):
    def on_start(self):
        logger = self.CFG.get("logger")
        level = self.CFG.get("level")
        recursive = self.CFG.get("recursive", False)
        svc = get_service_registry().services['system_management'].client(process=self)
        svc.set_log_level(logger=logger, level=level, recursive=recursive)

class ReportStats(ImmediateProcess):
    def on_start(self):
        svc = get_service_registry().services['system_management'].client(process=self)
        svc.perform_action(predicate=AllContainers(), action=ReportStatistics())

class ClearStats(ImmediateProcess):
    def on_start(self):
        svc = get_service_registry().services['system_management'].client(process=self)
        svc.perform_action(predicate=AllContainers(), action=ClearStatistics())
