#!/usr/bin/env python

"""Migrates OOINet persistent data from R2 to R3"""

__author__ = 'Michael Meisinger'

import argparse
import logging
log = logging.getLogger('SystemMigrator')

import pyon
from pyon.core import bootstrap, config
from pyon.core.exception import BadRequest
from putil.script_util import parse_args


class SystemMigrator(object):
    def __init__(self):
        pass

    def arg_initialize(self):
        if bootstrap.pyon_initialized:
            raise BadRequest("Pyon already initalized")

        parser = argparse.ArgumentParser()
        parser.add_argument("-s", "--sysname", dest="sysname", help="System name")

        options, extra = parser.parse_known_args()
        args, command_line_config = parse_args(extra)

        # -------------------------------------------------------------------------
        # Store config and interfaces

        # Set global testing flag to False. We are running as standalone script. This is NO TEST.
        bootstrap.testing = False

        # Set sysname if provided in startup argument
        if options.sysname:
            bootstrap.set_sys_name(options.sysname)

        # bootstrap_config - Used for running this store_interfaces script
        bootstrap_config = config.read_local_configuration(['res/config/pyon_min_boot.yml'])
        config.apply_local_configuration(bootstrap_config, pyon.DEFAULT_LOCAL_CONFIG_PATHS)
        config.apply_configuration(bootstrap_config, command_line_config)

        # Override sysname from config file or command line
        if not options.sysname and bootstrap_config.get_safe("system.name", None):
            new_sysname = bootstrap_config.get_safe("system.name")
            bootstrap.set_sys_name(new_sysname)

        # ion_config - Holds the new CFG object for the system (independent of this tool's config)
        ion_config = config.read_standard_configuration()
        config.apply_configuration(ion_config, command_line_config)

        # Bootstrap pyon's core. Load configuration etc.
        bootstrap.bootstrap_pyon(pyon_cfg=ion_config)


    def execute_op(self):
        self.op = bootstrap.CFG.get_safe("op", "migrate")
        if self.op == "migrate":
            self.migrate_system()
        else:
            raise BadRequest("Unknown op: %s" % self.op)

    def migrate_system(self):
        log.info("======================== OOINet Data Migrator ========================")
        # Check version

        # Consistency checks
        # - System up?
        #

        # Perform migration
        # - Database schema
        # - Data content
        # - RabbitMQ?
        pass
        log.info("======================== OOINet Data Migrator ========================")


def main():
    sys_migrator = SystemMigrator()
    sys_migrator.arg_initialize()
    sys_migrator.execute_op()

if __name__ == '__main__':
    main()
