
#!/usr/bin/env python

"""Bootstrap process for exchange"""

__author__ = 'Dave Foster <dfoster@asascience.com>'

from ion.core.bootstrap_process import BootstrapPlugin
from pyon.public import log
from pyon.public import get_sys_name, RT, PRED
from pyon.ion.exchange import ExchangeSpace, ExchangePoint, ExchangeName

class BootstrapExchange(BootstrapPlugin):
    """
    Bootstrap plugin for exchange/management
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        pass

    def on_restart(self, process, config, **kwargs):
        """
        Handles bootstrapping of system restart for exchange resources and broker state.

        - Ensures ExchangePoint and ExchangeSpace resources in system have a properly
          declared AMQP exchange
        - Ensures ExchangeName resources in system have a properly declared queue
        - Logs all exchanges/queues it didn't understand
        - Purges all service queues as long as no consumers are attached, or can be
          overridden with force=True on pycc command line
        """
        ex_manager = process.container.ex_manager

        # get list of queues from broker with full props that have to do with our sysname
        all_queues = ex_manager._list_queues()
        queues = {q['name']:q for q in all_queues if q['name'].startswith(get_sys_name())}

        # get list of exchanges from broker with full props
        all_exchanges = ex_manager._list_exchanges()
        exchanges = {e['name']:e for e in all_exchanges if e['name'].startswith(get_sys_name())}

        # now get list of XOs from RR
        xs_objs, _ = process.container.resource_registry.find_resources(RT.ExchangeSpace)
        xp_objs, _ = process.container.resource_registry.find_resources(RT.ExchangePoint)
        xn_objs, _ = process.container.resource_registry.find_resources(RT.ExchangeName)

        #
        # VERIFY XSs have a declared exchange
        #
        rem_exchanges = set(exchanges)

        for rrxs in xs_objs:
            xs = ExchangeSpace(ex_manager, ex_manager._priviledged_transport, rrxs.name)

            if xs.exchange in rem_exchanges:
                rem_exchanges.remove(xs.exchange)
            else:
                log.warn("BootstrapExchange restart: RR XS %s, id: %s NOT FOUND in exchanges", rrxs.name, rrxs._id)

        for rrxp in xp_objs:
            xp = ExchangePoint(ex_manager, ex_manager._priviledged_transport, rrxp.name)

            if xp.exchange in rem_exchanges:
                rem_exchanges.remove(xp.exchange)
            else:
                log.warn("BootstrapExchange restart: RR XP %s, id %s NOT FOUND in exchanges", rrxp.name, rrxp._id)

        # events and main service exchange should be left
        if get_sys_name() in rem_exchanges:
            rem_exchanges.remove(get_sys_name())
        else:
            log.warn("BootstrapExchange restart: no main service exchange %s", get_sys_name())

        event_ex = "%s.pyon.events" % get_sys_name()
        if event_ex in rem_exchanges:
            rem_exchanges.remove(event_ex)
        else:
            log.warn("BootstrapExchange restart: no events exchange %s", event_ex)

        # what is left?
        for exchange in rem_exchanges:
            log.warn("BootstrapExchange restart: unknown exchange on broker %s", exchange)

        #
        # VERIFY XNs have a declared queue
        #
        rem_queues = set(queues)

        for rrxn in xn_objs:
            # can instantiate ExchangeNames, don't need specific types
            # @TODO: except queue type, which needs to be fixed to record declared name type

            if rrxn.xn_type == "QUEUE":
                log.info("TODO: queue type XNs, %s", rrxn.name)
                continue

            exchange_space_list, assoc_list = process.container.resource_registry.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, rrxn._id)
            if not len(exchange_space_list) == 1:
                raise StandardError("Associated Exchange Space to Exchange Name %s does not exist" % rrxn._id)

            rrxs = exchange_space_list[0]

            xs = ExchangeSpace(ex_manager, ex_manager._priviledged_transport, rrxs.name)
            xn = ExchangeName(ex_manager, ex_manager._priviledged_transport, rrxn.name, xs)

            if xn.queue in rem_queues:
                rem_queues.remove(xn.queue)
            else:
                log.warn("BootstrapExchange restart: RR XN %s, type %s, id %s NOT FOUND in queues", xn.queue, xn.xn_type, xn._id)

        # get list of service name possibilities
        svc_objs, _ = process.container.resource_registry.find_resources(RT.ServiceDefinition)
        svc_names = [s.name for s in svc_objs]

        # PROCESS QUEUES + SERVICE QUEUES- not yet represented by resource
        proc_queues = set()
        svc_queues = set()

        for queue in list(rem_queues):

            # PROCESS QUEUES: proc manager spawned
            # pattern "<sysname>.<containerid>.<pid>"
            pieces = queue.split(".")
            if len(pieces) > 2 and pieces[-1].isdigit():
                proc_queues.add(queue)
                rem_queues.remove(queue)
                continue

            # SERVICE QUEUES
            # pattern "<sysname>.<service name>"
            if len(pieces) == 2:
                if pieces[-1] in svc_names:
                    svc_queues.add(queue)
                    rem_queues.remove(queue)

            # @TODO: PD-spawned process queues
            # pattern "<sysname>.<service_name><hex>"

        # leftover queues now
        for queue in rem_queues:
            log.warn("Unknown queue: %s", queue)

        #
        # EMPTY SERVICE QUEUES
        #
        for queue in svc_queues:
            if int(queues[queue]['consumers']) > 0 and not process.CFG.get_safe('force', False):
                log.warn("Refusing to empty service queue %s with consumers (%s), specify force=True to override", queue, queues[queue]['consumers'])
            else:
                ex_manager.purge_queue(queue)
                log.info("Purged service queue %s of %s messages", queue, queues[queue]['messages'])

