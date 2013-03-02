import sys
import logging
from ion.core.process.transform import TransformEventListener
from ooi.logging import log, config
from pyon.core.exception import BadRequest
# define types of messages that can be sent and handled

class AdminMessage(object):
    """ base class for messages

        subclasses can be represented as string values so that for any ContainerSelector x:
            x == ContainerSelector.from_string( str(x) )

        to implement this, any subclass must define str(x) as classname:details[:more_details...]
        and provide a constructor classname(persist=[details,...])
        or if no details are needed, str(x) should be classname and define a no-arg constructor (both are already defaults)

        representation as a string is needed so requests can be passed in a URL to the service gateway
    """
    def __init__(self, container_selector):
        if not container_selector:
            raise BadRequest('')
        self.container_selector = container_selector
    def perform_action(self, container):
        raise Exception('subclass must override this method')
    def should_handle(self, container):
        return self.container_selector.should_handle(container)
    def __str__(self):
        return "%s:%s" % (self.__class__.__name__, self.container_selector)

    @classmethod
    def from_string(cls, value):
        parts = value.split(':')
        subclass = getattr(sys.modules[__name__], parts[0])
        selector = ContainerSelector.from_string(parts[1])
        return subclass(container_selector=selector, persist=parts[2:]) if len(parts)>2 else subclass(container_selector=selector)

class LogAdmin(AdminMessage):
    """ message to request a change in log levels """
    def __init__(self, container_selector=None, logger=None, level=None, recursive=None, persist=None):
        super(LogAdmin,self).__init__(container_selector)
        if level:
            # configure from individual args
            self.logger = logger
            self.recursive = recursive
            self.level = level if isinstance(level, str) else logging.getLevelName(level)
        else:
            # configure from string
            if len(persist)!=3:
                raise BadRequest('expected exactly three string arguments: %r' % values)
            self.recursive = persist[2]=='True'
            self.logger = persist[0]
            self.level = persist[1]
    def perform_action(self, container):
        log.info('changing log level: %s: %s', self.logger, logging.getLevelName(self.level))
        config.set_level(self.logger, self.level, self.recursive)
    def __str__(self):
        name = (self.logger+'*') if self.recursive else self.logger
        return "%s:%s:%s:%s" % (super(LogAdmin,self).__str__(), self.logger, self.level, self.recursive)
    def __eq__(self, other):
        return isinstance(other, LogAdmin) and self.level==other.level and self.logger==other.logger and self.recursive==other.recursive

# TODO: other useful administrative actions
#
#class ThreadDump(AdminMessage):
#    """ request that containers perform a thread dump """
#    pass
#
#class LogTimingStats(AdminMessage):
#    """ request that containers log timing stats """
#    pass
#
#class ResetTimingStats(AdminMessage):
#    """ request that containers clear all timing stats """
#    pass

# define selectors to determine if this message should be handled by this container.
# used by the message, manager should not interact with this directly

class ContainerSelector(object):
    """ base class for predicate classes to select which containers should handle messages

        subclasses can be represented as string values so that for any ContainerSelector x:
            x == ContainerSelector.from_string( str(x) )

        to implement this, any subclass must define str(x) as classname,details[,more_details...]
        and provide a constructor classname(*details)
        or if no details are needed, str(x) should be classname and define a no-arg constructor (both are already defaults)
    """
    def should_handle(self, container):
        raise Exception('subclass must override this method')
    def _from_string(self, parts):
        """ configure from string value """
        pass # default action is do nothing
    def __str__(self):
        return self.__class__.__name__
    @classmethod
    def from_string(cls, value):
        parts = value.split(',')
        subclass = getattr(sys.modules[__name__], parts[0])
        return subclass(persist=parts[1:]) if len(parts)>1 else subclass()

class AllContainers(ContainerSelector):
    """ all containers should perform the action """
    def should_handle(self, container):
        return True
    def __eq__(self, other):
        return isinstance(other, AllContainers)

# TODO: more selectors
#class ContainersByName(ContainerSelector):
#    """ specific list of containers """
#    def __init__(self, name_list):
#        self.names = name_list.split(',')
#    def should_handle(self,container):
#        name = container.get_name() ??
#        return name in self.names
#class ContainersByIP(ContainerSelector):
#    pass

# process to handle the messages

class ContainerManager(TransformEventListener):
    def on_start(self):
        super(ContainerManager,self).on_start()
        log.debug('listening for events from %s',self.CFG.get('process.queue_name'))

    def process_event(self, msg, headers):
        if isinstance(msg, AdminMessage) and msg.should_handle(self.container):
            log.debug('handling admin message: %s', msg)
            msg.perform_action(self.container)
        else:
            log.trace('ignoring admin message: %s', msg)