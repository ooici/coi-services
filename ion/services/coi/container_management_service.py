from interface.services.coi.icontainer_management_service import BaseContainerManagementService

from ion.processes.event.container_manager import LogAdmin, AllContainers
from pyon.event.event import EventPublisher


class ContainerManagementService(BaseContainerManagementService):
    """ container management requests are handled by the event listener
        ion.processes.event.container_manager.ContainerManager
        which must be running on each container.
    """
    def on_start(self,*a,**b):
        super(ContainerManagementService,self).on_start(*a,**b)
        self.sender = EventPublisher()

    def perform_action(self, action):
        userid = None # get from context
        value = action if isinstance(action,str) else str(action)
        self.sender.publish_event(event_type="ContainerManagementRequest", origin=userid, value=value)

    def set_log_level(self, host_filter=AllContainers, logger='', level='', recursive=True):
        """
        @param host_filter: string indicating which hosts
            valid values:   all             apply change to all containers
                            engine:xyz      apply change to hosts belonging to the execution engine "xyz"
                            host:abc        apply change to any container on host abc
                            hosts:abc,def,ghi
                                            apply to specific list of hosts
        @param logger_filter: list of strings
        @param level: string level
        @param recursive: if true, reset any sublogger to match parent set
        """
        self.perform_action(LogAdmin(host_filter, logger, level, recursive))