__author__ = "Tim Giguere"

from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.driver_client import DriverClient

class DataHandler(DriverClient, InstrumentDriver):
    def cmd_dvr(self, cmd, *args, **kwargs):
        """
        Command a driver by request-reply messaging. Package command
        message and send on blocking command socket. Block on same socket
        to receive the reply. Return the driver reply.
        @param cmd The driver command identifier.
        @param args Positional arguments of the command.
        @param kwargs Keyword arguments of the command.
        @retval Command result.
        """
        # Package command dictionary.

        reply = None
        if cmd == 'configure':
            reply = configure(args, kwargs)
        elif cmd == 'connect':
            reply = connect(args, kwargs)
        elif cmd == 'disconnect':
            reply = disconnect(args, kwargs)
        elif cmd == 'initialize':
            reply = initialize(args, kwargs)
        elif cmd == 'start_autosample':
            reply = execute_start_autosample(args, kwargs)
        elif cmd == 'get_active_channels':
            reply = get_active_channels(args, kwargs)
        elif cmd == 'stop_autosample':
            reply = execute_stop_autosample(args, kwargs)

        return reply

    def connect(self, *args, **kwargs):
        return {}

    def configure(self, *args, **kwargs):
        return {}

    def disconnect(self, *args, **kwargs):
        return {}

    def initialize(self, *args, **kwargs):
        return None

    def get_active_channels(self, *args, **kwargs):
        return ['active_channel'] #return non-empty list to get agent state to IDLE

    def execute_start_autosample(self, *args, **kwargs):
        return {}

    def execute_stop_autosample(self, *args, **kwargs):
        return {}



