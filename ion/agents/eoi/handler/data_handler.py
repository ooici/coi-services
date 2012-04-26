__author__ = "Tim Giguere"

class DataHandler():
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

        #need to account for observatory_execute_resource commands
        #connect -> Not used
        #get_current_state -> Not used
        #discover -> Not used
        #disconnect -> Not used

        reply = None
        if cmd == 'configure':  #used to configure data handler
            reply = self.configure(args, kwargs)
        elif cmd == 'initialize':   #used to initialize data handler
            reply = self.initialize(args, kwargs)
        elif cmd == 'execute_start_autosample': #used to get data handler into streaming mode
            reply = self.execute_start_autosample(args, kwargs)
        elif cmd == 'execute_stop_autosample':  #used to get data handler back into observatory mode
            reply = self.execute_stop_autosample(args, kwargs)
        elif cmd == 'get':
            reply = self.get(args, kwargs)
        elif cmd == 'set':
            reply = self.set(args, kwargs)
        elif cmd == 'get_resource_params':
            reply = self.get_resource_params(args, kwargs)
        elif cmd == 'get_resource_commands':
            reply = self.get_resource_commands(args, kwargs)

        return reply

    def configure(self, *args, **kwargs):
        return {}

    def initialize(self, *args, **kwargs):
        return None

    def execute_start_autosample(self, *args, **kwargs):
        """
        @raises TimeoutError:
        @raises ProtocolError:
        @raises NotImplementedError:
        @raises ParameterError:
        """
        return None

    def execute_stop_autosample(self, *args, **kwargs):
        """
        @raises TimeoutError:
        @raises ProtocolError:
        @raises NotImplementedError:
        @raises ParameterError:
        """
        return None

    def get(self, *args, **kwargs):
        """
        @raises TimeoutError:
        @raises ProtocolError:
        @raises NotImplementedError:
        @raises ParameterError:
        """
        return None

    def set(self, *args, **kwargs):
        """
        @raises TimeoutError:
        @raises ProtocolError:
        @raises NotImplementedError:
        @raises ParameterError:
        """
        return None

    def get_resource_params(self, *args, **kwargs):
        return None

    def get_resource_commands(self, *args, **kwargs):
        return None

