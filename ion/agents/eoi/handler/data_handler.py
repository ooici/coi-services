__author__ = "Tim Giguere"

from pyon.public import log

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

        log.debug('cmd_dvr received command \'{0}\' with: args={1} kwargs={2}'.format(cmd, args, kwargs))

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
        elif cmd == 'execute_acquire_sample':
            reply = self.execute_acquire_sample(args, kwargs)
        elif cmd == 'go_active':
            reply = self.go_active(args, kwargs)
        elif cmd == 'run':
            reply = self.run(args, kwargs)
        elif cmd == 'reset':
            reply = self.reset(args, kwargs)
        elif cmd == 'go_observatory':
            reply = self.go_observatory(args, kwargs)
        elif cmd == 'execute_acquire_sample':
            reply = self.execute_acquire_sample(args, kwargs)

        return reply

    def configure(self, *args, **kwargs):
        return {}

    def initialize(self, *args, **kwargs):
        return None

    def go_active(self, *args, **kwargs):
        return None

    def run(self, *args, **kwargs):
        return None

    def reset(self, *args, **kwargs):
        return None

    def go_observatory(self, *args, **kwargs):
        return None

    def execute_acquire_sample(self, *args, **kwargs):
        # todo: Fix raises statements and add documentation
        return {'p': [-6.945], 'c': [0.08707], 't': [20.002], 'time': [1333752198.450622]}

    def execute_start_autosample(self, *args, **kwargs):
        # todo: Fix raises statements and add documentation
        """

        @raises TimeoutError:
        @raises ProtocolError:
        @raises NotImplementedError:
        @raises ParameterError:
        """
        return None

    def execute_stop_autosample(self, *args, **kwargs):
        # todo: Fix raises statements and add documentation
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
        return {
            'param1' : True,
            'param2' : 'value',
            }

    def set(self, *args, **kwargs):
        """
        @raises TimeoutError:
        @raises ProtocolError:
        @raises NotImplementedError:
        @raises ParameterError:
        """
        return None

    def get_resource_params(self, *args, **kwargs):
        """
        Return list of resource parameters. Implemented in specific handlers
        """
#        raise NotImplementedError('get_resource_params() not implemented in BaseDataHandler')
        return ['param1','param2']
#        return ['DRIVER_PARAMETER_ALL',
#                'CCALDATE',
#                'CG',
#                'CH',
#                'CI',
#                'CJ',
#                'CPCOR',
#                'CTCOR',
#                'INTERVAL',
#                'NAVG',
#                'OUTPUTSAL',
#                'OUTPUTSV',
#                'PA0',
#                'PA1',
#                'PA2',
#                'PCALDATE',
#                'POFFSET',
#                'PTCA0',
#                'PTCA1',
#                'PTCA2',
#                'PTCB0',
#                'PTCB1',
#                'PTCB2',
#                'RCALDATE',
#                'RTCA0',
#                'RTCA1',
#                'RTCA2',
#                'SAMPLENUM',
#                'STORETIME',
#                'SYNCMODE',
#                'SYNCWAIT',
#                'TA0',
#                'TA1',
#                'TA2',
#                'TA3',
#                'TCALDATE',
#                'TXREALTIME',
#                'WBOTC']

    def get_resource_commands(self, *args, **kwargs):
        """
        Return list of device execute commands available.
        """
        cmds = [cmd.replace('execute_','') for cmd in dir(self) if cmd.startswith('execute_')]
        return cmds

