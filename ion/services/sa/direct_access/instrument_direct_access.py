#!/usr/bin/env python

__author__ = 'Ian Katz'
__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'

from ion.services.sa.direct_access.direct_access import DirectAccess
from pyon.core.exception import BadRequest, NotFound
from pyon.util.log import log

class InstrumentDirectAccess(DirectAccess):
    """
    Class for direct access at the instrument level
    """

    def request(self, request_params={}):
        """
        @brief      Initiates direct access for the given instrument agent
        @param      request_params dict
                        "instrument_agent_id":  string  ID of the instrument agent
        @retval     results dict
                        "success":  bool    TRUE=success, FALSE=unable to connect
                        "ip":       string  IP address of the agent-side server
                        "port":     string  IP port of the agent-side server
        @throws     BadRequest if the instrument_agent_id is missing
        """
        log.debug("InstrumentDirectAccess: request()")

        log.debug("Requesting Direct Access for " +
                  str(request_params["instrument_agent_id"]))

        # Validate the instrument agent ID
        if not request_params["instrument_agent_id"]:
            raise BadRequest("no instrument agent ID")

        results = {"success": False, "da_ip": "", "da_port": ""}

        # TODO: Turn the following description into code
        # 2. Request IA transistion to direct access state
        #       - Call instrument_agent_request_direct_access(DA_type)
        #       - Expected responses:
        #           True
        #               Success.  The instrument agent did this:
        #                   - Saved the state and the current parameters of the instrument
        #                   - Launched Direct Access Server with DA type (telnet, SSH, VSP)
        #                          The da_server did this:
        #                             - Launched protocol server(s)
        #                             - Is effectively connected to the IA (via streams?)
        #                   - Changed state to "DIRECT_ACCESS"
        #                   - returns IP Address, Port Number
        #           False
        #               Failure.  Instrument agent could not complete its tasks
        #                   - State could be anything now
        #                   - return failure to caller

        return results

    def stop(self, stop_params={}):
        log.debug("InstrumentDirectAccess: stop()")

