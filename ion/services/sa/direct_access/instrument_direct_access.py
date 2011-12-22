#!/usr/bin/env python

__author__ = 'Ian Katz'
__author__ = 'Alon Yaari'
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
                        "ip":       string  IP address of the agent-side socat
                        "port":     string  IP port of the agent-side socat
        @throws     BadRequest if the instrument_agent_id is missing
        """
        log.debug("InstrumentDirectAccess: request()")

        log.debug("Requesting Direct Access for " +
                  str(request_params["instrument_agent_id"]))

        # Validate the instrument agent ID
        if not request_params["instrument_agent_id"]:
            throw BadRequest("")

        results = {"success": False, "da_ip": "", "da_port": ""}
        # TODO: Turn the following description into code

        # 1. Save the state and the current parameters of the instrument agent
        #       - Call to instrument_agent_get()
        #       - Store results in local variables
        # 2. Request transistion to direct access state
        #       - Call instrument_agent_request_direct_access()
        #       - Expected responses:
        #           True, (IP Address, Port Number)
        #               Success.  The instrument agent did this:
        #                   - Launched socat
        #                   - Is effectively connecting the driver to socat
        #                   - Changed state to "DIRECT_ACCESS"
        #           False, ()
        #               Failure.  Instrument agent could complete its tasks
        #                   - State could be anything now

        return results

    def stop(self, stop_params={}):
        log.debug("InstrumentDirectAccess: request()")

