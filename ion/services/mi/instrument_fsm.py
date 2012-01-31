#!/usr/bin/env python

"""
@package ion.services.mi.instrument_fsm Instrument Finite State Machine
@file ion/services/mi/instrument_fsm.py
@author Edward Hunter
@brief Simple state mahcine for driver and agent classes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import logging

mi_logger = logging.getLogger('mi_logger')

class InstrumentFSM():
    """
    Simple state mahcine for driver and agent classes.
    """

    def __init__(self, states, events, enter_event, exit_event, err_unhandled):
        """
        Initialize states, events, handlers.
        
        @param states The list of states that the FSM handles
        @param events The list of events that the FSM handles
        @param state_handlers A dict of which state maps to which handling
        routine
        @param enter_event The event that indicates a state is being entered
        @param exit_event The event that indicates a state is being exited
        """
        self.states = states
        self.events = events
        self.state_handlers = {}
        self.current_state = None
        self.previous_state = None
        self.enter_event = enter_event
        self.exit_event = exit_event
        self.err_unhandled = err_unhandled

    def get_current_state(self):
        """
        Return current state.
        """
        return self.current_state

    def add_handler(self, state, event, handler):
        """
        """
        if not self.states.has(state):
            return False
        
        if not self.events.has(event):
            return False

        self.state_handlers[(state,event)] = handler
        return True
        
    def start(self,state,params=None):
        """
        Start the state machine. Initializes current state and fires the
        EVENT_ENTER event.
        @param state The state to start execute
        @param params A list of parameters to hand to the handler
        @retval return True
        """
        
        #if state not in self.states:
        #    return False
        
        if not self.states.has(state):
            return False
                
        self.current_state = state
        handler = self.state_handlers.get((state, self.enter_event), None)
        if handler:
            handler(params)
        return True

    def on_event(self,event,params=None):
        """
        Handle an event. Call the current state handler passing the event
        and paramters.
        @param event A string indicating the event that has occurred.
        @param params Optional parameters to be sent with the event to the
            handler.
        @retval result from the handler executed by the current state/event pair.
        @throw InstrumentProtocolException
        @throw InstrumentTimeoutException
        """        
        next_state = None
        result = None
        
        if self.events.has(event):
            handler = self.state_handlers.get((self.current_state, event), None)
            if handler:
                (next_state, result) = handler(params)

        #if next_state in self.states:
        if self.states.has(next_state):
            self._on_transition(next_state, params)
                
        return result
            
    
    def _on_transition(self,next_state,params):
        """
        Call the sequence of events to cause a state transition. Called from
        on_event if the handler causes a transition.
        @param next_state The state to transition to.
        @param params Opional parameters passed from on_event
        """
        
        handler = self.state_handlers.get((self.current_state, self.exit_event), None)
        if handler:
            handler(params)
        self.previous_state = self.current_state
        self.current_state = next_state
        handler = self.state_handlers.get((self.current_state, self.enter_event), None)
        if handler:
            handler(params)

