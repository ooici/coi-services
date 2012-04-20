#!/usr/bin/env python

"""
@package ion.services.mi.instrument_fsm Instrument Finite State Machine
@file ion/services/mi/instrument_fsm.py
@author Edward Hunter
@brief Simple state mahcine for driver and agent classes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from ion.services.mi.exceptions import StateError


class InstrumentFSM():
    """
    Simple state mahcine for driver and agent classes.
    """

    def __init__(self, states, events, enter_event, exit_event):
        """
        Initialize states, events, handlers.
        @param states The list of states that the FSM handles
        @param events The list of events that the FSM handles
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

    def get_current_state(self):
        """
        Return current state.
        """
        return self.current_state

    def add_handler(self, state, event, handler):
        """
        Add an event handler.
        @param state the state to handler the event in.
        @param the event to handle.
        @retval True if successful, False otherwise.
        """
        if not self.states.has(state):
            return False
        
        if not self.events.has(event):
            return False

        self.state_handlers[(state,event)] = handler
        return True
        
    def start(self, state, *args, **kwargs):
        """
        Start the state machine. Initializes current state and fires the
        EVENT_ENTER event.
        @param state The state to start in.
        @param args positional arguments to pass to the handler.
        @param kwargs keyword arguments to pass to the handler.
        @retval True if successful, False otherwise.
        @raises Any exception raised by the enter handler.
        """        
        if not self.states.has(state):
            return False
                
        self.current_state = state
        handler = self.state_handlers.get((state, self.enter_event), None)
        if handler:
            handler(*args, **kwargs)
        return True

    def on_event(self, event, *args, **kwargs):
        """
        Handle an event. Call the current state handler passing the event
        and paramters.
        @param event A string indicating the event that has occurred.
        @param args positional arguments to pass to the handler.
        @param kwargs keyword arguments to pass to the handler.
        @retval result from the handler executed by the current state/event pair.
        @raises StateError if no handler for the event exists in current state.
        @raises Any exception raised by the handlers.
        """        
        next_state = None
        result = None
        
        if self.events.has(event):
            handler = self.state_handlers.get((self.current_state, event), None)
            if handler:
                (next_state, result) = handler(*args, **kwargs)
            else:
                raise StateError('Command not handled in current state.')
                
        #if next_state in self.states:
        if self.states.has(next_state):
            self._on_transition(next_state, *args, **kwargs)
                
        return result
            
    def _on_transition(self, next_state, *args, **kwargs):
        """
        Call the sequence of events to cause a state transition. Called from
        on_event if the handler causes a transition.
        @param next_state The state to transition to.
        @param args positional arguments to pass to the handler.
        @param kwargs keyword arguments to pass to the handler.
        @raises Any exception raised by the handlers.
        """
        
        handler = self.state_handlers.get((self.current_state, self.exit_event), None)
        if handler:
            handler(*args, **kwargs)
        self.previous_state = self.current_state
        self.current_state = next_state
        handler = self.state_handlers.get((self.current_state, self.enter_event), None)
        if handler:
            handler(*args, **kwargs)

