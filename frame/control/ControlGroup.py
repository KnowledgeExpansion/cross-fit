import time

from frame.base.CompBase import CompBase
from frame.base.ControlBase import ControlBase


class ControlGroup(ControlBase):
    def __init__(self, module_name="ctrlgrp", auto_link=True, log_level=CompBase.LOG_INFO):
        ControlBase.__init__(self, module_name)
        self._auto_link = auto_link
        self.prev_step_elapsed_time = None
        self.control_group_elapsed_time = None

    def get_prev_step_elapsed_time(self):
        return (self.control_group_elapsed_time, self.prev_step_elapsed_time)

    def start(self):
        if self._auto_link is True:
            self.i('automatically connect harness btn input and output')
            # for external inputs
            for signal in list(self._control_input_signals.values()):
                for comp_in in self.get_child_list():
                    comp_in.connect_input_signal(signal)
            # for sub control block
            for comp_out in self.get_child_list():
                for comp_in in self.get_child_list():
                    comp_in.setup_link_signals(comp_out)
        for comp in self.get_child_list():
            comp.start()

    def step(self):
        _prev_step_elapsed_time = dict()
        _control_group_start_time = time.time()

        for comp in self.get_child_list():
            _start_time = time.time()
            try:
                comp.step()
            except:
                self.traceback()
            elapsed_time = time.time() - _start_time
            _prev_step_elapsed_time[comp.get_name()] = elapsed_time
            self.prev_step_elapsed_time = _prev_step_elapsed_time

        self.control_group_elapsed_time = time.time() - _control_group_start_time

    def terminate(self):
        for comp in self.get_child_list():
            comp.terminate()

