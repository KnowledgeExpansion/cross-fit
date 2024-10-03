from CompBase import *


# Output of Control Interface
class ControlSignal(object):
    def __init__(self, name, dtype=float, init_value=None):
        self.name = name
        self.value = init_value
        self.dtype = dtype

    def get_name(self):
        return self.name

    def get_value(self):
        return self.value

    def set_value(self, value):
        self.value = value


class ControlInterface(object):
    '''
        Control Base Interface
    '''

    def __init__(self):
        self._control_input_signals = dict()
        self._control_output_signals = dict()

    def connect_input_signal(self, control_input_signal):
        signal_name = control_input_signal.get_name()
        if signal_name in self._control_input_signals:
            if self._control_input_signals[signal_name] is None:
                self._control_input_signals[signal_name] = control_input_signal
                return True
        return False

    def setup_input_signal(self, signal_name):
        self._control_input_signals[signal_name] = None

    def setup_input_signal_list(self, signal_names):
        for signal_name in signal_names:
            self.setup_input_signal(signal_name)

    def get_input_signals(self):
        return self._control_input_signals

    def get_input_signal_key(self, key):
        if key in self._control_input_signals:
            return self._control_input_signals[key]
        raise AssertionError('can not find key : {}'.format(key))

    def get_input_signal_value_key(self, key):
        if key in self._control_input_signals:
            signal = self._control_input_signals[key]
            if signal is not None:
                return signal.get_value()
        raise AssertionError('can not find key : {}'.format(key))

    def setup_link_signals(self, control):
        from_output_signals = control.get_output_signals()
        for key, signal_in in zip(list(self._control_input_signals.keys()), list(self._control_input_signals.values())):
            if signal_in is None:
                signal_out = control.find_output_signal_key(key)
                if signal_out is not None:
                    # self.d('set {}'.format(key))
                    self.connect_input_signal(signal_out)
                else:
                    pass
                    # self.d("cannot find {} at {}".format(key, control.get_name()))

    def get_input_names(self):
        return list(self._control_input_signals.keys())

    def get_output_names(self):
        return list(self._control_output_signals.keys())

    def find_output_signal_key(self, key):
        if key in self._control_output_signals:
            return self._control_output_signals[key]
        return None

    def get_output_value_key(self, key):
        if key in self._control_output_signals:
            return self._control_output_signals[key].get_value()
        raise AssertionError('can not find key : {}'.format(key))

    def get_output_signals(self):
        return self._control_output_signals

    def add_output_signal(self, signal):
        if type(signal) == ControlSignal:
            self._control_output_signals[signal.name] = signal
        else:
            raise AssertionError("ControlSignal")
        return signal

    def set_output_value_key(self, key, value):
        self._control_output_signals[key].set_value(value)

    def start(self):
        comp_list = self.get_child_list()
        for ctrl in comp_list:
            ctrl.start()

    def step(self):
        comp_list = self.get_child_list()
        for ctrl in comp_list:
            ctrl.step()

    def terminate(self):
        comp_list = self.get_child_list()
        for ctrl in comp_list:
            ctrl.terminate()


class ControlBase(CompBase, ControlInterface):
    def __init__(self, module_name='ControlBase', log_level=CompBase.LOG_INFO):
        CompBase.__init__(self, module_name=module_name, log_level=log_level)
        ControlInterface.__init__(self)

    def __str__(self):
        info = CompBase.__str__(self)

        names = self.get_input_names()
        if len(names) > 0:
            info += "\n input :"
            for signal in names:
                info += "\n  <-- " + signal
        names = self.get_output_names()
        if len(names) > 0:
            info += "\n output :"
            for signal in names:
                info += "\n  -->" + signal
        return info
