import sys
import traceback
import logging
import inspect


from utils import enum
from DbgBase import dbg


class Event(object):
    def __init__(self, event_name):
        self.listeners = {}
        self.event_name = event_name

    def dispatch(self, *args, **kwargs):
        if len(list(self.listeners.keys())) == 0:
            return False
        for call_list in list(self.listeners.values()):
            for callback, state in zip(list(call_list.keys()), list(call_list.values())):
                if state is None:
                    callback(*args, **kwargs)
                else:
                    # print state
                    if 'state' in kwargs:
                        if state == kwargs['state']:
                            callback(*args, **kwargs)
        return True

    def register(self, who, callback, state=None):
        if who in self.listeners:
            if callback in self.listeners[who]:
                # self.e("Multiple events are registed {}".format(self.event_name))
                return False
            else:
                call = self.listeners[who]
                call[callback] = state
        else:
            call = {callback: state}
            self.listeners[who] = call
        return True

    def unregister(self, who, callback=None):
        if who in self.listeners:
            if callback is None:
                # del self.listeners[who]
                self.listeners.pop(who)
                return True
            elif callback in self.listeners[who]:
                # del self.listeners[who][callback]
                self.listeners[who].pop(callback)
                if len(list(self.listeners[who].keys())) == 0:
                    self.listeners.pop(who)
                return True
        return False

    def count(self):
        return len(list(self.listeners.keys()))

    def __str__(self):
        msg = 'event name = {}\nregisted # : {}'.format(self.event_name, self.count())
        return msg


class TransAction(object):
    def __init__(self, next_state, condition=None, action=None, condition_target=True):
        self.__next_state = next_state
        self.__condition = condition
        self.__action = action
        self.__condition_target = condition_target

    def action(self, **kwargs):
        if self.__condition is not None:
            if self.__condition(**kwargs) is self.__condition_target:
                if self.__action is not None:
                    self.__action(**kwargs)
                return self.__next_state
        else:
            if self.__action is not None:
                self.__action(**kwargs)
            return self.__next_state
        return None


class CompBase(object):
    LOG_DEBUG = 3
    LOG_INFO = 2
    LOG_NOTICE = 1

    '''
        Base Class
    '''

    def __init__(self, module_name='', module_id=0, log_level=2):
        self._module_name = module_name
        self._module_id = module_id
        self._log_level = log_level

        # activate debug log at unittest mode
        if 'unittest' in sys.modules:
            self._log_level = CompBase.LOG_DEBUG

        self._param_list = []
        self._param_dic = dict()

        self.parent = None
        self._child_comp_list = []  #
        self.events = None
        self.state = None
        self.state_current = None
        self.state_before = None
        self.state_entry = None
        self.transition = None
        self.transition_table = None
        # self.d('Creating')
        self.event_state_changed = Event('State')
        # For elapsed time
        self.function_elapsed_time = {}

    def init_state_table(self, state_list, trans_list):
        # enum definition
        self.define_state(*state_list)
        self.define_transition(*trans_list)
        # initialize transition table
        row = self.state.count
        col = self.transition.count
        self.transition_table = [[None for c in range(col)] for r in range(row)]
        self.state_entry = [list() for r in range(row)]
        self.state_exit = [list() for r in range(row)]

        self.setup_trans_table()

    def get_state_info(self):
        if self.state is None:
            return (False, None)
        return (True, self.state.reverse_mapping)

    def define_state(self, *args, **kwargs):
        self.state = enum(*args)
        if 'state_init' in kwargs:
            self.state_current = kwargs['state_init']
        else:
            self.state_current = 0

    def add_entry(self, state, action):
        if not isinstance(state, list):
            state = [state]

        for s in state:
            self.state_entry[s].append(action)

    def add_entry_condition(self, state, action, condition):
        if not isinstance(state, list):
            state = [state]

        if condition:
            for s in state:
                self.state_entry[s].append(action)

    def add_exit(self, state, action):
        if not isinstance(state, list):
            state = [state]

        for s in state:
            self.state_exit[s].append(action)

    def add_trans_condition(self, state, transition, next_state, condition=None, action=None, condition_target=True):
        if not isinstance(state, list):
            state = [state]

        for s in state:
            if self.transition_table[s][transition] is None:
                self.transition_table[s][transition] = [TransAction(
                    next_state, condition, action, condition_target)]
            else:
                self.transition_table[s][transition].append(
                    TransAction(next_state, condition, action, condition_target))

    def define_transition(self, *args, **kwargs):
        self.transition = enum(*args)

    def setup_trans_table(self):
        self.e('Not defined setup_trans_table()')
        return False

    def state_machine(self, transition, **kwargs):
        state = self.get_state()
        self.d("[S]{}-[E]{}".format(self.get_state_name(), self.get_transition_name(transition)))
        if self.transition_table[state][transition] is None:
            # self.d('action fn empty')
            return False

        for trans_action in self.transition_table[state][transition]:
            next_state = trans_action.action(**kwargs)
            if next_state is not None:
                self.state_before = state
                # call exit function
                for call_exit in self.state_exit[state]:
                    call_exit()
                self.set_state(next_state)
                # call entry function
                for entry in self.state_entry[next_state]:
                    entry()
                return True
        return False

    def can_trans(self, key=None):
        self.w("should be overrided")
        return False

    '''
    @staticmethod
    def get_key(key, kwargs):
        if kwargs.has_key(key):
            return kwargs[key]
        return None
    '''

    def set_state(self, state_current):
        prev = self.state_current
        self.state_current = state_current
        # get state names
        prev_state_name = self.state.reverse_mapping[prev]
        new_state_name = self.state.reverse_mapping[state_current]

        self.i("##[[state]] : {} <- {}".format(new_state_name, prev_state_name))
        self.event_state_changed.dispatch(self.state_current, module_name=self._module_name, pre_state=prev)

    def get_state(self):
        return self.state_current

    def is_state(self, state):
        if self.state_current == state:
            return True
        return False

    def get_state_name(self, state=None):
        if state is None:
            return self.state.reverse_mapping[self.state_current]
        else:
            return self.state.reverse_mapping[state]

    def get_transition_name(self, transition):
        return self.transition.reverse_mapping[transition]

    def update_param(self):
        # update from parameter to local variables
        pass

    def add_param(self, param, key=None):
        self._param_list.append(param)
        if key is None:
            key = param.get_name()

        if key not in self._param_dic:
            self._param_dic[key] = param
        else:
            self.e('Already defined parameter')
            return False
            # raise AssertionError
        return True

    def get_param_at(self, index):
        if index < len(self._param_list):
            return self._param_list[index]
        return None

    def get_param_at_key(self, key):
        if key in self._param_dic:
            return self._param_dic[key]
        self.e('Not Exist defined parameter : {}'.format(key))
        return None

    def get_param_list(self):
        return self._param_list

    def get_param_value_at(self, index):
        if index < len(self._param_list):
            return self._param_list[index].get_value()
        return None

    def get_param_key(self, key):
        if key in self._param_dic:
            return self._param_dic[key]
        self.w('Cannot found param with key {}'.format(key))
        return None

    def get_param_value_at_key(self, key):
        if key in self._param_dic:
            param = self._param_dic[key]
            if param.get_type() == eParamType.Select:
                return param.get_selected()
            return param.get_value()
        self.w('Cannot found param with key {}'.format(key))
        return None

    def get_param_name(self, name):
        for p in self._param_list:
            if p.get_name() == name:
                return p
        return None

    def get_param_dict(self):
        return dict([(__param.get_name(), __param.get_value()) for __param in self.get_param_list()])

    def set_param_value_at_name(self, name, value):
        for p in self._param_list:
            if p.get_name() == name:
                self.d('Param {} changed : {} => {}'.format(name, p.get_value(), value))
                p.set_value(value)
                return True
        self.w('Param {} not found'.format(name))
        return False

    def set_param_value_at_key(self, key, value):
        if key in self._param_dic:
            param = self._param_dic[key]
            value_org = param.get_value()
            if value_org != value:
                # self.d('Param {} changed : {} => {}'.format(key, value_org, value))
                param.set_value(value)
            return True
        self.w('Cannot found param with key {}'.format(key))
        return False

    def set_params_key_value(self, key_value):
        # for key, value in zip(key_value.keys(), key_value.values()):
        for key, value in list(key_value.items()):
            self.set_param_value_at_key(key, value)

    def get_param_count(self):
        return len(self._param_list)

    def set_id(self, module_id):
        self._module_id = module_id

    def get_name(self):
        return self._module_name

    def get_id(self):
        return self._module_id

    def set_log_level(self, level, force_to_child=False):
        if self._log_level != level:
            self._log_level = level
            self.n('log level to {}'.format(level))

        if force_to_child is True:
            # set log level to all child components
            for comp in self._child_comp_list:
                comp.set_log_level(level, force_to_child=force_to_child)

    def get_log_level(self):
        return self._log_level

    '''
    def _log(self, msg, log_level):
        str_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        if self._module_name is None:
            print '['+str_time+'#N#'+str(log_level)+']'+str(msg)
        else:
            print '['+str_time+'#'+self._module_name+'#'+str(log_level)+']'+str(msg)
    '''

    def d(self, msg):
        if self._log_level >= self.LOG_DEBUG:
            # self._log(msg, self.LOG_DEBUG)
            dbg.d(self._module_name, msg)

    def i(self, msg):
        if self._log_level >= self.LOG_INFO:
            # self._log(msg, self.LOG_INFO)
            dbg.i(self._module_name, msg)

    def n(self, msg):
        if self._log_level >= self.LOG_NOTICE:
            # self._log(msg, self.LOG_NOTICE)
            dbg.n(self._module_name, msg)

    def w(self, msg):
        dbg.w(self._module_name, msg)
        # self._log(msg, 'W')

    def e(self, msg):
        dbg.e(self._module_name, msg)
        # self._log(msg, 'E')
        # raise AssertionError

    def log(self, msg, log_level=logging.INFO):
        dbg.log(self._module_name, msg, log_level=log_level)

    def traceback(self, msg='', log_level=logging.ERROR):
        traceback_string = traceback.format_exc()
        emit_func_map = {logging.DEBUG: dbg.d, logging.INFO: dbg.i, logging.WARN: dbg.w, logging.ERROR: dbg.e,
                         logging.CRITICAL: dbg.n}
        emit_func = emit_func_map.get(log_level, dbg.e)
        emit_func(self._module_name,
                  '{0}\n{2}BEGIN-OF-TRACEBACK{2}\n{1}{2}END-OF-TRACEBACK{2}'.format(msg, traceback_string, '=' * 20))
        return traceback_string

    def get_func_name(self):
        return inspect.stack()[1][3]

    '''
    def param_xml(self):
        comp = Element('Comp')
        comp.attrib['Name'] = self._module_name
        param_list = Element('ParamList')
        comp.append(param_list)
        for p in self._param_list:
            param = Element('Param')
            param.attrib['Name'] = p.get_name()
            param.attrib['Type'] = str(p.get_type_name())
            param.attrib['Default'] = str(p.get_value())
            param.attrib['Value'] = str(p.get_value())
            if p.get_type() == eParamType.Select or p.get_type() == eParamType.File:
                #if param.attrib.has_key('Content'):
                param.attrib['Content'] = p.get_content()

            param_list.append(param)
        indent(comp)
        #dump(comp)
        return comp
    '''
    # ---------------------------------------------
    # Observer Pattern
    # ---------------------------------------------
    '''
    def get_subscribers(self, event):
        return self.events[event]

    def dispatch(self, event):
        for subscriber, callback in self.get_subscribers(event).items():
            callback()

    def register(self, event, who, callback):
        if self.get_subscribers(event).has_key(who):
            self.e("Multiple events are registed {}".format(event))
        self.get_subscribers(event)[who] = callback

    def unregister(self, event, who):
        del self.get_subscribers(event)[who]    
    '''

    '''
    Access Child Component
    '''

    def get_at(self, index):
        if index < len(self._child_comp_list):
            return self._child_comp_list[index]
        return None

    def get_child_list(self):
        return self._child_comp_list

    def set_parent(self, parent):
        self.parent = parent

    def add_comp(self, comp):
        # self.d('adding comp:{}'.format(comp.get_name()))
        self._child_comp_list.append(comp)
        comp.set_parent(self)
        return comp

    def get_ancestor(self):
        ancestor = []
        p = self.parent
        while p is not None:
            ancestor.append(p)
            p = p.parent
        if len(ancestor) == 0:
            return None
        return ancestor[::-1]  # reversed

    def get_all_child_list(self):
        comp_list = list()
        for comp in self.get_child_list():
            comp_list.append(comp)
            comp_list += self.__depth_first_search(comp)
        return comp_list

    @staticmethod
    def __depth_first_search(parent):
        comp_list = list()
        for comp in parent.get_child_list():
            comp_list.append(comp)
            comp_list += comp.__depth_first_search(comp)
        return comp_list

    '''
    def update_after_check(self, file_path):
        if file_path is None:
            return
        if os.path.isfile(file_path):
            tree = parse(file_path)
            comps = tree.getroot()
            if self.is_changed_param_struct(comps):
                self.d('Saving parameters')
                self.write_param_info(file_path)
            else:
                self.d('Updating parameters')
                self.update_param_info(comps)
                #dump(comps)
        else:
            self.d('Writing parameters')
            self.write_param_info(file_path)

    def update_param_info(self, comps):
        comp_list = comps.find('CompList')
        comp = comp_list.findall('Comp')
        f_updated = False
        for i, c in enumerate(comp):
            c1 = self.get_at(i)
            param_list = c.find('ParamList')
            param = param_list.findall('Param')
            for j, p in enumerate(param):
                p1 = c1.get_param_at(j)
                if p1.get_type() == eParamType.Text or p1.get_type() == eParamType.File :
                    if p1.get_value() != p.attrib['Value']:
                        f_updated = True
                        self.d('Update {} - {} : {}'.format(c1.get_name(), p1.get_name(), p.attrib['Value']))
                        p1.set_value(p.attrib['Value'])
                else:
                    if p1.get_value() != float(p.attrib['Value']):
                        f_updated = True
                        self.d('Update {} - {} : {}'.format(c1.get_name(), p1.get_name(), p.attrib['Value']))
                        p1.set_value(float(p.attrib['Value']))

        return f_updated

    def is_changed_param_struct(self, comps):
        comp_list = comps.find('CompList')
        comp = comp_list.findall('Comp')
        if len(self._child_comp_list) != len(comp):
            self.d('Changed the number of Comps.')
            return True

        for i, c in enumerate(comp):
            c1 = self.get_at(i)
            if c1 is None:
                return True

            param_list = c.find('ParamList')
            param = param_list.findall('Param')

            if c1.get_param_count() != len(param):
                self.d('Changed the number of Param')
                return True
            for j, p in enumerate(param):
                p1 = c1.get_param_at(j)
                if p.attrib['Name'] != p1.get_name():
                    self.d('Name is Change : {} => {}'.format(p.attrib['Name'], p1.get_name()))
                    return True
                if p.attrib['Type'] != str(p1.get_type_name()):
                    self.d('Type is Change : {} => {}'.format(p.attrib['Type'], str(p1.get_type_name())))
                    return True
                if (p1.get_type() == eParamType.Select or p1.get_type() == eParamType.File) and not p.attrib.has_key('Content'):
                    self.d('Content : NOT found')
                    return True
                elif (p1.get_type() == eParamType.Select or p1.get_type() == eParamType.File) and p1.get_content() != p.attrib['Content']:
                    self.d('Selection List is Change : {} => {}'.format(p.attrib['Content'], str(p1.get_content())))
                    return True
        #self._d('Param Info is NOT Changed')
        return False

    def write_param_info(self, file_path):
        if file_path is None:
            return
        if os.path.exists(file_path) is False:
            return

        components = self.get_param_info()
        ElementTree(components).write(file_path)

    def display_param_info(self):
        components = self.get_param_info()
        dump(components)

    def get_param_info(self):
        components = Element('Components')
        comp_list = Element('CompList')
        components.append(comp_list)
        for c in self._child_comp_list:
            comp = c.param_xml()
            comp_list.append(comp)
        return components
    '''
    # def LayoutInVisible(self, layout):
    #     for i in range(layout.count()):
    #         item = layout.itemAt(i)
    #         if item.widget() is not None:
    #             item.widget().setVisible(False)
    #             # item.setVisible(False)
    #             # item.parent = None
    #         else:
    #             self.LayoutInVisible(item)

    '''
    def LayoutInVisible(self, layout):
        for i in range(layout.count()):
            item = layout.itemAt(i)
            if item.widget() is not None:
                item.widget().setVisible(False)
                # item.setVisible(False)
                # item.parent = None
            elif item.layout() is not None:
                if item.spaceItem() is not None:
                    self.LayoutInVisible(item.layout())
            else:
                # raise Exception
                pass
    '''

    def __str__(self):
        msg = '{} : {}'.format(self.__class__.__name__, self._module_name)
        if len(self._child_comp_list) > 0:
            msg += "\n # child list :"
            for comp in self._child_comp_list:
                msg += "\n - " + comp.__str__()

        return msg


eParamType = enum('Value', 'Select', 'Text', 'File', 'Dict', 'Bool')


class CompParam(object):
    def __init__(self, name, type):
        self._name = name
        self._value = None
        self._type = type

    def get_type(self):
        return self._type

    def get_type_name(self):
        if self._type == eParamType.Select:
            return 'Select'
        elif self._type == eParamType.Text:
            return 'Text'
        elif self._type == eParamType.File:
            return 'File'
        elif self._type == eParamType.Dict:
            return 'Dict'
        elif self._type == eParamType.Bool:
            return 'Bool'
        # if self._type == eParamType.Value:
        return 'Value'

    def get_name(self):
        return self._name

    def set_value(self, value):
        self._value = value

    def get_value(self):
        return self._value

    def get_selected(self):
        return ''

    def get_content(self):
        return ''


class CompParamValue(CompParam):
    def __init__(self, name, default_value: float = 0.0):
        CompParam.__init__(self, name, eParamType.Value)
        self._value = default_value


class CompParamBool(CompParam):
    def __init__(self, name, default_value: bool = False):
        CompParam.__init__(self, name, eParamType.Bool)
        self._value = default_value


class CompParamDict(CompParam):
    def __init__(self, name, default_value=None):
        CompParam.__init__(self, name, eParamType.Dict)
        self._value = default_value


class CompParamSelect(CompParam):
    def __init__(self, name, *sel_names):
        CompParam.__init__(self, name, eParamType.Select)
        self._value = 0
        self._select = []
        for sel in sel_names:
            self._select.append(sel)

    def set_value(self, value):
        if value < len(self._select):
            self._value = value

    def get_value(self):
        return self._value

    def get_selected(self):
        if self._value < len(self._select):
            return self._select[self._value]

    def get_content(self):
        sel = ''
        for i, s in enumerate(self._select):
            if i != 0:
                sel += '#'
            sel += s
        return sel

    def get_selection_list(self):
        return self._select

    def set_selection(self, selection):
        if selection in self._select:
            idx = self._select.index(selection)
            self.set_value(idx)
            return True
        return False


class CompParamText(CompParam):
    def __init__(self, name, text):
        CompParam.__init__(self, name, eParamType.Text)
        self._value = text


class CompParamFile(CompParam):
    def __init__(self, name, file_path=None, filter=''):
        CompParam.__init__(self, name, eParamType.File)
        self._value = file_path
        self._filter = filter

    def get_content(self):
        return '{}#{}'.format(self._value, self._filter)

    '''
def indent(elem, level=0):
    i = "\n" + level*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

    '''