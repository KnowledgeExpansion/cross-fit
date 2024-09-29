import logging
import os.path
import os
import platform
import traceback
import sys
import inspect

from datetime import datetime
from future.utils import with_metaclass
from utils import Singleton

try:
    from win32api import OutputDebugString
except ImportError:
    pass

from logging import currentframe


# output "logging" messages to DbgView via OutputDebugString (Windows only!)
# utputDebugString = ctypes.windll.kernel32.OutputDebugStringW


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def get_func_name():
    return inspect.stack()[1][3]


def findCallerPY3(stack_info):
    """
    Find the stack frame of the caller so that we can note the source
    file name, line number and function name.
    """
    import io

    f = currentframe()
    _srcfile = os.path.normcase(currentframe.__code__.co_filename)
    # On some versions of IronPython, currentframe() returns None if
    # IronPython isn't run with -X:Frames.
    if f is not None:
        f = f.f_back
    rv = "(unknown file)", 0, "(unknown function)"
    while hasattr(f, "f_code"):
        co = f.f_code
        filename = os.path.normcase(co.co_filename)
        if filename == _srcfile or co.co_name in ['d', 'i', 'w', 'e', 'n', 'traceback', 'DEBUG', 'INFO', 'WARN', 'WARNING', 'ERROR', 'TRACEBACK']:
            f = f.f_back
            continue
        sinfo = None
        if stack_info:
            sio = io.StringIO()
            sio.write('Stack (most recent call last):\n')
            traceback.print_stack(f, file=sio)
            sinfo = sio.getvalue()
            if sinfo[-1] == '\n':
                sinfo = sinfo[:-1]
            sio.close()
        rv = (co.co_filename, f.f_lineno, co.co_name, sinfo)
        break
    return rv


def getCurrentStackDump():
    """
    Find the stack frame of the caller so that we can note the source
    file name, line number and function name.
    """
    rvs = []
    f = currentframe()
    _srcfile = os.path.normcase(currentframe.__code__.co_filename)
    # On some versions of IronPython, currentframe() returns None if
    # IronPython isn't run with -X:Frames.
    if f is not None:
        f = f.f_back
    rv = "(unknown file)", 0, "(unknown function)"
    while f and hasattr(f, "f_code"):
        co = f.f_code
        filename = os.path.normcase(co.co_filename)
        rv = (co.co_filename, f.f_lineno, co.co_name)
        f = f.f_back
        rvs.append(rv)
    return rvs


class DbgViewHandler(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)

    def emit(self, record):
        try:
            OutputDebugString(self.format(record))
        except:
            pass


class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """

    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass


class ModuleNameFilter(logging.Filter):
    def filter(self, record):
        # override module_name from KPO to module
        if hasattr(record, 'module_name'):
            record.module = record.module_name
        return True


class DbgConfigNormal(object):
    log_name = None
    log_path = None

    def __init__(self, logger_name=None):
        self.logger_name = logger_name
        if logger_name is None:
            self.logger = logging.getLogger()
        else:
            self.logger = logging.getLogger(logger_name)
            self.logger.propagate = False

        # self.logger = logging.getLogger('root')
        self.logger.findCaller = findCallerPY3
        self.logger.addFilter(ModuleNameFilter())
        self.netlogger = logging.getLogger('root.netlog')
        self.netlogger.propagate = False
        self.netlogger.disabled = True
        self.netlogger.findCaller = findCallerPY3
        self.netlogger.addFilter(ModuleNameFilter())

        self.logger.setLevel(logging.DEBUG)
        self.clear()

        self.emit_func_map = {logging.DEBUG: self.d, logging.INFO: self.i, logging.WARN: self.w, logging.ERROR: self.e, logging.CRITICAL: self.n}

        self.fmt_detail_string = ('%(asctime)s [%(threadName)10s] %(levelname)-8s ' +
                                  '[%(funcName)-30s:%(lineno)4d] [%(process)6d] ' +
                                  '%(module)-10s %(message)s')
        self.fmt_string = ('%(asctime)s [%(threadName)10s] %(levelname)-8s ' +
                           '[%(funcName)-30s:%(lineno)4d] [%(process)6d,%(thread)6d] ' +
                           '%(module)-10s %(message)s')

        self._fmt_detail = logging.Formatter(fmt=self.fmt_detail_string)
        self._fmt = logging.Formatter(fmt=self.fmt_string)

        self._handlers = dict()  # dict of LogHandler
        self.setup_finished = False

    def set_formatters(self, fmt_detail_string=None, fmt_string=None):
        fmt_detail = None
        fmt = None

        if fmt_detail_string:
            fmt_detail = logging.Formatter(fmt=fmt_detail_string)
            self.fmt_detail_string = fmt_detail_string
        if fmt_string:
            fmt = logging.Formatter(fmt=fmt_string)
            self.fmt_string = fmt_string

        for name, handler in self._handlers.items():
            if fmt_detail and handler.formatter == self._fmt_detail:
                handler.setFormatter(fmt_detail)
            if fmt and handler.formatter == self._fmt:
                handler.setFormatter(fmt)

        self._fmt_detail = fmt_detail or self._fmt_detail
        self._fmt = fmt or self._fmt

    def enable_console(self):
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(self._fmt)
        self._handlers['console'] = handler
        self.logger.addHandler(handler)

    def disable_console(self):
        try:
            if 'console' in self._handlers:
                i = self.logger.handlers.index(self._handlers['console'])
                del self.logger.handlers[i]
                self._handlers.pop('console', None)
        except ValueError:
            pass

    def enable_debugview(self):
        # "OutputDebugString\DebugView"
        handler = DbgViewHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(self._fmt)
        self._handlers['ods'] = handler
        self.logger.addHandler(handler)

    def enable_file(self, file_name='dbg.log'):
        # File Handler
        from logging.handlers import RotatingFileHandler

        handler = RotatingFileHandler(filename=file_name)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(self._fmt_detail)
        self._handlers['file'] = handler
        self.logger.addHandler(handler)

    def set_file_handler_policy(self, maxBytes=None, backupCount=None, **kwargs):
        if 'file' in self._handlers:
            if maxBytes is not None:
                self._handlers['file'].maxBytes = maxBytes
            if backupCount is not None:
                self._handlers['file'].backupCount = backupCount

    def enable_netlog(self, **kwargs):
        if self.log_name and self.log_path:
            netlog_name = self.log_name.rstrip('.log')
            netlog_path = '{}/{}'.format(self.log_path, netlog_name)
            file_name = '{}/netlog-{}.log'.format(netlog_path, netlog_name)
            if not os.path.exists(netlog_path):
                os.makedirs(netlog_path)

            from logging.handlers import TimedRotatingFileHandler
            trfh = TimedRotatingFileHandler(file_name, **kwargs)
            trfh.setLevel(logging.INFO)
            trfh.setFormatter(self._fmt_detail)
            self._handlers['trfh'] = trfh
            self.netlogger.addHandler(trfh)
            self.netlogger.disabled = False

            self.i('DbgConfig', 'enabled netlog at {} with {}'.format(file_name, kwargs))

    def clear(self):
        while len(self.logger.handlers) > 0:
            h = self.logger.handlers[0]
            # log.debug('removing handler %s' % str(h))
            self.logger.removeHandler(h)
            # log.debug('%d more to go' % len(log.handlers))
            keys = [k for k, v in self._handlers.items() if v == h]
            for key in keys:
                self._handlers.pop(key, None)

    def amend_module_name(self, object_or_module_name):
        t = str(type(object_or_module_name))
        if t in ["<type 'str'>", "<class 'str'>"]:
            module_name = object_or_module_name
        elif 'class' in t or 'instance' in t:
            module_name = object_or_module_name.__class__.__name__
        else:
            module_name = t
        return module_name

    def d(self, module_name, msg, extra=dict()):
        # msg = '{:10} {}'.format(self.amend_module_name(module_name), msg)
        extra.update({'module_name': self.amend_module_name(module_name)})
        self.logger.debug(msg, extra=extra)

    def i(self, module_name, msg, extra=dict()):
        # msg = '{:10} {}'.format(self.amend_module_name(module_name), msg)
        extra.update({'module_name': self.amend_module_name(module_name)})
        self.logger.info(msg, extra=extra)

    def w(self, module_name, msg, extra=dict()):
        # msg = '{:10} {}'.format(self.amend_module_name(module_name), msg)
        extra.update({'module_name': self.amend_module_name(module_name)})
        self.logger.warning(msg, extra=extra)

    def e(self, module_name, msg, extra=dict()):
        # msg = '{:10} {}'.format(self.amend_module_name(module_name), msg)
        extra.update({'module_name': self.amend_module_name(module_name)})
        self.logger.error(msg, extra=extra)

    def n(self, module_name, msg, extra=dict()):
        # msg = '{:10} {}'.format(self.amend_module_name(module_name), msg)
        extra.update({'module_name': self.amend_module_name(module_name)})
        self.logger.critical(msg, extra=extra)

    def log(self, module_name, msg, log_level=logging.INFO, extra=dict()):
        emit_func = self.emit_func_map.get(log_level, self.e)
        emit_func(module_name, msg, extra=extra)

    def traceback(self, module_name, msg='', log_level=logging.ERROR, extra=dict()):
        traceback_string = traceback.format_exc()
        emit_func = self.emit_func_map.get(log_level, self.e)
        emit_func(self.amend_module_name(module_name),
                  '{0}\n{2}BEGIN-OF-TRACEBACK{2}\n{1}{2}END-OF-TRACEBACK{2}'.format(msg, traceback_string, '=' * 20), extra=extra)
        return traceback_string

    def netlog(self, module_name, msg, data='', extra=dict()):
        msg = '{:10} {} with data.len({})'.format(self.amend_module_name(module_name), msg, len(data))
        # self.logger.debug(msg)
        # self.netlogger.info(msg + '\n' + data + '\n')
        extra.update({'module_name': self.amend_module_name(module_name)})
        if not self.netlogger.disabled:
            self.logger.debug(msg, extra=extra)
            self.netlogger.info(msg + '\n' + data + '\n', extra=extra)


class DbgConfig(with_metaclass(Singleton, DbgConfigNormal)):
    def __init__(self):
        DbgConfigNormal.__init__(self)


dbg = DbgConfig()


def get_log_name_for_timestamp(project_name, module_name):
    log_name = ''
    if 'POM_START_TIME' in os.environ:
        log_name += os.environ.get('POM_START_TIME')
        if 'POM_START_TIME_STAMP' in os.environ:
            log_name += '_{}'.format(os.environ['POM_START_TIME_STAMP'])
    else:
        now = datetime.now()
        log_name += now.strftime('%Y%m%d_%H%M%S')
        log_name += '_{}'.format(int(now.timestamp()))
    log_name += '_' + project_name
    if module_name:
        log_name += '_' + module_name
    log_name += '.log'

    return log_name


def make_current_link(root_path, target_path, project_name, module_name=None):
    log_name = 'current'
    log_name += '_' + project_name
    if module_name:
        log_name += '_' + module_name

    log_current = os.path.join(root_path, log_name)

    if platform.system() == 'Linux':
        try:
            os.unlink(log_current)
        except:
            pass
        try:
            os.symlink(target_path, log_current)
        except:
            pass
    elif platform.system() == 'Windows':
        try:
            os.remove(log_current)
        except:
            pass
        try:
            import win32file
            win32file.CreateSymbolicLink(log_current, target_path)
        except:
            pass


def dbg_setup(app_path=None, dbg_console_on=False, dbg_view_on=False, module_name=None, project_name='crot', dbg_netlog=False, dbg_config=None):
    if not dbg_config:
        dbg_config = dbg  # use the singleton DbgConfig
        # print('dbg_setup with singleton DbgConfig', dbg)
    else:
        pass
        # print('dbg_setup with normal DbgConfig', dbg_config)

    if dbg_config and dbg_config.setup_finished:
        return False
    dbg_config.clear()
    if app_path is not None:
        log_name = get_log_name_for_timestamp(project_name, module_name)
        dbg_config.log_name = log_name
        dbg_config.log_path = os.path.join(app_path, 'log')
        if not os.path.exists(dbg_config.log_path):
            os.makedirs(dbg_config.log_path)
        log_file = os.path.join(dbg_config.log_path, dbg_config.log_name)
        dbg_config.enable_file(log_file)
        sys.stderr = StreamToLogger(logging.getLogger(), logging.WARN)  # redirect sys.stderr to DbgBase.WARN

        make_current_link(dbg_config.log_path, log_name, project_name, module_name)

    if dbg_console_on is True:
        dbg_config.enable_console()
    if dbg_view_on is True:
        dbg_config.enable_debugview()
    if dbg_netlog:
        dbg_config.enable_netlog(dbg_netlog)
    dbg_config.setup_finished = True
    return True
