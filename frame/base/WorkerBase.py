import time
import signal
import hashlib
import multiprocessing
from multiprocessing import Process

from DbgBase import dbg_setup
from CompBase import *

Q_EMPTY_DELAY = 0.1


class WorkerBase(CompBase, Process):
    def __init__(self, prefix=None, *args, **kwargs):
        CompBase.__init__(self, module_name=self.__class__.__name__, log_level=CompBase.LOG_DEBUG)
        Process.__init__(self)

        self.eventor = None
        self.shared_dict = None
        self.redis_rpc_client = None
        self.redis_rpc_server = None
        self.lane_prefix = prefix

    def register(self):
        raise NotImplementedError

    def first(self):
        raise NotImplementedError

    def loop(self):
        # raise NotImplementedError
        pass

    def terminate(self):
        raise NotImplementedError

    def set_eventor(self, eventor):
        self.eventor = eventor

    def set_shard_dict(self, shared_dict):
        self.shared_dict = shared_dict

    # def wait_worker(self):
    #     self.redis_rpc_client.rpc_wait_worker(self.__class__.__name__)

    def run(self):
        # ignore signal handlers from the parent.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        # Fixme: Move it to Configurations
        ROOT_PATH = ''
        PROJECT_NAME = ''
        LOGGING_CONSOLE = False
        dbg_setup(app_path='/'.join([ROOT_PATH, PROJECT_NAME]), module_name=self.get_name(),
                  project_name=PROJECT_NAME, dbg_console_on=LOGGING_CONSOLE)
        self.d("WorkerBase : Run {}".format(self.get_name()))

        self.register()

        self.eventor.init()
        self.eventor.set_lane_prefix(self.lane_prefix)
        self.eventor.set_terminate_callback(self.terminate)
        self.eventor.set_loop_callback(self.loop)

        self.first()
        # thread = threading.Thread(target=self.eventor.start_reactor, args=(self,))
        # thread.start()
        # thread.join()

        self.eventor.start_reactor(self)

    def stop(self):
        self.i("STOP 'WorkerBase'")

        self.eventor.stop_reactor()


class RedisWorkerEvent(CompBase, object):
    def __init__(self, delay=0.2):
        CompBase.__init__(self, module_name=self.__class__.__name__, log_level=CompBase.LOG_DEBUG)
        self._redis_event = None
        self._is_stop = multiprocessing.Event()

        # Use Manager.Event to work across child processes.
        # Commented out because eventor.loop does not work well for graceful termination at this time (2020.01.08)
        # self._is_stop = multiprocessing.Manager().Event()

        self.handler = dict()

        # Functions
        self.terminate = None
        self.loop = None
        self.delay = delay

        self.prefix = None

    def init(self):
        self._redis_event = RedisEvent(redis_conn)

    def set_loop_callback(self, func):
        self.loop = func

    def set_terminate_callback(self, func):
        self.terminate = func

    def set_lane_prefix(self, prefix):
        self.prefix = prefix

    def dispatch(self, item, prefix=""):
        item.prefix = prefix
        self._redis_event.put(item)

        # if settings.IS_REDIS_USED_GETSET:
        #     # self.d('GETSET function configuration: {}'.format(settings.IS_REDIS_USED_GETSET))
        #     # self.d('####### dispatch key type: {}'.format(item.type))
        #     # recv_BASE_JOB, recv_SPI_RESULT
        #     if item.type == 'recv_BASE_JOB':
        #         """ This kind of job is like KPOExport.txt """
        #         data = item.data
        #         lane_cd, job_name = data['SPI']['LaneCd'], data['SPI']['JobFileName']
        #         job_name = str(item.type) + '_' + str(job_name)
        #         self._redis_event.put_job_file(key=item.type, item=item)
        #
        #     elif item.type == 'set_KPO_JOB':
        #         """ dict keys(['JOB', 'JOB_CSV', 'JOB_FINGERPRINT', 'LaneCd']) """
        #         data = item.data
        #         lane_cd, job_name = data['LaneCd'], data['JOB']['PCBInfo']['PCBName']
        #         job_name = str(item.type) + '_' + str(job_name)
        #         self._redis_event.put_job_file(key=item.type, item=item)
        #
        #     # elif item.type == 'recv_SPI_RESULT' or item.type == 'recv_KPO_RESULT':
        #     #     _prefix = item.type.split('_')[1]
        #     #     data = item.data
        #     #     pcb_id, pcb_name = data['PCBINFO']['IDNO'], data['PCBINFO']['PCBNAME']
        #     #     pcb_id = str(_prefix) + str(pcb_id)
        #     #     self._redis_event.put_spi_kpo_result(key=item.type, item=item)
        #
        #     else:
        #         self._redis_event.put(item)
        # else:
        #     self._redis_event.put(item)

    def register(self, type, func):
        self.handler[type] = func

    def start_reactor(self, target_class):
        while not self._is_stop.is_set():
            time.sleep(self.delay)
            try:
                if self.loop:
                    self.loop()

                q_item = self._redis_event.get_message(timeout=Q_GET_TIMEOUT)
                if not q_item:
                    time.sleep(Q_EMPTY_DELAY)
                    continue

                # if settings.IS_REDIS_USED_GETSET:
                #     # self.d('GETSET function configuration: {}'.format(settings.IS_REDIS_USED_GETSET))
                #     if q_item.type == 'recv_BASE_JOB':
                #         get_data = self._redis_event.get_data(key=q_item.type)
                #         q_item.data = get_data
                #
                #     elif q_item.type == 'set_KPO_JOB':
                #         time.sleep(2)
                #         get_data = self._redis_event.get_data(key=q_item.type)
                #         q_item.data = get_data
                #
                #     elif q_item.type == 'recv_SPI_RESULT' or q_item.type == 'recv_KPO_RESULT':
                #         result_ind = q_item.data
                #         get_data = self._redis_event.get_data(key=q_item.type)
                #         self.d('### get_data keys: {}'.format(get_data.keys()))
                #         self.d('### get_data PADINFO lens: {}'.format(len(get_data['PADINFO'])))
                #         self.d('### get_data PCB IDNO: {}'.format(get_data['PCBINFO']['IDNO']))
                #         q_item.data = get_data

                # uuid = q_item.uuid

                prefix = q_item.prefix
                if prefix and self.prefix:
                    if prefix != self.prefix:
                        continue

                handle = self.handler.get(q_item.type)
                if not handle:
                    # self.d("'{}' is not resisted".format(q_item.type))
                    continue

                ret = handle(q_item)

            except Exception as ex:
                self.e('Exception[{}]: {}'.format(type(ex), ex))
                self.traceback()

        self.i('reactor stopped: {}'.format(self.__class__.__name__))
        if self.terminate:
            self.terminate()

    def stop_reactor(self):
        self.i("STOP 'stop_reactor': {}".format(self.__class__.__name__))

        self._is_stop.set()

    def add_event_queue(self, process_name=None):
        pass

    @classmethod
    def generate_job_index(cls, lane_cd, job_name):
        seed = '{},{}'.format(lane_cd, job_name)
        job_index = hashlib.sha256(seed.encode()).hexdigest()
        return job_index

    @classmethod
    def generate_result_index(cls, pcb_id, pcb_name):
        seed = '{},{}'.format(pcb_id, pcb_name)
        result_index = hashlib.sha256(seed.encode()).hexdigest()
        return result_index

# class CallbackQueueItem(object):
#     def __init__(self, type=None, data=None, uid=None):
#         self.uuid = uid or uuid.uuid4()
#         self.type = type
#
#         if data == None:
#             self.data = dict()
#         else:
#             self.data = data
#
#     def __str__(self):
#         return "{}[{}][{}] {}".format(self.__class__.__name__, self.uuid, self.type, self.data)