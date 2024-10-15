import time
import hashlib
import pickle
import multiprocessing
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from KafkaMgr_ex2 import KAFKA_IP_PORT
from frame.base.CompBase import *

Q_EMPTY_DELAY = 0.1
Q_GET_TIMEOUT = 1


class KafkaBase(object):
    """ sg means set_get """
    def __init__(self, kafka):
        self.kafka_conn = kafka

    def _serialize(self, data):
        self.pickle_protocol = pickle.HIGHEST_PROTOCOL
        return pickle.dumps(data, protocol=self.pickle_protocol)

    def _unserialize(self, data):
        return pickle.loads(data) if data else None


class KafkaEvent(KafkaBase):
    def __init__(self, kafka, *args, **kwargs):
        super(KafkaBase, self).__init__(kafka)
        self.event_consumer = self.kafka_conn.pu
        self.pubsub.subscribe(self.channel)

    def put(self, data):
        serialized_data = self._serialize(data)
        self.redis_conn.publish(self.channel, serialized_data)

    def get_data(self, key):
        # data = self.redis_conn.get(key)
        data = get_redis_crt(key, prefix=lane_prefix(LANE_ID_OPT))
        get_data = self._unserialize(data)

        return get_data

    def get_message(self, timeout=0):
        ret = None
        message = self.pubsub.get_message(timeout=timeout)
        if message:
            # print "message: ", message
            _type = message['type']
            _channel = message['channel']
            _serialized_data = message['data']

            if _type == "subscribe":
                return False
            elif _type == "message":
                data = self._unserialize(_serialized_data)
                return data
            else:
                return False

        return ret


class KafkaWorkerEvent(CompBase):
    def __init__(self, delay=0.2):
        CompBase.__init__(self, module_name=self.__class__.__name__, log_level=CompBase.LOG_DEBUG)
        self._kafka_event = None
        self._is_stop = multiprocessing.Event()

        # Use Manager.Event to work across child processes.
        # Commented out because eventor.loop does not work well for graceful termination at this time (2020.01.08)
        # self._is_stop = multiprocessing.Manager().Event()

        self.handler = dict()
        self.producer = self.create_producer()
        self.consumer = self.create_consumer()

        # Functions
        self.terminate = None
        self.loop = None
        self.delay = delay
        self.prefix = None

    def init(self):
        # TODO: Kafka Event
        self._kafka_event = KafkaEvent()

    def set_loop_callback(self, func):
        self.loop = func

    def set_terminate_callback(self, func):
        self.terminate = func

    def set_lane_prefix(self, prefix):
        self.prefix = prefix

    def dispatch(self, item, prefix=""):
        item.prefix = prefix
        self._kafka_event.put(item)

    def register(self, type, func):
        self.handler[type] = func

    def start_reactor(self, target_class):
        while not self._is_stop.is_set():
            time.sleep(self.delay)
            try:
                if self.loop:
                    self.loop()

                q_item = self._kafka_event.get_message(timeout=Q_GET_TIMEOUT)
                if not q_item:
                    time.sleep(Q_EMPTY_DELAY)
                    continue

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
