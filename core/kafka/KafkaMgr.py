import json
import time
import pickle
import subprocess
import os
import datetime
import inspect
import hashlib

from frame.base.DbgBase import DbgConfigNormal, dbg_setup
from core.main import CrotConfiguration
from kafka import KafkaProducer, KafkaConsumer
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError


module_name = 'KafkaMgr'
dbg_kafkamgr = DbgConfigNormal(logger_name='root.' + module_name)
dbg_setup(app_path=CrotConfiguration.ROOT_PATH, module_name=module_name, dbg_console_on=False, dbg_config=dbg_kafkamgr)

kafka_ip_port = CrotConfiguration.KAFKA_ADDR + ':' + CrotConfiguration.KAFKA_PORT
producer_conf = {'bootstrap.servers': kafka_ip_port, 'client.id': 'python-producer'}
consumer_conf = {'bootstrap.servers': kafka_ip_port, 'group.id': 'python-consumer-group',
                 'auto.offset.reset': 'earliest'}

producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)

_kafka_keys = dict()
_collection_types = dict()


# Decorators
def register(func):
    def wrapper():
        func()


def add_collection(_type, key):
    _collection_types[key] = _type


def del_collection(key):
    del _collection_types[key]


def consume_message():
    pass





add_collection(redis_collections.SyncableDict, 'job')
add_collection(redis_collections.SyncableDict, 'result')
add_collection(redis_collections.SyncableDict, 'kpo_status')
add_collection(redis_collections.SyncableDict, 'printer_status')
add_collection(redis_collections.SyncableDict, 'printer_info')
add_collection(redis_collections.SyncableDict, 'printer_condition')
add_collection(redis_collections.SyncableDict, 'opt_param')
add_collection(redis_collections.SyncableDict, 'print_param_pom')
add_collection(redis_collections.SyncableDict, 'print_param_pcm')
add_collection(redis_collections.SyncableDict, 'print_param_offset')
add_collection(redis_collections.SyncableDict, 'pom_module_status')
add_collection(redis_collections.SyncableDict, 'previous_initial_params')


def get_os_arch():
    try:
        os.environ["PROGRAMFILES(X86)"]
        bits = 64
    except:
        bits = 32
    return bits


def pub_protocol_log(msg, serializer=None):
    if serializer:
        msg = serializer(msg)
    redis_conn.publish('log_protocol', msg)


def pub_printer_status(msg, serializer=None):
    if serializer:
        msg = serializer(msg)
    redis_conn.publish('printer_status', msg)


def pub_chart_histogram(msg, serializer=None):
    if serializer:
        msg = serializer(msg)
    redis_conn.publish('chart_histogram', msg)


def pub_chart_offset(msg, serializer=None):
    if serializer:
        msg = serializer(msg)
    redis_conn.publish('chart_offset', msg)


def pub_param_table(msg, serializer=None):
    if serializer:
        msg = serializer(msg)
    redis_conn.publish('param_table', msg)


def check_redis_conn(host='localhost', port=8082, wait=True):
    def _check_redis_conn(host=host, port=port):
        try:
            redis_conn = redis.StrictRedis(host=host, port=port)
            redis_conn.ping()
        except redis.ConnectionError as ex:
            dbg_redismgr.d('KPOsim', "Retry Connecting to redis... ({})".format(ex))
            # print("Retry Connecting to redis... ({})".format(ex))
            return False

        return redis_conn

    if wait:
        while not _check_redis_conn(host=host, port=port):
            time.sleep(1)

    return _check_redis_conn(host=host, port=port)


class RedisBase(object):
    """ sg means set_get """

    def __init__(self, redis):
        self.redis_conn = redis

    def _serialize(self, data):
        self.pickle_protocol = pickle.HIGHEST_PROTOCOL
        return pickle.dumps(data, protocol=self.pickle_protocol)

    def _unserialize(self, data):
        return pickle.loads(data) if data else None


class RedisRPCBase(RedisBase):
    def __init__(self, redis):
        super(RedisRPCBase, self).__init__(redis)
        self.channel = "{}RedisRPC".format(lane_prefix(LANE_ID_OPT))


class RedisRPCServer(RedisRPCBase):
    def __init__(self, redis, interface=None, prefix=[""], *args, **kwargs):
        super(RedisRPCServer, self).__init__(redis)
        self.pubsub = self.redis_conn.pubsub()
        self._call_list = set()
        self.interface = interface or self
        prefix_list = prefix if isinstance(prefix, list) else [prefix]
        self.channels = list()

        for i in prefix_list:
            self.channels.append("{}.{}".format(i, self.channel))

        dbg_redismgr.d(module_name,
                       "RedisRPCServer prefix_list:{}, self.channels:{}".format(prefix_list, self.channels))

        for channel in self.channels:
            self.pubsub.subscribe(channel)

        # dbg_redismgr.d(module_name, "[{}:{}] channels: {}".format(inspect.stack()[1][1], inspect.stack()[1][2], self.channels))

    def call_register(self, func):
        self._call_list.add(func.__name__)

    def check_messages(self):
        ret = None
        message = self.pubsub.get_message()
        if message:
            # print "message: ", message
            _type = message['type']
            _channel = message['channel']
            _serialized_data = message['data']

            if _PY3:
                _channel = _channel.decode('utf-8')
            # dbg_redismgr.d(module_name, "channel: {}".format(_channel))
            # dbg_redismgr.d(module_name, "channels: {}".format(self.redis_conn.pubsub_channels()))

            if _type == "subscribe":
                return message
            elif _type == "message":
                data = self._unserialize(_serialized_data)
                # dbg_redismgr.d(module_name, "data: {}".format(data))

                _func = data['func']
                _args = data['args']
                _kwargs = data['kwargs']

                # dbg_redismgr.d(module_name, "[{}:{}] {} - {}.".format(
                #     inspect.stack()[1][1], inspect.stack()[1][2], _channel, _func))

                if _func not in self._call_list:
                    dbg_redismgr.e(module_name, "[{}:{}] {} is not in call list ({}).".format(
                        inspect.stack()[1][1], inspect.stack()[1][2], _func, self._call_list))
                    return False

                func = getattr(self.interface, "{}".format(_func))
                ret = func(*_args, **_kwargs)
                dbg_redismgr.d(module_name, "[{}:{}] RPC Run '{}'".format(
                    inspect.stack()[1][1], inspect.stack()[1][2], _func))
            else:
                return message

        return ret

    def start(self):
        pass


class RedisRPCClient(RedisRPCBase):
    def __init__(self, redis, prefix=[""], *args, **kwargs):
        super(RedisRPCClient, self).__init__(redis)
        prefix_list = prefix if isinstance(prefix, list) else [prefix]
        self.channels = list()
        for i in prefix_list:
            self.channels.append("{}.{}".format(i, self.channel))

        dbg_redismgr.d(module_name,
                       "RedisRPCClient prefix_list:{}, self.channels:{}".format(prefix_list, self.channels))

    def __call__(self, method, *args, **kwargs):
        # print method, args, kwargs
        data = {
            "func": method,
            "args": args,
            "kwargs": kwargs
        }

        serialized_data = self._serialize(data)

        for channel in self.channels:
            self.redis_conn.publish(channel, serialized_data)

        return

    def __getattr__(self, method):
        return lambda *args, **kargs: self(method, *args, **kargs)

    def call(self, method, *args, response_timeout=None, prefix=None, **kwargs):
        # print method, args, kwargs
        data = {
            "func": method,
            "args": args,
            "kwargs": kwargs
        }

        serialized_data = self._serialize(data)
        if prefix:
            prefix = prefix if isinstance(prefix, list) else [prefix]
            for p in prefix:
                channel = "{}.{}".format(p, self.channel)
                self.redis_conn.publish(channel, serialized_data)
                dbg_redismgr.d(module_name, "RedisMgr call ## truePre [{}:{}] {} - {}.".format(
                    inspect.stack()[1][1], inspect.stack()[1][2], channel, method))
        else:
            for channel in self.channels:
                self.redis_conn.publish(channel, serialized_data)
                dbg_redismgr.d(module_name, "RedisMgr call ## nonePre [{}:{}] {} - {}.".format(
                    inspect.stack()[1][1], inspect.stack()[1][2], channel, method))

        return


class RedisEvent(RedisBase):
    channel = "{}event_queue".format(lane_prefix(LANE_ID_OPT))

    def __init__(self, redis, *args, **kwargs):
        super(RedisEvent, self).__init__(redis)
        self.pubsub = self.redis_conn.pubsub()
        self.pubsub.subscribe(self.channel)

    def put(self, data):
        serialized_data = self._serialize(data)
        self.redis_conn.publish(self.channel, serialized_data)

    def get_data(self, key):
        # data = self.redis_conn.get(key)
        data = get_redis_crt(key, prefix=lane_prefix(LANE_ID_OPT))
        get_data = self._unserialize(data)

        return get_data

    def put_job_file(self, key, item):
        """ Ex. '05a72e89d38af821d326c52a94db1a4fa6f48fc9c4bcf5fc9b56f513cc198f9c' """
        """ Key: recv_BASE_JOB, set_KPO_JOB """
        if key == 'recv_BASE_JOB':
            data = item.data
            lane_cd, job_name = data['SPI']['LaneCd'], data['SPI']['JobFileName']
            job_name = str(key) + '_' + str(job_name)

            # Replace to job_index
            job_index = generate_job_index(lane_cd=lane_cd, job_name=job_name)
            item.data = job_index
            self.put(data=item)

            # Set Pickle File (large part) by pickle format
            encoder_data = self._serialize(data)
            set_redis_crt(key, encoder_data)
            # self.redis_conn.set(key, encoder_data)

        elif key == 'set_KPO_JOB':
            data = item.data
            lane_cd, job_name = data['LaneCd'], data['JOB']['PCBInfo']['PCBName']
            job_name = str(key) + '_' + str(job_name)

            # Set Pickle File (large part) by pickle format
            # TODO: Separate set_KPO_JOB data (JOB, JOB_CSV)
            """ data: dict keys(['JOB', 'JOB_CSV', 'JOB_FINGERPRINT', 'LaneCd']) """
            # for k, v in data.items():
            #     additional_key = str(key) + '_' + str(k)
            #     encoder_data = self._serialize(v)
            #     self.redis_conn.set(additional_key, encoder_data)
            #     time.sleep(0.2)
            encoder_data = self._serialize(data)
            set_redis_crt(key, encoder_data, prefix=lane_cd)
            # self.redis_conn.set(key, encoder_data)
            time.sleep(3)

            # Replace to job_index
            job_index = generate_job_index(lane_cd=lane_cd, job_name=job_name)
            item.data = job_index
            self.put(data=item)

    def put_spi_kpo_result(self, key, item):
        """ Ex. '05a72e89d38af821d326c52a94db1a4fa6f48fc9c4bcf5fc9b56f513cc198f9c' """
        """ Key: recv_SPI_RESULT, recv_KPO_RESULT """
        data = item.data
        pcb_id, pcb_name = data['PCBINFO']['IDNO'], data['PCBINFO']['PCBNAME']

        # Replace to result_index
        _prefix = key.split('_')[1]
        pcb_id = str(_prefix) + str(pcb_id)
        result_index = generate_result_index(pcb_id=pcb_id, pcb_name=pcb_name)
        item.data = result_index
        self.put(data=item)

        # Set result file (large part) by pickle format
        encoder_data = self._serialize(data)
        set_redis_crt(key, encoder_data)
        # self.redis_conn.set(key, encoder_data)

    def get_message(self, timeout=0):
        ret = None
        message = self.pubsub.get_message(timeout=timeout)
        if message:
            # print "message: ", message
            _type = message['type']
            _channel = message['channel']
            _serialized_data = message['data']

            if _PY3:
                _channel = _channel.decode('utf-8')

            if _type == "subscribe":
                return False
            elif _type == "message":
                data = self._unserialize(_serialized_data)
                return data
            else:
                return False

        return ret


if __name__ == '__main__':
    test_redis_sub("log_protocol")

# rc = RedisRPCClient(redis_conn)
# rc.ca('aa', {1:1})
#
# print rc.redis_conn
#
# df = pandas.DataFrame([[1,2,3],[4,5,6]])
# dict = get_redis('printer_status')

# with get_redis('opt_param') as dict:
#     dict.update({"a": "a", "n": []})
# dict['n'].append(1)
# dict.sync()
# key = dict.key
# print key
#
# print dict
#
# d = redis_collections.Dict(redis=redis_conn)
# d['a'] = 'a'
#
# redis_conn.publish('ca', {})
#
