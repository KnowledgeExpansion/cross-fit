import json
from core.main import CrotConfiguration
from frame.base.DbgBase import DbgConfigNormal, dbg_setup
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError


module_name = 'KafkaMgr'
dbg_kfk = DbgConfigNormal(logger_name='root.' + module_name)
dbg_setup(app_path=CrotConfiguration.ROOT_PATH, module_name=module_name,
          dbg_console_on=False, dbg_config=dbg_kfk)
KAFKA_IP_PORT = CrotConfiguration.KAFKA_ADDR + ':' + CrotConfiguration.KAFKA_PORT


def check_kafka_conn(duration=10):
    """Kafka 클러스터에 연결하여 ping 테스트 수행"""
    try:
        # Kafka Consumer 생성
        conf = {'bootstrap.servers': KAFKA_IP_PORT,
                'group.id': 'python-consumer-group', 'auto.offset.reset': 'earliest'}

        consumer = Consumer(conf)
        consumer.list_topics(timeout=duration)
        dbg_kfk.i(module_name, 'Successfully connected to Kafka cluster!')

        cluster_metadata = consumer.list_topics(timeout=duration)
        dbg_kfk.i(module_name, 'Cluster Metadata'.format(cluster_metadata))
        consumer.close()

    except KafkaException as e:
        dbg_kfk.e(module_name, 'Error connecting to Kafka: {}'.format(e))
    except KafkaError as e:
        dbg_kfk.e(module_name, 'Kafka Error: {}'.format(e))
    except Exception as e:
        dbg_kfk.e(module_name, 'Unexpected error: {}'.format(e))


class KafkaMgr(object):
    def __init__(self, group_id: str = None, interface=None):
        self.bootstrap_servers = KAFKA_IP_PORT
        self.group_id = group_id
        self.interface = interface or self

        self.call_list = set()
        self.producer = self.create_producer()
        self.consumer = self.create_consumer()

    def call_register(self, func):
        self.call_list.add(func.__name__)

    def create_producer(self) -> Producer:
        conf = {'bootstrap.servers': KAFKA_IP_PORT, 'client.id': 'python-kafka-producer'}
        return Producer(conf)

    def create_consumer(self) -> Consumer:
        conf = {'bootstrap.servers': KAFKA_IP_PORT,
                'group.id': self.group_id if self.group_id else 'python-kafka-consumer-group',
                'auto.offset.reset': 'earliest'}
        return Consumer(conf)

    def produce_message(self, topic: str, key: str, value: dict) -> None:
        try:
            # 메시지 전송
            self.producer.produce(topic, key=key, value=json.dumps(value), callback=self.delivery_report)
            self.producer.flush()
            dbg_kfk.i(module_name, 'Message sent to topic {}: {} --> {}'.format(topic, key, value))
        except KafkaException as e:
            dbg_kfk.traceback(module_name, 'Error sending message to Kafka: {}'.format(e))

    def delivery_report(self, err, msg):
        """Producer의 콜백 함수: 메시지가 성공적으로 전송되었는지 확인"""
        if err is not None:
            dbg_kfk.traceback(module_name, 'Message delivery failed: {}'.format(err))
        else:
            dbg_kfk.d(module_name, 'Message delivered to {} [{}] at offset {}'.format(
                msg.topic(), msg.partition(), msg.offset()))

    def consume_messages(self, topic: str, timeout: int = 1.0) -> None:
        """Kafka Consumer를 사용해 메시지 소비

        :param topic: 소비할 Kafka 토픽
        :param timeout: 메시지를 기다리는 시간 (초)
        """
        try:
            self.consumer.subscribe([topic])
            dbg_kfk.i(module_name, 'Consuming messages from topic: {}'.format(topic))

            while True:
                msg = self.consumer.poll(timeout=timeout)
                if msg is None:
                    continue  # 메시지가 없으면 다시 시도
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        dbg_kfk.i(module_name, 'End of partition reached: {} [{}] at offset {}'.format(
                            msg.topic(), msg.partition(), msg.offset()))
                    else:
                        dbg_kfk.traceback(module_name, '{}'.format(msg.error()))
                        raise KafkaException(msg.error())
                else:
                    # 메시지 파싱
                    _key = msg.key().decode('utf-8')
                    _value = json.loads(msg.value().decode('utf-8'))

                    if _key not in self.call_list:
                        dbg_kfk.e(module_name, 'Not func: {}'.format(_key))
                        return False

                    func = getattr(self.interface, '{}'.format(_key))
                    ret = func(_value)
                    dbg_kfk.i(module_name, 'Received message: {} --> {}'.format(_key, _value))

                    return ret
        except KeyboardInterrupt:
            dbg_kfk.traceback(module_name, 'Consuming stopped.')

        finally:
            self.consumer.close()

    def get_offsets(self, topic: str) -> None:
        """Kafka 토픽의 오프셋 확인"""
        try:
            partitions = self.consumer.list_topics(topic).topics[topic].partitions
            for partition in partitions.values():
                offset = self.consumer.position(partition)
                dbg_kfk.i(module_name, 'Topic: {}, Partition: {}, Offset: {}'.format(topic, partition, offset))

        except KafkaException as e:
            dbg_kfk.traceback(module_name, 'Error getting offsets for topic {}: {}'.format(topic, e))

    def close(self):
        """Kafka Producer와 Consumer 종료"""
        self.producer.flush()  # Producer 종료
        self.consumer.close()   # Consumer 종료
        dbg_kfk.i(module_name, 'Kafka manager closed.')


if __name__ == "__main__":
    kafka_manager = KafkaMgr(group_id='python-consumer-group')

    # 메시지 전송 예시
    kafka_manager.produce_message(topic='test_topic', key='user:1', value={'name': 'Alice', 'age': 30})

    # 메시지 소비 예시
    kafka_manager.consume_messages(topic='test_topic')

    # 오프셋 조회 예시
    kafka_manager.get_offsets(topic='test_topic')

    # Kafka 관리 리소스 종료
    kafka_manager.close()