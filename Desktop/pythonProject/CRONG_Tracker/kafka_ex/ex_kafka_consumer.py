


from kafka import KafkaConsumer
import json

# Kafka Consumer 설정
consumer = KafkaConsumer(
    'sensor_data',  # 구독할 토픽 이름
    bootstrap_servers='localhost:9092',  # Kafka 서버 주소
    auto_offset_reset='earliest',  # 가장 오래된 메시지부터 시작
    enable_auto_commit=True,  # 메시지 수신 후 자동 커밋
    group_id='my-group',  # Consumer 그룹 ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # JSON 형식으로 역직렬화
    api_version=(0, 10)
)

# 메시지를 수신하고 출력하는 함수
def consume_sensor_data():
    for message in consumer:
        sensor_data = message.value
        print(f"Received: {sensor_data}")

if __name__ == "__main__":
    consume_sensor_data()