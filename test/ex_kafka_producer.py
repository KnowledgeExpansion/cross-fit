from kafka import KafkaProducer
import json
import time
import random

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka 서버 주소
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # 데이터를 JSON 형식으로 직렬화
    api_version=(0, 10)
)

# 임의의 센서 데이터를 생성하고 전송하는 함수
def send_sensor_data():
    while True:
        sensor_data = {
            'sensor_id': 'sensor_1',
            'temperature': random.uniform(20.0, 30.0),  # 임의의 온도 생성
            'humidity': random.uniform(40.0, 60.0),    # 임의의 습도 생성
            'timestamp': time.time()                   # 타임스탬프 추가
        }
        producer.send('sensor_data', sensor_data)  # 'sensor_data' 토픽으로 메시지 전송
        print(f"Sent: {sensor_data}")
        time.sleep(2)  # 2초마다 전송

if __name__ == "__main__":
    send_sensor_data()