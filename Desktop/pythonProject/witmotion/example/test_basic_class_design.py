class Sensor:
    def __init__(self, sensor_id, sensor_type):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type

    def generate_data(self):
        # 데이터 생성 로직 (임의의 값 생성)
        return {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "value": self.read_value()
        }

    def read_value(self):
        # 센서 데이터 생성 (임의의 값)
        import random
        return random.uniform(20.0, 100.0)


class HttpSensorAuthorization:
    def __init__(self, authorized_sensors):
        self.authorized_sensors = authorized_sensors

    def authorize(self, sensor_data):
        # 센서 데이터의 인증 여부를 확인
        sensor_id = sensor_data.get("sensor_id")
        if sensor_id in self.authorized_sensors:
            print(f"Sensor {sensor_id} is authorized")
            return True
        else:
            print(f"Sensor {sensor_id} is not authorized")
            return False


class RawDataWorker:
    def __init__(self, message_broker):
        self.message_broker = message_broker

    def process_data(self, sensor_data):
        # 메시지 브로커로 데이터를 전달
        self.message_broker.send_message(sensor_data)
        print(f"Data from {sensor_data['sensor_id']} processed and sent to broker")


class MessageBroker:
    def __init__(self):
        self.queue = []

    def send_message(self, data):
        # 데이터를 큐에 추가
        self.queue.append(data)

    def receive_message(self):
        # 큐에서 데이터를 꺼내옴
        if self.queue:
            return self.queue.pop(0)
        else:
            return None


class SSCS:
    def __init__(self, message_broker, database):
        self.message_broker = message_broker
        self.database = database

    def process_stream(self):
        # 메시지 브로커에서 데이터를 수신하고 처리
        data = self.message_broker.receive_message()
        if data:
            print(f"Processing stream for {data['sensor_id']}")
            self.database.store_data(data)


class SOQS:
    def __init__(self, database):
        self.database = database

    def query_data(self, sensor_id):
        # 데이터베이스에서 센서 데이터를 쿼리
        return self.database.get_data(sensor_id)


class MonitoringDashboard:
    def __init__(self, soqs_service):
        self.soqs_service = soqs_service

    def display_data(self, sensor_id):
        # 쿼리된 데이터를 시각화
        data = self.soqs_service.query_data(sensor_id)
        if data:
            print(f"Displaying data for {sensor_id}: {data}")
        else:
            print(f"No data available for {sensor_id}")


class PostgreSQLRDBMS:
    def __init__(self):
        self.data_store = {}

    def store_data(self, data):
        sensor_id = data['sensor_id']
        if sensor_id not in self.data_store:
            self.data_store[sensor_id] = []
        self.data_store[sensor_id].append(data)
        print(f"Data stored for {sensor_id}")

    def get_data(self, sensor_id):
        return self.data_store.get(sensor_id, [])


# 시스템 초기화
message_broker = MessageBroker()
database = PostgreSQLRDBMS()
http_auth = HttpSensorAuthorization(authorized_sensors=["sensor_A", "sensor_B"])
raw_worker = RawDataWorker(message_broker)
sscs = SSCS(message_broker, database)
soqs = SOQS(database)
dashboard = MonitoringDashboard(soqs)

# 센서 데이터 처리
sensor_A = Sensor("sensor_A", "temperature")
sensor_data_A = sensor_A.generate_data()

if http_auth.authorize(sensor_data_A):
    raw_worker.process_data(sensor_data_A)

# 실시간 데이터 처리 및 저장
sscs.process_stream()

# 대시보드에서 데이터 조회
dashboard.display_data("sensor_A")