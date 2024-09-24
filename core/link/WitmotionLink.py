import asyncio
from bleak import BleakClient, BleakScanner
import threading

# WitMotion 센서 데이터를 수집하는 클래스
class WitMotionSensor(threading.Thread):
    def __init__(self, sensor_id, mac_address, data_char_uuid):
        super().__init__()
        self.sensor_id = sensor_id
        self.mac_address = mac_address
        self.data_char_uuid = data_char_uuid  # 센서 데이터 특성 UUID
        self.client = None
        self.running = False

    async def connect(self):
        self.client = BleakClient(self.mac_address)
        try:
            await self.client.connect()
            print(f"Connected to {self.mac_address} (Sensor ID: {self.sensor_id})")
            return True
        except Exception as e:
            print(f"Failed to connect to {self.mac_address}: {e}")
            return False

    async def read_data(self):
        try:
            if self.client.is_connected:
                # 센서로부터 데이터 읽기
                data = await self.client.read_gatt_char(self.data_char_uuid)
                return self.parse_data(data)
        except Exception as e:
            print(f"Error reading from {self.mac_address}: {e}")
        return None

    def parse_data(self, data):
        try:
            # 데이터 파싱 (WitMotion의 데이터 형식에 맞게 수정 필요)
            data_str = data.decode('utf-8').strip()
            data_values = data_str.split(',')
            if len(data_values) == 9:  # 예시: 가속도, 자이로스코프, 각도 데이터 가정
                parsed_data = {
                    "sensor_id": self.sensor_id,
                    "acceleration_x": float(data_values[0]),
                    "acceleration_y": float(data_values[1]),
                    "acceleration_z": float(data_values[2]),
                    "gyroscope_x": float(data_values[3]),
                    "gyroscope_y": float(data_values[4]),
                    "gyroscope_z": float(data_values[5]),
                    "angle_x": float(data_values[6]),
                    "angle_y": float(data_values[7]),
                    "angle_z": float(data_values[8]),
                }
                return parsed_data
        except Exception as e:
            print(f"Failed to parse data from {self.mac_address}: {e}")
        return None

    async def disconnect(self):
        if self.client is not None:
            await self.client.disconnect()
            print(f"Disconnected from {self.mac_address} (Sensor ID: {self.sensor_id})")

    # 각 센서에 대해 스레드를 실행
    def run(self):
        asyncio.run(self.collect_data())

    async def collect_data(self):
        if await self.connect():
            self.running = True
            while self.running:
                result = await self.read_data()
                if result:
                    print(f"Data from Sensor ID {result['sensor_id']}: {result}")
                await asyncio.sleep(1)  # 데이터 수집 주기 (1초)

            await self.disconnect()

    def stop(self):
        self.running = False


# BLE 장치를 검색하는 함수 (장치 MAC 주소를 찾기 위한 스캐너)
async def discover_ble_devices():
    devices = await BleakScanner.discover()
    for device in devices:
        print(f"Device: {device.name}, Address: {device.address}")


if __name__ == "__main__":
    # WitMotion 센서의 설정 (각 센서의 ID 및 BLE MAC 주소)
    sensor_configurations = [
        {'sensor_id': 1, 'mac_address': 'XX:XX:XX:XX:XX:XX'},  # 첫 번째 센서
        {'sensor_id': 2, 'mac_address': 'YY:YY:YY:YY:YY:YY'},  # 두 번째 센서
        {'sensor_id': 3, 'mac_address': 'ZZ:ZZ:ZZ:ZZ:ZZ:ZZ'}   # 세 번째 센서
    ]

    # WitMotion 센서의 데이터 특성 UUID (센서에 맞게 설정 필요)
    data_char_uuid = "0000xxxx-0000-1000-8000-00805f9b34fb"  # 예시 UUID (변경 필요)

    # 여러 WitMotion 센서로부터 데이터를 수집하는 스레드 실행
    sensors = [WitMotionSensor(config['sensor_id'], config['mac_address'], data_char_uuid) for config in sensor_configurations]

    try:
        # 각 센서를 위한 스레드 시작
        for sensor in sensors:
            sensor.start()

        # 메인 스레드에서는 다른 작업을 수행하거나 입력을 기다릴 수 있음
        input("Press Enter to stop...\n")

    finally:
        # 스레드 종료
        for sensor in sensors:
            sensor.stop()
            sensor.join()  # 스레드가 완전히 종료될 때까지 기다림