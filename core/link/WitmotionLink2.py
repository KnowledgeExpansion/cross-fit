import asyncio
from bleak import BleakScanner, BleakClient
import threading

# Witmotion 장치 UUID 또는 이름 (실제 값으로 변경 필요)
WITMOTION_NAME = "WitMotion"


# 데이터를 처리하기 위한 Thread 클래스
class DataThread(threading.Thread):
    def __init__(self, device_name, device_address, client):
        threading.Thread.__init__(self)
        self.device_name = device_name
        self.device_address = device_address
        self.client = client
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    async def notification_handler(self, sender, data):
        # 수신된 데이터를 처리하는 함수
        print(f"{self.device_name}({self.device_address})에서 데이터 수신: {data}")

    async def connect_and_listen(self):
        async with self.client as client:
            print(f"{self.device_name}({self.device_address})에 연결됨")
            # 데이터 전송을 위한 Notification 구독
            await client.start_notify("YOUR_CHARACTERISTIC_UUID", self.notification_handler)

            while not self.stopped():
                await asyncio.sleep(1)

    def run(self):
        asyncio.run(self.connect_and_listen())


async def find_witmotion_devices():
    devices = await BleakScanner.discover()
    witmotion_devices = []

    # Witmotion 장치 찾기
    for device in devices:
        if WITMOTION_NAME in device.name:
            print(f"Witmotion 장치 발견: {device.name}, 주소: {device.address}")
            witmotion_devices.append(device)

    return witmotion_devices


def main():
    loop = asyncio.get_event_loop()

    # Witmotion 장치 검색
    witmotion_devices = loop.run_until_complete(find_witmotion_devices())

    if not witmotion_devices:
        print("Witmotion 장치를 찾지 못했습니다.")
        return

    threads = []

    # 각 Witmotion 장치에 대해 Thread를 생성하고 데이터를 수신
    for device in witmotion_devices:
        client = BleakClient(device.address)
        thread = DataThread(device.name, device.address, client)
        thread.start()
        threads.append(thread)

    try:
        while True:
            # 메인 스레드가 계속 실행되도록 유지
            pass
    except KeyboardInterrupt:
        # 프로그램 종료 시 스레드 정리
        print("프로그램 종료 중...")
        for thread in threads:
            thread.stop()
            thread.join()


if __name__ == "__main__":
    main()