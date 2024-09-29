import asyncio
from bleak import BleakScanner, BleakClient
from frame.base.WorkerBase import WorkerBase
import threading

# Witmotion 장치 UUID 또는 이름 (실제 값으로 변경 필요)
WITMOTION_NAME = "WitMotion"


# 데이터를 처리하기 위한 Thread 클래스
class WitmotionThread(threading.Thread):
    def __init__(self, server_id, device_name, device_address, client):
        threading.Thread.__init__(self)
        self.server_id = server_id
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


class WitmotionDevices(WorkerBase):
    def __int__(self):
        WorkerBase.__init__(self, prefix=None)
        self.event_loop = asyncio.get_event_loop()
        self.witmotion_threads = []

    def first(self):
        self.event_loop.run_until_complete(self.find_witmotion_devices())

        if len(self.witmotion_threads) == 0:
            self.w('Can not find any witmotion devices')

    def register(self):
        pass

    def loop(self):
        try:
            self.redis_rpc_server.check_messages()
        except Exception as _ex:
            self.traceback()

    def run(self):
        # 각 Witmotion 장치에 대해 Thread를 생성하고 데이터를 수신
        for device in self.witmotion_threads:
            client = BleakClient(device.address)
            witmotion_thread = WitmotionThread(device.name, device.address, client)
            witmotion_thread.start()
            self.witmotion_threads.append(witmotion_thread)

    def terminate(self):
        self.i('Terminated WitmotionDevices !!!')

    def stop(self):
        self.i('Stop WitmotionDevices !!!')
        try:
            for w_thread in self.witmotion_threads:
                w_thread.stop()
                w_thread.join()

        except Exception as ex:
            self.traceback(ex)

        try:
            WorkerBase.stop(self)
        except Exception as ex:
            self.traceback(ex)

    async def find_witmotion_devices(self):
        devices = await BleakScanner.discover()

        # Witmotion 장치 찾기
        for device in devices:
            if WITMOTION_NAME in device.name:
                self.i('Find Witmotion - Name: {} / Addr: {}'.format(device.name, device.address))
                self.witmotion_threads.append(device)

# def main():
#     loop = asyncio.get_event_loop()
#
#     # Witmotion 장치 검색
#     witmotion_devices = loop.run_until_complete(find_witmotion_devices())
#
#     if not witmotion_devices:
#         print("Witmotion 장치를 찾지 못했습니다.")
#         return
#
#     threads = []
#
#     # 각 Witmotion 장치에 대해 Thread를 생성하고 데이터를 수신
#     for device in witmotion_devices:
#         client = BleakClient(device.address)
#         thread = DataThread(device.name, device.address, client)
#         thread.start()
#         threads.append(thread)
#
#     try:
#         while True:
#             # 메인 스레드가 계속 실행되도록 유지
#             pass
#     except KeyboardInterrupt:
#         # 프로그램 종료 시 스레드 정리
#         print("프로그램 종료 중...")
#         for thread in threads:
#             thread.stop()
#             thread.join()


if __name__ == "__main__":
    main()