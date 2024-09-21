# """ pybluez: with bluetooth library (recommended: witmotion) """
# import pywitmotion as wit
# import bluetooth
#
# # set your device's address
# imu = "00:0C:BF:02:1E:40"
#
# # Create the client socket
# socket = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
# socket.connect((imu, 1))
#
# msgs_num = 0
# while msgs_num < 100:
#     data = socket.recv(1024)
#     # split the data into messages
#     data = data.split(b'U')
#     for msg in data:
#         q = wit.get_quaternion(msg)
#         # q = wit.get_magnetic(msg)
#         # q = wit.get_angle(msg)
#         # q = wit.get_gyro(msg)
#         # q = wit.get_acceleration(msg)
#         if q is not None:
#             msgs_num = msgs_num+1
#             print(q)
# socket.close()
#
#
# """ pybluez: with serial library (recommended: witmotion) """
# import serial
# import time
# import pywitmotion as wit
#
# connected = False
# port = '/dev/rfcomm0'
# baud = 115400
#
# with serial.Serial(port, baud, timeout=5) as ser:
#     s = ser.read()
#
#     msgs_num = 0
#     while msgs_num < 100:
#         start = time.time()
#         s = ser.read_until(b'U')
#         q = wit.get_quaternion()
#         # q = wit.get_magnetic(msg)
#         # q = wit.get_angle(msg)
#         # q = wit.get_gyro(msg)
#         # q = wit.get_acceleration(msg)
#         if q is not None:
#             msgs_num = msgs_num+1
#             print(q)


""" bleak: to compare with pybluez """
# To discover Bluetooth devices that can be connected to:

import asyncio
from bleak import BleakScanner


# async def main():
#     devices = await BleakScanner.discover()
#     for d in devices:
#         print("address: {}, name: {}, uuid: {}".format(d.address, d.name, d.metadata))
#
# asyncio.run(main())

# Connect to a Bluetooth device and read its model number:

# import asyncio
# from bleak import BleakClient
#
# address = "CB:50:A0:D2:F4:16"
# # MODEL_NBR_UUID = "2A24"
# MODEL_NBR_UUID = '0000ffe5-0000-1000-8000-00805f9a34fb'
#
# async def main(address):
#     async with BleakClient(address) as client:
#         model_number = await client.read_gatt_char(MODEL_NBR_UUID)
#         print("Model Number: {0}".format("".join(map(chr, model_number))))
#
# asyncio.run(main(address))


from typing import Tuple
from imu_visualizer import ImuVisualizer
from util.app import App
from wit_motion_sensor import WitMotionSensor

def main():
    app = App()
    visualizer = ImuVisualizer(app)
    visualizer.start(on_stop=app.stop)

    def on_update(data: Tuple[float,float,float]):
        visualizer.rotation = data
    def on_sensor_terminated(e: Exception):
        print("Sensor terminated:", e)
        visualizer.stop()
    sensor = WitMotionSensor("EA:78:B5:4D:E3:21", app, on_update, on_sensor_terminated)
    sensor.start(calibration=True)

    app.run()
    sensor.stop()


if __name__ == "__main__":
    main()


