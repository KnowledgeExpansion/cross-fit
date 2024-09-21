import asyncio
from bleak import BleakClient

mac_address = "CB:50:A0:D2:F4:16"


def notification_handler(sender, data: bytearray):
    decoded = [int.from_bytes(data[i:i+2], byteorder='little', signed=True) for i in range(2, len(data), 2)]
    ax = decoded[0] / 32768.0 * 16 * 9.8
    ay = decoded[1] / 32768.0 * 16 * 9.8
    az = decoded[2] / 32768.0 * 16 * 9.8
    wx = decoded[3] / 32768.0 * 2000
    wy = decoded[4] / 32768.0 * 2000
    wz = decoded[5] / 32768.0 * 2000
    roll = decoded[6] / 32768.0 * 180
    pitch = decoded[7] / 32768.0 * 180
    yaw = decoded[8] / 32768.0 * 180
    print(f"ax: {ax:.3f}, ay: {ay:.3f}, az: {az:.3f}, wx: {wx:.3f}, wy: {wy:.3f}, wz: {wz:.3f}, "
          f"roll: {roll:.3f}, pitch: {pitch:.3f}, yaw: {yaw:.3f}")

async def run(address, loop):
    async with BleakClient(address, loop=loop) as client:
        x = client.is_connected
        print("Connected: {0}".format(x))
        # 0000ffe4-0000-1000-8000-00805f9a34fb
        await client.start_notify("0000ffe4-0000-1000-8000-00805f9a34fb", notification_handler)

        while True:
            await asyncio.sleep(1)

loop = asyncio.get_event_loop()
loop.run_until_complete(run(mac_address, loop))

