import asyncio
from bleak import BleakClient


address = 'CB:50:A0:D2:F4:16'

async def run(address, loop):
    async with BleakClient(address, loop=loop) as client:
        x = client.is_connected
        print("Connected: {}".format(x))
        for index, service in enumerate(client.services):
            print(f" index: {index}, {service.uuid}: {service.description}: {[f'{c.properties},{c.uuid}' for c in service.characteristics]}")

loop = asyncio.get_event_loop()
loop.run_until_complete(run(address, loop))
