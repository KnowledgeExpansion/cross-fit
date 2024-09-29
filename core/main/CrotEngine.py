from multiprocessing import freeze_support
from datetime import datetime
from frame.base.DbgBase import dbg, dbg_setup
from core.kafka.KafkaMgr import redis_conn, RedisRPCServer, RedisRPCClient, \
    check_redis_conn, subprocess_redis, lane_prefix, del_redis_crt

import CrotConfiguration
import signal
import sys
import os
import time
import threading
import glob  # to get directory name using wildcard
import json
from multiprocessing import Process, current_process


class KafkaRPCInterface(object):
    def __init__(self, crot_manager):
        self.crot_manager = crot_manager

    def stop_crot_manager(self):
        print("stop_crot_manager")
        self.crot_manager.stop()


def run_kafka_rpc_server(is_stop, kafka_rpc_server):
    while not is_stop.is_set():
        try:
            kafka_rpc_server.check_messages()
        except Exception as ex:
            print(ex)
        time.sleep(1)


def main():
    def signal_handler(signal, frame):
        print('You pressed Ctrl+C! {}'.format(current_process()))
        redis_rpc_client = KafkaRPCInterface(redis_conn)
        redis_rpc_client.kpolink_send_pri_kpoend({"PRINTER": {"Name": "PRINTER_FRONT"}})
        time.sleep(2)
        redis_rpc_server_is_stop.set()
        kpo_manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)  # SIGINT: To terminate by Ctrl + C in terminal
    signal.signal(signal.SIGTERM, signal_handler)  # SIGTERM: Request END to process (graceful shutdown)

    module_name = 'CrotEngine'
    freeze_support()
    now = datetime.now()
    os.environ['POM_START_TIME'] = now.strftime('%Y%m%d_%H%M%S')
    os.environ['POM_START_TIME_STAMP'] = str(int(now.timestamp()))
    os.environ['KPOENGINE_PID'] = str(os.getpid())
    dbg_setup(app_path=CrotConfiguration.ROOT_PATH, module_name=module_name, dbg_console_on=False)

    dbg.i(module_name, "KPOengine PID: {}".format(os.getpid()))

    # Start Redis Server
    # if not is_linux():
    #     p = Process(target=subprocess_redis, args=())
    #     p.start()

    # Check Connection to Redis Server
    check_redis_conn(host=CrotConfiguration.KAFKA_ADDR, port=CrotConfiguration.KAFKA_PORT, wait=True)

    # Check Connection to Redis SG Server
    # check_redis_sg(host=settings.REDIS_ADDR, port=settings.REDIS_PORT, wait=True)

    # Flush Redis all data
    # TODO
    # redis_conn.flushall() # Lane1, Lane2 All Remove
    del_redis_crt('all', prefix=lane_prefix(LANE_ID_OPT))

    dbg.d(module_name, "Connecting to redis")

    from kpo.solution.prt.link.KpoManager import KpoManager
    from kpo.solution.prt.service.RestServer import RestServer

    # from kpo.solution.prt.link.SimKpoServer import SimKpoServer

    from kpo.solution.prt.service.KpoServer import KpoServer
    from kpo.solution.prt.service.PomServer import PomServer
    from kpo.solution.prt.service.OffsetServer import OffsetServer
    from kpo.solution.prt.service.PcmServer import PcmServer
    from kpo.solution.prt.service.PdmServer import PdmServer
    from kpo.solution.prt.service.PamServer import PamServer
    from kpo.solution.prt.service.RepeaterServer import RepeaterServer

    dbg.d(module_name, "settings: {}".format(CrotConfiguration.conf))

    kpo_manager = KpoManager()
    # kpo_manager.add_server(SimKpoServer())
    if settings.ENABLE_KPO_SRV:
        dbg.i(module_name, "ENABLE_KPO_SRV")
        kpo_manager.add_server(KpoServer())
    if settings.ENABLE_OFFSET_SRV:
        dbg.i(module_name, "ENABLE_OFFSET_SRV")
        kpo_manager.add_server(OffsetServer())
    if settings.ENABLE_POM_SRV:
        dbg.i(module_name, "ENABLE_POM_SRV")
        kpo_manager.add_server(PomServer())
    if settings.ENABLE_PCM_SRV:
        dbg.i(module_name, "ENABLE_PCM_SRV")
        kpo_manager.add_server(PcmServer())
    if settings.ENABLE_PDM_SRV:
        dbg.i(module_name, "ENABLE_PDM_SRV")
        kpo_manager.add_server(PdmServer())
    if settings.ENABLE_PAM_SRV:
        dbg.i(module_name, "ENABLE_PAM_SRV")
        kpo_manager.add_server(PamServer())
    if settings.ENABLE_REST_SRV:
        dbg.i(module_name, "ENABLE_REST_SRV")
        kpo_manager.add_server(RestServer())

    # Redis RPC Server
    redis_rpc_server_is_stop = threading.Event()
    redis_rpc_interface = RedisRPCInterface(kpo_manager)
    redis_rpc_server = RedisRPCServer(redis_conn, interface=redis_rpc_interface, prefix=[lane_prefix(LANE_ID_OPT)])
    redis_rpc_server.call_register(redis_rpc_interface.stop_kpo_manager)
    redis_thread = threading.Thread(target=lambda: run_redis_rpc_server(redis_rpc_server_is_stop, redis_rpc_server))
    redis_thread.daemon = True
    redis_thread.start()

    # Start KPO Manager
    kpo_manager.start()

    # Do Something
    print("KPO Started")
    dbg.d(module_name, "KPO Started")

    # Finish KPO Manager
    kpo_manager.join()
    # time.sleep(10)

    redis_rpc_server_is_stop.set()
    # kpo_manager.stop()


if __name__ == "__main__":
    # signal.signal(signal.SIGINT, signal_handler)
    # signal.pause()

    main()
