from multiprocessing import freeze_support
from datetime import datetime
from frame.base.DbgBase import dbg, dbg_setup
from core.kafka.KafkaMgr_ex2 import KafkaMgr, check_kafka_conn

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
            kafka_rpc_server.consume_messages()
        except Exception as ex:
            print(ex)
        time.sleep(1)


def main():
    def signal_handler(signal, frame):
        print('You pressed Ctrl+C! {}'.format(current_process()))
        kafka_rpc_server_is_stop.set()
        crot_manager.stop()
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

    dbg.i(module_name, "CrotEngine PID: {}".format(os.getpid()))

    # Start Redis Server
    # if not is_linux():
    #     p = Process(target=subprocess_redis, args=())
    #     p.start()

    # Check Connection to Redis Server
    check_kafka_conn()
    dbg.d(module_name, "Connecting to redis")

    from CrotManager import CrotManager
    from core.worker.SupervisoryWorker import SupervisoryWorker
    from core.worker.RawDataWorker import RawDataWorker
    from core.worker.DatabaseWorker import DatabaseWorker
    from core.worker.AlgorithmWorker import AlgorithmWorker
    from core.worker.WebDashboardWorker import WebDashboardWorker

    dbg.d(module_name, "settings: {}".format(CrotConfiguration.conf))

    crot_manager = CrotManager()
    dbg.i(module_name, "ENABLE SupervisoryWorker")
    crot_manager.add_server(SupervisoryWorker())
    dbg.i(module_name, "ENABLE RawDataWorker")
    crot_manager.add_server(RawDataWorker())
    dbg.i(module_name, "ENABLE DatabaseWorker")
    crot_manager.add_server(DatabaseWorker())
    dbg.i(module_name, "ENABLE AlgorithmWorker")
    crot_manager.add_server(AlgorithmWorker())
    dbg.i(module_name, "ENABLE WebDashboardWorker")
    crot_manager.add_server(WebDashboardWorker())

    # Kafka RPC Server
    kafka_rpc_server_is_stop = threading.Event()
    kafka_rpc_interface = KafkaRPCInterface(crot_manager)
    kafka_rpc_server = KafkaMgr(interface=kafka_rpc_interface)
    kafka_rpc_server.call_register(kafka_rpc_interface.stop_kpo_manager)
    kafka_thread = threading.Thread(target=lambda: run_kafka_rpc_server(kafka_rpc_server_is_stop, kafka_rpc_server))
    kafka_thread.daemon = True
    kafka_thread.start()

    # Start KPO Manager
    crot_manager.start()

    # Do Something
    print("CRoT Started")
    dbg.d(module_name, "CRoT Started")

    # Finish KPO Manager
    crot_manager.join()
    # time.sleep(10)

    kafka_rpc_server_is_stop.set()
    # kpo_manager.stop()


if __name__ == "__main__":
    # signal.signal(signal.SIGINT, signal_handler)
    # signal.pause()

    main()
