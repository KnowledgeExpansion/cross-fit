import os
import warnings
warnings.simplefilter("ignore", Warning)

from frame.base.DbgBase import dbg_setup
from frame.base.CompBase import *
from frame.base.WorkerBase import *
from core.kafka.KafkaWorkerEvent import KafkaWorkerEvent
from core.link.WitmotionLink import WitMotionSensor
# import multiprocessing
import signal


class CrotManager(CompBase):
    def __init__(self, addr="", port=""):
        CompBase.__init__(self, module_name=self.__class__.__name__, log_level=CompBase.LOG_DEBUG)

        self._servers = list()
        self._servers_pid = list()
        self._mpevent = KafkaWorkerEvent()

        self._kpo_link = KpoLink(addr=addr, port=port)
        self._mpevent.add_event_queue(self._kpo_link.get_name())
        self._kpo_link.set_eventor(self._mpevent)

        # self.add_comp(self._kpo_link)

    def add_server(self, server):
        self._mpevent.add_event_queue(server.get_name())
        server.set_eventor(self._mpevent)
#        server.set_shared_dict(self.shared_dict)

        self._servers.append(server)
        # self.add_comp(server)

    def start(self):
        # KPO Link
        # del self._mpevent.mp_manager
        self._kpo_link.start()
        self._kpo_link_pid = self._kpo_link.pid
        time.sleep(1)

        # Servers
        for server in self._servers:
            self.d("{}: Start".format(server))
            server.start()
            self._servers_pid.append(server.pid)
            self.d("{}: Started ({})".format(server, server.pid))

    def stop(self):
        # KPO Link
        self._kpo_link.stop()

        # Servers
        for server in self._servers:
            server.stop()

        os.kill(self._kpo_link.pid, signal.SIGKILL)

        for server in self._servers:
            os.kill(server.pid, signal.SIGKILL)

    def join(self):
        # KPO Link
        self._kpo_link.join()

        # Servers
        for server in self._servers:
            server.join()
