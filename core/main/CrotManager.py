import os
import warnings
warnings.simplefilter("ignore", Warning)
from frame.base.WorkerBase import *
from core.kafka.KafkaWorkerEvent import KafkaWorkerEvent
from core.link.WitmotionLink2 import WitmotionDevices
import signal


class CrotManager(CompBase):
    def __init__(self):
        CompBase.__init__(self, module_name=self.__class__.__name__, log_level=CompBase.LOG_DEBUG)

        self._servers = list()
        self._servers_pid = list()
        self._mpevent = KafkaWorkerEvent()

        self._device_mgr = WitmotionDevices()
        self._mpevent.add_event_queue(self._device_mgr.get_name())
        self._device_mgr.set_eventor(self._mpevent)

    def add_server(self, server):
        self._mpevent.add_event_queue(server.get_name())
        server.set_eventor(self._mpevent)
#        server.set_shared_dict(self.shared_dict)

        self._servers.append(server)
        # self.add_comp(server)

    def start(self):
        # del self._mpevent.mp_manager
        self._device_mgr.start()
        self._device_mgr_pid = self._device_mgr.pid
        time.sleep(1)

        # Servers
        for server in self._servers:
            self.d("{}: Start".format(server))
            server.start()
            self._servers_pid.append(server.pid)
            self.d("{}: Started ({})".format(server, server.pid))

    def stop(self):
        self._device_mgr.stop()

        # Servers
        for server in self._servers:
            server.stop()

        os.kill(self._device_mgr.pid, signal.SIGKILL)

        for server in self._servers:
            os.kill(server.pid, signal.SIGKILL)

    def join(self):
        self._device_mgr.join()

        # Servers
        for server in self._servers:
            server.join()
