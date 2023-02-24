import math
import time
from threading import Thread

import docker


class DockerStatsCollectorThread(Thread):
    def __init__(self, container, output_path):
        super(DockerStatsCollectorThread, self).__init__()
        self.daemon = True
        self.container = container
        self.output_path = output_path

    def container_finished(self, ts):
        try:
            if ts == "0001-01-01T00:00:00Z":
                self.container.reload()
            return self.container.attrs["State"]["Status"] not in ("created", "running")
        except docker.errors.NotFound:
            return True

    def run(self):
        while True:
            try:
                d = self.container.stats(stream=False)
            except docker.errors.NotFound:
                break
            ts = d["read"]

            if self.container_finished(ts):
                break

            if ts != "0001-01-01T00:00:00Z":
                mem_usage, mem_limit = self.calculate_memory(d)
                bytes_in, bytes_out = self.calculate_network_bytes(d)
                blkio_rd, blkio_wr = self.calculate_blkio_bytes(d)
                line = (
                    f"{ts} - {self.calculate_cpu_percent(d):.2f}%, {mem_usage} / {mem_limit},"
                    f" {bytes_in} / {bytes_out}, {blkio_rd} / {blkio_wr},"
                    f" {d.get('pids_stats', {}).get('current', 0)}\n"
                )
                with open(self.output_path, mode="a") as fp:
                    fp.write(line)
            time.sleep(5)

    @staticmethod
    def convert_size(size_bytes, binary=True):
        if size_bytes == 0:
            return "0B"
        if binary:
            suffix = "i"
            base = 1024
        else:
            suffix = ""
            base = 1000
        size_name = (
            "B",
            f"K{suffix}B",
            f"M{suffix}B",
            f"G{suffix}B",
            f"T{suffix}B",
            f"P{suffix}B",
        )
        i = int(math.floor(math.log(size_bytes, base)))
        p = math.pow(base, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    @staticmethod
    def calculate_cpu_percent(d):
        cpu_count = len(d["cpu_stats"]["cpu_usage"].get("percpu_usage", [1]))
        cpu_percent = 0.0
        cpu_delta = float(d["cpu_stats"]["cpu_usage"]["total_usage"]) - float(
            d["precpu_stats"]["cpu_usage"]["total_usage"]
        )
        try:
            system_delta = float(d["cpu_stats"]["system_cpu_usage"]) - float(
                d["precpu_stats"]["system_cpu_usage"]
            )
        except KeyError:
            system_delta = 0.0
        if system_delta > 0.0:
            cpu_percent = cpu_delta / system_delta * 100.0 * cpu_count
        return cpu_percent

    def calculate_blkio_bytes(self, d):
        bytes_stats = d.get("blkio_stats", {}).get("io_service_bytes_recursive")
        if not bytes_stats:
            return 0, 0
        rd = wr = 0
        for s in bytes_stats:
            if s["op"] == "Read":
                rd += s["value"]
            elif s["op"] == "Write":
                wr += s["value"]
        return self.convert_size(rd, binary=False), self.convert_size(wr, binary=False)

    def calculate_network_bytes(self, d):
        networks = d.get("networks")
        if not networks:
            return 0, 0
        rx = tx = 0
        for data in networks.values():
            rx += data["rx_bytes"]
            tx += data["tx_bytes"]
        return self.convert_size(rx, binary=False), self.convert_size(tx, binary=False)

    def calculate_memory(self, d):
        memory = d.get("memory_stats")
        if not memory:
            return 0, 0
        return self.convert_size(
            memory.get("usage", 0), binary=True
        ), self.convert_size(memory.get("limit", 0), binary=True)
