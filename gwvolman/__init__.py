"""WholeTale Girder Worker Plugin."""
import os
import docker
from girder_worker import GirderWorkerPluginABC
from kombu.common import Broadcast, Exchange, Queue


class GWVolumeManagerPlugin(GirderWorkerPluginABC):
    """Custom WT Manager providing WT tasks."""

    def __init__(self, app, *args, **kwargs):
        """Constructor."""
        self.app = app
        # Here we can also change application settings. E.g.
        # changing the task time limit:
        #
        queues = []
        if node_id := os.environ.get("SWARM_NODE_ID"):
            cli = docker.from_env()
            node = cli.nodes.get(node_id)
            if node.attrs["Spec"]["Role"] == "manager":
                queues.append(
                    Queue("manager", Exchange("manager", type="direct"), routing_key="manager")
                )

            if len(cli.nodes.list()) == 1 or node.attrs["Spec"]["Role"] != "manager":
                queues.append(
                    Queue("celery", Exchange("celery", type="direct"), routing_key="celery")
                )

            queues += [
                Queue(node_id, Exchange(node_id, type="direct"), routing_key=node_id),
            ]
        else:
            queues = [
                Queue(
                    "celery",
                    Exchange("celery", type="direct"),
                    routing_key="celery",
                ),
            ]
        queues.append(Broadcast("broadcast_tasks"))

        self.app.conf.task_queues = queues
        self.app.conf.task_routes = {
            "gwvolman.tasks.shutdown_container": {"queue": "broadcast_tasks"}
        }
        # self.app.config.update({
        #     'TASK_TIME_LIMIT': 300
        # })

    def task_imports(self):
        """Return a list of python importable paths."""
        return ["gwvolman.tasks"]
