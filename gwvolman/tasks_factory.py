from .tasks_docker import DockerTasks
from .tasks_kubernetes import KubernetesTasks

MAP = {"docker": DockerTasks, "k8s": KubernetesTasks}


class TasksFactory:
    def __init__(self, _type):
        self._check_type(_type)
        self._type = _type

    def _check_type(self, _type):
        if _type not in MAP:
            raise Exception("Unsupported deployment type: %s" % _type)

    def getTasksInstance(self):
        return MAP[self._type]()
