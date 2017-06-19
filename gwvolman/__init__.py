from girder_worker import GirderWorkerPluginABC
from kombu.common import Broadcast, Exchange, Queue


class GWVolumeManagerPlugin(GirderWorkerPluginABC):
    def __init__(self, app, *args, **kwargs):
        self.app = app
        # Here we can also change application settings. E.g.
        # changing the task time limit:
        #
        self.app.conf.task_queues = (
            Queue('celery', Exchange('celery', type='direct'),
                  routing_key='celery'),
            Broadcast('broadcast_tasks')
        )
        self.app.conf.task_routes = {
            'gwvolman.tasks.shutdown_container': {'queue': 'broadcast_tasks'}
        }
        # self.app.config.update({
        #     'TASK_TIME_LIMIT': 300
        # })

    def task_imports(self):
        # Return a list of python importable paths to the
        # plugin's path directory
        return ['gwvolman.tasks']
