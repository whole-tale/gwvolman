"""WholeTale Girder Worker Plugin."""
import girder_client
from girder_worker import GirderWorkerPluginABC
from kombu.common import Broadcast, Exchange, Queue
import sys
import os
import base64
from .tasks_factory import TasksFactory

from .utils import _build_tale_workspace

class GWVolumeManagerPlugin(GirderWorkerPluginABC):
    """Custom WT Manager providing WT tasks."""

    def __init__(self, app, *args, **kwargs):
        """Constructor."""
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
        """Return a list of python importable paths."""
        return ['gwvolman.tasks']


def __build_tale_workspace__():
    girderApiUrl = sys.argv[1]
    instanceId = sys.argv[2]
    mountpoint = sys.argv[3]
    girderApiKey = os.environ.get('GIRDER_API_KEY')
    # TODO: delete
    print('apiKey: %s' % girderApiKey)
    print('Girder api url: %s' % girderApiUrl)
    gc = girder_client.GirderClient(apiUrl=girderApiUrl)
    gc.authenticate(apiKey=girderApiKey)

    _build_tale_workspace(gc, instanceId, mountpoint)
