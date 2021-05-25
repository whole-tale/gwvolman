import docker
from celery import Celery
from celery.schedules import crontab

app = Celery()

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):

    cli = docker.from_env(version='1.28')
    nodes = cli.nodes.list()
    for node in nodes:
        print('Adding rebuild task for node' + node.id)

        key = 'rebuild-image-cache-' + node.id
        app.conf.beat_schedule[key] = {
            'task': 'gwvolman.tasks.rebuild_image_cache',
            'schedule': crontab(minute=0, hour=0),
            'options': {
                'queue': node.id,
            }
        }
