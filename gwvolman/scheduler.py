import docker
import logging
from celery import Celery
from celery.schedules import crontab

app = Celery()


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    cli = docker.from_env(version="1.28")
    for node in cli.nodes.list(filters={"role": "worker"}):
        logging.info("Adding rebuild task for node %s", node.id)

        key = "rebuild-image-cache-" + node.id
        app.conf.beat_schedule[key] = {
            "task": "gwvolman.tasks.rebuild_image_cache",
            "schedule": crontab(minute=0, hour=0),
            "options": {"queue": node.id},
        }
