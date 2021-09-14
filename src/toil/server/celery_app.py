from typing import Optional
from celery import Celery  # type: ignore


"""
Contains functions related to Celery.

Also the entrypoint for starting celery workers.
"""


def create_celery_app(broker: Optional[str] = None) -> Celery:
    """ """
    if not broker:
        broker = "amqp://guest:guest@localhost:5672//"
    return Celery("runs", broker=broker)


celery = create_celery_app()
celery.autodiscover_tasks(packages=["toil.server.wes"], related_name="tasks")
