import os

from celery import Celery  # type: ignore

"""
Contains functions related to Celery.

Also the entrypoint for starting celery workers.
"""


def create_celery_app() -> Celery:
    """ """
    broker = os.environ.get("TOIL_WES_BROKER_URL", "amqp://guest:guest@localhost:5672//")
    app = Celery("toil_wes", broker=broker)

    # Celery configurations
    app.conf.broker_transport_options = {
        "max_retries": 5,
        "interval_start": 0,
        "interval_step": 0.2,
        "interval_max": 0.2,
    }

    return app


celery = create_celery_app()
celery.autodiscover_tasks(packages=["toil.server.wes"], related_name="tasks")
