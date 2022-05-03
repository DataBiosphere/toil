import os


def running_on_ecs() -> bool:
    """
    Return True if we are currently running on Amazon ECS, and false otherwise.
    """
    # We only care about relatively current ECS
    return 'ECS_CONTAINER_METADATA_URI_V4' in os.environ
