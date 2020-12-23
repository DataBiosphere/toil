from functools import lru_cache


@lru_cache(maxsize=None)
def client(*args, **kwargs):
    import boto3
    return boto3.client(*args, **kwargs)


@lru_cache(maxsize=None)
def resource(*args, **kwargs):
    import boto3
    return boto3.resource(*args, **kwargs)
