# https://pytest.org/latest/example/pythoncollection.html

collect_ignore = []

try:
    import azure
except ImportError:
    collect_ignore.append("azureJobStore.py")

try:
    import boto
except ImportError:
    collect_ignore.append("awsJobStore.py")
