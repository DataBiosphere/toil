from typing import Optional

from boto3 import Session
from botocore.credentials import JSONFileCache
from botocore.session import get_session


def establish_boto3_session(region_name: Optional[str] = None) -> Session:
    """
    This is the One True Place where Boto3 sessions should be established, and
    prepares them with the necessary credential caching.

    :param region_name: If given, the session will be associated with the given AWS region.
    """

    # Make sure to use credential caching when talking to Amazon via boto3
    # See https://github.com/boto/botocore/pull/1338/
    # And https://github.com/boto/botocore/commit/2dae76f52ae63db3304b5933730ea5efaaaf2bfc

    botocore_session = get_session()
    botocore_session.get_component('credential_provider').get_provider('assume-role').cache = JSONFileCache()
    return Session(botocore_session=botocore_session, region_name=region_name)


boto3_session = establish_boto3_session()
s3_boto3_resource = boto3_session.resource('s3')
s3_boto3_client = boto3_session.client('s3')
iam_boto3_client = boto3_session.client('iam')
