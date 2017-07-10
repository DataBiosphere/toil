from __future__ import absolute_import

from contextlib import contextmanager
import boto3
from botocore.exceptions import ClientError
import logging

from toil.jobStores.abstractJobStore import (
                                             NoSuchFileException,
                                             JobStoreExistsException,
                                             )
from toil.jobStores.utils import WritablePipe, ReadablePipe

logger = logging.getLogger(__name__)

class FileStore(object):
    def __init__(self, path):
        self.path = path.rstrip('/') + '/'
        bucketName, self.prefix = self.path.split('/', 1)
        self.bucket = boto3.resource('s3').Bucket(bucketName)

    def initialize(self, config):
        # boto3 does not currently set the region when creating the
        # bucket. This is not expected to change any time soon.
        # https://github.com/boto/boto3/issues/781
        # So we are fetching the configured region from boto3 client.
        region = self.bucket.meta.client._client_config.region_name
        try:
            self.bucket.create(CreateBucketConfiguration={
                'LocationConstraint': region
            })
            self.bucket.wait_until_exists()
        except ClientError as e:
            if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise e
            # Bucket already exists. Check path prefix:
            if list(self.bucket.objects.filter(Prefix=self.prefix).limit(1)):
                raise JobStoreExistsException(self.path)

    def resume(self):
        if not list(self.bucket.objects.filter(Prefix=self.prefix).limit(1)):
            raise NoSuchJobStoreException(self.path)

    def destroy(self):
        try:
            self.bucket.objects.filter(Prefix=self.prefix).delete()
            logger.debug('Successfully deleted fileStore:%s' % self.path)
        except botocore.errorfactory.NoSuchBucket:
            pass

    ##########################################
    # Function that deal with files
    ##########################################

    def writeFile(self, localFilePath, fileID):
        self.bucket.upload_file(localFilePath, fileID)

    def readFile(self, fileID, localFilePath):
        self.bucket.download_file(fileID, localFilePath)

    def deleteFile(self, fileID):
        obj = self.__get_object(fileID)
        obj.delete()
        obj.wait_until_not_exists()

    ##########################################
    #  Function that deal with file streams
    ##########################################

    @contextmanager
    def writeFileStream(self, fileID):
        obj = self.__get_object(fileID)
        with self.UploaderPipe(obj) as writable:
            yield writable

    @contextmanager
    def readFileStream(self, fileID):
        obj = self.__get_object(fileID)
        with self.DownloaderPipe(obj) as readable:
            yield readable

    ##########################################
    # Functions that deal with file URLs
    ##########################################

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() == 's3'

    @classmethod
    def _readFromUrl(cls, url, writable):
        obj = cls._get_object_from(url)
        obj.wait_until_exists()
        obj.download_fileobj(writable)


    @classmethod
    def _writeToUrl(cls, readable, url):
        obj = cls._get_object_from(url)
        obj.upload_fileobj(readable)
        obj.wait_until_exists()

    def getPublicUrl(self, fileID):
        return 's3://' + self.path + fileID

    ##########################################
    # Private Methods
    ##########################################

    def __get_key(self, fileID):
        return ("%s/%s" % (self.prefix, fileID)).strip('/')

    def __get_object(self, fileID):
        return self.bucket.Object(self.__get_key(fileID))

    @staticmethod
    def _get_object_from(url):
        bucket = url.netloc
        key = url.path[1:]
        return boto3.resource('s3').Bucket(bucket).Object(key)

    class UploaderPipe(WritablePipe):
        def __init__(self, obj):
            self.obj = obj
            super(FileStore.UploaderPipe, self).__init__()

        def readFrom(self, readable):
            self.obj.upload_fileobj(readable)
            self.obj.wait_until_exists()

    class DownloaderPipe(ReadablePipe):
        def __init__(self, obj):
            self.obj = obj
            super(FileStore.DownloaderPipe, self).__init__()

        def writeTo(self, writable):
            self.obj.wait_until_exists()
            self.obj.download_fileobj(writable)
