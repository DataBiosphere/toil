from urlparse import urlparse
import boto
import math
import itertools


class S3File:
    def __init__(self, url):
        self.s3 = boto.connect_s3()
        self.url = urlparse(url)
        assert self.url.scheme == 's3'
        self.key = self.url.path[1:]
        bucketName = self.url.netloc
        self.bucket = self.s3.get_bucket(bucketName)

    def size(self):
        return self.bucket.lookup(self.key).size

    def copyTo(self, dest, part_size=None):
        partSize = part_size or 2**20 * 50  # 50MiB
        totalSize = self.size()

        # initiate copy
        upload = dest.bucket.initiate_multipart_upload(dest.key)
        try:
            start = 0
            partIndex = itertools.count()
            while start < totalSize:
                end = min(start + partSize-1, totalSize-1)  # bytes are indexed form zero
                upload.copy_part_from_key(self.bucket.name,
                                          self.key,
                                          next(partIndex)+1,
                                          start,
                                          end)
                start += partSize
        except:
            upload.cancel_upload()
        else:
            upload.complete_upload()


def copy_key_multipart(src, dst, part_size=None):
    """
    :param str src: a string containing a URL of the form s3://BUCKET_NAME/KEY_NAME pointing at
           the source object/key

    :param str dst: a string containing a URL of the form s3://BUCKET_NAME/KEY_NAME pointing at
           the destination object/key
    """
    # set part_size
    part_size = part_size or int(2**20 * 50)  # default size of 5MiB
    assert part_size > int(2**20 * 5) - 1  # part_size too small
    assert part_size < int(2**30 * 5) + 1  # part_size too big

    # parse URLs
    srcUrl = urlparse(src)
    dstUrl = urlparse(dst)
    assert dstUrl.scheme == srcUrl.scheme == 's3'

    srcFile = S3File(src)
    dstFile = S3File(dst)

    srcFile.copyTo(dstFile, part_size=part_size)