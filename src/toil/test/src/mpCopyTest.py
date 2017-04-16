import boto
import uuid
import math
import itertools
import sys
import unittest
import io

from bd2k.util.expando import Expando
from toil.mpcopy import copy_key_multipart


class CopyKeyMultipartTest(unittest.TestCase):
    def setUp(self):
        super(CopyKeyMultipartTest, self).setUp()
        self.s3 = boto.connect_s3()

        srcName = 'mp_copy_test_src_bucket_' + str(uuid.uuid1())
        self.srcBucket = self.s3.create_bucket(srcName)
        dstName = 'mp_copy_test_dst_bucket_' + str(uuid.uuid1())
        self.dstBucket = self.s3.create_bucket(dstName)

    def tearDown(self):
        super(CopyKeyMultipartTest, self).tearDown()
        for bkt in [self.srcBucket, self.dstBucket]:
            for key in bkt.list():
                key.delete()
            self.s3.delete_bucket(bkt)

    def __uploadMultiPart(self, file_size, part_size):
        """
        Uploads random bytes to src bucket of size filesize in parts of size partsize.

        :param filesize int: size of file
        :param partsize int: size of each part
        :return: a dictionary with total size, part size, url, and fileid as keys
        """
        assert part_size > int(2**20 * 5) - 1  # part_size too small
        assert part_size < int(2**30 * 5) + 1  # part_size too big

        upload = Expando(totalSize=file_size,
                         partSize=part_size,
                         fileId='file_size_%d_%s' % (file_size, str(uuid.uuid1())))

        upload.srcUrl = "s3://%s/%s" % (self.srcBucket.name, upload.fileId)
        upload.dstUrl = "s3://%s/%s" % (self.dstBucket.name, upload.fileId)

        with open('/dev/urandom', 'r') as f:
            mp = self.srcBucket.initiate_multipart_upload(key_name=upload.fileId)
            start = 0
            partNum = itertools.count()
            try:
                while start <= upload.totalSize:
                    end = min(start + upload.partSize, upload.totalSize)
                    fPart = io.BytesIO(f.read(upload.partSize))
                    mp.upload_part_from_file(fp=fPart,
                                             part_num=next(partNum) + 1,
                                             size=end - start)
                    start = end
                    if start == upload.totalSize:
                        break

                assert start == upload.totalSize
            except:
                mp.cancel_upload()
                raise
            else:
                mp.complete_upload()
        return upload

    def __copyFromUpload(self, upload):
        """
        Copies file described by dict upload from self.src to self.dst
        :param upload: dictionary with keys describing a multipart uploaded file
        """
        copy_key_multipart(upload.srcUrl,  # if part_size of upload is different from copy
                           upload.dstUrl,  # part_size the etags will not match
                           part_size=upload.partSize)
        try:
            srcEtag = self.srcBucket.get_key(upload.fileId).etag
            dstEtag = self.dstBucket.get_key(upload.fileId).etag
            assert srcEtag == dstEtag
        except:
            print "etags do not match"
            raise

    def uploadAndCopy(self, filesize, partsize):
        upload = self.__uploadMultiPart(filesize, partsize)
        self.__copyFromUpload(upload)


fiftyMiB = 2 ** 20 * 50
PART_SIZE_LIST = [fiftyMiB, 2**20 * 5 - 1, 2**30 * 5 + 1]
FILE_SIZE_LIST = [0, 1, fiftyMiB, fiftyMiB-1, fiftyMiB+1]

for prt in PART_SIZE_LIST:
    for fl in FILE_SIZE_LIST:
        def fx(fl, prt):
            def f(self): self.uploadAndCopy(fl, prt)
            if prt > (2**20 * 5 - 1) and prt < (2**30 * 5 + 1):
                return f
            else:
                return unittest.expectedFailure(f)

        setattr(CopyKeyMultipartTest, "test_partsize%d_filesize%d" % (prt, fl), fx(fl, prt))
