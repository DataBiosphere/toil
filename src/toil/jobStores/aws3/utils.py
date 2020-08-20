# Copyright (C) 2015-2016 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import


class S3KeyWrapper:
    """
    *Temporary* Not part of the Toil API! This class will be deleted once usages of boto.s3.key.Key
    are converted to S3.Object. Only the methods and attributes used in Toil are implemented.
    """
    def __init__(self, obj):
        """
        :param obj: S3.Object
        """
        self.obj = obj

    # Attributes mapping:
    # -----------------------------------
    # boto.s3.key.Key       =>  S3.Object
    # size                  =>  content_length
    # name                  =>  key
    # version_id            =>  version_id
    # get_contents_as_string()      =>  get()
    # set_contents_from_string()    =>  put()
    # get_contents_to_file()        =>  get()
    # bucket                        =>  n/A
    #       name            =>  bucket_name
    #       connection      =>  n/A

    @property
    def size(self):
        return self.obj.content_length

    @property
    def name(self):
        return self.obj.key

    @property
    def version_id(self):
        return self.obj.version_id

    @property
    def bucket(self):
        return BucketWrapper(self.obj.bucket_name)

    def get_contents_as_string(self):
        return self.obj.get()['Body'].read()

    def set_contents_from_string(self, contents):
        pass

    def get_contents_to_file(self, fp):
        pass


class BucketWrapper:
    def __init__(self, bucket_name):
        self.bucketName = bucket_name

    @property
    def name(self):
        return self.bucketName

    @property
    def connection(self):
        return self.ConnectionWrapper()

    class ConnectionWrapper:
        def close(self):
            pass
