# Copyright (C) 2015-2021 Regents of the University of California
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
import hashlib
import http.server
import logging
import os
import shutil
import socketserver
import tempfile
import threading
import time
import pytest
import urllib.parse as urlparse
import uuid
import socket
import errno

from abc import ABCMeta, abstractmethod
from io import BytesIO
from itertools import chain, islice
from queue import Queue
from threading import Thread
from typing import Any, Tuple
from urllib.request import Request, urlopen
from stubserver import FTPStubServer
from boto.exception import BotoServerError, S3ResponseError
from botocore.exceptions import ClientError

from toil.jobStores.aws.jobStore import AWSJobStore
from toil.test import ToilTest


class AWSJobstoreUnitTests(ToilTest):
    def setUp(self):
        aws_jobstore = AWSJobStore()

    def tearDown(self):
        pass

    def test_set_encryption_from_config(self):
        pass

    def test_initialize(self):
        pass

    def test_resume(self):
        pass

    def test_unpickle_job(self):
        pass

    def test_pickle_job(self):
        pass

    def test_batch(self):
        pass

    def test_assign_job_id(self):
        pass

    def test_create(self):
        pass

    def test_job_exists(self):
        pass

    def test_jobs(self):
        pass

    def test_load_job(self):
        pass

    def test_update_job(self):
        pass

    def test_delete_job(self):
        pass

    def test_getEmptyFileStoreID(self):
        pass

    def test_importFile(self):
        pass

    def test_exportFile(self):
        pass

    def test_readFromUrl(self):
        pass

    def test_writeToUrl(self):
        pass

    def test_getObjectForUrl(self):
        pass

    def test_supportsUrl(self):
        pass

    def test_file_id(self):
        pass

    def test_writeFile(self):
        pass

    def test_writeFileStream(self):
        pass

    def test_writeSharedFileStream(self):
        pass

    def updateFile(self):
        pass

    def test_updateFileStream(self):
        pass

    def test_fileExists(self):
        pass

    def test_getFileSize(self):
        pass

    def test_getSize(self):
        pass

    def test_readFile(self):
        pass

    def test_readFileStream(self):
        pass

    def test_readSharedFileStream(self):
        pass

    def test_deleteFile(self):
        pass

    def test_writeStatsAndLogging(self):
        pass

    def test_readStatsAndLogging(self):
        pass

    def test_getPublicUrl(self):
        pass

    def test_getSharedPublicUrl(self):
        pass

    def test_sharedFileID(self):
        pass

    def test_destroy(self):
        pass

    def test_parse_jobstore_identifier(self):
        pass