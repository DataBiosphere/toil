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
import itertools
import logging
import os
import pickle
import re
import reprlib
import stat
import time
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from io import BytesIO
from typing import IO, TYPE_CHECKING, Optional, Union, cast
from urllib.parse import ParseResult, parse_qs, urlencode, urlsplit, urlunsplit

from botocore.exceptions import ClientError

import toil.lib.encryption as encryption
from toil.fileStores import FileID
from toil.job import Job, JobDescription
from toil.jobStores.abstractJobStore import (
    AbstractJobStore,
    ConcurrentFileModificationException,
    JobStoreExistsException,
    LocatorException,
    NoSuchFileException,
    NoSuchJobException,
    NoSuchJobStoreException,
)
from toil.jobStores.aws.utils import (
    SDBHelper,
    ServerSideCopyProhibitedError,
    copyKeyMultipart,
    fileSizeAndTime,
    no_such_sdb_domain,
    retry_sdb,
    sdb_unavailable,
    uploadFile,
    uploadFromPath,
)
from toil.jobStores.utils import ReadablePipe, ReadableTransformingPipe, WritablePipe
from toil.lib.aws import build_tag_dict_from_env
from toil.lib.aws.session import establish_boto3_session
from toil.lib.aws.utils import (
    NoBucketLocationError,
    boto3_pager,
    create_s3_bucket,
    enable_public_objects,
    flatten_tags,
    get_bucket_region,
    get_item_from_attributes,
    get_object_for_url,
    list_objects_for_url,
    retry_s3,
    retryable_s3_errors,
)
from toil.lib.compatibility import compat_bytes
from toil.lib.ec2nodes import EC2Regions
from toil.lib.exceptions import panic
from toil.lib.io import AtomicFileCreate
from toil.lib.memoize import strict_bool
from toil.lib.objects import InnerClass
from toil.lib.retry import get_error_code, get_error_status, retry

if TYPE_CHECKING:
    from mypy_boto3_sdb.type_defs import (
        AttributeTypeDef,
        DeletableItemTypeDef,
        ItemTypeDef,
        ReplaceableAttributeTypeDef,
        ReplaceableItemTypeDef,
        UpdateConditionTypeDef,
    )

    from toil import Config

boto3_session = establish_boto3_session()
s3_boto3_resource = boto3_session.resource("s3")
s3_boto3_client = boto3_session.client("s3")
logger = logging.getLogger(__name__)

# Sometimes we have to wait for multipart uploads to become real. How long
# should we wait?
CONSISTENCY_TICKS = 5
CONSISTENCY_TIME = 1


class ChecksumError(Exception):
    """Raised when a download from AWS does not contain the correct data."""


class DomainDoesNotExist(Exception):
    """Raised when a domain that is expected to exist does not exist."""

    def __init__(self, domain_name):
        super().__init__(f"Expected domain {domain_name} to exist!")


class AWSJobStore(AbstractJobStore):
    """
    A job store that uses Amazon's S3 for file storage and SimpleDB for storing job info and
    enforcing strong consistency on the S3 file storage. There will be SDB domains for jobs and
    files and a versioned S3 bucket for file contents. Job objects are pickled, compressed,
    partitioned into chunks of 1024 bytes and each chunk is stored as a an attribute of the SDB
    item representing the job. UUIDs are used to identify jobs and files.
    """

    # Dots in bucket names should be avoided because bucket names are used in HTTPS bucket
    # URLs where the may interfere with the certificate common name. We use a double
    # underscore as a separator instead.
    #
    bucketNameRe = re.compile(r"^[a-z0-9][a-z0-9-]+[a-z0-9]$")

    # See http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    #
    minBucketNameLen = 3
    maxBucketNameLen = 63
    maxNameLen = 10
    nameSeparator = "--"

    def __init__(self, locator: str, partSize: int = 50 << 20) -> None:
        """
        Create a new job store in AWS or load an existing one from there.

        :param int partSize: The size of each individual part used for multipart operations like
               upload and copy, must be >= 5 MiB but large enough to not exceed 10k parts for the
               whole file
        """
        super().__init__(locator)
        region, namePrefix = locator.split(":")
        regions = EC2Regions.keys()
        if region not in regions:
            raise ValueError(f'Region "{region}" is not one of: {regions}')
        if not self.bucketNameRe.match(namePrefix):
            raise ValueError(
                "Invalid name prefix '%s'. Name prefixes must contain only digits, "
                "hyphens or lower-case letters and must not start or end in a "
                "hyphen." % namePrefix
            )
        # Reserve 13 for separator and suffix
        if len(namePrefix) > self.maxBucketNameLen - self.maxNameLen - len(
            self.nameSeparator
        ):
            raise ValueError(
                "Invalid name prefix '%s'. Name prefixes may not be longer than 50 "
                "characters." % namePrefix
            )
        if "--" in namePrefix:
            raise ValueError(
                "Invalid name prefix '%s'. Name prefixes may not contain "
                "%s." % (namePrefix, self.nameSeparator)
            )
        logger.debug(
            "Instantiating %s for region %s and name prefix '%s'",
            self.__class__,
            region,
            namePrefix,
        )
        self.region = region
        self.name_prefix = namePrefix
        self.part_size = partSize
        self.jobs_domain_name: Optional[str] = None
        self.files_domain_name: Optional[str] = None
        self.files_bucket = None
        self.db = boto3_session.client(service_name="sdb", region_name=region)

        self.s3_resource = boto3_session.resource("s3", region_name=self.region)
        self.s3_client = self.s3_resource.meta.client

    def initialize(self, config: "Config") -> None:
        if self._registered:
            raise JobStoreExistsException(self.locator, "aws")
        self._registered = None
        try:
            self._bind(create=True)
        except:
            with panic(logger):
                self.destroy()
        else:
            super().initialize(config)
            # Only register after job store has been full initialized
            self._registered = True

    @property
    def sseKeyPath(self) -> Optional[str]:
        return self.config.sseKey

    def resume(self) -> None:
        if not self._registered:
            raise NoSuchJobStoreException(self.locator, "aws")
        self._bind(create=False)
        super().resume()

    def _bind(
        self,
        create: bool = False,
        block: bool = True,
        check_versioning_consistency: bool = True,
    ) -> None:
        def qualify(name):
            assert len(name) <= self.maxNameLen
            return self.name_prefix + self.nameSeparator + name

        # The order in which this sequence of events happens is important.  We can easily handle the
        # inability to bind a domain, but it is a little harder to handle some cases of binding the
        # jobstore bucket.  Maintaining this order allows for an easier `destroy` method.
        if self.jobs_domain_name is None:
            self.jobs_domain_name = qualify("jobs")
            self._bindDomain(self.jobs_domain_name, create=create, block=block)
        if self.files_domain_name is None:
            self.files_domain_name = qualify("files")
            self._bindDomain(self.files_domain_name, create=create, block=block)
        if self.files_bucket is None:
            self.files_bucket = self._bindBucket(
                qualify("files"),
                create=create,
                block=block,
                versioning=True,
                check_versioning_consistency=check_versioning_consistency,
            )

    @property
    def _registered(self) -> Optional[bool]:
        """
        A optional boolean property indicating whether this job store is registered. The
        registry is the authority on deciding if a job store exists or not. If True, this job
        store exists, if None the job store is transitioning from True to False or vice versa,
        if False the job store doesn't exist.

        :type: bool|None
        """
        # The weird mapping of the SDB item attribute value to the property value is due to
        # backwards compatibility. 'True' becomes True, that's easy. Toil < 3.3.0 writes this at
        # the end of job store creation. Absence of either the registry, the item or the
        # attribute becomes False, representing a truly absent, non-existing job store. An
        # attribute value of 'False', which is what Toil < 3.3.0 writes at the *beginning* of job
        # store destruction, indicates a job store in transition, reflecting the fact that 3.3.0
        # may leak buckets or domains even though the registry reports 'False' for them. We
        # can't handle job stores that were partially created by 3.3.0, though.
        registry_domain_name = "toil-registry"
        try:
            self._bindDomain(
                domain_name=registry_domain_name, create=False, block=False
            )
        except DomainDoesNotExist:
            return False

        for attempt in retry_sdb():
            with attempt:
                get_result = self.db.get_attributes(
                    DomainName=registry_domain_name,
                    ItemName=self.name_prefix,
                    AttributeNames=["exists"],
                    ConsistentRead=True,
                )
                attributes: list["AttributeTypeDef"] = get_result.get(
                    "Attributes", []
                )  # the documentation says 'Attributes' should always exist, but this is not true
                exists: Optional[str] = get_item_from_attributes(
                    attributes=attributes, name="exists"
                )
                if exists is None:
                    return False
                elif exists == "True":
                    return True
                elif exists == "False":
                    return None
                else:
                    assert False

    @_registered.setter
    def _registered(self, value: bool) -> None:
        registry_domain_name = "toil-registry"
        try:
            self._bindDomain(
                domain_name=registry_domain_name,
                # Only create registry domain when registering or
                # transitioning a store
                create=value is not False,
                block=False,
            )
        except DomainDoesNotExist:
            pass
        else:
            for attempt in retry_sdb():
                with attempt:
                    if value is False:
                        self.db.delete_attributes(
                            DomainName=registry_domain_name, ItemName=self.name_prefix
                        )
                    else:
                        if value is True:
                            attributes: list["ReplaceableAttributeTypeDef"] = [
                                {"Name": "exists", "Value": "True", "Replace": True}
                            ]
                        elif value is None:
                            attributes = [
                                {"Name": "exists", "Value": "False", "Replace": True}
                            ]
                        else:
                            assert False
                        self.db.put_attributes(
                            DomainName=registry_domain_name,
                            ItemName=self.name_prefix,
                            Attributes=attributes,
                        )

    def _checkItem(self, item: "ItemTypeDef", enforce: bool = True) -> None:
        """
        Make sure that the given SimpleDB item actually has the attributes we think it should.

        Throw otherwise.

        If enforce is false, log but don't throw.
        """
        self._checkAttributes(item["Attributes"], enforce)

    def _checkAttributes(
        self, attributes: list["AttributeTypeDef"], enforce: bool = True
    ) -> None:
        if get_item_from_attributes(attributes=attributes, name="overlargeID") is None:
            logger.error(
                "overlargeID attribute isn't present: either SimpleDB entry is "
                "corrupt or jobstore is from an extremely old Toil: %s",
                attributes,
            )
            if enforce:
                raise RuntimeError(
                    "encountered SimpleDB entry missing required attribute "
                    "'overlargeID'; is your job store ancient?"
                )

    def _awsJobFromAttributes(self, attributes: list["AttributeTypeDef"]) -> Job:
        """
        Get a Toil Job object from attributes that are defined in an item from the DB
        :param attributes: List of attributes
        :return: Toil job
        """
        self._checkAttributes(attributes)
        overlarge_id_value = get_item_from_attributes(
            attributes=attributes, name="overlargeID"
        )
        if overlarge_id_value:
            assert self.file_exists(overlarge_id_value)
            # This is an overlarge job, download the actual attributes
            # from the file store
            logger.debug("Loading overlarge job from S3.")
            with self.read_file_stream(overlarge_id_value) as fh:
                binary = fh.read()
        else:
            binary, _ = SDBHelper.attributesToBinary(attributes)
            assert binary is not None
        job = pickle.loads(binary)
        if job is not None:
            job.assignConfig(self.config)
        return job

    def _awsJobFromItem(self, item: "ItemTypeDef") -> Job:
        """
        Get a Toil Job object from an item from the DB
        :return: Toil Job
        """
        return self._awsJobFromAttributes(item["Attributes"])

    def _awsJobToAttributes(self, job: JobDescription) -> list["AttributeTypeDef"]:
        binary = pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL)
        if len(binary) > SDBHelper.maxBinarySize(extraReservedChunks=1):
            # Store as an overlarge job in S3
            with self.write_file_stream() as (writable, fileID):
                writable.write(binary)
            item = SDBHelper.binaryToAttributes(None)
            item["overlargeID"] = fileID
        else:
            item = SDBHelper.binaryToAttributes(binary)
            item["overlargeID"] = ""
        return SDBHelper.attributeDictToList(item)

    def _awsJobToItem(self, job: JobDescription, name: str) -> "ItemTypeDef":
        return {"Name": name, "Attributes": self._awsJobToAttributes(job)}

    jobsPerBatchInsert = 25

    @contextmanager
    def batch(self) -> None:
        self._batchedUpdates = []
        yield
        batches = [
            self._batchedUpdates[i : i + self.jobsPerBatchInsert]
            for i in range(0, len(self._batchedUpdates), self.jobsPerBatchInsert)
        ]

        for batch in batches:
            items: list["ReplaceableItemTypeDef"] = []
            for jobDescription in batch:
                item_attributes: list["ReplaceableAttributeTypeDef"] = []
                jobDescription.pre_update_hook()
                item_name = compat_bytes(jobDescription.jobStoreID)
                got_job_attributes: list["AttributeTypeDef"] = self._awsJobToAttributes(
                    jobDescription
                )
                for each_attribute in got_job_attributes:
                    new_attribute: "ReplaceableAttributeTypeDef" = {
                        "Name": each_attribute["Name"],
                        "Value": each_attribute["Value"],
                        "Replace": True,
                    }
                    item_attributes.append(new_attribute)
                items.append({"Name": item_name, "Attributes": item_attributes})

            for attempt in retry_sdb():
                with attempt:
                    self.db.batch_put_attributes(
                        DomainName=self.jobs_domain_name, Items=items
                    )
        self._batchedUpdates = None

    def assign_job_id(self, job_description: JobDescription) -> None:
        jobStoreID = self._new_job_id()
        logger.debug("Assigning ID to job %s", jobStoreID)
        job_description.jobStoreID = jobStoreID

    def create_job(self, job_description: JobDescription) -> JobDescription:
        if hasattr(self, "_batchedUpdates") and self._batchedUpdates is not None:
            self._batchedUpdates.append(job_description)
        else:
            self.update_job(job_description)
        return job_description

    def job_exists(self, job_id: Union[bytes, str]) -> bool:
        for attempt in retry_sdb():
            with attempt:
                return (
                    len(
                        self.db.get_attributes(
                            DomainName=self.jobs_domain_name,
                            ItemName=compat_bytes(job_id),
                            AttributeNames=[SDBHelper.presenceIndicator()],
                            ConsistentRead=True,
                        ).get("Attributes", [])
                    )
                    > 0
                )

    def jobs(self) -> Generator[Job, None, None]:
        job_items: Optional[list["ItemTypeDef"]] = None
        for attempt in retry_sdb():
            with attempt:
                job_items = boto3_pager(
                    self.db.select,
                    "Items",
                    ConsistentRead=True,
                    SelectExpression="select * from `%s`" % self.jobs_domain_name,
                )
        assert job_items is not None
        for jobItem in job_items:
            yield self._awsJobFromItem(jobItem)

    def load_job(self, job_id: FileID) -> Job:
        item_attributes = None
        for attempt in retry_sdb():
            with attempt:
                item_attributes = self.db.get_attributes(
                    DomainName=self.jobs_domain_name,
                    ItemName=compat_bytes(job_id),
                    ConsistentRead=True,
                ).get("Attributes", [])
        if not item_attributes:
            raise NoSuchJobException(job_id)
        job = self._awsJobFromAttributes(item_attributes)
        if job is None:
            raise NoSuchJobException(job_id)
        logger.debug("Loaded job %s", job_id)
        return job

    def update_job(self, job_description):
        logger.debug("Updating job %s", job_description.jobStoreID)
        job_description.pre_update_hook()
        job_attributes = self._awsJobToAttributes(job_description)
        update_attributes: list["ReplaceableAttributeTypeDef"] = [
            {"Name": attribute["Name"], "Value": attribute["Value"], "Replace": True}
            for attribute in job_attributes
        ]
        for attempt in retry_sdb():
            with attempt:
                self.db.put_attributes(
                    DomainName=self.jobs_domain_name,
                    ItemName=compat_bytes(job_description.jobStoreID),
                    Attributes=update_attributes,
                )

    itemsPerBatchDelete = 25

    def delete_job(self, job_id):
        # remove job and replace with jobStoreId.
        logger.debug("Deleting job %s", job_id)

        # If the job is overlarge, delete its file from the filestore
        for attempt in retry_sdb():
            with attempt:
                attributes = self.db.get_attributes(
                    DomainName=self.jobs_domain_name,
                    ItemName=compat_bytes(job_id),
                    ConsistentRead=True,
                ).get("Attributes", [])
        # If the overlargeID has fallen off, maybe we partially deleted the
        # attributes of the item? Or raced on it? Or hit SimpleDB being merely
        # eventually consistent? We should still be able to get rid of it.
        self._checkAttributes(attributes, enforce=False)
        overlarge_id_value = get_item_from_attributes(
            attributes=attributes, name="overlargeID"
        )
        if overlarge_id_value:
            logger.debug("Deleting job from filestore")
            self.delete_file(overlarge_id_value)
        for attempt in retry_sdb():
            with attempt:
                self.db.delete_attributes(
                    DomainName=self.jobs_domain_name, ItemName=compat_bytes(job_id)
                )
        items: Optional[list["ItemTypeDef"]] = None
        for attempt in retry_sdb():
            with attempt:
                items = list(
                    boto3_pager(
                        self.db.select,
                        "Items",
                        ConsistentRead=True,
                        SelectExpression=f"select version from `{self.files_domain_name}` where ownerID='{job_id}'",
                    )
                )
        assert items is not None
        if items:
            logger.debug(
                "Deleting %d file(s) associated with job %s", len(items), job_id
            )
            n = self.itemsPerBatchDelete
            batches = [items[i : i + n] for i in range(0, len(items), n)]
            for batch in batches:
                delete_items: list["DeletableItemTypeDef"] = [
                    {"Name": item["Name"]} for item in batch
                ]
                for attempt in retry_sdb():
                    with attempt:
                        self.db.batch_delete_attributes(
                            DomainName=self.files_domain_name, Items=delete_items
                        )
            for item in items:
                item: "ItemTypeDef"
                version = get_item_from_attributes(
                    attributes=item["Attributes"], name="version"
                )
                for attempt in retry_s3():
                    with attempt:
                        if version:
                            self.s3_client.delete_object(
                                Bucket=self.files_bucket.name,
                                Key=compat_bytes(item["Name"]),
                                VersionId=version,
                            )
                        else:
                            self.s3_client.delete_object(
                                Bucket=self.files_bucket.name,
                                Key=compat_bytes(item["Name"]),
                            )

    def get_empty_file_store_id(
        self, jobStoreID=None, cleanup=False, basename=None
    ) -> FileID:
        info = self.FileInfo.create(jobStoreID if cleanup else None)
        with info.uploadStream() as _:
            # Empty
            pass
        info.save()
        logger.debug("Created %r.", info)
        return info.fileID

    def _import_file(
        self,
        otherCls,
        uri: ParseResult,
        shared_file_name: Optional[str] = None,
        hardlink: bool = False,
        symlink: bool = True,
    ) -> Optional[FileID]:
        try:
            if issubclass(otherCls, AWSJobStore):
                srcObj = get_object_for_url(uri, existing=True)
                size = srcObj.content_length
                if shared_file_name is None:
                    info = self.FileInfo.create(srcObj.key)
                else:
                    self._requireValidSharedFileName(shared_file_name)
                    jobStoreFileID = self._shared_file_id(shared_file_name)
                    info = self.FileInfo.loadOrCreate(
                        jobStoreFileID=jobStoreFileID,
                        ownerID=str(self.sharedFileOwnerID),
                        encrypted=None,
                    )
                info.copyFrom(srcObj)
                info.save()
                return FileID(info.fileID, size) if shared_file_name is None else None
        except (NoBucketLocationError, ServerSideCopyProhibitedError):
            # AWS refuses to tell us where the bucket is or do this copy for us
            logger.warning(
                "Falling back to copying via the local machine. This could get expensive!"
            )

        # copy if exception
        return super()._import_file(otherCls, uri, shared_file_name=shared_file_name)

    def _export_file(self, otherCls, file_id: FileID, uri: ParseResult) -> None:
        try:
            if issubclass(otherCls, AWSJobStore):
                dstObj = get_object_for_url(uri)
                info = self.FileInfo.loadOrFail(file_id)
                info.copyTo(dstObj)
                return
        except (NoBucketLocationError, ServerSideCopyProhibitedError):
            # AWS refuses to tell us where the bucket is or do this copy for us
            logger.warning(
                "Falling back to copying via the local machine. This could get expensive!"
            )
        else:
            super()._default_export_file(otherCls, file_id, uri)

    @classmethod
    def _url_exists(cls, url: ParseResult) -> bool:
        try:
            get_object_for_url(url, existing=True)
            return True
        except FileNotFoundError:
            # Not a file
            # Might be a directory.
            return cls._get_is_directory(url)

    @classmethod
    def _get_size(cls, url: ParseResult) -> int:
        return get_object_for_url(url, existing=True).content_length

    @classmethod
    def _read_from_url(cls, url: ParseResult, writable):
        srcObj = get_object_for_url(url, existing=True)
        srcObj.download_fileobj(writable)
        return (srcObj.content_length, False)  # executable bit is always False

    @classmethod
    def _open_url(cls, url: ParseResult) -> IO[bytes]:
        src_obj = get_object_for_url(url, existing=True)
        response = src_obj.get()
        # We should get back a response with a stream in 'Body'
        if "Body" not in response:
            raise RuntimeError(f"Could not fetch body stream for {url}")
        return response["Body"]

    @classmethod
    def _write_to_url(
        cls, readable, url: ParseResult, executable: bool = False
    ) -> None:
        dstObj = get_object_for_url(url)

        logger.debug("Uploading %s", dstObj.key)
        # uploadFile takes care of using multipart upload if the file is larger than partSize (default to 5MB)
        uploadFile(
            readable=readable,
            resource=s3_boto3_resource,
            bucketName=dstObj.bucket_name,
            fileID=dstObj.key,
            partSize=5 * 1000 * 1000,
        )

    @classmethod
    def _list_url(cls, url: ParseResult) -> list[str]:
        return list_objects_for_url(url)

    @classmethod
    def _get_is_directory(cls, url: ParseResult) -> bool:
        # We consider it a directory if anything is in it.
        # TODO: Can we just get the first item and not the whole list?
        return len(list_objects_for_url(url)) > 0

    @classmethod
    def _supports_url(cls, url: ParseResult, export: bool = False) -> bool:
        return url.scheme.lower() == "s3"

    def write_file(
        self, local_path: FileID, job_id: Optional[FileID] = None, cleanup: bool = False
    ) -> FileID:
        info = self.FileInfo.create(job_id if cleanup else None)
        info.upload(local_path, not self.config.disableJobStoreChecksumVerification)
        info.save()
        logger.debug("Wrote %r of from %r", info, local_path)
        return info.fileID

    @contextmanager
    def write_file_stream(
        self,
        job_id: Optional[FileID] = None,
        cleanup: bool = False,
        basename=None,
        encoding=None,
        errors=None,
    ):
        info = self.FileInfo.create(job_id if cleanup else None)
        with info.uploadStream(encoding=encoding, errors=errors) as writable:
            yield writable, info.fileID
        info.save()
        logger.debug("Wrote %r.", info)

    @contextmanager
    def write_shared_file_stream(
        self, shared_file_name, encrypted=None, encoding=None, errors=None
    ):
        self._requireValidSharedFileName(shared_file_name)
        info = self.FileInfo.loadOrCreate(
            jobStoreFileID=self._shared_file_id(shared_file_name),
            ownerID=str(self.sharedFileOwnerID),
            encrypted=encrypted,
        )
        with info.uploadStream(encoding=encoding, errors=errors) as writable:
            yield writable
        info.save()
        logger.debug("Wrote %r for shared file %r.", info, shared_file_name)

    def update_file(self, file_id, local_path):
        info = self.FileInfo.loadOrFail(file_id)
        info.upload(local_path, not self.config.disableJobStoreChecksumVerification)
        info.save()
        logger.debug("Wrote %r from path %r.", info, local_path)

    @contextmanager
    def update_file_stream(self, file_id, encoding=None, errors=None):
        info = self.FileInfo.loadOrFail(file_id)
        with info.uploadStream(encoding=encoding, errors=errors) as writable:
            yield writable
        info.save()
        logger.debug("Wrote %r from stream.", info)

    def file_exists(self, file_id):
        return self.FileInfo.exists(file_id)

    def get_file_size(self, file_id):
        if not self.file_exists(file_id):
            return 0
        info = self.FileInfo.loadOrFail(file_id)
        return info.getSize()

    def read_file(self, file_id, local_path, symlink=False):
        info = self.FileInfo.loadOrFail(file_id)
        logger.debug("Reading %r into %r.", info, local_path)
        info.download(local_path, not self.config.disableJobStoreChecksumVerification)
        if getattr(file_id, "executable", False):
            os.chmod(local_path, os.stat(local_path).st_mode | stat.S_IXUSR)

    @contextmanager
    def read_file_stream(self, file_id, encoding=None, errors=None):
        info = self.FileInfo.loadOrFail(file_id)
        logger.debug("Reading %r into stream.", info)
        with info.downloadStream(encoding=encoding, errors=errors) as readable:
            yield readable

    @contextmanager
    def read_shared_file_stream(self, shared_file_name, encoding=None, errors=None):
        self._requireValidSharedFileName(shared_file_name)
        jobStoreFileID = self._shared_file_id(shared_file_name)
        info = self.FileInfo.loadOrFail(jobStoreFileID, customName=shared_file_name)
        logger.debug(
            "Reading %r for shared file %r into stream.", info, shared_file_name
        )
        with info.downloadStream(encoding=encoding, errors=errors) as readable:
            yield readable

    def delete_file(self, file_id):
        info = self.FileInfo.load(file_id)
        if info is None:
            logger.debug("File %s does not exist, skipping deletion.", file_id)
        else:
            info.delete()

    def write_logs(self, msg):
        info = self.FileInfo.create(str(self.statsFileOwnerID))
        with info.uploadStream(multipart=False) as writeable:
            if isinstance(msg, str):
                # This stream is for binary data, so encode any non-encoded things
                msg = msg.encode("utf-8", errors="ignore")
            writeable.write(msg)
        info.save()

    def read_logs(self, callback, read_all=False):
        itemsProcessed = 0

        for info in self._read_logs(callback, self.statsFileOwnerID):
            info._ownerID = str(self.readStatsFileOwnerID)  # boto3 requires strings
            info.save()
            itemsProcessed += 1

        if read_all:
            for _ in self._read_logs(callback, self.readStatsFileOwnerID):
                itemsProcessed += 1

        return itemsProcessed

    def _read_logs(self, callback, ownerId):
        items = None
        for attempt in retry_sdb():
            with attempt:
                items = boto3_pager(
                    self.db.select,
                    "Items",
                    ConsistentRead=True,
                    SelectExpression=f"select * from `{self.files_domain_name}` where ownerID='{str(ownerId)}'",
                )
        assert items is not None
        for item in items:
            info = self.FileInfo.fromItem(item)
            with info.downloadStream() as readable:
                callback(readable)
            yield info

    # TODO: Make this retry more specific?
    #  example: https://github.com/DataBiosphere/toil/issues/3378
    @retry()
    def get_public_url(self, jobStoreFileID):
        info = self.FileInfo.loadOrFail(jobStoreFileID)
        if info.content is not None:
            with info.uploadStream(allowInlining=False) as f:
                f.write(info.content)

        self.files_bucket.Object(compat_bytes(jobStoreFileID)).Acl().put(
            ACL="public-read"
        )

        url = self.s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.files_bucket.name,
                "Key": compat_bytes(jobStoreFileID),
                "VersionId": info.version,
            },
            ExpiresIn=self.publicUrlExpiration.total_seconds(),
        )

        # boto doesn't properly remove the x-amz-security-token parameter when
        # query_auth is False when using an IAM role (see issue #2043). Including the
        # x-amz-security-token parameter without the access key results in a 403,
        # even if the resource is public, so we need to remove it.
        scheme, netloc, path, query, fragment = urlsplit(url)
        params = parse_qs(query)
        if "x-amz-security-token" in params:
            del params["x-amz-security-token"]
        if "AWSAccessKeyId" in params:
            del params["AWSAccessKeyId"]
        if "Signature" in params:
            del params["Signature"]
        query = urlencode(params, doseq=True)
        url = urlunsplit((scheme, netloc, path, query, fragment))
        return url

    def get_shared_public_url(self, shared_file_name):
        self._requireValidSharedFileName(shared_file_name)
        return self.get_public_url(self._shared_file_id(shared_file_name))

    def _bindBucket(
        self,
        bucket_name: str,
        create: bool = False,
        block: bool = True,
        versioning: bool = False,
        check_versioning_consistency: bool = True,
    ):
        """
        Return the Boto Bucket object representing the S3 bucket with the given name. If the
        bucket does not exist and `create` is True, it will be created.

        :param str bucket_name: the name of the bucket to bind to

        :param bool create: Whether to create bucket the if it doesn't exist

        :param bool block: If False, return None if the bucket doesn't exist. If True, wait until
               bucket appears. Ignored if `create` is True.

        :rtype: Bucket|None
        :raises botocore.exceptions.ClientError: If `block` is True and the bucket still doesn't exist after the
                retry timeout expires.
        """
        assert self.minBucketNameLen <= len(bucket_name) <= self.maxBucketNameLen
        assert self.bucketNameRe.match(bucket_name)
        logger.debug("Binding to job store bucket '%s'.", bucket_name)

        def bucket_retry_predicate(error):
            """
            Decide, given an error, whether we should retry binding the bucket.
            """

            if isinstance(error, ClientError) and get_error_status(error) in (404, 409):
                # Handle cases where the bucket creation is in a weird state that might let us proceed.
                # https://github.com/BD2KGenomics/toil/issues/955
                # https://github.com/BD2KGenomics/toil/issues/995
                # https://github.com/BD2KGenomics/toil/issues/1093

                # BucketAlreadyOwnedByYou == 409
                # OperationAborted == 409
                # NoSuchBucket == 404
                return True
            if get_error_code(error) == "SlowDown":
                # We may get told to SlowDown by AWS when we try to create our
                # bucket. In that case, we should retry and use the exponential
                # backoff.
                return True
            return False

        bucketExisted = True
        for attempt in retry_s3(predicate=bucket_retry_predicate):
            with attempt:
                try:
                    # the head_bucket() call makes sure that the bucket exists and the user can access it
                    self.s3_client.head_bucket(Bucket=bucket_name)

                    bucket = self.s3_resource.Bucket(bucket_name)
                except ClientError as e:
                    error_http_status = get_error_status(e)
                    if error_http_status == 404:
                        bucketExisted = False
                        logger.debug("Bucket '%s' does not exist.", bucket_name)
                        if create:
                            bucket = create_s3_bucket(
                                self.s3_resource, bucket_name, self.region
                            )
                            # Wait until the bucket exists before checking the region and adding tags
                            bucket.wait_until_exists()

                            # It is possible for create_bucket to return but
                            # for an immediate request for the bucket region to
                            # produce an S3ResponseError with code
                            # NoSuchBucket. We let that kick us back up to the
                            # main retry loop.
                            assert (
                                get_bucket_region(bucket_name) == self.region
                            ), f"bucket_name: {bucket_name}, {get_bucket_region(bucket_name)} != {self.region}"

                            tags = build_tag_dict_from_env()

                            if tags:
                                flat_tags = flatten_tags(tags)
                                bucket_tagging = self.s3_resource.BucketTagging(
                                    bucket_name
                                )
                                bucket_tagging.put(Tagging={"TagSet": flat_tags})

                            # Configure bucket so that we can make objects in
                            # it public, which was the historical default.
                            enable_public_objects(bucket_name)
                        elif block:
                            raise
                        else:
                            return None
                    elif error_http_status == 301:
                        # This is raised if the user attempts to get a bucket in a region outside
                        # the specified one, if the specified one is not `us-east-1`.  The us-east-1
                        # server allows a user to use buckets from any region.
                        raise BucketLocationConflictException(
                            get_bucket_region(bucket_name)
                        )
                    else:
                        raise
                else:
                    bucketRegion = get_bucket_region(bucket_name)
                    if bucketRegion != self.region:
                        raise BucketLocationConflictException(bucketRegion)

                if versioning and not bucketExisted:
                    # only call this method on bucket creation
                    bucket.Versioning().enable()
                    # Now wait until versioning is actually on. Some uploads
                    # would come back with no versions; maybe they were
                    # happening too fast and this setting isn't sufficiently
                    # consistent?
                    time.sleep(1)
                    while not self._getBucketVersioning(bucket_name):
                        logger.warning(
                            f"Waiting for versioning activation on bucket '{bucket_name}'..."
                        )
                        time.sleep(1)
                elif check_versioning_consistency:
                    # now test for versioning consistency
                    # we should never see any of these errors since 'versioning' should always be true
                    bucket_versioning = self._getBucketVersioning(bucket_name)
                    if bucket_versioning != versioning:
                        assert False, "Cannot modify versioning on existing bucket"
                    elif bucket_versioning is None:
                        assert False, "Cannot use a bucket with versioning suspended"
                if bucketExisted:
                    logger.debug(
                        f"Using pre-existing job store bucket '{bucket_name}'."
                    )
                else:
                    logger.debug(
                        f"Created new job store bucket '{bucket_name}' with versioning state {versioning}."
                    )

                return bucket

    def _bindDomain(
        self, domain_name: str, create: bool = False, block: bool = True
    ) -> None:
        """
        Return the Boto3 domain name representing the SDB domain. When create=True, it will
        create the domain if it does not exist.
        Return the Boto Domain object representing the SDB domain of the given name. If the
        domain does not exist and `create` is True, it will be created.

        :param str domain_name: the name of the domain to bind to

        :param bool create: True if domain should be created if it doesn't exist

        :param bool block: If False, raise DomainDoesNotExist if the domain doesn't exist. If True, wait until
               domain appears. This parameter is ignored if create is True.

        :rtype: None
        :raises ClientError: If `block` is True and the domain still doesn't exist after the
                retry timeout expires.
        """
        logger.debug("Binding to job store domain '%s'.", domain_name)
        retryargs = dict(
            predicate=lambda e: no_such_sdb_domain(e) or sdb_unavailable(e)
        )
        if not block:
            retryargs["timeout"] = 15
        for attempt in retry_sdb(**retryargs):
            with attempt:
                try:
                    self.db.domain_metadata(DomainName=domain_name)
                    return
                except ClientError as e:
                    if no_such_sdb_domain(e):
                        if create:
                            self.db.create_domain(DomainName=domain_name)
                            return
                        elif block:
                            raise
                        else:
                            raise DomainDoesNotExist(domain_name)
                    else:
                        raise

    def _new_job_id(self):
        return str(uuid.uuid4())

    # A dummy job ID under which all shared files are stored
    sharedFileOwnerID = uuid.UUID("891f7db6-e4d9-4221-a58e-ab6cc4395f94")

    # A dummy job ID under which all unread stats files are stored
    statsFileOwnerID = uuid.UUID("bfcf5286-4bc7-41ef-a85d-9ab415b69d53")

    # A dummy job ID under which all read stats files are stored
    readStatsFileOwnerID = uuid.UUID("e77fc3aa-d232-4255-ae04-f64ee8eb0bfa")

    def _shared_file_id(self, shared_file_name):
        return str(uuid.uuid5(self.sharedFileOwnerID, shared_file_name))

    @InnerClass
    class FileInfo(SDBHelper):
        """
        Represents a file in this job store.
        """

        outer = None
        """
        :type: AWSJobStore
        """

        def __init__(
            self,
            fileID,
            ownerID,
            encrypted,
            version=None,
            content=None,
            numContentChunks=0,
            checksum=None,
        ):
            """
            :type fileID: str
            :param fileID: the file's ID

            :type ownerID: str
            :param ownerID: ID of the entity owning this file, typically a job ID aka jobStoreID

            :type encrypted: bool
            :param encrypted: whether the file is stored in encrypted form

            :type version: str|None
            :param version: a non-empty string containing the most recent version of the S3
            object storing this file's content, None if the file is new, or empty string if the
            file is inlined.

            :type content: str|None
            :param content: this file's inlined content

            :type numContentChunks: int
            :param numContentChunks: the number of SDB domain attributes occupied by this files

            :type checksum: str|None
            :param checksum: the checksum of the file, if available. Formatted
            as <algorithm>$<lowercase hex hash>.

            inlined content. Note that an inlined empty string still occupies one chunk.
            """
            super().__init__()
            self._fileID = fileID
            self._ownerID = ownerID
            self.encrypted = encrypted
            self._version = version
            self._previousVersion = version
            self._content = content
            self._checksum = checksum
            self._numContentChunks = numContentChunks

        @property
        def fileID(self):
            return self._fileID

        @property
        def ownerID(self):
            return self._ownerID

        @property
        def version(self):
            return self._version

        @version.setter
        def version(self, version):
            # Version should only change once
            assert self._previousVersion == self._version
            self._version = version
            if version:
                self.content = None

        @property
        def previousVersion(self):
            return self._previousVersion

        @property
        def content(self):
            return self._content

        @property
        def checksum(self):
            return self._checksum

        @checksum.setter
        def checksum(self, checksum):
            self._checksum = checksum

        @content.setter
        def content(self, content):
            assert content is None or isinstance(content, bytes)
            self._content = content
            if content is not None:
                self.version = ""

        @classmethod
        def create(cls, ownerID: str):
            return cls(
                str(uuid.uuid4()), ownerID, encrypted=cls.outer.sseKeyPath is not None
            )

        @classmethod
        def presenceIndicator(cls):
            return "encrypted"

        @classmethod
        def exists(cls, jobStoreFileID):
            for attempt in retry_sdb():
                with attempt:
                    return bool(
                        cls.outer.db.get_attributes(
                            DomainName=cls.outer.files_domain_name,
                            ItemName=compat_bytes(jobStoreFileID),
                            AttributeNames=[cls.presenceIndicator()],
                            ConsistentRead=True,
                        ).get("Attributes", [])
                    )

        @classmethod
        def load(cls, jobStoreFileID):
            for attempt in retry_sdb():
                with attempt:
                    self = cls.fromItem(
                        {
                            "Name": compat_bytes(jobStoreFileID),
                            "Attributes": cls.outer.db.get_attributes(
                                DomainName=cls.outer.files_domain_name,
                                ItemName=compat_bytes(jobStoreFileID),
                                ConsistentRead=True,
                            ).get("Attributes", []),
                        }
                    )
                    return self

        @classmethod
        def loadOrCreate(cls, jobStoreFileID, ownerID, encrypted):
            self = cls.load(jobStoreFileID)
            if encrypted is None:
                encrypted = cls.outer.sseKeyPath is not None
            if self is None:
                self = cls(jobStoreFileID, ownerID, encrypted=encrypted)
            else:
                assert self.fileID == jobStoreFileID
                assert self.ownerID == ownerID
                self.encrypted = encrypted
            return self

        @classmethod
        def loadOrFail(cls, jobStoreFileID, customName=None):
            """
            :rtype: AWSJobStore.FileInfo
            :return: an instance of this class representing the file with the given ID
            :raises NoSuchFileException: if given file does not exist
            """
            self = cls.load(jobStoreFileID)
            if self is None:
                raise NoSuchFileException(jobStoreFileID, customName=customName)
            else:
                return self

        @classmethod
        def fromItem(cls, item: "ItemTypeDef"):
            """
            Convert an SDB item to an instance of this class.

            :type item: Item
            """
            assert item is not None

            # Strings come back from SDB as unicode
            def strOrNone(s):
                return s if s is None else str(s)

            # ownerID and encrypted are the only mandatory attributes
            ownerID, encrypted, version, checksum = SDBHelper.get_attributes_from_item(
                item, ["ownerID", "encrypted", "version", "checksum"]
            )
            if ownerID is None:
                assert encrypted is None
                return None
            else:
                encrypted = strict_bool(encrypted)
                content, numContentChunks = cls.attributesToBinary(item["Attributes"])
                if encrypted:
                    sseKeyPath = cls.outer.sseKeyPath
                    if sseKeyPath is None:
                        raise AssertionError(
                            "Content is encrypted but no key was provided."
                        )
                    if content is not None:
                        content = encryption.decrypt(content, sseKeyPath)
                self = cls(
                    fileID=item["Name"],
                    ownerID=ownerID,
                    encrypted=encrypted,
                    version=version,
                    content=content,
                    numContentChunks=numContentChunks,
                    checksum=checksum,
                )
                return self

        def toItem(self) -> tuple[dict[str, str], int]:
            """
            Convert this instance to a dictionary of attribute names to values

            :return: the attributes dict and an integer specifying the the number of chunk
                     attributes in the dictionary that are used for storing inlined content.
            """
            content = self.content
            assert content is None or isinstance(content, bytes)
            if self.encrypted and content is not None:
                sseKeyPath = self.outer.sseKeyPath
                if sseKeyPath is None:
                    raise AssertionError(
                        "Encryption requested but no key was provided."
                    )
                content = encryption.encrypt(content, sseKeyPath)
            assert content is None or isinstance(content, bytes)
            attributes = self.binaryToAttributes(content)
            numChunks = int(attributes["numChunks"])
            attributes.update(
                dict(
                    ownerID=self.ownerID or "",
                    encrypted=str(self.encrypted),
                    version=self.version or "",
                    checksum=self.checksum or "",
                )
            )
            return attributes, numChunks

        @classmethod
        def _reservedAttributes(cls):
            return 3 + super()._reservedAttributes()

        @staticmethod
        def maxInlinedSize():
            return 256

        def save(self):
            attributes, numNewContentChunks = self.toItem()
            attributes_boto3 = SDBHelper.attributeDictToList(attributes)
            # False stands for absence
            if self.previousVersion is None:
                expected: "UpdateConditionTypeDef" = {
                    "Name": "version",
                    "Exists": False,
                }
            else:
                expected = {"Name": "version", "Value": cast(str, self.previousVersion)}
            try:
                for attempt in retry_sdb():
                    with attempt:
                        self.outer.db.put_attributes(
                            DomainName=self.outer.files_domain_name,
                            ItemName=compat_bytes(self.fileID),
                            Attributes=[
                                {
                                    "Name": attribute["Name"],
                                    "Value": attribute["Value"],
                                    "Replace": True,
                                }
                                for attribute in attributes_boto3
                            ],
                            Expected=expected,
                        )
                # clean up the old version of the file if necessary and safe
                if self.previousVersion and (self.previousVersion != self.version):
                    for attempt in retry_s3():
                        with attempt:
                            self.outer.s3_client.delete_object(
                                Bucket=self.outer.files_bucket.name,
                                Key=compat_bytes(self.fileID),
                                VersionId=self.previousVersion,
                            )
                self._previousVersion = self._version
                if numNewContentChunks < self._numContentChunks:
                    residualChunks = range(numNewContentChunks, self._numContentChunks)
                    residual_chunk_names = [self._chunkName(i) for i in residualChunks]
                    # boto3 requires providing the value as well as the name in the attribute, and we don't store it locally
                    # the php sdk resolves this issue by not requiring the Value key https://github.com/aws/aws-sdk-php/issues/185
                    # but this doesnt extend to boto3
                    delete_attributes = self.outer.db.get_attributes(
                        DomainName=self.outer.files_domain_name,
                        ItemName=compat_bytes(self.fileID),
                        AttributeNames=[chunk for chunk in residual_chunk_names],
                    ).get("Attributes")
                    for attempt in retry_sdb():
                        with attempt:
                            self.outer.db.delete_attributes(
                                DomainName=self.outer.files_domain_name,
                                ItemName=compat_bytes(self.fileID),
                                Attributes=delete_attributes,
                            )
                    self.outer.db.get_attributes(
                        DomainName=self.outer.files_domain_name,
                        ItemName=compat_bytes(self.fileID),
                    )

                self._numContentChunks = numNewContentChunks
            except ClientError as e:
                if get_error_code(e) == "ConditionalCheckFailed":
                    raise ConcurrentFileModificationException(self.fileID)
                else:
                    raise

        def upload(self, localFilePath, calculateChecksum=True):
            file_size, file_time = fileSizeAndTime(localFilePath)
            if file_size <= self.maxInlinedSize():
                with open(localFilePath, "rb") as f:
                    self.content = f.read()
                # Clear out any old checksum in case of overwrite
                self.checksum = ""
            else:
                headerArgs = self._s3EncryptionArgs()
                # Create a new Resource in case it needs to be on its own thread
                resource = boto3_session.resource("s3", region_name=self.outer.region)

                self.checksum = (
                    self._get_file_checksum(localFilePath)
                    if calculateChecksum
                    else None
                )
                self.version = uploadFromPath(
                    localFilePath,
                    resource=resource,
                    bucketName=self.outer.files_bucket.name,
                    fileID=compat_bytes(self.fileID),
                    headerArgs=headerArgs,
                    partSize=self.outer.part_size,
                )

        def _start_checksum(self, to_match=None, algorithm="sha1"):
            """
            Get a hasher that can be used with _update_checksum and
            _finish_checksum.

            If to_match is set, it is a precomputed checksum which we expect
            the result to match.

            The right way to compare checksums is to feed in the checksum to be
            matched, so we can see its algorithm, instead of getting a new one
            and comparing. If a checksum to match is fed in, _finish_checksum()
            will raise a ChecksumError if it isn't matched.
            """

            # If we have an expexted result it will go here.
            expected = None

            if to_match is not None:
                parts = to_match.split("$")
                algorithm = parts[0]
                expected = parts[1]

            wrapped = getattr(hashlib, algorithm)()
            logger.debug(f"Starting {algorithm} checksum to match {expected}")
            return algorithm, wrapped, expected

        def _update_checksum(self, checksum_in_progress, data):
            """
            Update a checksum in progress from _start_checksum with new data.
            """
            checksum_in_progress[1].update(data)

        def _finish_checksum(self, checksum_in_progress):
            """
            Complete a checksum in progress from _start_checksum and return the
            checksum result string.
            """

            result_hash = checksum_in_progress[1].hexdigest()

            logger.debug(
                f"Completed checksum with hash {result_hash} vs. expected {checksum_in_progress[2]}"
            )
            if checksum_in_progress[2] is not None:
                # We expected a particular hash
                if result_hash != checksum_in_progress[2]:
                    raise ChecksumError(
                        "Checksum mismatch. Expected: %s Actual: %s"
                        % (checksum_in_progress[2], result_hash)
                    )

            return "$".join([checksum_in_progress[0], result_hash])

        def _get_file_checksum(self, localFilePath, to_match=None):
            with open(localFilePath, "rb") as f:
                hasher = self._start_checksum(to_match=to_match)
                contents = f.read(1024 * 1024)
                while contents != b"":
                    self._update_checksum(hasher, contents)
                    contents = f.read(1024 * 1024)
                return self._finish_checksum(hasher)

        @contextmanager
        def uploadStream(
            self, multipart=True, allowInlining=True, encoding=None, errors=None
        ):
            """
            Context manager that gives out a binary or text mode upload stream to upload data.
            """

            # Note that we have to handle already having a content or a version
            # if we are overwriting something.

            # But make sure we don't have both.
            assert not (bool(self.version) and self.content is not None)

            info = self
            store = self.outer

            class MultiPartPipe(WritablePipe):
                def readFrom(self, readable):
                    # Get the first block of data we want to put
                    buf = readable.read(store.part_size)
                    assert isinstance(buf, bytes)

                    if allowInlining and len(buf) <= info.maxInlinedSize():
                        logger.debug("Inlining content of %d bytes", len(buf))
                        info.content = buf
                        # There will be no checksum
                        info.checksum = ""
                    else:
                        # We will compute a checksum
                        hasher = info._start_checksum()
                        logger.debug("Updating checksum with %d bytes", len(buf))
                        info._update_checksum(hasher, buf)

                        client = store.s3_client
                        bucket_name = store.files_bucket.name
                        headerArgs = info._s3EncryptionArgs()

                        for attempt in retry_s3():
                            with attempt:
                                logger.debug("Starting multipart upload")
                                # low-level clients are thread safe
                                upload = client.create_multipart_upload(
                                    Bucket=bucket_name,
                                    Key=compat_bytes(info.fileID),
                                    **headerArgs,
                                )
                                uploadId = upload["UploadId"]
                                parts = []
                                logger.debug("Multipart upload started as %s", uploadId)

                        for attempt in retry_s3():
                            with attempt:
                                for i in range(CONSISTENCY_TICKS):
                                    # Sometimes we can create a multipart upload and not see it. Wait around for it.
                                    response = client.list_multipart_uploads(
                                        Bucket=bucket_name,
                                        MaxUploads=1,
                                        Prefix=compat_bytes(info.fileID),
                                    )
                                    if (
                                        "Uploads" in response
                                        and len(response["Uploads"]) != 0
                                        and response["Uploads"][0]["UploadId"]
                                        == uploadId
                                    ):

                                        logger.debug(
                                            "Multipart upload visible as %s", uploadId
                                        )
                                        break
                                    else:
                                        logger.debug(
                                            "Multipart upload %s is not visible; we see %s",
                                            uploadId,
                                            response.get("Uploads"),
                                        )
                                        time.sleep(CONSISTENCY_TIME * 2**i)

                        try:
                            for part_num in itertools.count():
                                for attempt in retry_s3():
                                    with attempt:
                                        logger.debug(
                                            "Uploading part %d of %d bytes to %s",
                                            part_num + 1,
                                            len(buf),
                                            uploadId,
                                        )
                                        # TODO: include the Content-MD5 header:
                                        #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.complete_multipart_upload
                                        part = client.upload_part(
                                            Bucket=bucket_name,
                                            Key=compat_bytes(info.fileID),
                                            PartNumber=part_num + 1,
                                            UploadId=uploadId,
                                            Body=BytesIO(buf),
                                            **headerArgs,
                                        )

                                        parts.append(
                                            {
                                                "PartNumber": part_num + 1,
                                                "ETag": part["ETag"],
                                            }
                                        )

                                # Get the next block of data we want to put
                                buf = readable.read(info.outer.part_size)
                                assert isinstance(buf, bytes)
                                if len(buf) == 0:
                                    # Don't allow any part other than the very first to be empty.
                                    break
                                info._update_checksum(hasher, buf)
                        except:
                            with panic(log=logger):
                                for attempt in retry_s3():
                                    with attempt:
                                        client.abort_multipart_upload(
                                            Bucket=bucket_name,
                                            Key=compat_bytes(info.fileID),
                                            UploadId=uploadId,
                                        )

                        else:

                            while not store._getBucketVersioning(
                                store.files_bucket.name
                            ):
                                logger.warning(
                                    "Versioning does not appear to be enabled yet. Deferring multipart "
                                    "upload completion..."
                                )
                                time.sleep(1)

                            # Save the checksum
                            info.checksum = info._finish_checksum(hasher)

                            for attempt in retry_s3(timeout=600):
                                # Wait here for a bit longer if S3 breaks,
                                # because we have been known to flake out here
                                # in tests
                                # (https://github.com/DataBiosphere/toil/issues/3894)
                                with attempt:
                                    logger.debug("Attempting to complete upload...")
                                    completed = client.complete_multipart_upload(
                                        Bucket=bucket_name,
                                        Key=compat_bytes(info.fileID),
                                        UploadId=uploadId,
                                        MultipartUpload={"Parts": parts},
                                    )

                                    logger.debug(
                                        "Completed upload object of type %s: %s",
                                        str(type(completed)),
                                        repr(completed),
                                    )
                                    info.version = completed.get("VersionId")
                                    logger.debug(
                                        "Completed upload with version %s",
                                        str(info.version),
                                    )

                            if info.version is None:
                                # Somehow we don't know the version. Try and get it.
                                for attempt in retry_s3(
                                    predicate=lambda e: retryable_s3_errors(e)
                                    or isinstance(e, AssertionError)
                                ):
                                    with attempt:
                                        version = client.head_object(
                                            Bucket=bucket_name,
                                            Key=compat_bytes(info.fileID),
                                            **headerArgs,
                                        ).get("VersionId", None)
                                        logger.warning(
                                            "Loaded key for upload with no version and got version %s",
                                            str(version),
                                        )
                                        info.version = version
                                        assert info.version is not None

                    # Make sure we actually wrote something, even if an empty file
                    assert bool(info.version) or info.content is not None

            class SinglePartPipe(WritablePipe):
                def readFrom(self, readable):
                    buf = readable.read()
                    assert isinstance(buf, bytes)
                    dataLength = len(buf)
                    if allowInlining and dataLength <= info.maxInlinedSize():
                        logger.debug("Inlining content of %d bytes", len(buf))
                        info.content = buf
                        # There will be no checksum
                        info.checksum = ""
                    else:
                        # We will compute a checksum
                        hasher = info._start_checksum()
                        info._update_checksum(hasher, buf)
                        info.checksum = info._finish_checksum(hasher)

                        bucket_name = store.files_bucket.name
                        headerArgs = info._s3EncryptionArgs()
                        client = store.s3_client

                        buf = BytesIO(buf)

                        while not store._getBucketVersioning(bucket_name):
                            logger.warning(
                                "Versioning does not appear to be enabled yet. Deferring single part "
                                "upload..."
                            )
                            time.sleep(1)

                        for attempt in retry_s3():
                            with attempt:
                                logger.debug(
                                    "Uploading single part of %d bytes", dataLength
                                )
                                client.upload_fileobj(
                                    Bucket=bucket_name,
                                    Key=compat_bytes(info.fileID),
                                    Fileobj=buf,
                                    ExtraArgs=headerArgs,
                                )

                                # use head_object with the SSE headers to access versionId and content_length attributes
                                headObj = client.head_object(
                                    Bucket=bucket_name,
                                    Key=compat_bytes(info.fileID),
                                    **headerArgs,
                                )
                                assert dataLength == headObj.get("ContentLength", None)
                                info.version = headObj.get("VersionId", None)
                                logger.debug(
                                    "Upload received version %s", str(info.version)
                                )

                        if info.version is None:
                            # Somehow we don't know the version
                            for attempt in retry_s3(
                                predicate=lambda e: retryable_s3_errors(e)
                                or isinstance(e, AssertionError)
                            ):
                                with attempt:
                                    headObj = client.head_object(
                                        Bucket=bucket_name,
                                        Key=compat_bytes(info.fileID),
                                        **headerArgs,
                                    )
                                    info.version = headObj.get("VersionId", None)
                                    logger.warning(
                                        "Reloaded key with no version and got version %s",
                                        str(info.version),
                                    )
                                    assert info.version is not None

                    # Make sure we actually wrote something, even if an empty file
                    assert bool(info.version) or info.content is not None

            if multipart:
                pipe = MultiPartPipe(encoding=encoding, errors=errors)
            else:
                pipe = SinglePartPipe(encoding=encoding, errors=errors)

            with pipe as writable:
                yield writable

            if not pipe.reader_done:
                logger.debug(f"Version: {self.version} Content: {self.content}")
                raise RuntimeError(
                    "Escaped context manager without written data being read!"
                )

            # We check our work to make sure we have exactly one of embedded
            # content or a real object version.

            if self.content is None:
                if not bool(self.version):
                    logger.debug(f"Version: {self.version} Content: {self.content}")
                    raise RuntimeError("No content added and no version created")
            else:
                if bool(self.version):
                    logger.debug(f"Version: {self.version} Content: {self.content}")
                    raise RuntimeError("Content added and version created")

        def copyFrom(self, srcObj):
            """
            Copies contents of source key into this file.

            :param S3.Object srcObj: The key (object) that will be copied from
            """
            assert srcObj.content_length is not None
            if srcObj.content_length <= self.maxInlinedSize():
                self.content = srcObj.get().get("Body").read()
            else:
                # Create a new Resource in case it needs to be on its own thread
                resource = boto3_session.resource("s3", region_name=self.outer.region)
                self.version = copyKeyMultipart(
                    resource,
                    srcBucketName=compat_bytes(srcObj.bucket_name),
                    srcKeyName=compat_bytes(srcObj.key),
                    srcKeyVersion=compat_bytes(srcObj.version_id),
                    dstBucketName=compat_bytes(self.outer.files_bucket.name),
                    dstKeyName=compat_bytes(self._fileID),
                    sseAlgorithm="AES256",
                    sseKey=self._getSSEKey(),
                )

        def copyTo(self, dstObj):
            """
            Copies contents of this file to the given key.

            :param S3.Object dstObj: The key (object) to copy this file's content to
            """
            if self.content is not None:
                for attempt in retry_s3():
                    with attempt:
                        dstObj.put(Body=self.content)
            elif self.version:
                # Create a new Resource in case it needs to be on its own thread
                resource = boto3_session.resource("s3", region_name=self.outer.region)

                for attempt in retry_s3():
                    # encrypted = True if self.outer.sseKeyPath else False
                    with attempt:
                        copyKeyMultipart(
                            resource,
                            srcBucketName=compat_bytes(self.outer.files_bucket.name),
                            srcKeyName=compat_bytes(self.fileID),
                            srcKeyVersion=compat_bytes(self.version),
                            dstBucketName=compat_bytes(dstObj.bucket_name),
                            dstKeyName=compat_bytes(dstObj.key),
                            copySourceSseAlgorithm="AES256",
                            copySourceSseKey=self._getSSEKey(),
                        )
            else:
                assert False

        def download(self, localFilePath, verifyChecksum=True):
            if self.content is not None:
                with AtomicFileCreate(localFilePath) as tmpPath:
                    with open(tmpPath, "wb") as f:
                        f.write(self.content)
            elif self.version:
                headerArgs = self._s3EncryptionArgs()
                obj = self.outer.files_bucket.Object(compat_bytes(self.fileID))

                for attempt in retry_s3(
                    predicate=lambda e: retryable_s3_errors(e)
                    or isinstance(e, ChecksumError)
                ):
                    with attempt:
                        with AtomicFileCreate(localFilePath) as tmpPath:
                            obj.download_file(
                                Filename=tmpPath,
                                ExtraArgs={"VersionId": self.version, **headerArgs},
                            )

                        if verifyChecksum and self.checksum:
                            try:
                                # This automatically compares the result and matches the algorithm.
                                self._get_file_checksum(localFilePath, self.checksum)
                            except ChecksumError as e:
                                # Annotate checksum mismatches with file name
                                raise ChecksumError(
                                    "Checksums do not match for file %s."
                                    % localFilePath
                                ) from e
                                # The error will get caught and result in a retry of the download until we run out of retries.
                                # TODO: handle obviously truncated downloads by resuming instead.
            else:
                assert False

        @contextmanager
        def downloadStream(self, verifyChecksum=True, encoding=None, errors=None):
            """
            Context manager that gives out a download stream to download data.
            """
            info = self

            class DownloadPipe(ReadablePipe):
                def writeTo(self, writable):
                    if info.content is not None:
                        writable.write(info.content)
                    elif info.version:
                        headerArgs = info._s3EncryptionArgs()
                        obj = info.outer.files_bucket.Object(compat_bytes(info.fileID))
                        for attempt in retry_s3():
                            with attempt:
                                obj.download_fileobj(
                                    writable,
                                    ExtraArgs={"VersionId": info.version, **headerArgs},
                                )
                    else:
                        assert False

            class HashingPipe(ReadableTransformingPipe):
                """
                Class which checksums all the data read through it. If it
                reaches EOF and the checksum isn't correct, raises
                ChecksumError.

                Assumes info actually has a checksum.
                """

                def transform(self, readable, writable):
                    hasher = info._start_checksum(to_match=info.checksum)
                    contents = readable.read(1024 * 1024)
                    while contents != b"":
                        info._update_checksum(hasher, contents)
                        try:
                            writable.write(contents)
                        except BrokenPipeError:
                            # Read was stopped early by user code.
                            # Can't check the checksum.
                            return
                        contents = readable.read(1024 * 1024)
                    # We reached EOF in the input.
                    # Finish checksumming and verify.
                    info._finish_checksum(hasher)
                    # Now stop so EOF happens in the output.

            if verifyChecksum and self.checksum:
                with DownloadPipe() as readable:
                    # Interpose a pipe to check the hash
                    with HashingPipe(
                        readable, encoding=encoding, errors=errors
                    ) as verified:
                        yield verified
            else:
                # Readable end of pipe produces text mode output if encoding specified
                with DownloadPipe(encoding=encoding, errors=errors) as readable:
                    # No true checksum available, so don't hash
                    yield readable

        def delete(self):
            store = self.outer
            if self.previousVersion is not None:
                expected: "UpdateConditionTypeDef" = {
                    "Name": "version",
                    "Value": cast(str, self.previousVersion),
                }
                for attempt in retry_sdb():
                    with attempt:
                        store.db.delete_attributes(
                            DomainName=store.files_domain_name,
                            ItemName=compat_bytes(self.fileID),
                            Expected=expected,
                        )
                if self.previousVersion:
                    for attempt in retry_s3():
                        with attempt:
                            store.s3_client.delete_object(
                                Bucket=store.files_bucket.name,
                                Key=compat_bytes(self.fileID),
                                VersionId=self.previousVersion,
                            )

        def getSize(self):
            """
            Return the size of the referenced item in bytes.
            """
            if self.content is not None:
                return len(self.content)
            elif self.version:
                for attempt in retry_s3():
                    with attempt:
                        obj = self.outer.files_bucket.Object(compat_bytes(self.fileID))
                        return obj.content_length
            else:
                return 0

        def _getSSEKey(self) -> Optional[bytes]:
            sseKeyPath = self.outer.sseKeyPath
            if sseKeyPath:
                with open(sseKeyPath, "rb") as f:
                    sseKey = f.read()
                return sseKey

        def _s3EncryptionArgs(self):
            # the keys of the returned dictionary are unpacked to the corresponding boto3 optional
            # parameters and will be used to set the http headers
            if self.encrypted:
                sseKey = self._getSSEKey()
                assert (
                    sseKey is not None
                ), "Content is encrypted but no key was provided."
                assert len(sseKey) == 32
                # boto3 encodes the key and calculates the MD5 for us
                return {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": sseKey}
            else:
                return {}

        def __repr__(self):
            r = custom_repr
            d = (
                ("fileID", r(self.fileID)),
                ("ownerID", r(self.ownerID)),
                ("encrypted", r(self.encrypted)),
                ("version", r(self.version)),
                ("previousVersion", r(self.previousVersion)),
                ("content", r(self.content)),
                ("checksum", r(self.checksum)),
                ("_numContentChunks", r(self._numContentChunks)),
            )
            return "{}({})".format(
                type(self).__name__, ", ".join(f"{k}={v}" for k, v in d)
            )

    versionings = dict(Enabled=True, Disabled=False, Suspended=None)

    def _getBucketVersioning(self, bucket_name):
        """
        The status attribute of BucketVersioning can be 'Enabled', 'Suspended' or None (Disabled)
        which we map to True, None and False respectively. Note that we've never seen a versioning
        status of 'Disabled', only the None return value. Calling BucketVersioning.suspend() will
        cause BucketVersioning.status to then return 'Suspended' even on a new bucket that never
        had versioning enabled.

        :param bucket_name: str
        """
        for attempt in retry_s3():
            with attempt:
                status = self.s3_resource.BucketVersioning(bucket_name).status
                return self.versionings.get(status) if status else False

    # TODO: Make this retry more specific?
    #  example: https://github.com/DataBiosphere/toil/issues/3378
    @retry()
    def destroy(self):
        # FIXME: Destruction of encrypted stores only works after initialize() or .resume()
        # See https://github.com/BD2KGenomics/toil/issues/1041
        try:
            self._bind(create=False, block=False, check_versioning_consistency=False)
        except BucketLocationConflictException:
            # If the unique jobstore bucket name existed, _bind would have raised a
            # BucketLocationConflictException before calling destroy.  Calling _bind here again
            # would reraise the same exception so we need to catch and ignore that exception.
            pass
        # TODO: Add other failure cases to be ignored here.
        self._registered = None
        if self.files_bucket is not None:
            self._delete_bucket(self.files_bucket)
            self.files_bucket = None
        for name in "files_domain_name", "jobs_domain_name":
            domainName = getattr(self, name)
            if domainName is not None:
                self._delete_domain(domainName)
                setattr(self, name, None)
        self._registered = False

    def _delete_domain(self, domainName):
        for attempt in retry_sdb():
            with attempt:
                try:
                    self.db.delete_domain(DomainName=domainName)
                except ClientError as e:
                    if not no_such_sdb_domain(e):
                        raise

    @staticmethod
    def _delete_bucket(bucket):
        """
        :param bucket: S3.Bucket
        """
        for attempt in retry_s3():
            with attempt:
                try:
                    uploads = s3_boto3_client.list_multipart_uploads(
                        Bucket=bucket.name
                    ).get("Uploads")
                    if uploads:
                        for u in uploads:
                            s3_boto3_client.abort_multipart_upload(
                                Bucket=bucket.name, Key=u["Key"], UploadId=u["UploadId"]
                            )

                    bucket.objects.all().delete()
                    bucket.object_versions.delete()
                    bucket.delete()
                except s3_boto3_client.exceptions.NoSuchBucket:
                    pass
                except ClientError as e:
                    if get_error_status(e) != 404:
                        raise


aRepr = reprlib.Repr()
aRepr.maxstring = 38  # so UUIDs don't get truncated (36 for UUID plus 2 for quotes)
custom_repr = aRepr.repr


class BucketLocationConflictException(LocatorException):
    def __init__(self, bucketRegion):
        super().__init__(
            "A bucket with the same name as the jobstore was found in another region (%s). "
            "Cannot proceed as the unique bucket name is already in use.",
            locator=bucketRegion,
        )
