# Copyright (C) 2015-2018 Regents of the University of California
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

"""
Contains an adapter to allow Boto 2 to use credentials obtained via Boto 3.
This allows it to do things like assuming roles as specified in Boto 3's config files.
"""

from __future__ import absolute_import
import errno
import logging
import threading
import time
import os
from datetime import datetime

from toil.lib.misc import mkdir_p

from boto.provider import Provider
from botocore.session import Session
from botocore.credentials import create_credential_resolver, RefreshableCredentials

# We cache the final credentials so that we don't send multiple processes to
# simultaneously bang on the EC2 metadata server or ask for MFA pins from the
# user.
cache_path = '~/.cache/aws/cached_temporary_credentials'
datetime_format = "%Y-%m-%dT%H:%M:%SZ"  # incidentally the same as the format used by AWS
log = logging.getLogger(__name__)

class BotoCredentialAdapter(Provider):
    """
    Adapter to allow Boto 2 to use AWS credentials obtained via Boto 3's
    credential finding logic. This allows for automatic role assumption
    respecting the Boto 3 config files, even when parts of the app still use
    Boto 2.
    
    This class also handles cacheing credentials in multi-process environments
    to avoid loads of processes swamping the EC2 metadata service.
    """
    
    def __init__(self):
        """
        Create a new BotoCredentialAdapter.
        """
        
        # Get ahold of a Boto3 credential resolver.
        # We need this before the superclass constructor calls get_credentials
        self._boto3_resolver = create_credential_resolver(Session())
        
        # Hardcode to use AWS. This approach can't work against Google Cloud.
        super(BotoCredentialAdapter, self).__init__('aws')
        
    def kwargs(self):
        """
        Produce a dictionary with 'aws_access_key_id', 'aws_secret_access_key',
        and 'security_token', if applicable, suitable for **-ing into the
        arguments to e.g. IAMConnection, which does not take a 'provider'.
        """
        
        self._obtain_credentials_from_cache_or_boto3()
        
        creds = {'aws_access_key_id': self._access_key, 'aws_secret_access_key': self._secret_key}
        
        if self._security_token is not None:
            creds['security_token'] = self._security_token
        
        return creds
        
    def get_credentials(self, access_key=None, secret_key=None, security_token=None, profile_name=None):
        """
        Make sure our credential fields are populated. Called by the base class
        constructor. All arguments ought to be None, because this class doesn't
        support picking up this info from anywhere but Boto 3.
        """
        
        assert(access_key is None)
        assert(secret_key is None)
        assert(security_token is None)
        assert(profile_name is None)
        
        # Go get the credentials from the cache, or from boto3 if not cached.
        # We need to be eager here; having the default None
        # _credential_expiry_time makes the accessors never try to refresh.
        self._obtain_credentials_from_cache_or_boto3()
        
    def _populate_keys_from_metadata_server(self):
        """
        This override is misnamed; it's actually the only hook we have to catch
        _credential_expiry_time being too soon and refresh the credentials. We
        actually just go back and poke the cache to see if it feels like
        getting us new credentials.
        
        Boto 2 hardcodes a refresh within 5 minutes of expiry:
        https://github.com/boto/boto/blob/591911db1029f2fbb8ba1842bfcc514159b37b32/boto/provider.py#L247
        
        Boto 3 wants to refresh 15 or 10 minutes before expiry:
        https://github.com/boto/botocore/blob/8d3ea0e61473fba43774eb3c74e1b22995ee7370/botocore/credentials.py#L279
        
        So if we ever want to refresh, Boto 3 wants to refresh too.
        """
        
        self._obtain_credentials_from_cache_or_boto3()
    
    def _obtain_credentials_from_boto3(self):
        """
        We know the current cached credentials are not good. Fill in our
        credential fields (_access_key, _secret_key, _security_token,
        _credential_expiry_time) from Boto 3.
        """
        
        # We get a Credentials object
        # <https://github.com/boto/botocore/blob/8d3ea0e61473fba43774eb3c74e1b22995ee7370/botocore/credentials.py#L227>
        # or a RefreshableCredentials
        creds = self._boto3_resolver.load_credentials()
        
        # Make sure the credentials actually has some credentials if it is lazy
        creds.get_frozen_credentials()
        
        # Get when the credentials will expire, if ever
        if isinstance(creds, RefreshableCredentials):
            # Credentials may expire
            self._credential_expiry_time = creds._expiry_time
        else:
            # Credentials never expire
            self._credential_expiry_time = None
        
        # Then, atomically get all the credentials bits. They may be newer than we think they are, but never older.
        frozen = creds.get_frozen_credentials()
        
        # Copy them into us
        self._access_key = frozen.access_key
        self._secret_key = frozen.secret_key
        self._security_token = frozen.token
    
    def _obtain_credentials_from_cache_or_boto3(self):
        """
        Get the cached credentials, or retrieve them from Boto 3 and cache them
        (or wait for another cooperating process to do so) if they are missing
        or not fresh enough.
        """
        path = os.path.expanduser(cache_path)
        tmp_path = path + '.tmp'
        while True:
            log.debug('Attempting to read cached credentials from %s.', path)
            try:
                with open(path, 'r') as f:
                    content = f.read()
                    if content:
                        record = content.split('\n')
                        assert len(record) == 4
                        self._access_key = record[0]
                        self._secret_key = record[1]
                        self._security_token = record[2]
                        self._credential_expiry_time = str_to_datetime(record[3])
                    else:
                        log.debug('%s is empty. Credentials are not temporary.', path)
                        return
            except IOError as e:
                if e.errno == errno.ENOENT:
                    log.debug('Cached credentials are missing.')
                    dir_path = os.path.dirname(path)
                    if not os.path.exists(dir_path):
                        log.debug('Creating parent directory %s', dir_path)
                        # A race would be ok at this point
                        mkdir_p(dir_path)
                else:
                    raise
            else:
                if self._credentials_need_refresh():
                    log.debug('Cached credentials are expired.')
                else:
                    log.debug('Cached credentials exist and are still fresh.')
                    return
            # We get here if credentials are missing or expired
            log.debug('Racing to create %s.', tmp_path)
            # Only one process, the winner, will succeed
            try:
                fd = os.open(tmp_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    log.debug('Lost the race to create %s. Waiting on winner to remove it.', tmp_path)
                    while os.path.exists(tmp_path):
                        time.sleep(0.1)
                    log.debug('Winner removed %s. Trying from the top.', tmp_path)
                else:
                    raise
            else:
                try:
                    log.debug('Won the race to create %s.  Requesting credentials from backend.', tmp_path)
                    self._obtain_credentials_from_boto3()
                except:
                    os.close(fd)
                    fd = None
                    log.debug('Failed to obtain credentials, removing %s.', tmp_path)
                    # This unblocks the loosers.
                    os.unlink(tmp_path)
                    # Bail out. It's too likely to happen repeatedly
                    raise
                else:
                    if self._credential_expiry_time is None:
                        os.close(fd)
                        fd = None
                        log.debug('Credentials are not temporary.  Leaving %s empty and renaming it to %s.', tmp_path, path)
                        # No need to actually cache permanent credentials,
                        # because we hnow we aren't getting them from the
                        # metadata server or by assuming a role. Those both
                        # give temporary credentials.
                    else:
                        log.debug('Writing credentials to %s.', tmp_path)
                        with os.fdopen(fd, 'w') as fh:
                            fd = None
                            fh.write('\n'.join([
                                self._access_key,
                                self._secret_key,
                                self._security_token,
                                datetime_to_str(self._credential_expiry_time)]))
                        log.debug('Wrote credentials to %s. Renaming to %s.', tmp_path, path)
                    os.rename(tmp_path, path)
                    return
                finally:
                    if fd is not None:
                        os.close(fd)

        
        


def datetime_to_str(dt):
    """
    Convert a naive (implicitly UTC) datetime object into a string, explicitly UTC.

    >>> datetime_to_str(datetime(1970, 1, 1, 0, 0, 0))
    '1970-01-01T00:00:00Z'
    """
    return dt.strftime(datetime_format)


def str_to_datetime(s):
    """
    Convert a string, explicitly UTC into a naive (implicitly UTC) datetime object.

    >>> str_to_datetime( '1970-01-01T00:00:00Z' )
    datetime.datetime(1970, 1, 1, 0, 0)

    Just to show that the constructor args for seconds and microseconds are optional:
    >>> datetime(1970, 1, 1, 0, 0, 0)
    datetime.datetime(1970, 1, 1, 0, 0)
    """
    return datetime.strptime(s, datetime_format)

