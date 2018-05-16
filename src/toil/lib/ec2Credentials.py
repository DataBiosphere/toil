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

# 5.14.2018: copied into Toil from https://github.com/BD2KGenomics/bd2k-python-lib

from __future__ import absolute_import
import errno
import logging
import threading
import time
import os
from datetime import datetime

log = logging.getLogger( __name__ )

cache_path = '~/.cache/aws/cached_temporary_credentials'

datetime_format = "%Y-%m-%dT%H:%M:%SZ"  # incidentally the same as the format used by AWS


def mkdir_p( path ):
    """
    The equivalent of mkdir -p
    """
    try:
        os.makedirs( path )
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir( path ):
            pass
        else:
            raise

def datetime_to_str( dt ):
    """
    Convert a naive (implicitly UTC) datetime object into a string, explicitly UTC.

    >>> datetime_to_str( datetime( 1970, 1, 1, 0, 0, 0 ) )
    '1970-01-01T00:00:00Z'
    """
    return dt.strftime( datetime_format )


def str_to_datetime( s ):
    """
    Convert a string, explicitly UTC into a naive (implicitly UTC) datetime object.

    >>> str_to_datetime( '1970-01-01T00:00:00Z' )
    datetime.datetime(1970, 1, 1, 0, 0)

    Just to show that the constructor args for seconds and microseconds are optional:
    >>> datetime(1970, 1, 1, 0, 0, 0)
    datetime.datetime(1970, 1, 1, 0, 0)
    """
    return datetime.strptime( s, datetime_format )


monkey_patch_lock = threading.RLock( )
_populate_keys_from_metadata_server_orig = None


def enable_metadata_credential_caching( ):
    """
    Monkey-patches Boto to allow multiple processes using it to share one set of cached, temporary
    IAM role credentials. This helps avoid hitting request limits imposed on the metadata service
    when too many processes concurrently request those credentials. Function is idempotent.

    This function should be called before any AWS connections attempts are made with Boto.
    """
    global _populate_keys_from_metadata_server_orig
    with monkey_patch_lock:
        if _populate_keys_from_metadata_server_orig is None:
            from boto.provider import Provider
            _populate_keys_from_metadata_server_orig = Provider._populate_keys_from_metadata_server
            Provider._populate_keys_from_metadata_server = _populate_keys_from_metadata_server


def disable_metadata_credential_caching( ):
    """
    Reverse the effect of enable_metadata_credential_caching()
    """
    global _populate_keys_from_metadata_server_orig
    with monkey_patch_lock:
        if _populate_keys_from_metadata_server_orig is not None:
            from boto.provider import Provider
            Provider._populate_keys_from_metadata_server = _populate_keys_from_metadata_server_orig
            _populate_keys_from_metadata_server_orig = None


def _populate_keys_from_metadata_server( self ):
    global _populate_keys_from_metadata_server_orig
    path = os.path.expanduser( cache_path )
    tmp_path = path + '.tmp'
    while True:
        log.debug( 'Attempting to read cached credentials from %s.', path )
        try:
            with open( path, 'r' ) as f:
                content = f.read( )
                if content:
                    record = content.split( '\n' )
                    assert len(record) == 4
                    self._access_key = record[ 0 ]
                    self._secret_key = record[ 1 ]
                    self._security_token = record[ 2 ]
                    self._credential_expiry_time = str_to_datetime( record[ 3 ] )
                else:
                    log.debug( '%s is empty. Credentials are not temporary.', path )
                    return
        except IOError as e:
            if e.errno == errno.ENOENT:
                log.debug( 'Cached credentials are missing.' )
                dir_path = os.path.dirname( path )
                if not os.path.exists( dir_path ):
                    log.debug( 'Creating parent directory %s', dir_path )
                    # A race would be ok at this point
                    mkdir_p( dir_path )
            else:
                raise
        else:
            if self._credentials_need_refresh( ):
                log.debug( 'Cached credentials are expired.' )
            else:
                log.debug( 'Cached credentials exist and are still fresh.' )
                return
        # We get here if credentials are missing or expired
        log.debug( 'Racing to create %s.', tmp_path )
        # Only one process, the winner, will succeed
        try:
            fd = os.open( tmp_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600 )
        except OSError as e:
            if e.errno == errno.EEXIST:
                log.debug( 'Lost the race to create %s. Waiting on winner to remove it.', tmp_path )
                while os.path.exists( tmp_path ):
                    time.sleep( .1 )
                log.debug( 'Winner removed %s. Trying from the top.', tmp_path )
            else:
                raise
        else:
            try:
                log.debug( 'Won the race to create %s. '
                           'Requesting credentials from metadata service.', tmp_path )
                _populate_keys_from_metadata_server_orig( self )
            except:
                os.close( fd )
                fd = None
                log.debug( 'Failed to obtain credentials, removing %s.', tmp_path )
                # This unblocks the loosers.
                os.unlink( tmp_path )
                # Bail out. It's too likely to happen repeatedly
                raise
            else:
                if self._credential_expiry_time is None:
                    os.close( fd )
                    fd = None
                    log.debug( 'Credentials are not temporary. '
                               'Leaving %s empty and renaming it to %s.', tmp_path, path )
                else:
                    log.debug( 'Writing credentials to %s.', tmp_path )
                    with os.fdopen( fd, 'w' ) as fh:
                        fd = None
                        fh.write( '\n'.join( [
                            self._access_key,
                            self._secret_key,
                            self._security_token,
                            datetime_to_str( self._credential_expiry_time ) ] ) )
                    log.debug( 'Wrote credentials to %s. '
                               'Renaming it to %s.', tmp_path, path )
                os.rename( tmp_path, path )
                return
            finally:
                if fd is not None:
                    os.close( fd )
