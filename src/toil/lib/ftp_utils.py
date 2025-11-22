# Copyright 2017 Oregon Health and Science University
#
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


import ftplib
import logging
import netrc
import os
from contextlib import closing
from typing import IO, Any, cast
from urllib.parse import urlparse
from urllib.request import urlopen

logger = logging.getLogger(__name__)


class FtpFsAccess:
    """
    FTP access with upload.

    Taken and modified from https://github.com/ohsu-comp-bio/cwl-tes/blob/03f0096f9fae8acd527687d3460a726e09190c3a/cwl_tes/ftp.py#L37-L251
    """

    # TODO: Properly support FTP over SSL

    def __init__(self, cache: dict[Any, ftplib.FTP] | None = None):
        """
        FTP object to handle FTP connections. By default, connect over FTP with TLS.

        :param cache: cache of generated FTP objects
        """
        self.cache = cache or {}
        self.netrc = None
        try:
            if "HOME" in os.environ:
                if os.path.exists(os.path.join(os.environ["HOME"], ".netrc")):
                    self.netrc = netrc.netrc(os.path.join(os.environ["HOME"], ".netrc"))
            elif os.path.exists(os.path.join(os.curdir, ".netrc")):
                self.netrc = netrc.netrc(os.path.join(os.curdir, ".netrc"))
        except netrc.NetrcParseError as err:
            logger.debug(err)

    def exists(self, fn: str) -> bool:
        """
        Check if a file/directory exists over an FTP server
        :param fn: FTP url
        :return: True or false depending on whether the object exists on the server
        """
        return self.isfile(fn) or self.isdir(fn)

    def isfile(self, fn: str) -> bool:
        """
        Check if the FTP url points to a file
        :param fn: FTP url
        :return: True if url is file, else false
        """
        ftp = self._connect(fn)
        if ftp:
            try:
                if not self.size(fn) is None:
                    return True
                else:
                    return False
            except ftplib.all_errors:
                return False
        return False

    def isdir(self, fn: str) -> bool:
        """
        Check if the FTP url points to a directory
        :param fn: FTP url
        :return: True if url is directory, else false
        """
        ftp = self._connect(fn)
        if ftp:
            try:
                cwd = ftp.pwd()
                ftp.cwd(urlparse(fn).path)
                ftp.cwd(cwd)
                return True
            except ftplib.all_errors:
                return False
        return False

    def open(self, fn: str, mode: str) -> IO[bytes]:
        """
        Open an FTP url.

        Only supports reading, no write support.
        :param fn: FTP url
        :param mode: Mode to open FTP url in
        :return:
        """
        if "r" in mode:
            host, port, user, passwd, path = self._parse_url(fn)
            handle = urlopen(f"ftp://{user}:{passwd}@{host}:{port}/{path}")
            return cast(IO[bytes], closing(handle))
        # TODO: support write mode
        raise Exception("Write mode FTP not implemented")

    def _parse_url(self, url: str) -> tuple[str, int, str | None, str | None, str]:
        """
        Parse an FTP url into hostname, username, password, and path
        :param url:
        :return: hostname, username, password, path
        """
        parse = urlparse(url)
        user = parse.username
        passwd = parse.password
        host = parse.hostname
        port = parse.port
        path = parse.path
        if host is None:
            # The URL we connect to must have a host
            raise RuntimeError(f"FTP URL does not contain a host: {url}")
        # default port is 21
        if port is None:
            port = 21
        if parse.scheme == "ftp":
            if not user and self.netrc:
                if host is not None:
                    creds = self.netrc.authenticators(host)
                    if creds:
                        user, _, passwd = creds
        if not user:
            if host is not None:
                user, passwd = self._recall_credentials(host)
                if passwd is None:
                    passwd = "anonymous@"
                    if user is None:
                        user = "anonymous"
        return host, port, user, passwd, path

    def _connect(self, url: str) -> ftplib.FTP | None:
        """
        Connect to an FTP server. Handles authentication.
        :param url: FTP url
        :return: FTP object
        """
        parse = urlparse(url)
        if parse.scheme == "ftp":
            host, port, user, passwd, _ = self._parse_url(url)
            if host is None:
                # there has to be a host
                return None
            if (host, user, passwd) in self.cache:
                if self.cache[(host, user, passwd)].pwd():
                    return self.cache[(host, user, passwd)]
            ftp = ftplib.FTP_TLS()
            # Note: the FTP lib logger handles logging itself and doesn't go through our logging implementation
            ftp.set_debuglevel(1 if logger.isEnabledFor(logging.DEBUG) else 0)
            ftp.connect(host, port)
            env_user = os.getenv("TOIL_FTP_USER")
            env_passwd = os.getenv("TOIL_FTP_PASSWORD")
            if env_user:
                user = env_user
            if env_passwd:
                passwd = env_passwd
            ftp.login(user or "", passwd or "", secure=False)
            self.cache[(host, user, passwd)] = ftp
            return ftp
        return None

    def _recall_credentials(self, desired_host: str) -> tuple[str | None, str | None]:
        """
        Grab the cached credentials
        :param desired_host: FTP hostname
        :return: username, password
        """
        for host, user, passwd in self.cache:
            if desired_host == host:
                return user, passwd
        return None, None

    def size(self, fn: str) -> int | None:
        """
        Get the size of an FTP object
        :param fn: FTP url
        :return: Size of object
        """
        ftp = self._connect(fn)
        if ftp:
            host, port, user, passwd, path = self._parse_url(fn)
            try:
                return ftp.size(path)
            except ftplib.all_errors as e:
                if str(e) == "550 SIZE not allowed in ASCII mode":
                    # some servers don't allow grabbing size in ascii mode
                    # https://stackoverflow.com/questions/22090001/get-folder-size-using-ftplib/22093848#22093848
                    ftp.voidcmd("TYPE I")
                    return ftp.size(path)
                handle = urlopen(f"ftp://{user}:{passwd}@{host}:{port}/{path}")
                info = handle.info()
                handle.close()
                if "Content-length" in info:
                    return int(info["Content-length"])
                return None

        return None
