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
"""
Updates Toil's internal list of EC2 instance types.
"""
import logging
import socket

from toil.lib.ec2nodes import updateStaticEC2Instances

logger = logging.getLogger( __name__ )


def internetConnection():
    """
    Returns True if there is an internet connection present, and False otherwise.

    :return:
    """
    try:
        socket.create_connection(("www.stackoverflow.com", 80))
        return True
    except OSError:
        pass
    return False


def main():
    if not internetConnection():
        raise RuntimeError('No internet.  Updating the EC2 Instance list requires internet.')
    updateStaticEC2Instances()

if __name__=="__main__":
    main()
