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


class DeadlockException(Exception):
    """
    Exception thrown by the Leader or BatchSystem when a deadlock is encountered due to insufficient
    resources to run the workflow
    """
    def __init__(self, msg):
        self.msg = f"Deadlock encountered: {msg}"
        super().__init__()

    def __str__(self):
        """
        Stringify the exception, including the message.
        """
        return self.msg
