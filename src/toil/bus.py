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

"""
Message types and message bus for leader component coordination.
"""

import collections
import inspect
import logging
import threading
from typing import Any, Dict, Iterator, List, NamedTuple, Type, TypeVar

logger = logging.getLogger( __name__ )

class JobUpdatedMessage(NamedTuple):
    """
    Produced when a job is "updated" and ready to have something happen to it.
    """
    # The job store ID of the job
    job_id: str
    # The error code/return code for the job, which is nonzero if something has
    # gone wrong, and 0 otherwise.
    result_status: int

class MessageBus:
    """
    Holds messages that should cause jobs to change their scheduling states.
    Messages are put in and buffered, and can be taken out and handled as
    batches when convenient.

    All messages are NamedTuple objects of various subtypes.

    Message order is not necessarily preserved.

    TODO: Not yet thread safe, but should be made thread safe if we want e.g.
    the ServiceManager to talk to it. Note that defaultdict itself isn't
    necessarily thread safe.
    """

    def __init__(self) -> None:
        # This holds all the messages on the bus, organized by type.
        self.__messages_by_type: Dict[type, List[NamedTuple]] = collections.defaultdict(list)

    # All our messages are NamedTuples, but NamedTuples don't actually inherit
    # from NamedTupe, so MyPy complains if we require that here.
    def put(self, message: Any) -> None:
        """
        Put a message onto the bus.
        """

        # Log who sent the message, and what it is
        # See: <https://stackoverflow.com/a/57712700>
        our_frame = inspect.currentframe()
        assert our_frame is not None, "Interpreter for Toil must have Python stack frame support"
        caller_frame = our_frame.f_back
        assert caller_frame is not None, "MessageBus.put() cannot determine its caller"
        logger.debug('%s sent: %s', caller_frame.f_code.co_name, message)

        self.__messages_by_type[type(message)].append(message)

    def count(self, message_type: type) -> int:
        """
        Get the number of pending messages of the given type.
        """

        return len(self.__messages_by_type[message_type])

    def empty(self) -> bool:
        """
        Return True if no messages are pending, and false otherwise.
        """

        return all((len(v) == 0 for v in self.__messages_by_type.values()))

    # This next function returns things of the type that was passed in as a
    # runtime argument, which we can explain to MyPy using a TypeVar and Type[]
    MessageType = TypeVar('MessageType')
    def for_each(self, message_type: Type[MessageType]) -> Iterator[MessageType]:
        """
        Loop over all messages currently pending of the given type. Each that
        is handled without raising an exception will be removed.

        Messages sent while this function is running will not be yielded by the
        current call.
        """

        # Grab the message buffer for this kind of message.
        message_list = self.__messages_by_type[message_type]
        # Make a new buffer. TODO: Will be hard to be thread-safe because other
        # threads could have a reference to the old buffer.
        self.__messages_by_type[message_type] = []

        try:
            while len(message_list) > 0:
                # We need to handle the case where a new message of this type comes
                # in while we're looping, from the handler. So we take each off the
                # list from the end while we handle it, and put it back if
                # something goes wrong.
                message = message_list.pop()
                handled = False
                try:
                    # Emit the message
                    assert isinstance(message, message_type), f"Unacceptable message type {type(message)} in list for type {message_type}"
                    yield message
                    # If we get here it was handled without error.
                    handled = True
                finally:
                    if not handled:
                        # An exception happened, and we're bailing out of the
                        # while loop. Make sure the message isn't dropped, in
                        # case someone wants to recover and handle it again
                        # later with another for_each call.
                        message_list.append(message)
        finally:
            # Dump anything remaining in our buffer back into the main buffer.
            self.__messages_by_type[message_type] += message_list



