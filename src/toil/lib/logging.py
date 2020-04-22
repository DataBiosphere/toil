# Copyright (C) 2020 Regents of the University of California
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

import sys

import logging

class StatusLineHandler(logging.Handler):
    """
    A logging handler that prints out log messages to the terminal, but keeps a
    persistent status line at the bottom of the screen.
    
    Modeled on the cool progress bars you get from the `apt` command:
    https://github.com/Debian/apt/blob/acf0da45c3ad0078573e393225e6eaae541fb18c/apt-pkg/install-progress.cc#L221
    
    Doesn't interfere with terminal scrollback.
    """
    
    # Define some magic escape code constants.
    # Unicode can represent ESC just fine
    ESCAPE = '\033'
    SAVE_CURSOR = ESCAPE + '7'
    SET_SCROLL_REGION_TO = ESCAPE + '[0;{}r' # Needs to be formatted with number
    RESTORE_CURSOR = ESCAPE + '8'
    MOVE_CURSOR_UP = ESCAPE + '[1A'
    CLEAR_BELOW_CURSOR = ESCAPE + '[J'
    SET_CURSOR_ROW_TO = ESCAPE + '[{};0f' # Needs to be formatted with destination row
    
    def __init__(self, level=logging.NOTSET):
        """
        Create a new StatusLineHandler talking to the terminal on stderr.
        """
        
        super(self, StatusLineHandler).__init__(level=level)
        
        # Control terminal via standard error.
        self.stream = sys.stderr
        
        
    def _configure(terminal_height):
        """
        Configure the terminal to use the top rows as the scroll area and the
        bottom row as the status line.
        
        Takes the total height of the terminal.
        """
        
        if total_height < 2:
            # We need at least one line of each.
            # Otherwise, don't show the status bar
            return
            
        # Apt has trouble when reducing height by one row unless it puts a
        # newline here.
        self.stream.write('\n')
        
        self.stream.write(SAVE_CURSOR)
        self.stream.write(SET_SCROLL_REGION_TO.format(terminal_height - 1))
        self.stream.write(RESTORE_CURSOR)
        self.stream.write(MOVE_CURSOR_UP)
        self.stream.flush()
            
        
        
    def emit(record):
        """
        Actually emit the given logging module record.
        """
        
        try:
            # Make it into a string
            formatted = self.format(record)
            
            
        except Exception:
            # Report our errors back to logging's code
            self.handleError(record)
