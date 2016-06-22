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


overhead = 0


# noinspection PyUnusedLocal
def encrypt(message, keyPath):
    _bail()


# noinspection PyUnusedLocal
def decrypt(ciphertext, keyPath):
    _bail()


def _bail():
    raise NotImplementedError("Encryption support is not installed. Consider re-installing toil "
                              "with the 'encryption' extra along with any other extras you might "
                              "want, e.g. 'pip install toil[encryption,...]'.")
