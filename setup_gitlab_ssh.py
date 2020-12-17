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
import json
import os

good_spot = os.path.expanduser('~/.ssh')
if not os.path.exists(good_spot):
    os.mkdir(good_spot)


def format_private_key(private_key):
    private_key_header = '-----BEGIN RSA PRIVATE KEY-----'
    private_key_footer = '-----END RSA PRIVATE KEY-----'

    assert private_key.startswith(private_key_header)
    private_key_body = private_key[len(private_key_header):]
    assert private_key_body.endswith(private_key_footer)
    private_key_body = private_key_body[:-len(private_key_footer)]
    return private_key_header + private_key_body.replace(' ', '\n') + private_key_footer


with open(os.environ['GITLAB_SECRET_FILE_SSH_KEYS'], 'r') as keys_json_file:
    keys = json.loads(keys_json_file.read())
with open(os.path.join(good_spot, 'id_rsa.pub'), 'w') as f:
    f.write(keys['public'])
with open(os.path.join(good_spot, 'id_rsa'), 'w') as f:
    f.write(format_private_key(keys['private']))
