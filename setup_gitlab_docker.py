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
import subprocess
import sys

env_var = 'GITLAB_SECRET_FILE_QUAY_CREDENTIALS'

if env_var not in os.environ:
    print('Error: could not find environment variable ' + env_var)
    sys.exit(1)

print('Starting quay.io login process...')
with open(os.environ[env_var], 'r') as f:
    keys = json.load(f)
process = subprocess.Popen('docker login quay.io -u "{user}" --password-stdin'.format(user=keys['user']),
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                           shell=True)
print('Logging into quay.io...')
stdout, stderr = process.communicate(input=keys['password'].encode('utf-8'))

if 'Login Succeeded' in str(stdout):
    print('Login Succeeded.')
else:
    print('Error while attempting to log into quay.io:\n' + str(stderr))
    sys.exit(1)
