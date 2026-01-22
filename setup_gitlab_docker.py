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
import time

ENV_VAR = 'GITLAB_SECRET_FILE_QUAY_CREDENTIALS'

if ENV_VAR not in os.environ:
    print('Error: could not find environment variable ' + ENV_VAR)
    sys.exit(1)

print('Starting quay.io login process...')
with open(os.environ[ENV_VAR], 'r') as f:
    keys = json.load(f)

MAX_RETRY_TIME = 600
MAX_DELAY = 60
INITIAL_DELAY = 1

delay = INITIAL_DELAY
start_time = time.time()
attempt = 1

while time.time() - start_time < MAX_RETRY_TIME:
    print(f'Logging into quay.io (attempt {attempt})...')
    process = subprocess.Popen(['docker', 'login', 'quay.io', '-u', keys['user'], '--password-stdin'],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    stdout, stderr = process.communicate(input=keys['password'].encode('utf-8'))

    if 'Login Succeeded' in stdout.decode('utf-8', errors='replace'):
        print(f'Login Succeeded on attempt {attempt}.')
        break

    print(f'Attempt {attempt} failed:')
    print(stderr.decode('utf-8', errors='replace'))
    print(f'Retrying in {delay} seconds...')
    time.sleep(delay)
    delay = min(delay * 2, MAX_DELAY)
    attempt += 1
else:
    print(f'Error: Failed to log in after {time.time() - start_time:.1f} seconds')
    sys.exit(1)
