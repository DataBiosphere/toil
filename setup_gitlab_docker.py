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
