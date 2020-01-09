import json
import os
import subprocess
import sys

stderr = 'Login was not attempted'

try:
    with open(os.environ['GITLAB_SECRET_FILE_QUAY_CREDENTIALS'], 'r') as cred_json_file:
        keys = json.loads(cred_json_file.read())
    
    process = subprocess.Popen('docker login quay.io -u "{user}" --password-stdin'.format(user=keys['user']),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                               shell=True)
    stdout, stderr = process.communicate(input=keys['password'])
    if 'Login Succeeded' in stdout:
        print('Login Succeeded')
    else:
        raise RuntimeError
except:
    print('Error while attempting to log into quay.io:\n' + str(stderr))
    sys.exit(1)
