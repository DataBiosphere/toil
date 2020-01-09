import json
import os
import subprocess
import sys

stderr = 'Login was not attempted'

env_var = 'GITLAB_SECRET_FILE_QUAY_CREDENTIALS'

try:
    if env_var not in os.environ:
        print('Error: could not find environment variable ' + env_var)
        sys.exit(1)

    filename = os.environ[env_var]

    if not os.path.exists(filename):
        print('Error: could not find file referenced by ' + env_var)
        sys.exit(1)
        
    print('Opening key file...')
    with open(filename, 'r') as cred_json_file:
        print('Reading keys...')
        keys = json.loads(cred_json_file.read())
        print('Read and decoded keys')
    
    print('Starting login process...')
    process = subprocess.Popen('docker login quay.io -u "{user}" --password-stdin'.format(user=keys['user']),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                               shell=True)
    print('Logging in...')
    stdout, stderr = process.communicate(input=keys['password'])
    if 'Login Succeeded' in stdout:
        print('Login Succeeded')
    else:
        raise RuntimeError
except:
    print('Error while attempting to log into quay.io:\n' + str(stderr))
    sys.exit(1)
