import json
import os
import subprocess
import sys

good_spot = os.path.expanduser('~/.ssh')
os.mkdir(good_spot)

try:
    with open(os.environ['GITLAB_SECRET_FILE_SSH_KEYS'], 'r') as keys_json_file:
        keys = json.loads(keys_json_file.read())
    with open(os.path.join(good_spot, 'id_rsa.pub'), 'w') as f:
        f.write(keys['public'])
    with open(os.path.join(good_spot, 'id_rsa'), 'w') as f:
        f.write(keys['private'])
except:
    print('While attempting to set up the ssh keys.')
    sys.exit(1)
