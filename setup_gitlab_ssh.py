import subprocess
import json
import os

p = subprocess.Popen('aws secretsmanager --region us-west-2 get-secret-value --secret-id /toil/gitlab/ssh_key',
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
stdout, stderr = p.communicate()

with open(os.path.expanduser('~/.ssh/id_rsa.pub')) as f:
    f.write(json.loads(json.loads(stdout)['SecretString'])['id_rsa'])

if stderr:
    print('While attempting to set up the ssh key:\n' + str(stderr))
