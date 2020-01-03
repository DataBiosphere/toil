import subprocess
import json

p = subprocess.Popen('aws secretsmanager --region us-west-2 get-secret-value --secret-id /toil/gitlab/quay',
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
stdout, stderr = p.communicate()

try:
    keys = json.loads(json.loads(stdout)['SecretString'])
    process = subprocess.Popen('docker login quay.io -u "{user}" --password-stdin'.format(user=keys['user']),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                               shell=True)
    stdout, stderr = process.communicate(input=keys['password'])
    if 'Login Succeeded' in stdout:
        print('Login Succeeded')
    else:
        raise RuntimeError
except:
    print('While attempting to log into quay.io:\n' + str(stderr))
