import subprocess
import json

p = subprocess.Popen('aws secretsmanager --region us-west-2 get-secret-value --secret-id /toil/gitlab/quay',
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
stdout, stderr = p.communicate()

try:
    # TODO: Go back to using "--password-stdin" which has stopped working but used to work?  >.<
    keys = json.loads(json.loads(stdout)['SecretString'])
    process = subprocess.Popen(f'docker login quay.io -u "{keys["user"]}" -p {keys["password"]}',
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = process.communicate()
    if b'Login Succeeded' in stdout:
        print('Login Succeeded')
    else:
        raise RuntimeError
except:
    print('While attempting to log into quay.io:\n' + str(stderr))
