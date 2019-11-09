import subprocess
import json
import os


def format_private_key(private_key):
    private_key_header = '-----BEGIN RSA PRIVATE KEY-----'
    private_key_footer = '-----END RSA PRIVATE KEY-----'

    assert private_key.startswith(private_key_header)
    private_key_body = private_key[len(private_key_header):]
    assert private_key_body.endswith(private_key_footer)
    private_key_body = private_key_body[:-len(private_key_footer)]
    return private_key_header + private_key_body.replace(' ', '\n') + private_key_footer

p = subprocess.Popen('aws secretsmanager --region us-west-2 get-secret-value --secret-id /toil/gitlab/ssh_key',
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
stdout, stderr = p.communicate()

good_spot = os.path.expanduser('/home/quokka/git/py3-toil/toil/src/toil/test')
if not os.path.exists(good_spot):
    os.mkdir(good_spot)

try:
    keys = json.loads(json.loads(stdout)['SecretString'])
    with open(os.path.join(good_spot, 'id_rsa.pub'), 'w') as f:
        f.write(keys['public'])

    with open(os.path.join(good_spot, 'id_rsa'), 'w') as f:
        f.write(format_private_key(keys['private']))
except:
    print('While attempting to set up the ssh key:\n' + str(stderr))
