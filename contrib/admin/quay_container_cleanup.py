#!/usr/bin/env python3
"""
Script to scan and delete unnecessary (non-Toil Release) quay.io containers/tags.

Long tag names in 'ucsc_cgl/toil' with the 'a1-' denoting a dev version will be printed as a list and
the user asked to confirm their deletion.

NOTE TO USER: Try not to delete images for currently running tests.  Check CI/CD first before running.
"""
import requests
import os


# more of a comment; this list really doesn't need to be updated since we filter by length.
whitelist = [
    '3.5.0',
    '3.5.0-py2.7',
    '3.5.1',
    '3.5.1-py2.7',
    '3.5.2',
    '3.5.2-py2.7',
    '3.5.3',
    '3.5.3-py2.7',
    '3.6.0',
    '3.6.0-py2.7',
    '3.7.0',
    '3.7.0-py2.7',
    '3.8.0',
    '3.8.0-py2.7',
    '3.9.0',
    '3.9.0-py2.7',
    '3.9.1',
    '3.9.1-py2.7',
    '3.10.0',
    '3.10.0-py2.7',
    '3.10.1',
    '3.10.1-py2.7',
    '3.11.0',
    '3.11.0-py2.7',
    '3.12.0',
    '3.12.0-py2.7',
    '3.13.0',
    '3.13.0-py2.7',
    '3.13.1',
    '3.13.1-py2.7',
    '3.14.0',
    '3.14.0-py2.7',
    '3.15.0',
    '3.15.0-py2.7',
    '3.16.0',
    '3.16.0-py2.7',
    '3.17.0',
    '3.17.0-py2.7',
    '3.18.0',
    '3.18.0-py2.7',
    '3.19.0',
    '3.19.0-py2.7',
    '3.20.0',
    '3.20.0-py2.7',
    '3.21.0',
    '3.21.0-py2.7',
    '3.22.0',
    '3.22.0-py2.7',
    '3.23.0',
    '3.23.0-py2.7',
    '3.23.0-py3.6',
    '3.23.1',
    '3.23.1-py2.7',
    '3.23.1-py3.6',
    '3.24.0',
    '3.24.0-py2.7',
    '3.24.0-py3.6',
    '4.0.0',
    '4.0.0-py3.6',
    '4.0.0-py3.7',
    '4.0.0-py3.8',
    '4.1.0',
    '4.1.0-py3.6',
    '4.1.0-py3.7',
    '4.1.0-py3.8',
    '4.2.0',
    '4.2.0-py3.6',
    '4.2.0-py3.7',
    '4.2.0-py3.8',
    '5.0.0',
    '5.0.0-py3.6',
    '5.0.0-py3.7',
    '5.0.0-py3.8',
    '5.1.0',
    '5.1.0-py3.6',
    '5.1.0-py3.7',
    '5.1.0-py3.8',
    '5.2.0-py3.6',
    '5.2.0-py3.7',
    '5.2.0-py3.8',
    '5.3.0',
    '5.3.0-py3.6',
    '5.3.0-py3.7',
    '5.3.0-py3.8',
    '5.4.0',
    '5.4.0-py3.6',
    '5.4.0-py3.7',
    '5.4.0-py3.8',
    'latest'
]

QUAY_AUTH_TOKEN = os.environ.get('QUAY_AUTH_TOKEN')
if not QUAY_AUTH_TOKEN:
    raise RuntimeError('Please set QUAY_AUTH_TOKEN to run this script.  '
                       'Mint the token here: https://quay.io/organization/ucsc_cgl?tab=applications')

headers = {'Authorization': f'Bearer {QUAY_AUTH_TOKEN}'}
repo = 'ucsc_cgl/toil'
total_tags_deleted = 0

response = requests.get(f'https://quay.io/api/v1/repository/{repo}', headers=headers)

tags = []
for tag in response.json()['tags']:
    tag = tag.strip()
    dev_version = tag.split('-')[0].endswith('a1')
    non_release_minimum_tag_length = len('5.5.0a1-dce7a69af77da45c4ab1939e304ec92865ad9c57-py3.7')
    if len(tag) >= non_release_minimum_tag_length and tag not in whitelist and dev_version:
        tags.append(tag)
        print(tag)

user_reply = input('\nDo you wish to delete these tags?\n\n')
print('\n')

if user_reply == 'y':
    for tag in tags:
        response = requests.delete(f'https://quay.io/api/v1/repository/{repo}/tag/{tag}', headers=headers)
        print(f'Deletion {response}: {tag}')
    print(f'\n{len(tags)} Tags Deleted.')
