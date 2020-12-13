import boto3

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

sdb_client = boto3.client('sdb')


def delete_s3_bucket(bucket):
    print('==============================================')
    print(f'Deleting s3 bucket and all contents: {bucket}')
    print('==============================================')

    paginator = s3_client.get_paginator('list_object_versions')
    for response in paginator.paginate(Bucket=bucket):
        versions = response.get('Versions', []) + response.get('DeleteMarkers', [])
        for version in versions:
            print(f"    Deleting {version['Key']} version {version['VersionId']}")
            s3_client.delete_object(Bucket=bucket, Key=version['Key'], VersionId=version['VersionId'])
    s3_resource.Bucket(bucket).delete()
    print(f'\n * Deleted s3 bucket successfully: {bucket}\n\n')


def matches(n):
    if n.endswith('--files') and n != 'jxu125-jobstore--files':
        return n
    for prefix in ('jobstore-test-',
                   'toil-test-',
                   'sort-test-',
                   'import-export-',
                   'domain-test-',
                   'cache-tests-'):
        if n.startswith(prefix):
            return n


def find_buckets_to_cleanup():
    buckets_to_cleanup = []
    for bucket in s3_resource.buckets.all():
        if matches(bucket.name):
            buckets_to_cleanup.append(bucket.name)
    return buckets_to_cleanup


def find_sdb_domains_to_cleanup():
    sdb_domains_to_cleanup = []
    for sdb_domain in sdb_client.list_domains()['DomainNames']:
        if matches(sdb_domain):
            sdb_domains_to_cleanup.append(sdb_domain)
    return sdb_domains_to_cleanup


def main():
    buckets = find_buckets_to_cleanup()
    print('\n'.join(buckets))
    response = input('Do you wish to delete these buckets?  (Y)es (N)o: ')
    if response.lower() in ('y', 'yes'):
        print('\nOkay, now deleting...')
        for bucket in buckets:
            delete_s3_bucket(bucket)

    sdb_domains = find_sdb_domains_to_cleanup()
    print('\n'.join(sdb_domains))
    response = input('Do you wish to delete these SDB domains?  (Y)es (N)o: ')
    if response.lower() in ('y', 'yes'):
        print('\nOkay, now deleting...')
        for sdb_domain in sdb_domains:
            sdb_client.delete_domain(DomainName=sdb_domain)


if __name__ == '__main__':
    main()
