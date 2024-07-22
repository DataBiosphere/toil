#!/usr/bin/env python
"""
cleanup_aws_resources.py

Convenience script to clean up extraneous test buckets, sbd domains, instance profiles, and/or roles
that testing may have left behind.

Manually requires the user to inspect and submit "yes" or "y" to delete buckets/domains.

Failing or canceled tests may miss clean up, and leftover test buckets and sdb domains can build up.
Run this script occasionally or when needed (there are limits to the number of buckets/domains we can have and we
can hit those limits).
"""
import argparse
import copy
import os
import re
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from src.toil.lib import aws
from src.toil.lib.aws import session
from src.toil.lib.aws.iam import delete_iam_instance_profile, delete_iam_role
from src.toil.lib.aws.utils import delete_s3_bucket, delete_sdb_domain
from src.toil.lib.generatedEC2Lists import regionDict

# put us-west-2 first as our default test region; that way anything with a universal region shows there
regions = ['us-west-2'] + [region for region in regionDict if region != 'us-west-2']

# never show these buckets; never offer to delete them; never forget
absolutely_do_not_delete_these_buckets = ['318423852362-cgcloud',  # not sure what this is
                                          'aws-config20201211232942693800000001',  # AWS logging; ask Erich?
                                          'cgl-pipeline',  # something important?
                                          'cgl-rnaseq-recompute-fixed-toil',  # the 20,000 toil-rna-seq recompute data
                                          'toil-cloudtrail-bucket',  # also AWS logging... ?; ask Erich
                                          'toil-cwl-infra-test-bucket-dont-delete',  # test infra; never delete
                                          'toil-datasets',  # test infra; never delete
                                          'toil-no-location-bucket-dont-delete',  # test infra; never delete
                                          'toil-preserve-file-permissions-tests']  # test infra; never delete


def contains_uuid(string):
    """
    Determines if a string contains a pattern like: '28064c76-a491-43e7-9b50-da424f920354',
    which toil uses in its test generated bucket names.
    """
    return bool(re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{8,12}').findall(string))


def contains_uuid_with_underscores(string):
    """
    Determines if a string contains a pattern like: '28064c76-a491-43e7-9b50-da424f920354',
    which toil uses in its test generated IAM role names.
    """
    return bool(re.compile('[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}').findall(string))


def contains_num_only_uuid(string):
    """
    Determines if a string contains a pattern like: '13614-31311-31347',
    which toil uses in its test generated sdb domain names.
    """
    return bool(re.compile('[0-9]{4,5}-[0-9]{4,5}-[0-9]{4,5}').findall(string))


def contains_toil_test_patterns(string):
    return contains_uuid(string) or contains_num_only_uuid(string) or contains_uuid_with_underscores(string)


def matches(resource_name):
    if (resource_name.endswith('--files') or resource_name.endswith('--jobs') or resource_name.endswith('_toil')
            or resource_name.endswith('--internal') or resource_name.startswith('toil-s3test-')):
        if contains_toil_test_patterns(resource_name):
            return resource_name

    if resource_name.startswith('import-export-test-'):
        return resource_name


def find_buckets_to_cleanup(include_all, match):
    buckets = dict()
    for region in regions:
        print(f'\n[{region}] Buckets:')
        try:
            s3_resource = session.resource('s3', region_name=region)
            buckets_in_region = find_buckets_in_region(s3_resource, include_all, match)
            new_buckets = [b for b in buckets_in_region if b not in buckets]
            print('    ' + '\n    '.join(new_buckets))
            for bucket in new_buckets:
                buckets[bucket] = region
        except Exception as e:
            # Occurs with botocore.exceptions.ClientError.
            if 'Your account is not signed up for the S3 service' in str(e):
                print('    Your account is not signed up for the S3 service in this region.')
            else:
                print(f'    An error occurred in this region: {e}')
    return buckets


def find_sdb_domains_to_cleanup(include_all, match):
    sdb_domains = dict()
    for region in regions:
        print(f'\n[{region}] SimpleDB Domains:')
        try:
            sdb_client = session.client('sdb', region_name=region)
            domains_in_region = find_sdb_domains_in_region(sdb_client, include_all, match)
            new_domains = [b for b in domains_in_region if b not in sdb_domains]
            print('    ' + '\n    '.join(new_domains))
            for sdb_domain in new_domains:
                sdb_domains[sdb_domain] = region
        except Exception as e:
            # Occurs with botocore.exceptions.SSLError in regions that don't support SDB.
            # Don't hard-code supported regions, just in case AWS changes these, in order to avoid blind spots.
            if 'SSL validation failed' in str(e):
                print('    SimpleDB is not offered in this region.')
            else:
                print(f'    An error occurred in this region: {e}')
    return sdb_domains


def find_iam_roles_to_cleanup(include_all, match):
    iam_roles = dict()
    for region in regions:
        print(f'\n[{region}] IAM Roles:')
        try:
            iam_client = session.client('iam', region_name=region)
            roles_in_region = find_iam_roles_in_region(iam_client, include_all, match)

            new_roles = [b for b in roles_in_region if b not in iam_roles]
            print('    ' + '\n    '.join(new_roles))
            for iam_role in new_roles:
                iam_roles[iam_role] = region
        except Exception as e:
            print(f'    An error occurred in this region: {e}')
    return iam_roles


def find_instance_profile_names_to_cleanup(include_all, match):
    instance_profiles = dict()
    for region in regions:
        print(f'\n[{region}] IAM Instance Profiles:')
        try:
            iam_resource = session.resource('iam', region_name=region)
            iam_client = session.client('iam')
            instance_profiles_in_region = find_instance_profile_names_in_region(iam_client, include_all, match)

            new_instance_profiles = [b for b in instance_profiles_in_region if b not in instance_profiles]
            for new_profile in new_instance_profiles:
                print(f'    {new_profile}')
                associated_roles = [r.name for r in iam_resource.InstanceProfile(new_profile).roles]
                print('     - Roles:')
                if associated_roles:
                    print('       - ' + f'\n       - '.join(associated_roles) + '\n')
                else:
                    print('       - No roles found to be associated.\n')
                instance_profiles[new_profile] = region
        except Exception as e:
            print(f'    An error occurred in this region: {e}')
    return instance_profiles


def find_buckets_in_region(s3_resource, include_all, match):
    buckets_to_cleanup = []
    for bucket in s3_resource.buckets.all():
        if bucket.name not in absolutely_do_not_delete_these_buckets:
            if match:
                for m in match:
                    if m in bucket.name:
                        buckets_to_cleanup.append(bucket.name)
            elif matches(bucket.name) or include_all:
                buckets_to_cleanup.append(bucket.name)
    return buckets_to_cleanup


def find_sdb_domains_in_region(sdb_client, include_all, match):
    sdb_domains_to_cleanup = []
    paginator = sdb_client.get_paginator('list_domains')
    for sdb_domains in paginator.paginate():
        for sdb_domain in sdb_domains.get('DomainNames', []):
            if match:
                for m in match:
                    if m in sdb_domain:
                        sdb_domains_to_cleanup.append(sdb_domain)
            elif matches(sdb_domain) or include_all:
                sdb_domains_to_cleanup.append(sdb_domain)
    return sdb_domains_to_cleanup


def find_iam_roles_in_region(iam_client, include_all, match):
    iam_roles_to_cleanup = []
    paginator = iam_client.get_paginator('list_roles')
    for iam_roles in paginator.paginate():
        for iam_role in iam_roles.get('Roles', []):
            iam_role = iam_role['RoleName']
            if match:
                for m in match:
                    if m in iam_role:
                        iam_roles_to_cleanup.append(iam_role)
            elif matches(iam_role) or include_all:
                iam_roles_to_cleanup.append(iam_role)
    return iam_roles_to_cleanup


def find_instance_profile_names_in_region(iam_client, include_all, match):
    instance_profiles_to_cleanup = []
    paginator = iam_client.get_paginator('list_instance_profiles')
    for instance_profiles in paginator.paginate():
        for instance_profile in instance_profiles.get('InstanceProfiles', []):
            instance_profile = instance_profile['InstanceProfileName']
            if match:
                for m in match:
                    if m in instance_profile:
                        instance_profiles_to_cleanup.append(instance_profile)
            elif matches(instance_profile) or include_all:
                instance_profiles_to_cleanup.append(instance_profile)
    return instance_profiles_to_cleanup


def main(argv):
    parser = argparse.ArgumentParser(
        description='View and/or clean up s3 buckets and/or sdb domains in an AWS Account.')

    parser.add_argument("--include-all", dest='include_all',
                        action='store_true', required=False,
                        help="Don't filter based on buckets/domains that look like test objects.  "
                             "List everything in the account.")
    parser.add_argument("--view-only", dest='view_only',
                        action='store_true', required=False,
                        help="Don't ask to delete.  Just view everything.")
    parser.add_argument("--skip-buckets", dest='skip_buckets',
                        action='store_true', required=False,
                        help="Skip doing anything with buckets.")
    parser.add_argument("--skip-sdb", dest='skip_sdb',
                        action='store_true', required=False,
                        help="Skip doing anything with SimpleDB domains.")
    parser.add_argument("--skip-iam-roles", dest='skip_iam_roles',
                        action='store_true', required=False,
                        help="Skip doing anything with IAM roles.")
    parser.add_argument("--skip-iam-instance-profiles", dest='skip_iam_instance_profiles',
                        action='store_true', required=False,
                        help="Skip doing anything with IAM roles.")
    parser.add_argument("--match", dest='match',
                        type=str, required=False, default='',
                        help="Only return resources containing the comma-delimited keywords.  "
                             "For example, adding --match='hello,goodbye' would return any "
                             "buckets or domains that include either 'hello' or 'goodbye'.")
    parser.add_argument("--regions", dest='regions',
                        type=str, required=False, default='all',
                        help="Only return resources in the regions (comma-delimited).  "
                             "For example, adding --regions='us-west-2,us-east-1' will "
                             "only act resources in those two regions.")
    # parser.set_defaults(view_only=False, include_all=False, skip_buckets=False, skip_sdb=False, match='')

    options = parser.parse_args(argv)

    account_aliases = session.client('iam').list_account_aliases()['AccountAliases']
    account_name = account_aliases[0] if account_aliases else "[no name]"
    print(f'\n\nNow running for AWS account: {account_name}.')

    match = [m.strip() for m in options.match.split(',') if m.strip()]

    if match and options.include_all:
        raise ValueError('Cannot filter on match patterns AND include everything.  Please specify either '
                         '"--view-only" or "--match", but not both.')

    if options.regions and options.regions != 'all':
        options.regions = [r.strip() for r in options.regions.split(',') if r.strip()]
        frozen_regions = copy.deepcopy(regions)
        for region in frozen_regions:
            if region not in options.regions and region in regions:
                regions.remove(region)

    if not options.skip_buckets:
        buckets = find_buckets_to_cleanup(options.include_all, match)
        if not options.view_only:
            if not buckets:
                print('Nothing to be done.')
            else:
                response = input(f'\nDo you wish to delete these buckets in account: {account_name}?  (Y)es (N)o: ')
                if response.lower() in ('y', 'yes'):
                    print('\nOkay, now deleting...')
                    for bucket, region in buckets.items():
                        s3_resource = session.resource('s3', region_name=region)
                        delete_s3_bucket(s3_resource, bucket)
                    print('S3 Bucket Deletions Successful.')

    if not options.skip_sdb:
        sdb_domains = find_sdb_domains_to_cleanup(options.include_all, match)
        if not options.view_only:
            if not sdb_domains:
                print('Nothing to be done.')
            else:
                response = input(f'\nDo you wish to delete these SDB domains in account: {account_name}?  (Y)es (N)o: ')
                if response.lower() in ('y', 'yes'):
                    print('\nOkay, now deleting...')
                    for sdb_domain, region in sdb_domains.items():
                        delete_sdb_domain(sdb_domain, region)
                    print('SimpleDB Domain Deletions Successful.')

    if not options.skip_iam_instance_profiles:
        instance_profile_names = find_instance_profile_names_to_cleanup(options.include_all, match)
        if not options.view_only:
            if not instance_profile_names:
                print('Nothing to be done.')
            else:
                response = input(f'\nDo you wish to delete these IAM instance profiles names in account: {account_name}?  (Y)es (N)o: ')
                if response.lower() in ('y', 'yes'):
                    print('\nOkay, now deleting...')
                    for instance_profile_name, region in instance_profile_names.items():
                        delete_iam_instance_profile(instance_profile_name, region)
                    print('Instance Profile Deletions Successful.')

    if not options.skip_iam_roles:
        iam_roles = find_iam_roles_to_cleanup(options.include_all, match)
        if not options.view_only:
            if not iam_roles:
                print('Nothing to be done.')
            else:
                response = input(f'\nDo you wish to delete these IAM roles in account: {account_name}?  (Y)es (N)o: ')
                if response.lower() in ('y', 'yes'):
                    print('\nOkay, now deleting...')
                    for iam_role, region in iam_roles.items():
                        delete_iam_role(iam_role, region)
                    print('Role Deletions Successful.')


if __name__ == '__main__':
    main(sys.argv[1:])
