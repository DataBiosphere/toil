# Copyright (C) 2015-2021 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import os
import re
from urllib.parse import unquote

from boto import iam, sns, sqs, vpc
from boto.exception import BotoServerError
from boto.s3.connection import S3Connection
from boto.utils import get_instance_metadata

from toil.lib.ec2 import UserError
from toil.lib.aws import zone_to_region
from toil.lib.memoize import memoize

logger = logging.getLogger(__name__)


class Boto2Context(object):
    """
    Encapsulates all Boto2 connections used by the AWSProvisioner.

    Also performs namespacing to keep clusters isolated.
    """
    name_prefix_re = re.compile(r'^(/([0-9a-zA-Z.-][_0-9a-zA-Z.-]*))*')
    name_re = re.compile(name_prefix_re.pattern + '/?$')
    namespace_re = re.compile(name_prefix_re.pattern + '/$')

    def __init__(self, availability_zone, namespace):
        """
        Create a Boto2Context object.

        :param availability_zone: The availability zone to place EC2 resources like volumes and
        instances into. The AWS region to operate in is implied by this parameter since the
        region is a prefix of the availability zone string

        :param namespace: The prefix for names of EC2 resources. The namespace is string starting
        in '/' followed by zero or more components, separated by '/'. Components are non-empty
        strings consisting only of alphanumeric characters, '.', '-' or '_' and that don't start
        with '_'. The namespace argument is restricted to ASCII and will be converted to a
        non-unicode string if available. Unicode strings that can't be encoded as ASCII will be
        rejected.

        A note about our namespaces vs IAM's resource paths. IAM paths don't provide namespace
        isolation. In other words, it is not possible to have two users of the same name in two
        different paths. The by itself name has to be unique. For that reason, IAM resource paths
        are pretty much useless.

        >>> ctx = Boto2Context( 'us-west-1b', None )
        Traceback (most recent call last):
        ....
        ValueError: Need namespace

        >>> Boto2Context('us-west-1b', namespace='/').namespace
        '/'

        >>> Boto2Context('us-west-1b', namespace='/foo/').namespace
        '/foo/'

        >>> Boto2Context('us-west-1b', namespace='/foo/bar/').namespace
        '/foo/bar/'

        >>> Boto2Context('us-west-1b', namespace='')
        Traceback (most recent call last):
        ....
        ValueError: Invalid namespace ''

        >>> Boto2Context('us-west-1b', namespace='foo')
        Traceback (most recent call last):
        ....
        ValueError: Invalid namespace 'foo'

        >>> Boto2Context('us-west-1b', namespace='/foo')
        Traceback (most recent call last):
        ....
        ValueError: Invalid namespace '/foo'

        >>> Boto2Context('us-west-1b', namespace='//foo/')
        Traceback (most recent call last):
        ....
        ValueError: Invalid namespace '//foo/'

        >>> Boto2Context('us-west-1b', namespace='/foo//')
        Traceback (most recent call last):
        ....
        ValueError: Invalid namespace '/foo//'

        >>> Boto2Context('us-west-1b', namespace='han//nes')
        Traceback (most recent call last):
        ....
        ValueError: Invalid namespace 'han//nes'

        >>> Boto2Context('us-west-1b', namespace='/_foo/')
        Traceback (most recent call last):
        ....
        ValueError: Invalid namespace '/_foo/'

        >>> Boto2Context('us-west-1b', namespace=u'/foo/').namespace # doctest: +ALLOW_UNICODE
        '/foo/'

        >>> Boto2Context('us-west-1b', namespace=u'/föo/').namespace # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ....
        ValueError: 'ascii' codec can't encode ...: ordinal not in range(128)

        >>> import string
        >>> component = string.ascii_letters + string.digits + '-_.'
        >>> namespace = '/' + component + '/'
        >>> Boto2Context('us-west-1b', namespace=namespace).namespace == namespace
        True
        """
        super().__init__()

        self.__iam = None
        self.__vpc = None
        self.__s3 = None
        self.__sns = None
        self.__sqs = None

        self.availability_zone = availability_zone
        self.region = zone_to_region(self.availability_zone)

        if namespace is None:
            raise ValueError('Need namespace')
        try:
            # Encode the namespace as ASCII, so we know it is representable in ASCII.
            # But keep using it as text.
            namespace.encode('ascii')
        except UnicodeEncodeError as e:
            raise ValueError(e)

        namespace = self.resolve_me(str(namespace))

        if not re.match(self.namespace_re, namespace):
            raise ValueError("Invalid namespace '%s'" % namespace)

        self.namespace = namespace

    @property
    def iam(self):
        """
        :rtype: IAMConnection
        """
        if self.__iam is None:
            self.__iam = self.__aws_connect(iam, 'universal')
        return self.__iam

    # VPCConnection extends EC2Connection so we can use one instance of the former for both

    @property
    def vpc(self):
        """
        :rtype: VPCConnection
        """
        if self.__vpc is None:
            self.__vpc = self.__aws_connect(vpc)
        return self.__vpc

    # ec2 = vpc works, too, but confuses the type hinter in PyCharm

    @property
    def ec2(self):
        """
        :rtype: VPCConnection
        """
        return self.vpc

    @property
    def s3(self):
        """
        :rtype: S3Connection
        """
        if self.__s3 is None:
            # We let S3 route buckets to regions for us. If we connected to a specific region,
            # bucket lookups (HEAD request against bucket URL) would fail with 301 status but
            # without a Location header.
            self.__s3 = S3Connection()
        return self.__s3

    @property
    def sns(self):
        """
        :rtype: SNSConnection
        """
        if self.__sns is None:
            self.__sns = self.__aws_connect(sns)
        return self.__sns

    @property
    def sqs(self):
        """
        :rtype: SQSConnection
        """
        if self.__sqs is None:
            self.__sqs = self.__aws_connect(sqs)
        return self.__sqs

    def __aws_connect(self, aws_module, region=None, **kwargs):
        if region is None:
            region = self.region
        conn = aws_module.connect_to_region(region, **kwargs)
        if conn is None:
            raise RuntimeError("%s couldn't connect to region %s" % (
                aws_module.__name__, region))
        return conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.__vpc is not None:
            self.__vpc.close()
        if self.__s3 is not None:
            self.__s3.close()
        if self.__iam is not None:
            self.__iam.close()
        if self.__sns is not None:
            self.__sns.close()
        if self.__sqs is not None:
            self.__sqs.close()

    @staticmethod
    def is_absolute_name(name):
        """
        Returns True if the given name starts with a namespace.
        """
        return name[0:1] == '/'

    class InvalidPathError(ValueError):
        def __init__(self, invalid_path):
            super(Context.InvalidPathError, self).__init__("Invalid path '%s'" % invalid_path)

    def absolute_name(self, name):
        """
        Returns the absolute form of the specified resource name. If the specified name is
        already absolute, that name will be returned unchanged, otherwise the given name will be
        prefixed with the namespace this object was configured with.

        Relative names starting with underscores are disallowed.

        >>> ctx = Boto2Context( 'us-west-1b', namespace='/' )
        >>> ctx.absolute_name('bar')
        '/bar'
        >>> ctx.absolute_name('/bar')
        '/bar'
        >>> ctx.absolute_name('')
        '/'
        >>> ctx.absolute_name('/')
        '/'
        >>> ctx.absolute_name('_bar') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidPathError: Invalid path '/_bar'
        >>> ctx.absolute_name('/_bar') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidPathError: Invalid path '/_bar'

        >>> ctx = Boto2Context( 'us-west-1b', namespace='/foo/' )
        >>> ctx.absolute_name('bar')
        '/foo/bar'
        >>> ctx.absolute_name('bar/')
        '/foo/bar/'
        >>> ctx.absolute_name('bar1/bar2')
        '/foo/bar1/bar2'
        >>> ctx.absolute_name('/bar')
        '/bar'
        >>> ctx.absolute_name('')
        '/foo/'
        >>> ctx.absolute_name('/')
        '/'
        >>> ctx.absolute_name('_bar') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidPathError: Invalid path '/foo/_bar'
        >>> ctx.absolute_name('/_bar') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidPathError: Invalid path '/_bar'
        """
        if self.is_absolute_name(name):
            result = name
        else:
            result = self.namespace + name
        if not self.name_re.match(result):
            raise self.InvalidPathError(result)
        return result

    def to_aws_name(self, name):
        """
        Returns a transliteration of the name that safe to use for resource names on AWS. If the
        given name is relative, it converted to its absolute form before the transliteration.

        The transliteration uses two consequitive '_' to encode a single '_' and a single '_' to
        separate the name components. AWS-safe names are by definition absolute such that the
        leading separator can be removed. This leads to fairly readable AWS-safe names,
        especially for names in the root namespace, where the transliteration is the identity
        function if the input does not contain any '_'.

        This scheme only works if name components don't start with '_'. Without that condition,
        '/_' would become '___' the inverse of which is '_/'.

        >>> ctx = Boto2Context( 'us-west-1b', namespace='/' )

        >>> ctx.to_aws_name( 'foo' )
        'foo'
        >>> ctx.from_aws_name( 'foo' )
        'foo'

        Illegal paths that would introduce ambiguity need to raise an exception
        >>> ctx.to_aws_name('/_') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidPathError: Invalid path '/_'
        >>> ctx.to_aws_name('/_/') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidPathError: Invalid path '/_/'
        >>> ctx.from_aws_name('___') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidPathError: Invalid path '/_/'

        >>> ctx.to_aws_name( 'foo_bar')
        'foo__bar'
        >>> ctx.from_aws_name( 'foo__bar')
        'foo_bar'

        >>> ctx.to_aws_name( '/sub_ns/foo_bar')
        'sub__ns_foo__bar'
        >>> ctx.to_aws_name( 'sub_ns/foo_bar')
        'sub__ns_foo__bar'
        >>> ctx.from_aws_name( 'sub__ns_foo__bar' )
        'sub_ns/foo_bar'

        >>> ctx.to_aws_name( 'g_/' )
        'g___'
        >>> ctx.from_aws_name( 'g___' )
        'g_/'

        >>> ctx = Boto2Context( 'us-west-1b', namespace='/this_ns/' )

        >>> ctx.to_aws_name( 'foo' )
        'this__ns_foo'
        >>> ctx.from_aws_name( 'this__ns_foo' )
        'foo'

        >>> ctx.to_aws_name( 'foo_bar')
        'this__ns_foo__bar'
        >>> ctx.from_aws_name( 'this__ns_foo__bar')
        'foo_bar'

        >>> ctx.to_aws_name( '/other_ns/foo_bar' )
        'other__ns_foo__bar'
        >>> ctx.from_aws_name( 'other__ns_foo__bar' )
        '/other_ns/foo_bar'

        >>> ctx.to_aws_name( 'other_ns/foo_bar' )
        'this__ns_other__ns_foo__bar'
        >>> ctx.from_aws_name( 'this__ns_other__ns_foo__bar' )
        'other_ns/foo_bar'

        >>> ctx.to_aws_name( '/this_ns/foo_bar' )
        'this__ns_foo__bar'
        >>> ctx.from_aws_name( 'this__ns_foo__bar' )
        'foo_bar'
        """
        name = self.absolute_name(name)
        assert name.startswith('/')
        return name[1:].replace('_', '__').replace('/', '_')

    def from_aws_name(self, name):
        """
        The inverse of to_aws_name(), except that the namespace is stripped from the input if it
        is relative to this context's name space.

        >>> zone = 'us-west-1b'
        >>> Boto2Context( zone, namespace='/foo/' ).from_aws_name('bar__x')
        '/bar_x'
        >>> Boto2Context( zone, namespace='/foo_x/' ).from_aws_name('foo__x_bar')
        'bar'
        >>> Boto2Context( zone, namespace='/' ).from_aws_name('foo__x_bar__x')
        'foo_x/bar_x'
        >>> Boto2Context( zone, namespace='/bla/' ).from_aws_name('foo__x_bar__x')
        '/foo_x/bar_x'
        """
        name = '_'.join(s.replace('_', '/') for s in name.split('__'))
        name = '/' + name
        if not self.name_re.match(name):
            raise self.InvalidPathError(name)
        if name.startswith(self.namespace):
            name = name[len(self.namespace):]
        return name

    def base_name(self, name):
        """
        Return the last component of a name, absolute or relative.

        >>> ctx = Boto2Context( 'us-west-1b', namespace='/foo/bar/')
        >>> ctx.base_name('')
        ''
        >>> ctx.base_name('/')
        ''
        >>> ctx.base_name('/a')
        'a'
        >>> ctx.base_name('/a/')
        ''
        >>> ctx.base_name('/a/b')
        'b'
        >>> ctx.base_name('/a/b/')
        ''
        """
        return name.split('/')[-1]

    def contains_name(self, name):
        return not self.is_absolute_name(name) or name.startswith(self.namespace)

    def contains_aws_name(self, aws_name):
        """
        >>> def c(n): return Boto2Context( 'us-west-1b', namespace=n)
        >>> c('/foo/' ).contains_aws_name('bar_x')
        False
        >>> c('/foo/' ).contains_aws_name('foo_x')
        True
        >>> c('/foo/' ).contains_aws_name('foo_bar_x')
        True
        >>> c('/foo/' ).contains_aws_name('bar_foo_x')
        False
        >>> c('/' ).contains_aws_name('bar_x')
        True
        >>> c('/' ).contains_aws_name('foo_x')
        True
        >>> c('/' ).contains_aws_name('foo_bar_x')
        True
        >>> c('/' ).contains_aws_name('bar_foo_x')
        True
        """
        return self.contains_name(self.from_aws_name(aws_name))

    def try_contains_aws_name(self, aws_name):
        try:
            return self.contains_aws_name(aws_name)
        except self.InvalidPathError:
            return False

    @property
    @memoize
    def account(self):
        try:
            arn = self.iam.get_user().arn
        except BaseException:
            # Agent boxes run with IAM role credentials instead of user credentials.
            arn = get_instance_metadata()['iam']['info']['InstanceProfileArn']
        _, partition, service, region, account, resource = arn.split(':', 6)
        return account

    ssh_pubkey_s3_key_prefix = 'ssh_pubkey:'

    @property
    @memoize
    def iam_user_name(self):
        try:
            return self.iam.get_user().user_name
        except BaseException:
            logger.warning("IAMConnection.get_user() failed.", exc_info=True)
            return None

    current_user_placeholder = '__me__'

    @staticmethod
    def drop_hostname(email):
        """
        >>> Boto2Context.drop_hostname("foo")
        'foo'
        >>> Boto2Context.drop_hostname("foo@bar.com")
        'foo'
        >>> Boto2Context.drop_hostname("")
        ''
        >>> Boto2Context.drop_hostname("@")
        ''
        """
        try:
            n = email.index("@")
        except ValueError:
            return email
        else:
            return email[0:n]

    def resolve_me(self, s, drop_hostname=True):
        placeholder = self.current_user_placeholder
        if placeholder in s:
            try:
                me = os.environ['CGCLOUD_ME']
            except KeyError:
                me = self.iam_user_name
            if not me:
                raise UserError(
                    "Can't determine current IAM user name. Be sure to put valid AWS credentials "
                    "in ~/.boto or ~/.aws/credentials. For details, refer to %s. On an EC2 "
                    "instance that is authorized via IAM roles, you can set the CGCLOUD_ME "
                    "environment variable (uncommon)." %
                    'http://boto.readthedocs.org/en/latest/boto_config_tut.html')
            if drop_hostname:
                me = self.drop_hostname(me)
            return s.replace(placeholder, me)
        else:
            return s

    def setup_iam_ec2_role(self, role_name, policies):
        aws_role_name = self.to_aws_name(role_name)
        try:
            logger.debug('Creating IAM role...')
            self.iam.create_role(aws_role_name, assume_role_policy_document=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": ["ec2.amazonaws.com"]},
                    "Action": ["sts:AssumeRole"]}
                ]}))
            logger.debug('Created new IAM role')
        except BotoServerError as e:
            if e.status == 409 and e.error_code == 'EntityAlreadyExists':
                logger.debug('IAM role already exists. Reusing.')
                pass
            else:
                raise

        self.__setup_entity_policies(aws_role_name, policies,
                                     list_policies=self.iam.list_role_policies,
                                     delete_policy=self.iam.delete_role_policy,
                                     get_policy=self.iam.get_role_policy,
                                     put_policy=self.iam.put_role_policy)

        return aws_role_name

    def __setup_entity_policies(self, entity_name, policies,
                                list_policies, delete_policy, get_policy, put_policy):
        # Delete superfluous policies
        policy_names = set(list_policies(entity_name).policy_names)
        for policy_name in policy_names.difference(set(list(policies.keys()))):
            delete_policy(entity_name, policy_name)

        # Create expected policies
        for policy_name, policy in policies.items():
            current_policy = None
            try:
                current_policy = json.loads(unquote(
                    get_policy(entity_name, policy_name).policy_document))
            except BotoServerError as e:
                if e.status == 404 and e.error_code == 'NoSuchEntity':
                    pass
                else:
                    raise
            if current_policy != policy:
                put_policy(entity_name, policy_name, json.dumps(policy))

    _agent_topic_name = "cgcloud-agent-notifications"

    def _pager(self, requestor_callable, result_attribute_name):
        marker = None
        while True:
            result = requestor_callable(marker=marker)
            for p in getattr(result, result_attribute_name):
                yield p
            if result.is_truncated == 'true':
                marker = result.marker
            else:
                break

    def local_roles(self):
        return [r for r in self._get_all_roles() if self.try_contains_aws_name(r.role_name)]

    def _get_all_roles(self):
        return self._pager(self.iam.list_roles, 'roles')

    def local_instance_profiles(self):
        return [p for p in self._get_all_instance_profiles() if self.try_contains_aws_name(p.instance_profile_name)]

    def _get_all_instance_profiles(self):
        return self._pager(self.iam.list_instance_profiles, 'instance_profiles')
