import errno
import hashlib
import logging
import time
from collections import Iterator
from operator import attrgetter

from bd2k.util.exceptions import panic
from bd2k.util.retry import retry
from boto.ec2.ec2object import TaggedEC2Object
from boto.ec2.instance import Instance
from boto.ec2.spotinstancerequest import SpotInstanceRequest
from boto.exception import EC2ResponseError
from toil.lib.misc import partition_seq
from Crypto.PublicKey import RSA

a_short_time = 5

a_long_time = 60 * 60

log = logging.getLogger(__name__)


class UserError(RuntimeError):
    def __init__(self, message=None, cause=None):
        if (message is None) == (cause is None):
            raise RuntimeError("Must pass either message or cause.")
        super(
            UserError, self).__init__(
            message if cause is None else cause.message)


def not_found(e):
    return e.error_code.endswith('.NotFound')


def retry_ec2(
        retry_after=a_short_time,
        retry_for=10 *
        a_short_time,
        retry_while=not_found):
    t = retry_after
    return retry(
        delays=(
            t,
            t,
            t * 2,
            t * 4),
        timeout=retry_for,
        predicate=retry_while)


class EC2VolumeHelper(object):
    """
    A helper for creating, looking up and attaching an EBS volume in EC2
    """

    def __init__(
            self,
            ec2,
            name,
            size,
            availability_zone,
            volume_type="standard"):
        """
        :param ec2: the Boto EC2 connection object
        :type ec2: boto.ec2.connection.EC2Connection
        """
        super(EC2VolumeHelper, self).__init__()
        self.availability_zone = availability_zone
        self.ec2 = ec2
        self.name = name
        self.volume_type = volume_type
        volume = self.__lookup()
        if volume is None:
            log.info("Creating volume %s, ...", self.name)
            volume = self.ec2.create_volume(
                size, availability_zone, volume_type=self.volume_type)
            self.__wait_transition(volume, {'creating'}, 'available')
            volume.add_tag('Name', self.name)
            log.info('... created %s.', volume.id)
            volume = self.__lookup()
        self.volume = volume

    def attach(self, instance_id, device):
        if self.volume.attach_data.instance_id == instance_id:
            log.info("Volume '%s' already attached to instance '%s'." %
                     (self.volume.id, instance_id))
        else:
            self.__assert_attachable()
            self.ec2.attach_volume(volume_id=self.volume.id,
                                   instance_id=instance_id,
                                   device=device)
            self.__wait_transition(self.volume, {'available'}, 'in-use')
            if self.volume.attach_data.instance_id != instance_id:
                raise UserError("Volume %s is not attached to this instance.")

    def __lookup(self):
        """
        Ensure that an EBS volume of the given name is available in the current availability zone.
        If the EBS volume exists but has been placed into a different zone, or if it is not
        available, an exception will be thrown.

        :rtype: boto.ec2.volume.Volume
        """
        volumes = self.ec2.get_all_volumes(filters={'tag:Name': self.name})
        if len(volumes) < 1:
            return None
        if len(volumes) > 1:
            raise UserError("More than one EBS volume named %s" % self.name)
        return volumes[0]

    @staticmethod
    def __wait_transition(volume, from_states, to_state):
        wait_transition(volume, from_states, to_state, attrgetter('status'))

    def __assert_attachable(self):
        if self.volume.status != 'available':
            raise UserError("EBS volume %s is not available." % self.name)
        expected_zone = self.availability_zone
        if self.volume.zone != expected_zone:
            raise UserError(
                "Availability zone of EBS volume %s is %s but should be %s." %
                (self.name, self.volume.zone, expected_zone))


class UnexpectedResourceState(Exception):
    def __init__(self, resource, to_state, state):
        super(UnexpectedResourceState, self).__init__(
            "Expected state of %s to be '%s' but got '%s'" %
            (resource, to_state, state))


def wait_transition(resource, from_states, to_state,
                    state_getter=attrgetter('state')):
    """
    Wait until the specified EC2 resource (instance, image, volume, ...) transitions from any
    of the given 'from' states to the specified 'to' state. If the instance is found in a state
    other that the to state or any of the from states, an exception will be thrown.

    :param resource: the resource to monitor
    :param from_states:
        a set of states that the resource is expected to be in before the  transition occurs
    :param to_state: the state of the resource when this method returns
    """
    state = state_getter(resource)
    while state in from_states:
        time.sleep(a_short_time)
        for attempt in retry_ec2():
            with attempt:
                resource.update(validate=True)
        state = state_getter(resource)
    if state != to_state:
        raise UnexpectedResourceState(resource, to_state, state)


def running_on_ec2():
    try:
        with open('/sys/hypervisor/uuid') as f:
            return f.read(3) == 'ec2'
    except IOError as e:
        if e.errno == errno.ENOENT:
            return False
        else:
            raise


from collections import namedtuple

InstanceType = namedtuple('InstanceType', [
    'name',  # the API name of the instance type
    'cores',  # the number of cores
    'ecu',  # the computational power of the core times the number of cores
    'memory',  # RAM in GB
    'virtualization_types',  # the supported virtualization types, in order of preference
    'disks',  # the number of ephemeral (aka 'instance store') volumes
    'disk_type',  # the type of ephemeral volume
    'disk_capacity',  # the capacity of each ephemeral volume in GB
    'spot_availability'  # can this instance type be used on the spot market?
])

hvm = 'hvm'  # hardware virtualization
pv = 'paravirtual'  # para-virtualization
ssd = 'SSD'  # solid-state disk
hdd = 'HDD'  # spinning disk
variable_ecu = -1  # variable ecu

_ec2_instance_types = [
    # current generation instance types
    InstanceType('t2.nano', 1, variable_ecu, 0.5, [hvm], 0, None, 0, False),
    InstanceType('t2.micro', 1, variable_ecu, 1, [hvm], 0, None, 0, False),
    InstanceType('t2.small', 1, variable_ecu, 2, [hvm], 0, None, 0, False),
    InstanceType('t2.medium', 2, variable_ecu, 4, [hvm], 0, None, 0, False),
    InstanceType('t2.large', 2, variable_ecu, 8, [hvm], 0, None, 0, False),
    InstanceType('t2.xlarge', 4, variable_ecu, 16, [hvm], 0, None, 0, False),
    InstanceType('t2.2xlarge', 8, variable_ecu, 32, [hvm], 0, None, 0, False),

    InstanceType('m3.medium', 1, 3, 3.75, [hvm, pv], 1, ssd, 4, True),
    InstanceType('m3.large', 2, 6.5, 7.5, [hvm, pv], 1, ssd, 32, True),
    InstanceType('m3.xlarge', 4, 13, 15, [hvm, pv], 2, ssd, 40, True),
    InstanceType('m3.2xlarge', 8, 26, 30, [hvm, pv], 2, ssd, 80, True),

    InstanceType('m4.large', 2, 6.5, 8, [hvm], 0, None, 0, True),
    InstanceType('m4.xlarge', 4, 13, 16, [hvm], 0, None, 0, True),
    InstanceType('m4.2xlarge', 8, 26, 32, [hvm], 0, None, 0, True),
    InstanceType('m4.4xlarge', 16, 53.5, 64, [hvm], 0, None, 0, True),
    InstanceType('m4.10xlarge', 40, 124.5, 160, [hvm], 0, None, 0, True),
    InstanceType('m4.16xlarge', 64, 188, 256, [hvm], 0, None, 0, True),

    InstanceType('c4.large', 2, 8, 3.75, [hvm], 0, None, 0, True),
    InstanceType('c4.xlarge', 4, 16, 7.5, [hvm], 0, None, 0, True),
    InstanceType('c4.2xlarge', 8, 31, 15, [hvm], 0, None, 0, True),
    InstanceType('c4.4xlarge', 16, 62, 30, [hvm], 0, None, 0, True),
    InstanceType('c4.8xlarge', 36, 132, 60, [hvm], 0, None, 0, True),

    InstanceType('c3.large', 2, 7, 3.75, [hvm, pv], 2, ssd, 16, True),
    InstanceType('c3.xlarge', 4, 14, 7.5, [hvm, pv], 2, ssd, 40, True),
    InstanceType('c3.2xlarge', 8, 28, 15, [hvm, pv], 2, ssd, 80, True),
    InstanceType('c3.4xlarge', 16, 55, 30, [hvm, pv], 2, ssd, 160, True),
    InstanceType('c3.8xlarge', 32, 108, 60, [hvm, pv], 2, ssd, 320, True),

    InstanceType('p2.xlarge', 4, 12, 61, [hvm], 0, None, 0, True),
    InstanceType('p2.8xlarge', 32, 94, 488, [hvm], 0, None, 0, True),
    InstanceType('p2.16xlarge', 64, 188, 732, [hvm], 0, None, 0, True),
    InstanceType('g3.4xlarge', 16, 47, 122, [hvm], 0, None, 0, True),
    InstanceType('g3.8xlarge', 32, 94, 244, [hvm], 0, None, 0, True),
    InstanceType('g3.16xlarge', 64, 188, 488, [hvm], 0, None, 0, True),

    InstanceType('g2.2xlarge', 8, 26, 15, [hvm], 1, ssd, 60, True),
    InstanceType('g2.8xlarge', 32, 104, 60, [hvm], 2, ssd, 120, True),

    InstanceType('x1.16large', 64, 174.5, 976, [hvm], 1, ssd, 1920, True),
    InstanceType('x1.32large', 128, 349, 1952, [hvm], 2, ssd, 1920, True),
    InstanceType('x1e.32large', 128, 349, 3904, [hvm], 2, ssd, 1920, True),

    InstanceType('r3.large', 2, 6.5, 15, [hvm], 1, ssd, 32, True),
    InstanceType('r3.xlarge', 4, 13, 30.5, [hvm], 1, ssd, 80, True),
    InstanceType('r3.2xlarge', 8, 26, 61, [hvm], 1, ssd, 160, True),
    InstanceType('r3.4xlarge', 16, 52, 122, [hvm], 1, ssd, 320, True),
    InstanceType('r3.8xlarge', 32, 104, 244, [hvm], 2, ssd, 320, True),

    InstanceType('r4.large', 2, 6.5, 15, [hvm], 0, None, 0, True),
    InstanceType('r4.xlarge', 4, 13, 30.5, [hvm], 0, None, 0, True),
    InstanceType('r4.2xlarge', 8, 26, 61, [hvm], 0, None, 0, True),
    InstanceType('r4.4xlarge', 16, 52, 122, [hvm], 0, None, 0, True),
    InstanceType('r4.8xlarge', 32, 104, 244, [hvm], 0, None, 0, True),
    InstanceType('r4.16xlarge', 64, 195, 488, [hvm], 0, None, 0, True),

    InstanceType('i2.xlarge', 4, 14, 30.5, [hvm], 1, ssd, 800, False),
    InstanceType('i2.2xlarge', 8, 27, 61, [hvm], 2, ssd, 800, False),
    InstanceType('i2.4xlarge', 16, 53, 122, [hvm], 4, ssd, 800, False),
    InstanceType('i2.8xlarge', 32, 104, 244, [hvm], 8, ssd, 800, False),

    InstanceType('i3.large', 2, 7, 15.25, [hvm], 1, ssd, 475, True),
    InstanceType('i3.xlarge', 4, 13, 30.5, [hvm], 1, ssd, 950, True),
    InstanceType('i3.2xlarge', 8, 27, 61, [hvm], 1, ssd, 1900, True),
    InstanceType('i3.4xlarge', 16, 53, 122, [hvm], 2, ssd, 1900, True),
    InstanceType('i3.8xlarge', 32, 104, 244, [hvm], 4, ssd, 1900, True),
    InstanceType('i3.16xlarge', 64, 200, 488, [hvm], 8, ssd, 1900, True),

    InstanceType('d2.xlarge', 4, 14, 30.5, [hvm], 3, hdd, 2000, True),
    InstanceType('d2.2xlarge', 8, 28, 61, [hvm], 6, hdd, 2000, True),
    InstanceType('d2.4xlarge', 16, 56, 122, [hvm], 12, hdd, 2000, True),
    InstanceType('d2.8xlarge', 36, 116, 244, [hvm], 24, hdd, 2000, True),

    # previous generation instance types
    InstanceType('m1.small', 1, 1, 1.7, [pv], 1, hdd, 160, True),
    InstanceType('m1.medium', 1, 2, 3.75, [pv], 1, hdd, 410, True),
    InstanceType('m1.large', 2, 4, 7.5, [pv], 2, hdd, 420, True),
    InstanceType('m1.xlarge', 4, 8, 15, [pv], 4, hdd, 420, True),

    InstanceType('c1.medium', 2, 5, 1.7, [pv], 1, hdd, 350, True),
    InstanceType('c1.xlarge', 8, 20, 7, [pv], 4, hdd, 420, True),

    InstanceType('cc2.8xlarge', 32, 88, 60.5, [hvm], 4, hdd, 840, True),

    InstanceType('m2.xlarge', 2, 6.5, 17.1, [pv], 1, hdd, 420, True),
    InstanceType('m2.2xlarge', 4, 13, 34.2, [pv], 1, hdd, 850, True),
    InstanceType('m2.4xlarge', 8, 26, 68.4, [pv], 2, hdd, 840, True),

    InstanceType('cr1.8xlarge', 32, 88, 244, [hvm], 2, ssd, 120, True),

    InstanceType('hi1.4xlarge', 16, 35, 60.5, [hvm, pv], 2, ssd, 1024, True),
    InstanceType('hs1.8xlarge', 16, 35, 117, [hvm, pv], 24, hdd, 2048, False),

    InstanceType('t1.micro', 1, variable_ecu, 0.615, [pv], 0, None, 0, True)]

ec2_instance_types = dict((_.name, _) for _ in _ec2_instance_types)


def wait_instances_running(ec2, instances):
    """
    Wait until no instance in the given iterable is 'pending'. Yield every instance that
    entered the running state as soon as it does.

    :param boto.ec2.connection.EC2Connection ec2: the EC2 connection to use for making requests
    :param Iterator[Instance] instances: the instances to wait on
    :rtype: Iterator[Instance]
    """
    running_ids = set()
    other_ids = set()
    while True:
        pending_ids = set()
        for i in instances:
            if i.state == 'pending':
                pending_ids.add(i.id)
            elif i.state == 'running':
                assert i.id not in running_ids
                running_ids.add(i.id)
                yield i
            else:
                assert i.id not in other_ids
                other_ids.add(i.id)
                yield i
        log.info('%i instance(s) pending, %i running, %i other.',
                 *map(len, (pending_ids, running_ids, other_ids)))
        if not pending_ids:
            break
        seconds = max(a_short_time, min(len(pending_ids), 10 * a_short_time))
        log.info('Sleeping for %is', seconds)
        time.sleep(seconds)
        for attempt in retry_ec2():
            with attempt:
                instances = ec2.get_only_instances(list(pending_ids))


def wait_spot_requests_active(ec2, requests, timeout=None, tentative=False):
    """
    Wait until no spot request in the given iterator is in the 'open' state or, optionally,
    a timeout occurs. Yield spot requests as soon as they leave the 'open' state.

    :param Iterator[SpotInstanceRequest] requests:

    :param float timeout: Maximum time in seconds to spend waiting or None to wait forever. If a
    timeout occurs, the remaining open requests will be cancelled.

    :param bool tentative: if True, give up on a spot request at the earliest indication of it
    not being fulfilled immediately

    :rtype: Iterator[list[SpotInstanceRequest]]
    """

    if timeout is not None:
        timeout = time.time() + timeout
    active_ids = set()
    other_ids = set()
    open_ids = None

    def cancel():
        log.warn('Cancelling remaining %i spot requests.', len(open_ids))
        ec2.cancel_spot_instance_requests(list(open_ids))

    def spot_request_not_found(e):
        error_code = 'InvalidSpotInstanceRequestID.NotFound'
        return isinstance(e, EC2ResponseError) and e.error_code == error_code

    try:
        while True:
            open_ids, eval_ids, fulfill_ids = set(), set(), set()
            batch = []
            for r in requests:
                if r.state == 'open':
                    open_ids.add(r.id)
                    if r.status.code == 'pending-evaluation':
                        eval_ids.add(r.id)
                    elif r.status.code == 'pending-fulfillment':
                        fulfill_ids.add(r.id)
                    else:
                        log.info(
                            'Request %s entered status %s indicating that it will not be '
                            'fulfilled anytime soon.', r.id, r.status.code)
                elif r.state == 'active':
                    assert r.id not in active_ids
                    active_ids.add(r.id)
                    batch.append(r)
                else:
                    assert r.id not in other_ids
                    other_ids.add(r.id)
                    batch.append(r)
            if batch:
                yield batch
            log.info('%i spot requests(s) are open (%i of which are pending evaluation and %i '
                     'are pending fulfillment), %i are active and %i are in another state.',
                     *map(len, (open_ids, eval_ids, fulfill_ids, active_ids, other_ids)))
            if not open_ids or tentative and not eval_ids and not fulfill_ids:
                break
            sleep_time = 2 * a_short_time
            if timeout is not None and time.time() + sleep_time >= timeout:
                log.warn('Timed out waiting for spot requests.')
                break
            log.info('Sleeping for %is', sleep_time)
            time.sleep(sleep_time)
            for attempt in retry_ec2(retry_while=spot_request_not_found):
                with attempt:
                    requests = ec2.get_all_spot_instance_requests(
                        list(open_ids))
    except BaseException:
        if open_ids:
            with panic(log):
                cancel()
        raise
    else:
        if open_ids:
            cancel()


def create_spot_instances(
        ec2,
        price,
        image_id,
        spec,
        num_instances=1,
        timeout=None,
        tentative=False,
        tags=None):
    """
    :rtype: Iterator[list[Instance]]
    """
    def spotRequestNotFound(e):
        return e.error_code == "InvalidSpotInstanceRequestID.NotFound"

    for attempt in retry_ec2(retry_for=a_long_time,
                             retry_while=inconsistencies_detected):
        with attempt:
            requests = ec2.request_spot_instances(
                price, image_id, count=num_instances, **spec)

    if tags is not None:
        for requestID in (request.id for request in requests):
            for attempt in retry_ec2(retry_while=spotRequestNotFound):
                with attempt:
                    ec2.create_tags([requestID], tags)

    num_active, num_other = 0, 0
    # noinspection PyUnboundLocalVariable,PyTypeChecker
    # request_spot_instances's type annotation is wrong
    for batch in wait_spot_requests_active(ec2,
                                           requests,
                                           timeout=timeout,
                                           tentative=tentative):
        instance_ids = []
        for request in batch:
            if request.state == 'active':
                instance_ids.append(request.instance_id)
                num_active += 1
            else:
                log.info(
                    'Request %s in unexpected state %s.',
                    request.id,
                    request.state)
                num_other += 1
        if instance_ids:
            # This next line is the reason we batch. It's so we can get multiple instances in
            # a single request.
            yield ec2.get_only_instances(instance_ids)
    if not num_active:
        message = 'None of the spot requests entered the active state'
        if tentative:
            log.warn(message + '.')
        else:
            raise RuntimeError(message)
    if num_other:
        log.warn('%i request(s) entered a state other than active.', num_other)


def inconsistencies_detected(e):
    if e.code == 'InvalidGroup.NotFound':
        return True
    m = e.error_message.lower()
    return 'invalid iam instance profile' in m or 'no associated iam roles' in m


def create_ondemand_instances(ec2, image_id, spec, num_instances=1):
    """
    Requests the RunInstances EC2 API call but accounts for the race between recently created
    instance profiles, IAM roles and an instance creation that refers to them.

    :rtype: list[Instance]
    """
    instance_type = spec['instance_type']
    log.info('Creating %s instance(s) ... ', instance_type)
    for attempt in retry_ec2(retry_for=a_long_time,
                             retry_while=inconsistencies_detected):
        with attempt:
            return ec2.run_instances(image_id,
                                     min_count=num_instances,
                                     max_count=num_instances,
                                     **spec).instances


def tag_object_persistently(tagged_ec2_object, tags_dict):
    """
    Object tagging occasionally fails with "NotFound" types of errors so we need to
    retry a few times. Sigh ...

    :type tagged_ec2_object: TaggedEC2Object
    """
    for attempt in retry_ec2():
        with attempt:
            tagged_ec2_object.add_tags(tags_dict)


def ec2_keypair_fingerprint(ssh_key, reject_private_keys=False):
    """
    Computes the fingerprint of a public or private OpenSSH key in the way Amazon does it for
    keypairs resulting from either importing a SSH public key or generating a new keypair.

    :param ssh_key: a RSA public key in OpenSSH format, or an RSA private key in PEM format

    :return: The fingerprint of the key, in pairs of two hex digits with a colon between
    pairs.

    >>> ssh_pubkey = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCvdDMvcwC1/5ByUhO1wh1sG6ficwgGHRab/p'\\
    ... 'm6LN60rgxv+u2eJRao2esGB9Oyt863+HnjKj/NBdaiHTHcAHNq/TapbvEjgHaKgrVdfeMdQbJhWjJ97rql9Yn8k'\\
    ... 'TNsXOeSyTW7rIKE0zeQkrwhsztmATumbQmJUMR7uuI31BxhQUfD/CoGZQrxFalWLDZcrcYY13ynplaNA/Hd/vP6'\\
    ... 'qWO5WC0dTvzROEp7VwzJ7qeN2kP1JTh+kgVRoYd9mSm6x9UVjY6jQtZHa01Eg05sFraWgvNAvKhk9LS9Kiwhq8D'\\
    ... 'xHdWdTamnGLtwXYQbn7RjG3UADAiTOWk+QSmU2igZvQ2F hannes@soe.ucsc.edu\\n'
    >>> ec2_keypair_fingerprint(ssh_pubkey)
    'a5:5a:64:8a:1e:3f:4e:46:cd:1f:e9:b3:fc:cf:c5:19'

    >>> # This is not a private key that is in use, in case you were wondering
    >>> ssh_private_key = \\
    ... '-----BEGIN RSA PRIVATE KEY-----\\n'+\\
    ... 'MIIEpQIBAAKCAQEAi3shPK00+/6dwW8u+iDkUYiwIKl/lv0Ay5IstLszwb3CA4mVRlyq769HzE8f\\n'\\
    ... 'cnzQUX/NI8y9MTO0UNt2JDMJWW5L49jmvxV0TjxQjKg8KcNzYuHsEny3k8LxezWMsmwlrrC89O6e\\n'\\
    ... 'oo6boc8ForSdjVdIlJbvWu/82dThyFgTjWd5B+1O93xw8/ejqY9PfZExBeqpKjm58OUByTpVhvWe\\n'\\
    ... 'jmbZ9BL60XJhwz9bDTrlKpjcGsMZ74G6XfQAhyyqXYeD/XOercCSJgQ/QjYKcPE9yMRyucHyuYZ8\\n'\\
    ... 'HKzmG+u4p5ffnFb43tKzWCI330JQcklhGTldyqQHDWA41mT1QMoWfwIDAQABAoIBAF50gryRWykv\\n'\\
    ... 'cuuUfI6ciaGBXCyyPBomuUwicC3v/Au+kk1M9Y7RoFxyKb/88QHZ7kTStDwDITfZmMmM5QN8oF80\\n'\\
    ... 'pyXkM9bBE6MLi0zFfQCXQGN9NR4L4VGqGVfjmqUVQat8Omnv0fOpeVFpXZqij3Mw4ZDmaa7+iA+H\\n'\\
    ... '72J56ru9i9wcBNqt//Kh5BXARekp7tHzklYrlqJd03ftDRp9GTBIFAsaPClTBpnPVhwD/rAoJEhb\\n'\\
    ... 'KM9g/EMjQ28cUMQSHSwOyi9Rg/LtwFnER4u7pnBz2tbJFvLlXE96IQbksQL6/PTJ9H6Zpp+1fDcI\\n'\\
    ... 'k/MKSQZtQOgfV8V1wlvHX+Q0bxECgYEA4LHj6o4usINnSy4cf6BRLrCA9//ePa8UjEK2YDC5rQRV\\n'\\
    ... 'huFWqWJJSjWI9Ofjh8mZj8NvTJa9RW4d4Rn6F7upOuAer9obwfrmi4BEQSbvUwxQIuHOZ6itH/0L\\n'\\
    ... 'klqQBuhJeyr3W+2IhudJUQz9MEoddOfYIybXqkF7XzDl2x6FcjcCgYEAnunySmjt+983gUKK9DgK\\n'\\
    ... '/k1ki41jCAcFlGd8MbLEWkJpwt3FJFiyq6vVptoVH8MBnVAOjDneP6YyNBv5+zm3vyMuVJtKNcAP\\n'\\
    ... 'MAxrl5/gyIBHRxD+avoqpQX/17EmrFsbMaG8IM0ZWB2lSDt45sDvpmSlcTjzrHIEGoBbOzkOefkC\\n'\\
    ... 'gYEAgmS5bxSz45teBjLsNuRCOGYVcdX6krFXq03LqGaeWdl6CJwcPo/bGEWZBQbM86/6fYNcw4V2\\n'\\
    ... 'sSQGEuuQRtWQj6ogJMzd7uQ7hhkZgvWlTPyIRLXloiIw1a9zV6tWiaujeOamRaLC6AawdWikRbG9\\n'\\
    ... 'BmrE8yFHZnY5sjQeL9q2dmECgYEAgp5w1NCirGCxUsHLTSmzf4tFlZ9FQxficjUNVBxIYJguLkny\\n'\\
    ... '/Qka8xhuqJKgwlabQR7IlmIKV+7XXRWRx/mNGsJkFo791GhlE21iEmMLdEJcVAGX3X57BuGDhVrL\\n'\\
    ... 'GuhX1dfGtn9e0ZqsfE7F9YWodfBMPGA/igK9dLsEQg2H5KECgYEAvlv0cPHP8wcOL3g9eWIVCXtg\\n'\\
    ... 'aQ+KiDfk7pihLnHTJVZqXuy0lFD+O/TqxGOOQS/G4vBerrjzjCXXXxi2FN0kDJhiWlRHIQALl6rl\\n'\\
    ... 'i2LdKfL1sk1IA5PYrj+LmBuOLpsMHnkoH+XRJWUJkLvowaJ0aSengQ2AD+icrc/EIrpcdjU=\\n'+\\
    ... '-----END RSA PRIVATE KEY-----\\n'
    >>> ec2_keypair_fingerprint(ssh_private_key)
    'ac:23:ae:c3:9a:a3:78:b1:0f:8a:31:dd:13:cc:b1:8e:fb:51:42:f8'
    """
    rsa_key = RSA.importKey(ssh_key)
    is_private_key = rsa_key.has_private()
    if is_private_key and reject_private_keys:
        raise ValueError('Private keys are disallowed')
    der_rsa_key = rsa_key.exportKey(format='DER', pkcs=(8 if is_private_key else 1))
    key_hash = (hashlib.sha1 if is_private_key else hashlib.md5)(der_rsa_key)
    return ':'.join(partition_seq(key_hash.hexdigest(), 2))
