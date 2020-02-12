import logging
import time
from collections import Iterator
from operator import attrgetter
from past.builtins import map
from toil.lib.exceptions import panic
from toil.lib.retry import retry
from boto.ec2.instance import Instance
from boto.ec2.spotinstancerequest import SpotInstanceRequest
from boto.exception import EC2ResponseError

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


def retry_ec2(t=a_short_time, retry_for=10 * a_short_time, retry_while=not_found):
    return retry(delays=(t, t, t * 2, t * 4),
                 timeout=retry_for,
                 predicate=retry_while)


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
        log.warning('Cancelling remaining %i spot requests.', len(open_ids))
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
                log.warning('Timed out waiting for spot requests.')
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


def create_spot_instances(ec2, price, image_id, spec, num_instances=1, timeout=None, tentative=False, tags=None):
    """
    :rtype: Iterator[list[Instance]]
    """
    def spotRequestNotFound(e):
        return getattr(e, 'error_code', None) == "InvalidSpotInstanceRequestID.NotFound"

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
            log.warning(message + '.')
        else:
            raise RuntimeError(message)
    if num_other:
        log.warning('%i request(s) entered a state other than active.', num_other)


def inconsistencies_detected(e):
    if getattr(e, 'code', None) == 'InvalidGroup.NotFound':
        return True
    m = getattr(e, 'error_message', '').lower()
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
