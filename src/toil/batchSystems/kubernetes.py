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
"""
Batch system for running Toil workflows on Kubernetes.

Ony useful with network-based job stores, like AWSJobStore.

Within non-privileged Kubernetes containers, additional Docker containers
cannot yet be launched. That functionality will need to wait for user-mode
Docker
"""
import datetime
import logging
import math
import os
from queue import Empty, Queue
import string
import sys
import tempfile
from threading import Event, Thread, Condition, RLock
import time
import uuid
from argparse import ArgumentParser, _ArgumentGroup
from typing import (Any,
                    Callable,
                    Dict,
                    Iterator,
                    List,
                    Set,
                    Literal,
                    Optional,
                    Tuple,
                    Type,
                    TypeVar,
                    Union,
                    cast,
                    overload)

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec
if sys.version_info >= (3, 8):
    from typing import Protocol, TypedDict, runtime_checkable
else:
    from typing_extensions import Protocol, TypedDict, runtime_checkable
# TODO: When this gets into the standard library, get it from there and drop
import urllib3
import yaml
# The Right Way to use the Kubernetes module is to `import kubernetes` and then you get all your stuff as like ApiClient. But this doesn't work for the stubs: the stubs seem to only support importing things from the internal modules in `kubernetes` where they are actually defined. See for example <https://github.com/MaterializeInc/kubernetes-stubs/issues/9 and <https://github.com/MaterializeInc/kubernetes-stubs/issues/10>. So we just import all the things we use into our global namespace here.
from kubernetes.client import (BatchV1Api,
                               CoreV1Api,
                               CustomObjectsApi,
                               V1Affinity,
                               V1Container,
                               V1ContainerStatus,
                               V1EmptyDirVolumeSource,
                               V1HostPathVolumeSource,
                               V1Job,
                               V1JobCondition,
                               V1JobSpec,
                               V1NodeAffinity,
                               V1NodeSelector,
                               V1NodeSelectorRequirement,
                               V1NodeSelectorTerm,
                               V1ObjectMeta,
                               V1Pod,
                               V1PodSpec,
                               V1PodTemplateSpec,
                               V1PreferredSchedulingTerm,
                               V1ResourceRequirements,
                               V1SecretVolumeSource,
                               V1Toleration,
                               V1Volume,
                               V1VolumeMount)
from kubernetes.client.api_client import ApiClient
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException
from kubernetes.config.incluster_config import load_incluster_config
from kubernetes.config.kube_config import (list_kube_config_contexts,
                                           load_kube_config)
# TODO: Watch API is not typed yet
from kubernetes.watch import Watch  # type: ignore
# typing-extensions dependency on Pythons that are new enough.
from typing_extensions import NotRequired

from toil import applianceSelf
from toil.batchSystems.abstractBatchSystem import (EXIT_STATUS_UNAVAILABLE_VALUE,
                                                   BatchJobExitReason,
                                                   InsufficientSystemResources,
                                                   ResourcePool,
                                                   UpdatedBatchJobInfo)
from toil.batchSystems.cleanup_support import BatchSystemCleanupSupport
from toil.batchSystems.contained_executor import pack_job
from toil.batchSystems.options import OptionSetter
from toil.common import Config, Toil, SYS_MAX_SIZE
from toil.job import JobDescription, Requirer
from toil.lib.conversions import human2bytes
from toil.lib.misc import get_user_name, slow_down, utc_now
from toil.lib.retry import ErrorCondition, retry
from toil.resource import Resource

logger = logging.getLogger(__name__)
retryable_kubernetes_errors: List[Union[Type[Exception], ErrorCondition]] = [
    urllib3.exceptions.MaxRetryError,
    urllib3.exceptions.ProtocolError,
    ApiException
]


def is_retryable_kubernetes_error(e: Exception) -> bool:
    """
    A function that determines whether or not Toil should retry or stop given
    exceptions thrown by Kubernetes.
    """
    for error in retryable_kubernetes_errors:
        if isinstance(error, type) and isinstance(e, error):
            return True
    return False

# Represents a collection of label or taint keys and their sets of acceptable (or unacceptable) values.
KeyValuesList = List[Tuple[str, List[str]]]

class KubernetesBatchSystem(BatchSystemCleanupSupport):
    @classmethod
    def supportsAutoDeployment(cls) -> bool:
        return True

    class _ApiStorageDict(TypedDict):
        """
        Type-enforcing dict for our API object cache.
        """

        namespace: NotRequired[str]
        batch: NotRequired[BatchV1Api]
        core: NotRequired[CoreV1Api]
        customObjects: NotRequired[CustomObjectsApi]


    def __init__(self, config: Config, maxCores: int, maxMemory: int, maxDisk: int) -> None:
        super().__init__(config, maxCores, maxMemory, maxDisk)

        # Re-type the config to make sure it has all the fields we need.
        assert isinstance(config, KubernetesBatchSystem.KubernetesConfig)

        # Turn down log level for Kubernetes modules and dependencies.
        # Otherwise if we are at debug log level, we dump every
        # request/response to Kubernetes, including tokens which we shouldn't
        # reveal on CI.
        logging.getLogger('kubernetes').setLevel(logging.ERROR)
        logging.getLogger('requests_oauthlib').setLevel(logging.ERROR)

        # This will hold the last time our Kubernetes credentials were refreshed
        self.credential_time: Optional[datetime.datetime] = None
        # And this will hold our cache of API objects
        self._apis: KubernetesBatchSystem._ApiStorageDict = {}

        # Get our namespace (and our Kubernetes credentials to make sure they exist)
        self.namespace = self._api('namespace')

        # Decide if we are going to mount a Kubernetes host path as the Toil
        # work dir in the workers, for shared caching.
        self.host_path = config.kubernetes_host_path

        # Get the service account name to use, if any.
        self.service_account = config.kubernetes_service_account

        # Get how long we should wait for a pod that lands on a node to
        # actually start.
        self.pod_timeout = config.kubernetes_pod_timeout

        # Get the username to mark jobs with
        username = config.kubernetes_owner
        # And a unique ID for the run
        self.unique_id = uuid.uuid4()

        # Create a prefix for jobs, starting with our username
        self.job_prefix = f'{username}-toil-{self.unique_id}-'
        # Instead of letting Kubernetes assign unique job names, we assign our
        # own based on a numerical job ID. This functionality is managed by the
        # BatchSystemLocalSupport.

        # The UCSC GI Kubernetes cluster (and maybe other clusters?) appears to
        # relatively promptly destroy finished jobs for you if they don't
        # specify a ttlSecondsAfterFinished. This leads to "lost" Toil jobs,
        # and (if your workflow isn't allowed to run longer than the default
        # lost job recovery interval) timeouts of e.g. CWL Kubernetes
        # conformance tests. To work around this, we tag all our jobs with an
        # explicit TTL that is long enough that we're sure we can deal with all
        # the finished jobs before they expire.
        self.finished_job_ttl = 3600  # seconds

        # Here is where we will store the user script resource object if we get one.
        self.user_script: Optional[Resource] = None

        # Ge the image to deploy from Toil's configuration
        self.docker_image = applianceSelf()

        # Try and guess what Toil work dir the workers will use.
        # We need to be able to provision (possibly shared) space there.
        self.worker_work_dir = Toil.getToilWorkDir(config.workDir)
        if (config.workDir is None and
            os.getenv('TOIL_WORKDIR') is None and
            self.worker_work_dir == tempfile.gettempdir()):

            # We defaulted to the system temp directory. But we think the
            # worker Dockerfiles will make them use /var/lib/toil instead.
            # TODO: Keep this in sync with the Dockerfile.
            self.worker_work_dir = '/var/lib/toil'

        # A Toil-managed Kubernetes cluster will have most of its temp space at
        # /var/tmp, which is where really large temp files really belong
        # according to https://systemd.io/TEMPORARY_DIRECTORIES/. So we will
        # set the default temporary directory to there for all our jobs.
        self.environment['TMPDIR'] = '/var/tmp'

        # Get the name of the AWS secret, if any, to mount in containers.
        self.aws_secret_name = os.environ.get("TOIL_AWS_SECRET_NAME", None)

        # Set this to True to enable the experimental wait-for-job-update code
        self.enable_watching = os.environ.get("KUBE_WATCH_ENABLED", False)

        # This will be a label to select all our jobs.
        self.run_id = f'toil-{self.unique_id}'

        # Keep track of available resources.
        maxMillicores = int(SYS_MAX_SIZE if self.maxCores == SYS_MAX_SIZE else self.maxCores * 1000)
        self.resource_sources = [
            # A pool representing available job slots
            ResourcePool(self.config.max_jobs, 'job slots'),
            # A pool representing available CPU in units of millicores (1 CPU
            # unit = 1000 millicores)
            ResourcePool(maxMillicores, 'cores'),
            # A pool representing available memory in bytes
            ResourcePool(self.maxMemory, 'memory'),
            # A pool representing the available space in bytes
            ResourcePool(self.maxDisk, 'disk'),
        ]

        # A set of job IDs that are queued (useful for getIssuedBatchJobIDs())
        self._queued_job_ids: Set[int] = set()

        # Keep track of the acquired resources for each job
        self._acquired_resources: Dict[str, List[int]] = {}

        # Queue for jobs to be submitted to the Kubernetes cluster
        self._jobs_queue: Queue[Tuple[int, JobDescription, V1PodSpec]] = Queue()

        # A set of job IDs that should be killed
        self._killed_queue_jobs: Set[int] = set()

        # We use this event to signal shutdown
        self._shutting_down = Event()

        # A lock to protect critical regions when working with queued jobs.
        self._mutex = RLock()

        # A condition set to true when there is more work to do. e.g.: new job
        # in the queue or any resource becomes available.
        self._work_available = Condition(lock=self._mutex)

        self.schedulingThread = Thread(target=self._scheduler, daemon=True)
        self.schedulingThread.start()

    def _pretty_print(self, kubernetes_object: Any) -> str:
        """
        Pretty-print a Kubernetes API object to a YAML string. Recursively
        drops boring fields.
        Takes any Kubernetes API object; not clear if any base type exists for
        them.
        """

        if not kubernetes_object:
            return 'None'

        # We need a Kubernetes widget that knows how to translate
        # its data structures to nice YAML-able dicts. See:
        # <https://github.com/kubernetes-client/python/issues/1117#issuecomment-939957007>
        api_client: ApiClient = ApiClient()

        # Convert to a dict
        root_dict = api_client.sanitize_for_serialization(kubernetes_object)

        def drop_boring(here: Dict[str, Any]) -> None:
            """
            Drop boring fields recursively.
            """
            boring_keys = []
            for k, v in here.items():
                if isinstance(v, dict):
                    drop_boring(v)
                if k in ['managedFields']:
                    boring_keys.append(k)
            for k in boring_keys:
                del here[k]

        drop_boring(root_dict)
        return yaml.dump(root_dict)

    @overload
    def _api(
        self, kind: Literal['batch'], max_age_seconds: float = 5 * 60, errors: Optional[List[int]] = None
    ) -> BatchV1Api:
        ...

    @overload
    def _api(
        self, kind: Literal['core'], max_age_seconds: float = 5 * 60, errors: Optional[List[int]] = None
    ) -> CoreV1Api:
        ...

    @overload
    def _api(
        self, kind: Literal['customObjects'], max_age_seconds: float = 5 * 60, errors: Optional[List[int]] = None
    ) -> CustomObjectsApi:
        ...

    @overload
    def _api(
        self, kind: Literal['namespace'], max_age_seconds: float = 5 * 60
    ) -> str:
        ...

    def _api(
        self,
        kind: Union[Literal['batch'], Literal['core'], Literal['customObjects'], Literal['namespace']],
        max_age_seconds: float = 5 * 60,
        errors: Optional[List[int]] = None
    ) -> Union[BatchV1Api, CoreV1Api, CustomObjectsApi, str]:
        """
        The Kubernetes module isn't clever enough to renew its credentials when
        they are about to expire. See
        https://github.com/kubernetes-client/python/issues/741.

        We work around this by making sure that every time we are about to talk
        to Kubernetes, we have fresh credentials. And we do that by reloading
        the config and replacing our Kubernetes API objects before we do any
        Kubernetes things.

        TODO: We can still get in trouble if a single watch or listing loop
        goes on longer than our credentials last, though.

        This method is the Right Way to get any Kubernetes API. You call it
        with the API you want ('batch', 'core', or 'customObjects') and it
        returns an API object with guaranteed fresh credentials.

        It also recognizes 'namespace' and returns our namespace as a string.

        max_age_seconds needs to be << your cluster's credential expiry time.

        :param errors: If set, API calls made on the returned object will be retried
               if they produce retriable errors, except that Kubernetes API
               exceptions with HTTP status codes in the list will not be
               retried. Useful values are [] to retry on all errors, and [404]
               to retry on errors not caused by the thing in question not
               existing.
        """

        now = utc_now()

        if self.credential_time is None or (now - self.credential_time).total_seconds() > max_age_seconds:
            # Credentials need a refresh
            try:
                # Load ~/.kube/config or KUBECONFIG
                load_kube_config()
                # Worked. We're using kube config
                config_source = 'kube'
            except ConfigException:
                # Didn't work. Try pod-based credentials in case we are in a pod.
                try:
                    load_incluster_config()
                    # Worked. We're using in_cluster config
                    config_source = 'in_cluster'
                except ConfigException:
                    raise RuntimeError('Could not load Kubernetes configuration from ~/.kube/config, $KUBECONFIG, or current pod.')

            # Now fill in the API objects with these credentials
            self._apis['batch'] = BatchV1Api()
            self._apis['core'] = CoreV1Api()
            self._apis['customObjects'] = CustomObjectsApi()

            # And save the time
            self.credential_time = now
        if kind == 'namespace':
            # We just need the namespace string
            if config_source == 'in_cluster':
                # Our namespace comes from a particular file.
                with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as fh:
                    return fh.read().strip()
            else:
                # Find all contexts and the active context.
                # The active context gets us our namespace.
                contexts, activeContext = list_kube_config_contexts()
                if not contexts:
                    raise RuntimeError("No Kubernetes contexts available in ~/.kube/config or $KUBECONFIG")

                # Identify the namespace to work in
                namespace = activeContext.get('context', {}).get('namespace', 'default')
                assert isinstance(namespace, str)
                return namespace

        else:
            # We need an API object
            try:
                api_object = self._apis[kind]
                if errors is None:
                    # No wrapping needed
                    return api_object
                else:
                    # We need to wrap it up so that all calls that raise errors are retried.
                    error_list = list(retryable_kubernetes_errors)
                    if len(errors) > 0:
                        # Say don't retry on these particular errors
                        error_list.append(
                            ErrorCondition(
                                error=ApiException,
                                error_codes=errors,
                                retry_on_this_condition=False
                            )
                        )
                    decorator = retry(errors=error_list)
                    wrapper = KubernetesBatchSystem.DecoratorWrapper(api_object, decorator)
                    return cast(Union[BatchV1Api, CoreV1Api, CustomObjectsApi], wrapper)
            except KeyError:
                raise RuntimeError(f"Unknown Kubernetes API type: {kind}")

    class DecoratorWrapper:
        """
        Class to wrap an object so all its methods are decorated.
        """

        P = ParamSpec("P")
        def __init__(self, to_wrap: Any, decorator: Callable[[Callable[P, Any]], Callable[P, Any]]) -> None:
            """
            Make a wrapper around the given object.
            When methods on the object are called, they will be called through
            the given decorator.
            """
            self._wrapped = to_wrap
            self._decorator = decorator

        def __getattr__(self, name: str) -> Any:
            """
            Get a member as if we are actually the wrapped object.
            If it looks callable, we will decorate it.
            """

            attr = getattr(self._wrapped, name)
            if callable(attr):
                # Can be called, so return a wrapped version
                return self._decorator(attr)
            else:
                # Can't be called, so pass it by itself
                return attr

    ItemT = TypeVar("ItemT")
    class _ItemsHaver(Protocol[ItemT]):
        """
        Anything that has a .items that is a list of something.
        """
        # KubernetesBatchSystem isn't defined until the class executes, so any
        # up-references to types from there that are in signatures (and not
        # method code) need to be quoted
        items: List["KubernetesBatchSystem.ItemT"]

    CovItemT = TypeVar("CovItemT", covariant=True)
    class _WatchEvent(Protocol[CovItemT]):
        """
        An event from a Kubernetes watch stream.
        See https://github.com/kubernetes-client/python/blob/1271465acdb80bf174c50564a384fd6898635ea6/kubernetes/base/watch/watch.py#L130-L136.
        """

        # TODO: this can't be a TypedDict because of
        # https://stackoverflow.com/a/71945477 so we go with an overloaded
        # __getitem__ instead.

        @overload
        def __getitem__(self, name: Literal['type']) -> str:
            ...

        @overload
        def __getitem__(self, name: Literal['object']) -> "KubernetesBatchSystem.CovItemT":
            ...

        @overload
        def __getitem__(self, name: Literal['raw_object']) -> Dict[str, Any]:
            ...

        def __getitem__(self, name: Union[Literal['type'], Literal['object'], Literal['raw_object']]) -> Any:
            ...

    P = ParamSpec("P")
    R = TypeVar("R")
    def _stream_until_error(self, method: Callable[P, _ItemsHaver[R]], *args: P.args, **kwargs: P.kwargs) -> Iterator[_WatchEvent[R]]:
        """
        Kubernetes kubernetes.watch.Watch().stream() streams can fail and raise
        errors. We don't want to have those errors fail the entire workflow, so
        we handle them here.

        When you want to stream the results of a Kubernetes API method, call
        this instead of stream().

        To avoid having to do our own timeout logic, we finish the watch early
        if it produces an error.
        """

        w = Watch()

        # We will set this to bypass our second catch in the case of user errors.
        userError = False

        try:
            for item in w.stream(method, *args, **kwargs):
                # For everything the watch stream gives us
                try:
                    # Show the item to user code
                    yield item
                except Exception as e:
                    # If we get an error from user code, skip our catch around
                    # the Kubernetes generator.
                    userError = True
                    raise
        except Exception as e:
            # If we get an error
            if userError:
                # It wasn't from the Kubernetes watch generator. Pass it along.
                raise
            else:
                # It was from the Kubernetes watch generator we manage.
                if is_retryable_kubernetes_error(e):
                    # This is just cloud weather.
                    # TODO: We will also get an APIError if we just can't code good against Kubernetes. So make sure to warn.
                    logger.warning("Received error from Kubernetes watch stream: %s", e)
                    # Just end the watch.
                    return
                else:
                    # Something actually weird is happening.
                    raise

    def _scheduler(self) -> None:
        """
        The scheduler thread looks at jobs from the input queue, and submits
        them to the Kubernetes cluster when there are resources available.
        """
        while True:
            with self._work_available:
                # Wait until we get notified to do work.
                self._work_available.wait()

                if self._shutting_down.is_set():
                    # We're shutting down.
                    logger.info("Shutting down scheduler thread.")
                    break

                # Either we have new jobs inside the queue or more resources
                # have become available.

                # Loop through all jobs inside the queue and see if any of them
                # could be launched.
                jobs: Queue[Tuple[int, JobDescription, V1PodSpec]] = Queue()
                while True:
                    try:
                        job = self._jobs_queue.get_nowait()
                        job_id, job_desc, spec = job

                        # Check if this job has been previously killed.
                        if job_id in self._killed_queue_jobs:
                            self._killed_queue_jobs.remove(job_id)
                            logger.debug(f"Skipping killed job {job_id}")
                            continue

                        job_name = f'{self.job_prefix}{job_id}'
                        result = self._launch_job(job_name, job_desc, spec)
                        if result is False:
                            # Not enough resources to launch this job.
                            jobs.put(job)
                        else:
                            self._queued_job_ids.remove(job_id)

                    except Empty:
                        break

                # We've gone over the entire queue and these are the jobs that
                # have not been scheduled, so put them back to the queue.
                while True:
                    try:
                        self._jobs_queue.put(jobs.get_nowait())
                    except Empty:
                        break
                logger.debug(f"Roughly {self._jobs_queue.qsize} jobs in the queue")

    def setUserScript(self, userScript: Resource) -> None:
        logger.info(f'Setting user script for deployment: {userScript}')
        self.user_script = userScript

    # setEnv is provided by BatchSystemSupport, updates self.environment

    class Placement:
        """
        Internal format for pod placement constraints and preferences.
        """

        def __init__(self) -> None:
            """
            Make a new empty set of placement constraints.
            """

            self.required_labels: KeyValuesList = []
            """
            Labels which are required to be present (with these values).
            """
            self.desired_labels: KeyValuesList = []
            """
            Labels which are optional, but preferred to be present (with these values).
            """
            self.prohibited_labels: KeyValuesList = []
            """
            Labels which are not allowed to be present (with these values).
            """
            self.tolerated_taints: KeyValuesList = []
            """
            Taints which are allowed to be present (with these values).
            """

        def set_preemptible(self, preemptible: bool) -> None:
            """
            Add constraints for a job being preemptible or not.

            Preemptible jobs will be able to run on preemptible or non-preemptible
            nodes, and will prefer preemptible nodes if available.

            Non-preemptible jobs will not be allowed to run on nodes that are
            marked as preemptible.

            Understands the labeling scheme used by EKS, and the taint scheme used
            by GCE. The Toil-managed Kubernetes setup will mimic at least one of
            these.
            """

            # We consider nodes preemptible if they have any of these label or taint values.
            # We tolerate all effects of specified taints.
            # Amazon just uses a label, while Google
            # <https://cloud.google.com/kubernetes-engine/docs/how-to/preemptible-vms>
            # uses a label and a taint.
            PREEMPTIBLE_SCHEMES = {'labels': [('eks.amazonaws.com/capacityType', ['SPOT']),
                                              ('cloud.google.com/gke-preemptible', ['true'])],
                                   'taints': [('cloud.google.com/gke-preemptible', ['true'])]}

            if preemptible:
                # We want to seek preemptible labels and tolerate preemptible taints.
                self.desired_labels += PREEMPTIBLE_SCHEMES['labels']
                self.tolerated_taints += PREEMPTIBLE_SCHEMES['taints']
            else:
                # We want to prohibit preemptible labels
                self.prohibited_labels += PREEMPTIBLE_SCHEMES['labels']


        def apply(self, pod_spec: V1PodSpec) -> None:
            """
            Set ``affinity`` and/or ``tolerations`` fields on pod_spec, so that
            it runs on the right kind of nodes for the constraints we represent.
            """

            # Convert our collections to Kubernetes expressions.

            # REQUIRE that ALL of these requirements be satisfied
            required_selector_requirements: List[V1NodeSelectorRequirement] = []
            # PREFER that EACH of these terms be satisfied
            preferred_scheduling_terms: List[V1PreferredSchedulingTerm] = []
            # And this list of tolerations to apply
            tolerations: List[V1Toleration] = []

            for label, values in self.required_labels:
                # Collect requirements for the required labels
                has_label = V1NodeSelectorRequirement(key=label,
                                                      operator='In',
                                                      values=values)
                required_selector_requirements.append(has_label)
            for label, values in self.desired_labels:
                # Collect preferences for the preferred labels
                has_label = V1NodeSelectorRequirement(key=label,
                                                      operator='In',
                                                      values=values)
                term = V1NodeSelectorTerm(
                    match_expressions=[has_label]
                )
                # Each becomes a separate preference, more is better.
                preference = V1PreferredSchedulingTerm(weight=1,
                                                       preference=term)

                preferred_scheduling_terms.append(preference)
            for label, values in self.prohibited_labels:
                # So we need to say that each label either doesn't
                # have any of the banned values, or doesn't exist.
                # Although the docs don't really say so, NotIn also matches
                # cases where the label doesn't exist. This is suggested by
                # <https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#set-based-requirement>
                # So we create a NotIn for each label and AND them
                # all together.
                not_labeled = V1NodeSelectorRequirement(key=label,
                                                        operator='NotIn',
                                                        values=values)
                required_selector_requirements.append(not_labeled)
            for taint, values in self.tolerated_taints:
                for value in values:
                    # Each toleration can tolerate one value
                    taint_ok = V1Toleration(key=taint,
                                            value=value)
                    tolerations.append(taint_ok)

            # Now combine everything
            if preferred_scheduling_terms or required_selector_requirements:
                # We prefer or require something about labels.

                requirements_selector: Optional[V1NodeSelector] = None
                if required_selector_requirements:
                    # Make a term that says we match all the requirements
                    requirements_term = V1NodeSelectorTerm(
                        match_expressions=required_selector_requirements
                    )
                    # And a selector to hold the term
                    requirements_selector = V1NodeSelector(node_selector_terms=[requirements_term])

                # Make an affinity that prefers the preferences and requires the requirements
                node_affinity = V1NodeAffinity(
                    preferred_during_scheduling_ignored_during_execution=preferred_scheduling_terms if preferred_scheduling_terms else None,
                    required_during_scheduling_ignored_during_execution=requirements_selector
                )

                # Apply the affinity
                pod_spec.affinity = V1Affinity(node_affinity = node_affinity)

            if tolerations:
                # Apply the tolerations
                pod_spec.tolerations = tolerations

    def _check_accelerator_request(self, requirer: Requirer) -> None:
        for accelerator in requirer.accelerators:
            if accelerator['kind'] != 'gpu' and 'model' not in accelerator:
                # We can only provide GPUs or things with a model right now
                raise InsufficientSystemResources(requirer, 'accelerators', details=[
                    f'The accelerator {accelerator} could not be provided.',
                    'The Toil Kubernetes batch system only knows how to request gpu accelerators or accelerators with a defined model.'
                ])

    def _create_pod_spec(
            self,
            job_desc: JobDescription,
            job_environment: Optional[Dict[str, str]] = None
    ) -> V1PodSpec:
        """
        Make the specification for a pod that can execute the given job.
        """

        environment = self.environment.copy()
        if job_environment:
            environment.update(job_environment)

        # Make a command to run it in the executor
        command_list = pack_job(job_desc, self.user_script, environment=environment)

        # The Kubernetes API makes sense only in terms of the YAML format. Objects
        # represent sections of the YAML files. Except from our point of view, all
        # the internal nodes in the YAML structure are named and typed.

        # For docs, start at the root of the job hierarchy:
        # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md

        # Make a definition for the container's resource requirements.
        # Add on a bit for Kubernetes overhead (Toil worker's memory, hot deployed
        # user scripts).
        # Kubernetes needs some lower limit of memory to run the pod at all without
        # OOMing. We also want to provision some extra space so that when
        # we test _isPodStuckOOM we never get True unless the job has
        # exceeded job_desc.memory.
        requirements_dict = {'cpu': job_desc.cores,
                             'memory': job_desc.memory + 1024 * 1024 * 512,
                             'ephemeral-storage': job_desc.disk + 1024 * 1024 * 512}

        # Also start on the placement constraints
        placement = KubernetesBatchSystem.Placement()
        placement.set_preemptible(job_desc.preemptible)

        for accelerator in job_desc.accelerators:
            # Add in requirements for accelerators (GPUs).
            # See https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/

            if accelerator['kind'] == 'gpu':
                # We can't schedule GPUs without a brand, because the
                # Kubernetes resources are <brand>.com/gpu. If no brand is
                # specified, default to nvidia, which is very popular.
                vendor = accelerator.get('brand', 'nvidia')
                key = f'{vendor}.com/{accelerator["kind"]}'
                if key not in requirements_dict:
                    requirements_dict[key] = 0
                requirements_dict[key] += accelerator['count']

            if 'model' in accelerator:
                # TODO: What if the cluster uses some other accelerator model labeling scheme?
                placement.required_labels.append(('accelerator', [accelerator['model']]))

            # TODO: Support AMD's labeling scheme: https://github.com/RadeonOpenCompute/k8s-device-plugin/tree/master/cmd/k8s-node-labeller
            # That just has each trait of the accelerator as a separate label, but nothing that quite corresponds to a model.

        # Make all the resource requests into strings
        requests_dict = {k: str(v) for k, v in requirements_dict.items()}

        # Use the requests as the limits, for predictable behavior, and because
        # the UCSC Kubernetes admins want it that way. For GPUs, Kubernetes
        # requires them to be equal.
        limits_dict = requests_dict
        resources = V1ResourceRequirements(limits=limits_dict,
                                           requests=requests_dict)

        # Collect volumes and mounts
        volumes = []
        mounts = []

        def mount_host_path(volume_name: str, host_path: str, mount_path: str, create: bool = False) -> None:
            """
            Add a host path volume with the given name to mount the given path.

            :param create: If True, create the directory on the host if it does
                   not exist. Otherwise, when the directory does not exist, the
                   pod will wait for it to come into existence.
            """
            volume_type = 'DirectoryOrCreate' if create else 'Directory'
            volume_source = V1HostPathVolumeSource(path=host_path, type=volume_type)
            volume = V1Volume(name=volume_name,
                                                host_path=volume_source)
            volumes.append(volume)
            volume_mount = V1VolumeMount(mount_path=mount_path, name=volume_name)
            mounts.append(volume_mount)

        if self.host_path is not None:
            # Provision Toil WorkDir from a HostPath volume, to share with other pods.
            # Create the directory if it doesn't exist already.
            mount_host_path('workdir', self.host_path, self.worker_work_dir, create=True)
            # We also need to mount across /run/lock, where we will put
            # per-node coordiantion info.
            # Don't create this; it really should always exist.
            mount_host_path('coordination', '/run/lock', '/run/lock')
        else:
            # Provision Toil WorkDir as an ephemeral volume
            ephemeral_volume_name = 'workdir'
            ephemeral_volume_source = V1EmptyDirVolumeSource()
            ephemeral_volume = V1Volume(name=ephemeral_volume_name,
                                                          empty_dir=ephemeral_volume_source)
            volumes.append(ephemeral_volume)
            ephemeral_volume_mount = V1VolumeMount(mount_path=self.worker_work_dir, name=ephemeral_volume_name)
            mounts.append(ephemeral_volume_mount)
            # And don't share coordination directory

        if self.aws_secret_name is not None:
            # Also mount an AWS secret, if provided.
            # TODO: make this generic somehow
            secret_volume_name = 's3-credentials'
            secret_volume_source = V1SecretVolumeSource(secret_name=self.aws_secret_name)
            secret_volume = V1Volume(name=secret_volume_name,
                                                       secret=secret_volume_source)
            volumes.append(secret_volume)
            secret_volume_mount = V1VolumeMount(mount_path='/root/.aws', name=secret_volume_name)
            mounts.append(secret_volume_mount)

        # Make a container definition
        container = V1Container(command=command_list,
                                                  image=self.docker_image,
                                                  name="runner-container",
                                                  resources=resources,
                                                  volume_mounts=mounts)
        # Wrap the container in a spec
        pod_spec = V1PodSpec(containers=[container],
                                               volumes=volumes,
                                               restart_policy="Never")
        # Tell the spec where to land
        placement.apply(pod_spec)

        if self.service_account:
            # Apply service account if set
            pod_spec.service_account_name = self.service_account

        return pod_spec

    def _release_acquired_resources(self, resources: List[int], notify: bool = False) -> None:
        """
        Release all resources acquired for a job.

        :param resources: The resources in the order: core, memory, disk.
        :param notify: If True, notify the threads that are waiting on the
                       self._work_available condition.
        """
        # TODO: accelerators?

        for resource, request in zip(self.resource_sources, resources):
            assert isinstance(resource, ResourcePool) and isinstance(request, int)
            resource.release(request)

        if notify:
            with self._work_available:
                self._work_available.notify_all()

    def _launch_job(
        self,
        job_name: str,
        job_desc: JobDescription,
        pod_spec: V1PodSpec
    ) -> bool:
        """
        Try to launch the given job to the Kubernetes cluster. Return False if
        we don't have enough resources to submit the job.
        """

        # Limit the amount of resources requested at a time.
        resource_requests: List[int] = [1, int(job_desc.cores * 1000), job_desc.memory, job_desc.disk]

        acquired = []
        for source, request in zip(self.resource_sources, resource_requests):
            # For each kind of resource we want, go get it
            assert ((isinstance(source, ResourcePool) and isinstance(request, int)))
            if source.acquireNow(request):
                acquired.append(request)
            else:
                # We can't get everything
                self._release_acquired_resources(acquired,
                    # Put it back quietly.
                    notify=False)
                return False

        self._acquired_resources[job_name] = acquired

        # We have all the resources we need; submit it to the cluster.

        # Make metadata to label the job/pod with info.
        # Don't let the cluster autoscaler evict any Toil jobs.
        metadata = V1ObjectMeta(name=job_name,
                                labels={"toil_run": self.run_id},
                                annotations={"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"})

        # Wrap the spec in a template
        template = V1PodTemplateSpec(spec=pod_spec, metadata=metadata)

        # Make another spec for the job, asking to run the template with no
        # backoff/retry. Specify our own TTL to avoid catching the notice
        # of over-zealous abandoned job cleanup scripts.
        job_spec = V1JobSpec(template=template,
                             backoff_limit=0,
                             ttl_seconds_after_finished=self.finished_job_ttl)

        # And make the actual job
        job = V1Job(spec=job_spec,
                    metadata=metadata,
                    api_version="batch/v1",
                    kind="Job")

        # Launch the job
        launched = self._api('batch', errors=[]).create_namespaced_job(self.namespace, job)

        logger.debug(f"Launched job: {job_name}")

        return True

    def _delete_job(
        self,
        job_name: str, *,
        propagation_policy: Literal["Foreground", "Background"] = "Foreground",
        gone_ok: bool = False,
        resource_notify: bool = True
    ) -> None:
        """
        Given the name of a kubernetes job, delete the job and release all
        acquired resources for the job.

        :param job_name: The Kubernetes job name.
        :param propagation_policy: The propagation policy for the delete.
        :param gone_ok: If True, don't raise an error if the job is already gone.
        :param resource_notify: If True, notify the threads that are waiting on
            the self._work_available condition.
        """
        try:
            logger.debug(f'Deleting Kubernetes job {job_name}')
            self._api('batch', errors=[404] if gone_ok else []).delete_namespaced_job(
                job_name,
                self.namespace,
                propagation_policy=propagation_policy
            )
        finally:
            # We should always release the acquired resources.
            resources = self._acquired_resources.get(job_name)
            if not resources:
                logger.warning(f"Cannot get the acquired resources from {job_name}")
                return
            self._release_acquired_resources(resources, notify=resource_notify)
            del self._acquired_resources[job_name]

    def issueBatchJob(self, job_desc: JobDescription, job_environment: Optional[Dict[str, str]] = None) -> int:
        # Try the job as local
        localID = self.handleLocalJob(job_desc)
        if localID is not None:
            # It is a local job
            return localID

        # We actually want to send to the cluster

        # Check resource requirements
        self.check_resource_request(job_desc)

        # Make a pod that describes running the job
        pod_spec = self._create_pod_spec(job_desc, job_environment=job_environment)

        # Make a batch system scope job ID
        job_id = self.getNextJobID()

        # Put job inside queue to be launched to the cluster
        self._queued_job_ids.add(job_id)

        # Also keep track of the ID for getIssuedBatchJobIDs()
        self._jobs_queue.put((job_id, job_desc, pod_spec))

        # Let scheduler know that there is work to do.
        with self._work_available:
            self._work_available.notify_all()

        logger.debug(f"Enqueued job {job_id}")

        return job_id

    class _ArgsDict(TypedDict):
        """
        Dict for kwargs to list_namespaced_job/pod, so MyPy can know we're
        passign the right types when we make a dict and unpack it into kwargs.

        We need to do unpacking because the methods inspect the keys in their
        kwargs, so we can't just set unused ones to None. But we also don't
        want to duplicate code for every combination of possible present keys.
        """
        _continue: NotRequired[str]
        label_selector: NotRequired[str]
        field_selector: NotRequired[str]

    def _ourJobObject(self, onlySucceeded: bool = False) -> Iterator[V1Job]:
        """
        Yield Kubernetes V1Job objects that we are responsible for that the
        cluster knows about. Ignores jobs in the process of being deleted.

        Doesn't support a free-form selector, because there's only about 3
        things jobs can be selected on: https://stackoverflow.com/a/55808444

        :param bool onlySucceeded: restrict results to succeeded jobs.
        :param int limit: max results to yield.
        """

        # We need to page through the list from the cluster with a continuation
        # token. These expire after about 5 minutes. If we use an expired one,
        # we get a 410 error and a new token, and we can use the new token to
        # get the rest of the list, but the list will be updated.
        #
        # TODO: How to get the new token isn't clear. See
        # https://github.com/kubernetes-client/python/issues/953. For now we
        # will just throw an error if we don't get to the end of the list in
        # time.

        token = None

        while True:
            kwargs: KubernetesBatchSystem._ArgsDict = {'label_selector': f"toil_run={self.run_id}"}

            if onlySucceeded:
                kwargs['field_selector'] = "status.successful==1"

            if token is not None:
                kwargs['_continue'] = token

            results = self._api('batch', errors=[]).list_namespaced_job(
                self.namespace,
                **kwargs
            )
            
            # These jobs belong to us
            yield from (j for j in results.items if not self._is_deleted(j))

            # Remember the continuation token, if any
            token = getattr(results.metadata, 'continue', None)

            if token is None:
                # There isn't one. We got everything.
                break


    def _ourPodObject(self) -> Iterator[V1Pod]:
        """
        Yield Kubernetes V1Pod objects that we are responsible for that the
        cluster knows about.
        """

        token = None

        while True:
            kwargs: KubernetesBatchSystem._ArgsDict = {'label_selector': f"toil_run={self.run_id}"}

            if token is not None:
                kwargs['_continue'] = token

            results = self._api('core', errors=[]).list_namespaced_pod(
                self.namespace,
                **kwargs
            )

            yield from (j for j in results.items if not self._is_deleted(j))
            # Remember the continuation token, if any
            token = getattr(results.metadata, 'continue', None)

            if token is None:
                # There isn't one. We got everything.
                break


    def _getPodForJob(self, jobObject: V1Job) -> Optional[V1Pod]:
        """
        Get the pod that belongs to the given job, or None if the job's pod is
        missing. The pod knows about things like the job's exit code.

        :param V1Job jobObject: a Kubernetes job to look up
                                       pods for.

        :return: The pod for the job, or None if no pod is found.
        :rtype: V1Pod
        """

        # Make sure the job has the fields we need
        assert(jobObject.metadata is not None)

        token = None

        while True:
            kwargs: KubernetesBatchSystem._ArgsDict = {'label_selector': f'job-name={jobObject.metadata.name}'}
            if token is not None:
                kwargs['_continue'] = token
            results = self._api('core', errors=[]).list_namespaced_pod(self.namespace, **kwargs)

            for pod in results.items:
                # Return the first pod we find
                return pod

            # Remember the continuation token, if any
            token = getattr(results.metadata, 'continue', None)

            if token is None:
                # There isn't one. We got everything.
                break

        # If we get here, no pages had any pods.
        return None

    def _getLogForPod(self, podObject: V1Pod) -> str:
        """
        Get the log for a pod.

        :param V1Pod podObject: a Kubernetes pod with one
               container to get the log from.

        :return: The log for the only container in the pod.
        :rtype: str

        """

        assert podObject.metadata is not None
        assert podObject.metadata.name is not None

        return self._api('core', errors=[]).read_namespaced_pod_log(
            podObject.metadata.name,
            namespace=self.namespace
        )

    def _isPodStuckOOM(self, podObject: V1Pod, minFreeBytes: float = 1024 * 1024 * 2) -> bool:
        """
        Poll the current memory usage for the pod from the cluster.

        Return True if the pod looks to be in a soft/stuck out of memory (OOM)
        state, where it is using too much memory to actually make progress, but
        not enough to actually trigger the OOM killer to kill it. For some
        large memory limits, on some Kubernetes clusters, pods can get stuck in
        this state when their memory limits are high (approx. 200 Gi).

        We operationalize "OOM" as having fewer than minFreeBytes bytes free.

        We assume the pod has only one container, as Toil's pods do.

        If the metrics service is not working, we treat the pod as not being
        stuck OOM. Otherwise, we would kill all functioning jobs on clusters
        where the metrics service is down or isn't installed.

        :param V1Pod podObject: a Kubernetes pod with one
                                       container to check up on.
        :param int minFreeBytes: Minimum free bytes to not be OOM.

        :return: True if the pod is OOM, False otherwise.
        :rtype: bool
        """

        assert podObject.metadata is not None
        assert podObject.metadata.name is not None

        # Compose a query to get just the pod we care about
        query = f'metadata.name={podObject.metadata.name}'

        # Look for it, but manage our own exceptions
        try:
            # TODO: When the Kubernetes Python API actually wraps the metrics API, switch to that
            response = self._api('customObjects').list_namespaced_custom_object('metrics.k8s.io', 'v1beta1',
                                                                                self.namespace, 'pods',
                                                                                field_selector=query)
        except Exception as e:
            # We couldn't talk to the metrics service on this attempt. We don't
            # retry, but we also don't want to just ignore all errors. We only
            # want to ignore errors we expect to see if the problem is that the
            # metrics service is not working.
            if type(e) in retryable_kubernetes_errors:
                # This is the sort of error we would expect from an overloaded
                # Kubernetes or a dead metrics service.
                # We can't tell that the pod is stuck, so say that it isn't.
                logger.warning("Could not query metrics service: %s", e)
                return False
            else:
                raise

        # Pull out the items
        items = response.get('items', [])

        if len(items) == 0:
            # If there's no statistics we can't say we're stuck OOM
            return False

        # Assume the first result is the right one, because of the selector.
        # That means we don't need to bother with _continue.
        # Assume it has exactly one pod, because we made it.
        containers = items[0].get('containers', [{}])

        if len(containers) == 0:
            # If there are no containers (because none have started yet?), we can't say we're stuck OOM
            return False

        # Otherwise, assume it just has one container.
        # Grab the memory usage string, like 123Ki, and convert to bytes.
        # If anything is missing, assume 0 bytes used.
        bytesUsed = human2bytes(containers[0].get('usage', {}).get('memory', '0'))

        # Also get the limit out of the pod object's spec
        assert podObject.spec is not None
        assert len(podObject.spec.containers) > 0
        assert podObject.spec.containers[0].resources is not None
        assert podObject.spec.containers[0].resources.limits is not None
        assert 'memory' in podObject.spec.containers[0].resources.limits
        bytesAllowed = human2bytes(podObject.spec.containers[0].resources.limits['memory'])

        if bytesAllowed - bytesUsed < minFreeBytes:
            # This is too much!
            logger.warning('Pod %s has used %d of %d bytes of memory; reporting as stuck due to OOM.',
                           podObject.metadata.name, bytesUsed, bytesAllowed)

            return True
        else:
            return False

    def _isPodStuckWaiting(self, pod_object: V1Pod, reason: Optional[str] = None, timeout: Optional[float] = None) -> bool:
        """
        Return True if the pod looks to be in a waiting state, and false otherwise.

        :param pod_object: a Kubernetes pod with one container to check up on.
        :param reason: Only match pods with a waiting reason equal to this.
        :param timeout: Only match pods that have a start_time (scheduling time) older than this many seconds.

        :return: True if the pod is probably stuck, False otherwise.
        """

        if pod_object.status is None:
            return False

        # Get the statuses of the pod's containers
        container_statuses = pod_object.status.container_statuses
        if container_statuses is None or len(container_statuses) == 0:
            # Pod exists but has no container statuses
            # This happens when the pod is just "Scheduled"
            # ("PodScheduled" status event) and isn't actually starting
            # to run yet.
            # Can't be stuck
            return False

        waiting_info = getattr(getattr(container_statuses[0], 'state', None), 'waiting', None)
        if waiting_info is None:
            # Pod is not waiting
            return False

        if reason is not None and waiting_info.reason != reason:
            # Pod fails reason filter
            return False

        start_time = getattr(pod_object.status, 'start_time', None)
        if timeout is not None and (start_time is None or (utc_now() - start_time).total_seconds() < timeout):
            # It hasn't been waiting too long, or we care but don't know how
            # long it has been waiting
            return False

        return True

    def _is_deleted(self, kube_thing: Union['V1Job', 'V1Pod']) -> bool:
        """
        Determine if a job or pod is in the process od being deleted, and
        shouldn't count anymore.
        """

        # Kubernetes "Terminating" is the same as having the deletion_timestamp
        # set in the metadata of the object.

        deletion_timestamp: Optional[datetime.datetime] = getattr(getattr(kube_thing, 'metadata', None), 'deletion_timestamp', None)
        # If the deletion timestamp is set to anything, it is in the process of
        # being deleted. We will treat that as as good as gone.
        return deletion_timestamp is not None

    def _getIDForOurJob(self, jobObject: V1Job) -> int:
        """
        Get the JobID number that belongs to the given job that we own.

        :param V1Job jobObject: a Kubernetes job object that is a job we issued.

        :return: The JobID for the job.
        :rtype: int
        """

        assert jobObject.metadata is not None
        assert jobObject.metadata.name is not None
        return int(jobObject.metadata.name[len(self.job_prefix):])


    def getUpdatedBatchJob(self, maxWait: float) -> Optional[UpdatedBatchJobInfo]:

        entry = datetime.datetime.now()

        result = self._getUpdatedBatchJobImmediately()

        if result is not None or maxWait == 0:
            # We got something on the first try, or we only get one try.
            # If we wait
            return result

        # Otherwise we need to maybe wait.
        if self.enable_watching and maxWait >= 1:
            # We can try a watch. Watches can only work in whole seconds.
            for event in self._stream_until_error(self._api('batch').list_namespaced_job,
                                                  self.namespace,
                                                  label_selector=f"toil_run={self.run_id}",
                                                  timeout_seconds=math.floor(maxWait)):
                # Grab the metadata data, ID, the list of conditions of the current job, and the total pods
                jobObject = event['object']
                
                if self._is_deleted(jobObject):
                    # Job is already deleted, so ignore it.
                    logger.warning('Kubernetes job %s is deleted; ignore its update', getattr(getattr(jobObject, 'metadata', None), 'name', None))
                    continue
                
                assert jobObject.metadata is not None
                assert jobObject.metadata.name is not None
                
                jobID = int(jobObject.metadata.name[len(self.job_prefix):])
                if jobObject.status is None:
                    # Can't tell what is up with this job.
                    continue
                # Fetch out the pod counts, make sure they are numbers
                active_pods = jobObject.status.active or 0
                succeeded_pods = jobObject.status.succeeded or 0
                failed_pods = jobObject.status.failed or 0
                # Fetch out the condition object that has info about how the job is going.
                condition: Optional[V1JobCondition] = None
                if jobObject.status.conditions is not None and len(jobObject.status.conditions) > 0:
                    condition = jobObject.status.conditions[0]

                totalPods = active_pods + succeeded_pods + failed_pods
                # Exit Reason defaults to 'Successfully Finished` unless said otherwise
                exitReason = BatchJobExitReason.FINISHED
                exitCode = 0

                # Check if there are any active pods
                if active_pods > 0:
                    logger.info("%s has %d pods running" % jobObject.metadata.name, active_pods)
                    continue
                elif succeeded_pods > 0 or failed_pods > 0:
                    # No more active pods in the current job ; must be finished
                    logger.info("%s RESULTS -> Succeeded: %d Failed:%d Active:%d" % jobObject.metadata.name,
                                                                                    succeeded_pods, failed_pods, active_pods)
                    # Log out success/failure given a reason
                    logger.info("%s REASON: %s", getattr(condition, 'type', None), getattr(condition, 'reason', None))

                    # Log out reason of failure and pod exit code
                    if failed_pods > 0:
                        exitReason = BatchJobExitReason.FAILED
                        exitCode = EXIT_STATUS_UNAVAILABLE_VALUE
                        logger.debug("Failed job %s", self._pretty_print(jobObject))
                        if condition is not None:
                            logger.warning("Failed Job Message: %s", condition.message)
                        pod = self._getPodForJob(jobObject)
                        statuses: List[V1ContainerStatus] = getattr(getattr(pod, 'status', None), 'container_statuses', [])
                        if len(statuses) > 0 and statuses[0].state is not None and statuses[0].state.terminated is not None:
                            exitCode = statuses[0].state.terminated.exit_code

                    raw_runtime = 0.0
                    if jobObject.status.completion_time is not None and jobObject.status.start_time is not None:
                        raw_runtime = (jobObject.status.completion_time - jobObject.status.start_time).total_seconds()
                    runtime = slow_down(raw_runtime)
                    result = UpdatedBatchJobInfo(jobID=jobID, exitStatus=exitCode, wallTime=runtime, exitReason=exitReason)

                    if (exitReason == BatchJobExitReason.FAILED) or (succeeded_pods + failed_pods == totalPods):
                        # Cleanup if job is all finished or there was a pod that failed
                        # TODO: use delete_job() to release acquired resources
                        self._delete_job(
                            jobObject.metadata.name,
                            propagation_policy='Foreground'
                        )
                        # Make sure the job is deleted so we won't see it again.
                        self._waitForJobDeath(jobObject.metadata.name)
                        return result
                    continue
                else:
                    # Job is not running/updating ; no active, successful, or failed pods yet
                    logger.debug("Job {} -> {}".format(jobObject.metadata.name, getattr(condition, 'reason', None)))
                    # Pod could be pending; don't say it's lost.
                    continue
        else:
            # Try polling instead
            while result is None and (datetime.datetime.now() - entry).total_seconds() < maxWait:
                # We still have nothing and we haven't hit the timeout.

                # Poll
                result = self._getUpdatedBatchJobImmediately()

                if result is None:
                    # Still nothing. Wait a second, or some fraction of our max wait time.
                    time.sleep(min(maxWait/2, 1.0))

            # When we get here, either we found something or we ran out of time
            return result


    def _getUpdatedBatchJobImmediately(self) -> Optional[UpdatedBatchJobInfo]:
        """
        Return None if no updated (completed or failed) batch job is currently
        available, and jobID, exitCode, runtime if such a job can be found.
        """

        # See if a local batch job has updated and is available immediately
        local_tuple = self.getUpdatedLocalJob(0)
        if local_tuple:
            # If so, use it
            return local_tuple

        # Otherwise we didn't get a local job.

        # Go looking for other jobs

        # Everybody else does this with a queue and some other thread that
        # is responsible for populating it.
        # But we can just ask kubernetes now.

        # Find a job that is done, failed, or stuck
        jobObject = None
        # Put 'done', 'failed', or 'stuck' here
        chosenFor = ''

        for j in self._ourJobObject(onlySucceeded=True):
            # Look for succeeded jobs because that's the only filter Kubernetes has
            jobObject = j
            chosenFor = 'done'

        if jobObject is None:
            for j in self._ourJobObject():
                # If there aren't any succeeded jobs, scan all jobs
                # See how many times each failed
                failCount = getattr(j.status, 'failed', 0)
                if failCount is None:
                    # Make sure it is an int
                    failCount = 0
                if failCount > 0:
                    # Take the first failed one you find
                    jobObject = j
                    chosenFor = 'failed'
                    break

        if jobObject is None:
            # If no jobs are failed, look for jobs with pods that are stuck for various reasons.
            for j in self._ourJobObject():
                pod = self._getPodForJob(j)

                if pod is None:
                    # Skip jobs with no pod
                    continue

                # Containers can get stuck in Waiting with reason ImagePullBackOff
                if self._isPodStuckWaiting(pod, reason='ImagePullBackoff'):
                    # Assume it will never finish, even if the registry comes back or whatever.
                    # We can get into this state when we send in a non-existent image.
                    # See https://github.com/kubernetes/kubernetes/issues/58384
                    jobObject = j
                    chosenFor = 'stuck'
                    logger.warning('Failing stuck job (ImagePullBackoff); did you try to run a non-existent Docker image?'
                                   ' Check TOIL_APPLIANCE_SELF.')
                    break

                # Containers can also get stuck in Waiting with reason
                # ContainerCreating, if for example their mounts don't work.
                if self._isPodStuckWaiting(pod, reason='ContainerCreating', timeout=self.pod_timeout):
                    # Assume that it will never finish.
                    jobObject = j
                    chosenFor = 'stuck'
                    logger.warning('Failing stuck job (ContainerCreating longer than %s seconds); did you try to mount something impossible?', self.pod_timeout)
                    break

                # Pods can also get stuck nearly but not quite out of memory,
                # if their memory limits are high and they try to exhaust them.

                if self._isPodStuckOOM(pod):
                    # We found a job that probably should be OOM! Report it as stuck.
                    # Polling function takes care of the logging.
                    jobObject = j
                    chosenFor = 'stuck'
                    break

        if jobObject is None:
            # Say we couldn't find anything
            return None
        else:
            # We actually have something
            logger.debug('Identified stopped Kubernetes job %s as %s', getattr(jobObject.metadata, 'name', None), chosenFor)


        # Otherwise we got something.

        # Work out what the job's ID was (whatever came after our name prefix)
        assert jobObject.metadata is not None
        assert jobObject.metadata.name is not None
        jobID = int(jobObject.metadata.name[len(self.job_prefix):])

        # Grab the pod
        pod = self._getPodForJob(jobObject)

        if pod is not None:
            if chosenFor == 'done' or chosenFor == 'failed':
                # The job actually finished or failed

                # Get the statuses of the pod's containers
                containerStatuses = getattr(getattr(pod, 'status', None), 'container_statuses', None)

                # Get when the pod started (reached the Kubelet) as a datetime
                start_time = self._get_start_time(pod, jobObject)

                if containerStatuses is None or len(containerStatuses) == 0:
                    # No statuses available.
                    # This happens when a pod is "Scheduled". But how could a
                    # 'done' or 'failed' pod be merely "Scheduled"?
                    # Complain so we can find out.
                    logger.warning('Exit code and runtime unavailable; pod has no container statuses')
                    logger.warning('Pod: %s', str(pod))
                    exitCode = EXIT_STATUS_UNAVAILABLE_VALUE
                    # Say it stopped now and started when it was scheduled/submitted.
                    # We still need a strictly positive runtime.
                    runtime = slow_down((utc_now() - start_time).total_seconds())
                else:
                    # Get the termination info from the pod's main (only) container
                    terminatedInfo = getattr(getattr(containerStatuses[0], 'state', None), 'terminated', None)
                    if terminatedInfo is None:
                        logger.warning('Exit code and runtime unavailable; pod stopped without container terminating')
                        logger.warning('Pod: %s', str(pod))
                        exitCode = EXIT_STATUS_UNAVAILABLE_VALUE
                        # Say it stopped now and started when it was scheduled/submitted.
                        # We still need a strictly positive runtime.
                        runtime = slow_down((utc_now() - start_time).total_seconds())
                    else:
                        # Extract the exit code
                        exitCode = terminatedInfo.exit_code

                        # Compute how long the job actually ran for (subtract
                        # datetimes). We need to look at the pod's start time
                        # because the job's start time is just when the job is
                        # created. And we need to look at the pod's end time
                        # because the job only gets a completion time if
                        # successful.
                        runtime = slow_down((terminatedInfo.finished_at -
                                             start_time).total_seconds())

                        if chosenFor == 'failed':
                            # Warn the user with the failed pod's log
                            # TODO: cut this down somehow?
                            logger.warning('Log from failed pod: %s', self._getLogForPod(pod))

            else:
                # The job has gotten stuck

                assert chosenFor == 'stuck'

                # Synthesize an exit code
                exitCode = EXIT_STATUS_UNAVAILABLE_VALUE
                # Say it ran from when the job was submitted to when the pod got stuck
                runtime = slow_down((utc_now() - self._get_start_time(job=jobObject)).total_seconds())
        else:
            # The pod went away from under the job.
            logging.warning('Exit code and runtime unavailable; pod vanished')
            exitCode = EXIT_STATUS_UNAVAILABLE_VALUE
            # Say it ran from when the job was submitted to when the pod vanished
            runtime = slow_down((utc_now() - self._get_start_time(job=jobObject)).total_seconds())


        try:
            # Delete the job and all dependents (pods), hoping to get a 404 if it's magically gone
            self._delete_job(jobObject.metadata.name, propagation_policy='Foreground', gone_ok=True)

            # That just kicks off the deletion process. Foreground doesn't
            # actually block. See
            # https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/#foreground-cascading-deletion
            # We wait for the job to vanish, or at least pass _is_deleted so
            # we ignore it later.
            self._waitForJobDeath(jobObject.metadata.name)

        except ApiException as e:
            if e.status != 404:
                # Something is wrong, other than the job already being deleted.
                raise
            # Otherwise everything is fine and the job is gone.

        # Return the one finished job we found
        return UpdatedBatchJobInfo(jobID=jobID, exitStatus=exitCode, wallTime=runtime, exitReason=None)

    def _waitForJobDeath(self, jobName: str) -> None:
        """
        Block until the job with the given name no longer exists.
        """

        # We do some exponential backoff on the polling
        # TODO: use a wait instead of polling?
        backoffTime = 0.1
        maxBackoffTime = 6.4
        while True:
            try:
                # Look for the job
                job_object = self._api('batch', errors=[404]).read_namespaced_job(jobName, self.namespace)
                if self._is_deleted(job_object):
                    # The job looks deleted, so we can treat it as not being there.
                    return
                # If we didn't 404, and the job doesn't look deleted, wait a
                # bit with exponential backoff
                time.sleep(backoffTime)
                if backoffTime < maxBackoffTime:
                    backoffTime *= 2
            except ApiException as e:
                # We finally got a failure!
                if e.status != 404:
                    # But it wasn't due to the job being gone; something is wrong.
                    raise
                # It was a 404; the job is gone. Stop polling it.
                break

    def shutdown(self) -> None:

        # Shutdown local processes first
        self.shutdownLocal()

        # Shutdown scheduling thread
        self._shutting_down.set()
        with self._work_available:
            self._work_available.notify_all()   # Wake it up.

        self.schedulingThread.join()

        # Kill all of our jobs and clean up pods that are associated with those jobs
        try:
            logger.debug('Deleting all Kubernetes jobs for toil_run=%s', self.run_id)
            self._api('batch', errors=[404]).delete_collection_namespaced_job(
                self.namespace,
                label_selector=f"toil_run={self.run_id}",
                propagation_policy='Background'
            )
            logger.debug('Killed jobs with delete_collection_namespaced_job; cleaned up')
            # TODO: should we release all resources? We're shutting down so would it matter?
        except ApiException as e:
            if e.status != 404:
                # Anything other than a 404 is weird here.
                logger.error("Exception when calling BatchV1Api->delete_collection_namespaced_job: %s" % e)

            # If batch delete fails, try to delete all remaining jobs individually.
            logger.debug('Deleting Kubernetes jobs individually for toil_run=%s', self.run_id)
            for job_id in self._getIssuedNonLocalBatchJobIDs():
                job_name = f'{self.job_prefix}{job_id}'
                self._delete_job(job_name, propagation_policy='Background', resource_notify=False)

            # Aggregate all pods and check if any pod has failed to cleanup or is orphaned.
            ourPods = self._ourPodObject()

            for pod in ourPods:
                try:
                    phase = getattr(pod.status, 'phase', None)
                    if phase == 'Failed':
                            logger.debug('Failed pod encountered at shutdown:\n%s', self._pretty_print(pod))
                    if phase == 'Orphaned':
                            logger.debug('Orphaned pod encountered at shutdown:\n%s', self._pretty_print(pod))
                except:
                    # Don't get mad if that doesn't work.
                    pass
                if pod.metadata is not None and pod.metadata.name is not None:
                    try:
                        logger.debug('Cleaning up pod at shutdown: %s', pod.metadata.name)
                        response = self._api('core', errors=[404]).delete_namespaced_pod(
                            pod.metadata.name,
                            self.namespace,
                            propagation_policy='Background'
                        )
                    except ApiException as e:
                        if e.status != 404:
                            # Anything other than a 404 is weird here.
                            logger.error("Exception when calling CoreV1Api->delete_namespaced_pod: %s" % e)


    def _getIssuedNonLocalBatchJobIDs(self) -> List[int]:
        """
        Get the issued batch job IDs that are not for local jobs.
        """
        jobIDs = []
        with self._mutex:
            got_list = self._ourJobObject()
            for job in got_list:
                # Get the ID for each job
                jobIDs.append(self._getIDForOurJob(job))
        return jobIDs

    def getIssuedBatchJobIDs(self) -> List[int]:
        # Make sure to send the local jobs and queued jobs also
        with self._mutex:
            queued_jobs = list(self._queued_job_ids)
        return self._getIssuedNonLocalBatchJobIDs() + list(self.getIssuedLocalJobIDs()) + queued_jobs

    def _get_start_time(self, pod: Optional[V1Pod] = None, job: Optional[V1Job] = None) -> datetime.datetime:
        """
        Get an actual or estimated start time for a pod.
        """

        # Get when the pod started (reached the Kubelet) as a datetime
        start_time = getattr(getattr(pod, 'status', None), 'start_time', None)
        if start_time is None:
            # If the pod never made it to the kubelet to get a
            # start_time, say it was when the job was submitted.
            start_time = getattr(getattr(job, 'status', None), 'start_time', None)
        if start_time is None:
            # If this is still unset, say it was just now.
            start_time = utc_now()
        return start_time

    def getRunningBatchJobIDs(self) -> Dict[int, float]:
        # We need a dict from jobID (integer) to seconds it has been running
        secondsPerJob = dict()
        for job in self._ourJobObject():
            # Grab the pod for each job
            pod = self._getPodForJob(job)

            if pod is None:
                # Jobs whose pods are gone are not running
                continue

            if getattr(pod.status, 'phase', None) == 'Running':
                # The job's pod is running

                # Estimate the runtime
                runtime = (utc_now() - self._get_start_time(pod, job)).total_seconds()

                # Save it under the stringified job ID
                secondsPerJob[self._getIDForOurJob(job)] = runtime
        # Mix in the local jobs
        secondsPerJob.update(self.getRunningLocalJobIDs())
        return secondsPerJob

    def killBatchJobs(self, jobIDs: List[int]) -> None:

        # Kill all the ones that are local
        self.killLocalJobs(jobIDs)

        # Clears workflow's jobs listed in jobIDs.

        # First get the jobs we even issued non-locally
        issued_on_kubernetes = set(self._getIssuedNonLocalBatchJobIDs())
        deleted_jobs: List[str] = []

        for job_id in jobIDs:
            # For each job we are supposed to kill
            if job_id not in issued_on_kubernetes:
                # It never went to Kubernetes (or wasn't there when we just
                # looked), so we can't kill it on Kubernetes.

                # It is likely that the job is still waiting inside the jobs
                # queue and hasn't been submitted to Kubernetes. In this case,
                # it is not reliable to loop through the queue and try to
                # delete it. Instead, let's keep track of it and don't submit
                # when we encounter it.
                with self._mutex:
                    self._killed_queue_jobs.add(job_id)

                    # Make sure we don't keep track of it for getIssuedBatchJobIDs().
                    if job_id in self._queued_job_ids:
                        self._queued_job_ids.remove(job_id)

                continue
            # Work out what the job would be named
            job_name = self.job_prefix + str(job_id)

            # Delete the requested job in the foreground.
            # This doesn't block, but it does delete expeditiously.
            self._delete_job(job_name, propagation_policy='Foreground')

            deleted_jobs.append(job_name)
            logger.debug('Killed job by request: %s', job_name)

        for job_name in deleted_jobs:
            # Now we need to wait for all the jobs we killed to be gone.

            # Block until the delete takes.
            # The user code technically might stay running forever, but we push
            # the potential deadlock (if the user code needs exclusive access to
            # a resource) onto the user code, instead of always hanging
            # whenever we can't certify that a faulty node is no longer running
            # the user code. 
            self._waitForJobDeath(job_name)

    @classmethod
    def get_default_kubernetes_owner(cls) -> str:
        """
        Get the default Kubernetes-acceptable username string to tack onto jobs.
        """

        # Make a Kubernetes-acceptable version of our username: not too long,
        # and all lowercase letters, numbers, or - or .
        acceptable_chars = set(string.ascii_lowercase + string.digits + '-.')

        return ''.join([c for c in get_user_name().lower() if c in acceptable_chars])[:100]

    @runtime_checkable
    class KubernetesConfig(Protocol):
        """
        Type-enforcing protocol for Toil configs that have the extra Kubernetes
        batch system fields.

        TODO: Until MyPY lets protocols inherit form non-protocols, we will
        have to let the fact that this also has to be a Config just be manually
        enforced.
        """
        kubernetes_host_path: Optional[str]
        kubernetes_owner: str
        kubernetes_service_account: Optional[str]
        kubernetes_pod_timeout: float


    @classmethod
    def add_options(cls, parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
        parser.add_argument("--kubernetesHostPath", dest="kubernetes_host_path", default=None,
                            help="Path on Kubernetes hosts to use as shared inter-pod temp directory.  "
                                 "(default: %(default)s)")
        parser.add_argument("--kubernetesOwner", dest="kubernetes_owner", default=cls.get_default_kubernetes_owner(),
                            help="Username to mark Kubernetes jobs with.  "
                                 "(default: %(default)s)")
        parser.add_argument("--kubernetesServiceAccount", dest="kubernetes_service_account", default=None,
                            help="Service account to run jobs as.  "
                                 "(default: %(default)s)")
        parser.add_argument("--kubernetesPodTimeout", dest="kubernetes_pod_timeout", default=120,
                            help="Seconds to wait for a scheduled Kubernetes pod to start running.  "
                                 "(default: %(default)s)")

    OptionType = TypeVar('OptionType')
    @classmethod
    def setOptions(cls, setOption: OptionSetter) -> None:
        setOption("kubernetes_host_path", default=None, env=['TOIL_KUBERNETES_HOST_PATH'])
        setOption("kubernetes_owner", default=cls.get_default_kubernetes_owner(), env=['TOIL_KUBERNETES_OWNER'])
        setOption("kubernetes_service_account", default=None, env=['TOIL_KUBERNETES_SERVICE_ACCOUNT'])
        setOption("kubernetes_pod_timeout", default=120, env=['TOIL_KUBERNETES_POD_TIMEOUT'])

