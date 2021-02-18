
.. _runningKubernetes:

Running on Kubernetes
=====================

`Kubernetes <https://kubernetes.io/>`_ is a very popular container orchestration tool that has become a *de facto* cross-cloud-provider API for accessing cloud resources. Major cloud providers like `Amazon <https://aws.amazon.com/kubernetes/>`_, `Microsoft <https://azure.microsoft.com/en-us/overview/kubernetes-getting-started/>`_, Kubernetes owner `Google <https://cloud.google.com/kubernetes-engine/>`_, and `DigitalOcean <https://www.digitalocean.com/products/kubernetes/>`_ have invested heavily in making Kubernetes work well on their platforms, by writing their own deployment documentation and developing provider-managed Kubernetes-based products. Using `minikube <https://github.com/kubernetes/minikube>`_, Kubernetes can even be run on a single machine.

Toil supports running Toil workflows against a Kubernetes cluster, either in the cloud or deployed on user-owned hardware.

.. _prepareKubernetes:

Preparing your Kubernetes environment
-------------------------------------

#. **Get a Kubernetes cluster**

   To run Toil workflows on Kubernetes, you need to have a Kubernetes cluster set up. This will not be covered here, but there are many options available, and which one you choose will depend on which cloud ecosystem if any you use already, and on pricing. If you are just following along with the documentation, use ``minikube`` on your local machine.

   **Note that currently the only way to run a Toil workflow on Kubernetes is to use the AWS Job Store, so your Kubernetes workflow will currently have to store its data in Amazon's cloud regardless of where you run it. This can result in significant egress charges from Amazon if you run it outside of Amazon.**

   Kubernetes Cluster Providers:

   * Your own institution
   * `Amazon EKS <https://aws.amazon.com/eks/>`_
   * `Microsoft Azure AKS <https://docs.microsoft.com/en-us/azure/aks/>`_
   * `Google GKE <https://cloud.google.com/kubernetes-engine/>`_
   * `DigitalOcean Kubernetes <https://www.digitalocean.com/docs/kubernetes/>`_
   * `minikube <https://kubernetes.io/docs/tasks/tools/install-minikube/>`__

#. **Get a Kubernetes context on your local machine**

   There are two main ways to run Toil workflows on Kubernetes. You can either run the Toil leader on a machine outside the cluster, with jobs submitted to and run on the cluster, or you can submit the Toil leader itself as a job and have it run inside the cluster. Either way, you will need to configure your own machine to be able to submit jobs to the Kubernetes cluster. Generally, this involves creating and populating a file named ``.kube/config`` in your user's home directory, and specifying the cluster to connect to, the certificate and token information needed for mutual authentication, and the Kubernetes namespace within which to work. However, Kubernetes configuration can also be picked up from other files in the ``.kube`` directory, environment variables, and the enclosing host when running inside a Kubernetes-managed container.

   You will have to do different things here depending on where you got your Kubernetes cluster:

   * `Configuring for Amazon EKS <https://docs.aws.amazon.com/eks/latest/userguide/create-kubeconfig.html>`_
   * `Configuring for Microsoft Azure AKS <https://docs.microsoft.com/en-us/cli/azure/aks?view=azure-cli-latest#az-aks-get-credentials>`_
   * `Configuring for Google GKE <https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl>`_
   * `Configuring for DigitalOcean Kubernetes Clusters <https://www.digitalocean.com/docs/kubernetes/how-to/connect-to-cluster/>`_
   * `Configuring for minikube <https://kubernetes.io/docs/setup/learning-environment/minikube/#kubectl>`_

   Toil's internal Kubernetes configuration logic mirrors that of the ``kubectl`` command. Toil workflows will use the current ``kubectl`` context to launch their Kubernetes jobs.

#. **If running the Toil leader in the cluster, get a service account**

   If you are going to run your workflow's leader within the Kubernetes cluster (see :ref:`kubernetesLeaderInside`), you will need a service account in your chosen Kubernetes namespace. Most namespaces should have a service account named ``default`` which should work fine. If your cluster requires you to use a different service account, you will need to obtain its name and use it when launching the Kubernetes job containing the Toil leader.

#. **Set up appropriate permissions**

   Your local Kubernetes context and/or the service account you are using to run the leader in the cluster will need to have certain permissions in order to run the workflow. Toil needs to be able to interact with jobs and pods in the cluster, and to retrieve pod logs. You as a user may need permission to set up an AWS credentials secret, if one is not already available. Additionally, it is very useful for you as a user to have permission to interact with nodes, and to shell into pods.

   The appropriate permissions may already be available to you and your service account by default, especially in managed or ease-of-use-optimized setups such as EKS or minikube.

   However, if the appropriate permissions are not already available, you or your cluster administrator will have to grant them manually. The following ``Role`` (``toil-user``) and ``ClusterRole`` (``node-reader``), to be applied with ``kubectl apply -f filename.yaml``, should grant sufficient permissions to run Toil workflows when bound to your account and the service account used by Toil workflows. Be sure to replace ``YOUR_NAMESPACE_HERE`` with the namespace you are running your workflows in ::

      apiVersion: rbac.authorization.k8s.io/v1
      kind: Role
      metadata:
        namespace: YOUR_NAMESPACE_HERE
        name: toil-user
      rules:
      - apiGroups: ["*"]
        resources: ["*"]
        verbs: ["explain", "get", "watch", "list", "describe", "logs", "attach", "exec", "port-forward", "proxy", "cp", "auth"]
      - apiGroups: ["batch"]
        resources: ["*"]
        verbs: ["get", "watch", "list", "create", "run", "set", "delete"]
      - apiGroups: [""]
        resources: ["secrets", "pods", "pods/attach", "podtemplates", "configmaps", "events", "services"]
        verbs: ["patch", "get", "update", "watch", "list", "create", "run", "set", "delete", "exec"]
      - apiGroups: [""]
        resources: ["pods", "pods/log"]
        verbs: ["get", "list"]
      - apiGroups: [""]
        resources: ["pods/exec"]
        verbs: ["create"]

   ::

      apiVersion: rbac.authorization.k8s.io/v1
      kind: ClusterRole
      metadata:
        name: node-reader
      rules:
      - apiGroups: [""]
        resources: ["nodes"]
        verbs: ["get", "list", "describe"]
      - apiGroups: [""]
        resources: ["namespaces"]
        verbs: ["get", "list", "describe"]
      - apiGroups: ["metrics.k8s.io"]
        resources: ["*"]
        verbs: ["*"]

   To bind a user or service account to the ``Role`` or ``ClusterRole`` and actually grant the permissions, you will need a ``RoleBinding`` and a ``ClusterRoleBinding``, respectively. Make sure to fill in the namespace, username, and service account name, and add more user stanzas if your cluster is to support multiple Toil users.

   ::

      apiVersion: rbac.authorization.k8s.io/v1
      kind: RoleBinding
      metadata:
        name: toil-developer-member
        namespace: toil
      subjects:
      - kind: User
        name: YOUR_KUBERNETES_USERNAME_HERE
        apiGroup: rbac.authorization.k8s.io
      - kind: ServiceAccount
        name: YOUR_SERVICE_ACCOUNT_NAME_HERE
        namespace: YOUR_NAMESPACE_HERE
      roleRef:
        kind: Role
        name: toil-user
        apiGroup: rbac.authorization.k8s.io

   ::

      apiVersion: rbac.authorization.k8s.io/v1
      kind: ClusterRoleBinding
      metadata:
        name: read-nodes
      subjects:
      - kind: User
        name: YOUR_KUBERNETES_USERNAME_HERE
        apiGroup: rbac.authorization.k8s.io
      - kind: ServiceAccount
        name: YOUR_SERVICE_ACCOUNT_NAME_HERE
        namespace: YOUR_NAMESPACE_HERE
      roleRef:
        kind: ClusterRole
        name: node-reader
        apiGroup: rbac.authorization.k8s.io

.. _awsJobStoreForKubernetes:

AWS Job Store for Kubernetes
----------------------------

Currently, the only job store, which is what Toil uses to exchange data between jobs, that works with jobs running on Kubernetes is the AWS Job Store. This requires that the Toil leader and Kubernetes jobs be able to connect to and use Amazon S3 and Amazon SimpleDB. It also requires that you have an Amazon Web Services account.

#. **Get access to AWS S3 and SimpleDB**

   In your AWS account, you need to create an AWS access key. First go to the IAM dashboard; for "us-west1", the link would be: ::

    https://console.aws.amazon.com/iam/home?region=us-west-1#/home

   Then create an access key, and save the Access Key ID and the Secret Key. As documented in `the AWS documentation <https://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html>`_:

   1. On the IAM Dashboard page, choose your account name in the navigation bar, and then choose My Security Credentials.
   2. Expand the Access keys (access key ID and secret access key) section.
   3. Choose Create New Access Key. Then choose Download Key File to save the access key ID and secret access key to a file on your computer. After you close the dialog box, you can't retrieve this secret access key again.

   Make sure that, if your AWS infrastructure requires your user to authenticate with a multi-factor authentication (MFA) token, you obtain a second secret key and access key that don't have this requirement. The secret key and access key used to populate the Kubernetes secret that allows the jobs to contact the job store need to be usable without human intervention.

#. **Configure AWS access from the local machine**

   This only really needs to happen if you run the leader on the local machine. But we need the files in place to fill in the secret in the next step. Run: ::

      $ aws configure

   Then when prompted, enter your secret key and access key. This should create a file ``~/.aws/credentials`` that looks like this: ::

      [default]
      aws_access_key_id =  BLAH
      aws_secret_access_key =  blahblahblah

#. **Create a Kubernetes secret to give jobs access to AWS**

  Go into the directory where the ``credentials`` file is: ::

     $ cd ~/.aws

  Then, create a Kubernetes secret that contains it. We'll call it ``aws-credentials``: ::

     $ kubectl create secret generic aws-credentials --from-file credentials

Configuring Toil for your Kubernetes environment
------------------------------------------------

To configure your workflow to run on Kubernetes, you will have to configure several environment variables, in addition to passing the ``--batchSystem kubernetes`` option. Doing the research to figure out what values to give these variables may require talking to your cluster provider.

#. ``TOIL_AWS_SECRET_NAME`` is the most important, and **must** be set to the secret that contains your AWS ``credentials`` file, **if** your cluster nodes don't otherwise have access to S3 and SimpleDB (such as through IAM roles). This is required for the AWS job store to work, which is currently the only job store that can be used on Kubernetes. In this example we are using ``aws-credentials``.

#. ``TOIL_KUBERNETES_HOST_PATH`` **can** be set to allow Toil jobs on the same physical host to share a cache. It should be set to a path on the host where the shared cache should be stored. It will be mounted as ``/var/lib/toil``, or at ``TOIL_WORKDIR`` if specified, inside the container. This path must already exist on the host, and must have as much free space as your Kubernetes node offers to jobs. In this example, we are using ``/data/scratch``. To actually make use of caching, make sure to also pass ``--disableCaching false`` to your Toil workflow.

#. ``TOIL_KUBERNETES_OWNER`` **should** be set to the username of the user running the Toil workflow. The jobs that Toil creates will include this username, so they can be more easily recognized, and cleaned up by the user if anything happens to the Toil leader. In this example we are using ``demo-user``.

Note that Docker containers cannot be run inside of unprivileged Kubernetes pods (which are themselves containers). The Docker daemon does not (yet) support this. Other tools, such as Singularity in its user-namespace mode, are able to run containers from within containers. If using Singularity to run containerized tools, and you want downloaded container images to persist between Toil jobs, you will also want to set ``TOIL_KUBERNETES_HOST_PATH`` and make sure that Singularity is downloading its containers under the Toil work directory (``/var/lib/toil`` buy default) by setting ``SINGULARITY_CACHEDIR``. However, you will need to make sure that no two jobs try to download the same container at the same time; Singularity has no synchronization or locking around its cache, but the cache is also not safe for simultaneous access by multiple Singularity invocations. Some Toil workflows use their own custom workaround logic for this problem; this work is likely to be made part of Toil in a future release.

Running workflows
-----------------

To run the workflow, you will need to run the Toil leader process somewhere. It can either be run inside Kubernetes as a Kubernetes job, or outside Kubernetes as a normal command.

.. _kubernetesLeaderInside:

Option 1: Running the Leader Inside Kubernetes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once you have determined a set of environment variable values for your workflow run, write a YAML file that defines a Kubernetes job to run your workflow with that configuration. Some configuration items (such as your username, and the name of your AWS credentials secret) need to be written into the YAML so that they can be used from the leader as well.

Note that the leader pod will need your workflow script, its other dependencies, and Toil all installed. An easy way to get Toil installed is to start with the Toil appliance image for the version of Toil you want to use. In this example, we use ``quay.io/ucsc_cgl/toil:4.1.0``.

Here's an example YAML file to run a test workflow: ::

   apiVersion: batch/v1
   kind: Job
   metadata:
     # It is good practice to include your username in your job name.
     # Also specify it in TOIL_KUBERNETES_OWNER
     name: demo-user-toil-test
   # Do not try and rerun the leader job if it fails

   spec:
    backoffLimit: 0
    template:
      spec:
        # Do not restart the pod when the job fails, but keep it around so the
        # log can be retrieved
        restartPolicy: Never
        volumes:
        - name: aws-credentials-vol
          secret:
            # Make sure the AWS credentials are available as a volume.
            # This should match TOIL_AWS_SECRET_NAME
            secretName: aws-credentials
        # You may need to replace this with a different service account name as
        # appropriate for your cluster.
        serviceAccountName: default
        containers:
        - name: main
          image: quay.io/ucsc_cgl/toil:4.1.0
          env:
          # Specify your username for inclusion in job names
          - name: TOIL_KUBERNETES_OWNER
            value: demo-user
          # Specify where to find the AWS credentials to access the job store with
          - name: TOIL_AWS_SECRET_NAME
            value: aws-credentials
          # Specify where per-host caches should be stored, on the Kubernetes hosts.
          # Needs to be set for Toil's caching to be efficient.
          - name: TOIL_KUBERNETES_HOST_PATH
            value: /data/scratch
          volumeMounts:
          # Mount the AWS credentials volume
          - mountPath: /root/.aws
            name: aws-credentials-vol
          resources:
            # Make sure to set these resource limits to values large enough
            # to accomodate the work your workflow does in the leader
            # process, but small enough to fit on your cluster.
            #
            # Since no request values are specified, the limits are also used
            # for the requests.
            limits:
              cpu: 2
              memory: "4Gi"
              ephemeral-storage: "10Gi"
          command:
          - /bin/bash
          - -c
          - |
            # This Bash script will set up Toil and the workflow to run, and run them.
            set -e
            # We make sure to create a work directory; Toil can't hot-deploy a
            # script from the root of the filesystem, which is where we start.
            mkdir /tmp/work
            cd /tmp/work
            # We make a virtual environment to allow workflow dependencies to be
            # hot-deployed.
            #
            # We don't really make use of it in this example, but for workflows
            # that depend on PyPI packages we will need this.
            #
            # We use --system-site-packages so that the Toil installed in the
            # appliance image is still available.
            virtualenv --python python3 --system-site-packages venv
            . venv/bin/activate
            # Now we install the workflow. Here we're using a demo workflow
            # script from Toil itself.
            wget https://raw.githubusercontent.com/DataBiosphere/toil/releases/4.1.0/src/toil/test/docs/scripts/tutorial_helloworld.py
            # Now we run the workflow. We make sure to use the Kubernetes batch
            # system and an AWS job store, and we set some generally useful
            # logging options. We also make sure to enable caching.
            python3 tutorial_helloworld.py \
                aws:us-west-2:demouser-toil-test-jobstore \
                --batchSystem kubernetes \
                --realTimeLogging \
                --logInfo \
                --disableCaching false

You can save this YAML as ``leader.yaml``, and then run it on your Kubernetes installation with: ::

   $ kubectl apply -f leader.yaml

To monitor the progress of the leader job, you will want to read its logs. If you are using a Kubernetes dashboard such as `k9s <https://github.com/derailed/k9s>`_, you can simply find the pod created for the job in the dashboard, and view its logs there. If not, you will need to locate the pod by hand.

.. _debugKubeJob:

Monitoring and Debugging Kubernetes Jobs and Pods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following techniques are most useful for looking at the pod which holds the Toil leader, but they can also be applied to individual Toil jobs on Kubernetes, even when the leader is outside the cluster.

Kubernetes names pods for jobs by appending a short random string to the name of the job. You can find the name of the pod for your job by doing: ::

   $ kubectl get pods | grep demo-user-toil-test
   demo-user-toil-test-g5496                                         1/1     Running     0          2m

Assuming you have set ``TOIL_KUBERNETES_OWNER`` correctly, you should be able to find all of your workflow's pods by searching for your username: ::

   $ kubectl get pods | grep demo-user

If the status of a pod is anything other than ``Pending``, you will be able to view its logs with: ::

   $ kubectl logs demo-user-toil-test-g5496

This will dump the pod's logs from the beginning to now and terminate. To follow along with the logs from a running pod, add the ``-f`` option: ::

   $ kubectl logs -f demo-user-toil-test-g5496

A status of ``ImagePullBackoff`` suggests that you have requested to use an image that is not available. Check the ``image`` section of your YAML if you are looking at a leader, or the value of ``TOIL_APPLIANCE_SELF`` if you are delaying with a worker job. You also might want to check your Kubernetes node's Internet connectivity and DNS function; in Kubernetes, DNS depends on system-level pods which can be terminated or evicted in cases of resource oversubscription, just like user workloads.

If your pod seems to be stuck ``Pending``, ``ContainerCreating``, you can get information on what is wrong with it by using ``kubectl describe pod``: ::

   $ kubectl describe pod demo-user-toil-test-g5496

Pay particular attention to the ``Events:`` section at the end of the output. An indication that a job is too big for the available nodes on your cluster, or that your cluster is too busy for your jobs, is ``FailedScheduling`` events: ::

  Type     Reason            Age                  From               Message
  ----     ------            ----                 ----               -------
  Warning  FailedScheduling  13s (x79 over 100m)  default-scheduler  0/4 nodes are available: 1 Insufficient cpu, 1 Insufficient ephemeral-storage, 4 Insufficient memory.

If a pod is running but seems to be behaving erratically, or seems stuck, you can shell into it and look around: ::

   $ kubectl exec -ti demo-user-toil-test-g5496 /bin/bash

One common cause of stuck pods is attempting to use more memory than allowed by Kubernetes (or by the Toil job's memory resource requirement), but in a way that does not trigger the Linux OOM killer to terminate the pod's processes. In these cases, the pod can remain stuck at nearly 100% memory usage more or less indefinitely, and attempting to shell into the pod (which needs to start a process within the pod, using some of its memory) will fail. In these cases, the recommended solution is to kill the offending pod and increase its (or its Toil job's) memory requirement, or reduce its memory needs by adapting user code.

When Things Go Wrong
^^^^^^^^^^^^^^^^^^^^

The Toil Kubernetes batch system includes cleanup code to terminate worker jobs when the leader shuts down. However, if the leader pod is removed by Kubernetes, is forcibly killed or otherwise suffers a sudden existence failure, it can go away while its worker jobs live on. It is not recommended to restart a workflow in this state, as jobs from the previous invocation will remain running and will be trying to modify the job store concurrently with jobs from the new invocation.

To clean up dangling jobs, you can use the following snippet: ::

   $ kubectl get jobs | grep demo-user | cut -f1 -d' ' | xargs -n10 kubectl delete job

This will delete all jobs with ``demo-user``'s username in their names, in batches of 10. You can also use the UUID that Toil assigns to a particular workflow invocation in the filter, to clean up only the jobs pertaining to that workflow invocation.

Option 2: Running the Leader Outside Kubernetes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you don't want to run your Toil leader inside Kubernetes, you can run it locally instead. This can be useful when developing a workflow; files can be hot-deployed from your local machine directly to Kubernetes. However, your local machine will have to have (ideally role-assumption- and MFA-free) access to AWS, and access to Kubernetes. Real time logging will not work unless your local machine is able to listen for incoming UDP packets on arbitrary ports on the address it uses to contact the IPv4 Internet; Toil does no NAT traversal or detection.

Note that if you set ``TOIL_WORKDIR`` when running your workflow like this, it will need to be a directory that exists both on the host and in the Toil appliance.

Here is an example of running our test workflow leader locally, outside of Kubernetes: ::

   $ export TOIL_KUBERNETES_OWNER=demo-user  # This defaults to your local username if not set
   $ export TOIL_AWS_SECRET_NAME=aws-credentials
   $ export TOIL_KUBERNETES_HOST_PATH=/data/scratch
   $ virtualenv --python python3 --system-site-packages venv
   $ . venv/bin/activate
   $ wget https://raw.githubusercontent.com/DataBiosphere/toil/releases/4.1.0/src/toil/test/docs/scripts/tutorial_helloworld.py
   $ python3 tutorial_helloworld.py \
         aws:us-west-2:demouser-toil-test-jobstore \
         --batchSystem kubernetes \
         --realTimeLogging \
         --logInfo \
         --disableCaching false







