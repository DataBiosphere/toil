
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
  
  Then, create a Kubernetes secret that contains it. We'll call it ``s3-credentials``: ::
  
     $ kubectl create secret generic s3-credentials --from-file credentials

Configuring Toil for your Kubernetes environment
------------------------------------------------

#. **Work out environment variable settings**

   There are several environment variables that are important for running Toil on Kubernetes. Doing the research to figure out what their values should be may require talking to your cluster provider.
   
   #. ``TOIL_AWS_SECRET_NAME`` is the most important, and **MUST** be set to the secret that contains your AWS ``credentials`` file, if your cluster nodes don't otherwise have access to S3 and SimpleDB (such as through IAM roles), if you want the AWS job store to work. In this example we are using ``s3-credentials``.
   
   #. ``TOIL_KUBERNETES_HOST_PATH`` can be set to allow Toil jobs on the same physical host to share a cache. It should be set to a path on the host where the shared cache should be stored. It will be mounted as ``/var/lib/toil`` inside the container. This path must already exist on the host, and must have as much free space as your Kubernetes node offers to jobs. In this example, we are using ``/data/scratch``. To actually make use of caching, make sure to also pass ``--disableCaching false`` to your Toil workflow.
   
   #. ``TOIL_KUBERNETES_OWNER`` **should** be set to the user name of the user running the Toil workflow. The jobs that Toil creates will include this user name, so they can be more easily recognized, and cleaned up by the user if anything happens to the Toil leader. In this example we are using ``demouser``.
   
   Note that Docker containers cannot be run inside of unprivileged Kubernetes pods (which are themselves containers). The Docker daemon does not (yet) support this. Other tools, such as Singularity, are able to run containers from within containers. If using Singularity to run containerized tools, you will also want to make sure that Singularity is downloading its containers under ``/var/lib/toil`` somewhere by setting ``SINGULARITY_CACHEDIR``. But you will need to make sure that no two jobs try to download the same container at the same time; Singularity has no synchronization or locking around its cache, but the cache is also not safe for simultaneous access by multiple Singularity invocations. Some Toil workflows use their own custom workaround logic for this problem; this work is likely to be made part of Toil in a future release.
   
  
   
Running workflows
-----------------
   
#. **Launch the Toil workflow with a local leader**

#. **Alternately, run the Toil workflow as a Kubernetes job**

   Here's an example YAML: ::

    apiVersion: batch/v1
    kind: Job
    metadata:
      name: demouser-toil-test
    spec:
      template:
        spec:
          containers:
          - name: main
            image: quay.io/ucsc_cgl/toil:4.1.0
            command:
            - /bin/bash
            - -c
            - |
              set -e
              mkdir /tmp/work
              cd /tmp/work
              wget https://raw.githubusercontent.com/DataBiosphere/toil/releases/4.1.0/src/toil/test/docs/scripts/tutorial_helloworld.py
              python3 tutorial_helloworld.py \
                  aws:us-west-2:demouser-toil-test-jobstore \
                  --logInfo \
                  --batchSystem kubernetes \
                  --disableCaching false
            volumeMounts:
            - mountPath: /root/.aws
              name: s3-credentials
            resources:
              limits:
                cpu: 2
                memory: "4Gi"
                ephemeral-storage: "10Gi"
            env:
            - name: TOIL_KUBERNETES_OWNER
              value: demouser
            - name: TOIL_AWS_SECRET_NAME
              value: s3-credentials
            - name: TOIL_KUBERNETES_HOST_PATH
              value: /data/scratch
          restartPolicy: Never
          volumes:
          - name: scratch-volume
            emptyDir: {}
          - name: s3-credentials
            secret:
              secretName: shared-s3-credentials
          serviceAccountName: toil-workflow-svc
      backoffLimit: 0
    EOF

   



