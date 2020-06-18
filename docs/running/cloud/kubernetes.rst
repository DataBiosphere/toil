
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
   * `minikube <https://kubernetes.io/docs/tasks/tools/install-minikube/>`_

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

   In your AWS account, you need to create an AWS access key. First go to the IAM dashboard; for "us-west1", the link would be::

    https://console.aws.amazon.com/iam/home?region=us-west-1#/home

   Then create an access key, and save the Access Key ID and the Secret Key. As documented in `the AWS documentation <https://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html>`_:

   1. On the IAM Dashboard page, choose your account name in the navigation bar, and then choose My Security Credentials.
   2. Expand the Access keys (access key ID and secret access key) section.
   3. Choose Create New Access Key. Then choose Download Key File to save the access key ID and secret access key to a file on your computer. After you close the dialog box, you can't retrieve this secret access key again.

   Make sure that, if your AWS infrastructure requires your user to authenticate with a multi-factor authentication (MFA) token, you obtain a second secret key and access key that don't have this requirement. The secret key and access key used to populate the Kubernetes secret that allows the jobs to contact the job store need to be usable without human intervention.

#. **Configure AWS access from the local machine**

   This only really needs to happen if you run the leader on the local machine. But we need the files in place to fill in the secret in the next step.

#. **Create a Kubernetes secret to give jobs access to AWS**

Configuring Toil for your Kubernetes environment
------------------------------------------------

#. **Work out environment variable settings**

   Make sure to set up a host path volume if you want to use caching. This may not be possible on most cloud-provider-managed Kubernetes clusters.
   Make sure to use Sigularity and not Toil-integrated Docker for running containers, as Docker cannot be run inside unprivileged Kubernetes jobs.
   If using Singularity, make sure to put the image cache outside the container (in the host path volume) so that the container doesn't need to be re-downloaded by every job.
   
Running workflows
-----------------
   
#. **Launch the Toil workflow with a local leader**

#. **Alternately, run the Toil workflow as a Kubernetes job**

   



