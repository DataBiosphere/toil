# Mesos cluster with Toil

This Microsoft Azure template creates an Apache Mesos cluster with Toil on a configurable number of machines.

<a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FBD2KGenomics%2Ftoil%2Fmaster%2Fcontrib%2Fazure%2Fazuredeploy.json" target="_blank">
    <img src="http://azuredeploy.net/deploybutton.png"/>
</a>

Once your cluster has been created you will have a resource group containing 3 parts:

1. A set of 1 (default), 3, or 5 masters in a master specific availability set. Each master's SSH can be accessed via the public dns address at ports 2211..2215

2. A set of agents in an agent specific availability set. The agent VMs must be accessed through the master, or jumpbox

3. (Optional) a windows or linux jumpbox

The following image is an example of a cluster with 1 jumpbox, 3 masters, and 3 agents:

![Image of Mesos cluster on azure](images/mesos.png)

You can see the following parts:

1. **Mesos on port 5050** - Mesos is the distributed systems kernel that abstracts cpu, memory and other resources, and offers these to services named "frameworks" for scheduling of workloads. **Note that Mesos masters only listen on the 10.0.0.0/18 subnet**. In particular, **note that Mesos does not listen on localhost**. If you run a Toil job on the master, you will want to pass `--batchSystem mesos --mesosMaster 10.0.0.5:5050`.
2. **Docker on port 2375** - The Docker engine runs containerized workloads and each Master and Agent run the Docker engine. Mesos runs Docker workloads, and examples on how to do this are provided in the Marathon and Chronos walkthrough sections of this readme.
3. **(Optional) Marathon on port 8080** - Marathon is a scheduler for Mesos that is equivalent to init on a single linux machine: it schedules long running tasks for the whole cluster. The Swarm framework is disabled by default.
4. **(Optional) Chronos on port 4400** - Chronos is a scheduler for Mesos that is equivalent to cron on a single linux machine: it schedules periodic tasks for the whole cluster. The Swarm framework is disabled by default.
5. **(Optional) Swarm on port 2376** - Swarm is an experimental framework from Docker used for scheduling docker style workloads. The Swarm framework is disabled by default because it has [a showstopper bug where it grabs all the resources](https://github.com/docker/swarm/issues/1183). As a workaround, you will notice in the walkthrough below, you can run your Docker workloads in Marathon and Chronos.

All VMs are on the same private subnet, 10.0.0.0/18, and fully accessible to each other. The masters start at 10.0.0.5, and the agents at 10.0.0.50.

# Installation Notes

Here are notes for troubleshooting:
 * the installation log for the linux jumpbox, masters, and agents are in /var/log/azure/cluster-bootstrap.log
 * event though the VMs finish quickly Mesos can take 5-15 minutes to install, check /var/log/azure/cluster-bootstrap.log for the completion status.
 * the linux jumpbox is based on https://github.com/Azure/azure-quickstart-templates/tree/master/ubuntu-desktop and will take 1 hour to configure. Visit https://github.com/Azure/azure-quickstart-templates/tree/master/ubuntu-desktop to learn how to know when setup is completed, and then how to access the desktop via VNC and an SSH tunnel.

# Template Parameters
When you launch the installation of the cluster, you need to specify the following parameters:
* `newStorageAccountNamePrefix`: make sure this is a unique identifier. Azure Storage's accounts are global so make sure you use a prefix that is unique to your account otherwise there is a good change it will clash with names already in use.
* `adminUsername`: self-explanatory. This is the account used on all VMs in the cluster including the jumpbox
* `adminPassword`: self-explanatory
* `dnsNameForMastersPublicIP`: this is the public DNS name for the public IP that the masters sit behind. If you don't set up a jumpbox, you will probably SSH to port 2211 at this hostname. You just need to specify an unique name, the FQDN will be created by adding the necessary subdomains based on where the cluster is going to be created. For example, you might specify <userID>ToilCluster, and Azure will add something like .westus.cloudapp.azure.com to create the FQDN for the cluster.
* `dnsNameForJumpboxPublicIP`: this is the public DNS name for the jumpbox, a more full-featured host on the cluster network that can be used for debugging or a GUI. It is only consulted if a jumpbox is to be used, so if you aren't making one just fill it in with a dummy value.
* `agentCount`: the number of Mesos Agents that you want to create in the cluster
* `masterCount`: Number of Masters. Currently the template supports 3 configurations: 1, 3 and 5 Masters cluster configuration.
* `jumpboxConfiguration`: You can choose if you want a jumpbox, and if so, whether the jumpbox should be Windows or Linux.
* `masterConfiguration`: You can specify if you want Masters to be Agents as well. This is a Mesos supported configuration. By default, Masters will not be used to run workloads.
* `agentVMSize`: The type of VM that you want to use for each node in the cluster. The default size is A5 (2 cores, 14GB RAM) but you can change that (perhaps to one of the G-type instances) if you expect to run workloads that require more RAM or CPU resources.
* `masterVMSize`: size of the master machines; the default is A5 (2 cores, 14GB RAM)
* `jumpboxVMSize`: size of the jumpbox machine, if used; the default is A5 (2 cores, 14GB RAM)
* `clusterPrefix`: this is the prefix that will be used to create all VM names. You can use the prefix to easily identify the machines that belongs to a specific cluster. If, for instance, prefix is 'c1', machines will be created as c1master1, c1master2, ...c1agent1, c1agent5, ...
* `swarmEnabled`: true if you want to enable Swarm as a framework on the cluster
* `marathonEnabled`: true if you want to enable the Marathon framework on the cluster
* `chronosEnabled`: true if you want to enable the Chronos framework on the cluster
* `toilEnabled`: false if you want to disable the Toil framework on the cluster

# Questions
**Q.** Does this cluster have a shared filesystem? Can I use the `fileJobStore`? 

**A.** No. You should probably try out the `azureJobStore`.

**Q.** My tasks on the agents can't connect to the `azureJobStore`!

**A.** That's not a question. But, make sure either that you have distributed a `.toilAzureCredentials` file to each agent manually, or that you set the `AZURE_ACCOUNT_KEY` environment variable for your Toil master and are running a version of Toil that makes the master environment available at worker startup.

**Q.** How do I get to the Mesos web UI?

**A.** If you set up a jumpbox, run your browser on the juimpbox, and browse to http://10.0.0.5:5050/. If you did not set up a jumpbox, the easiest way is to use SSH port forwarding as a SOCKS proxy. From your local machine, run `ssh <youruser>@<yourcluster>.<yourzone>.cloudapp.azure.com -p 2211 -D8080`, and then set your browser to use the SOCKS proxy this creates at `localhost:8080` (perhaps with a proxy switcher extension). Then, browse to http://10.0.0.5:5050/. Make sure to turn off the proxy when you close your SSH session, or your browser won't be able to load any pages. Also note that this routes all your web traffic through Azure.

**Q.** How do I get to the slave pages in the Mesos web UI?

**A.** You're probably using the proxy method above. Make sure your browser is doing DNS through the proxy. The Mesos web UI expects to be able to resolve names like `yourclusteragent1`, which you probably can't do on your machine. In Firefox, you need to set `network.proxy.socks_remote_dns` to `true` in `about:config`.

**Q.** My cluster just completed but Mesos is not up/has no slaves!

**A.** That's not a question either. But after your template finishes, your cluster is still running installation. That installation might fail, if bugs still exist in the setup code or if needed Internet resources disappear. You can run "tail -f /var/log/azure/cluster-bootstrap.log" on each master or agent to see how the installation is going and what might have gone wrong.
