#!/bin/bash

###########################################################
# Configure Mesos One Box
#
# This installs the following components
# - zookeepr
# - mesos master
# - marathon
# - mesos agent
# - swarm
# - chronos
# - toil
###########################################################

set -x

echo "starting mesos cluster configuration"
date
ps ax

#############
# Parameters
#############

MASTERCOUNT=$1
MASTERMODE=$2
MASTERPREFIX=$3
SWARMENABLED=$4
MARATHONENABLED=$5
CHRONOSENABLED=$6
TOILENABLED=$7
ACCOUNTNAME=$8
set +x
ACCOUNTKEY=$9
set -x
AZUREUSER=${10}
SSHKEY=${11}
GITHUB_SOURCE=${12}
GITHUB_BRANCH=${13}
PYTHON_PACKAGES="${14}"
HOMEDIR="/home/$AZUREUSER"
VMNAME=`hostname`
VMNUMBER=`echo $VMNAME | sed 's/.*[^0-9]\([0-9]\+\)*$/\1/'`
VMPREFIX=`echo $VMNAME | sed 's/\(.*[^0-9]\)*[0-9]\+$/\1/'`

# TODO: make this configurable?
TARGET_MESOS_VERSION="0.23.0"
BINDINGS_MESOS_VERSION="0.22.0"

echo "Master Count: $MASTERCOUNT"
echo "Master Mode: $MASTERMODE"
echo "Master Prefix: $MASTERPREFIX"
echo "vmname: $VMNAME"
echo "VMNUMBER: $VMNUMBER, VMPREFIX: $VMPREFIX"
echo "SWARMENABLED: $SWARMENABLED, MARATHONENABLED: $MARATHONENABLED, CHRONOSENABLED: $CHRONOSENABLED, TOILENABLED: $TOILENABLED"
echo "ACCOUNTNAME: $ACCOUNTNAME"
echo "TARGET_MESOS_VERSION: $TARGET_MESOS_VERSION"

###################
# setup ssh access
###################

SSHDIR=$HOMEDIR/.ssh
AUTHFILE=$SSHDIR/authorized_keys
if [ `echo $SSHKEY | sed 's/^\(ssh-rsa \).*/\1/'` == "ssh-rsa" ] ; then
  if [ ! -d $SSHDIR ] ; then
    sudo -i -u $AZUREUSER mkdir $SSHDIR
    sudo -i -u $AZUREUSER chmod 700 $SSHDIR
  fi

  if [ ! -e $AUTHFILE ] ; then
    sudo -i -u $AZUREUSER touch $AUTHFILE
    sudo -i -u $AZUREUSER chmod 600 $AUTHFILE
  fi
  echo $SSHKEY | sudo -i -u $AZUREUSER tee -a $AUTHFILE
else
  echo "no valid key data"
fi

###################
# Common Functions
###################

ismaster ()
{
  if [ "$MASTERPREFIX" == "$VMPREFIX" ]
  then
    return 0
  else
    return 1
  fi
}
if ismaster ; then
  echo "this node is a master"
fi

isagent()
{
  if ismaster ; then
    if [ "$MASTERMODE" == "masters-are-agents" ]
    then
      return 0
    else
      return 1
    fi
  else
    return 0
  fi
}
if isagent ; then
  echo "this node is an agent"
fi

zkhosts()
{
  zkhosts=""
  for i in `seq 1 $MASTERCOUNT` ;
  do
    if [ "$i" -gt "1" ]
    then
      zkhosts="${zkhosts},"
    fi

    IPADDR=`getent hosts ${MASTERPREFIX}${i} | awk '{ print $1 }'`
    zkhosts="${zkhosts}${IPADDR}:2181"
    # due to mesos team experience ip addresses are chosen over dns names
    #zkhosts="${zkhosts}${MASTERPREFIX}${i}:2181"
  done
  echo $zkhosts
}

zkconfig()
{
  postfix="$1"
  zkhosts=$(zkhosts)
  zkconfigstr="zk://${zkhosts}/${postfix}"
  echo $zkconfigstr
}

################
# Install Docker
################

echo "Installing and configuring docker and swarm"

for DOCKER_TRY in {1..10}
do

    # Remove the config file that will mess up package installation (by
    # prompting for overwrite in a weird subshell)
    sudo rm -f /etc/default/docker

    # Try installing docker
    time wget -qO- https://get.docker.com | sh

    # Start Docker and listen on :2375 (no auth, but in vnet)
    echo 'DOCKER_OPTS="-H unix:///var/run/docker.sock -H 0.0.0.0:2375"' | sudo tee /etc/default/docker
    # the following insecure registry is for OMS
    echo 'DOCKER_OPTS="$DOCKER_OPTS --insecure-registry 137.135.93.9"' | sudo tee -a /etc/default/docker
    sudo service docker restart

    ensureDocker()
    {
      # ensure that docker is healthy
      dockerHealthy=1
      for i in {1..3}; do
        sudo docker info
        if [ $? -eq 0 ]
        then
          # hostname has been found continue
          dockerHealthy=0
          echo "Docker is healthy"
          sudo docker ps -a
          break
        fi
        sleep 10
      done
      if [ $dockerHealthy -ne 0 ]
      then
        echo "Docker is not healthy"
      fi
    }
    ensureDocker
    
    if [ "$dockerHealthy" == "0" ]
    then
        # Contrary to what you might expect, a 0 here means docker is working
        # properly. Break out of the loop.
        echo "Installed docker successfully."
        break
    fi
    
    echo "Retrying docker install after a bit."
    sleep 120
    
done

if [ "$dockerHealthy" == "1" ]
then
    echo "WARNING: Docker could not be installed! Continuing anyway!"
fi

# Authorize the normal user to use Docker
sudo usermod -aG docker $AZUREUSER

############
# setup OMS
############

if [ $ACCOUNTNAME != "none" ]
then
  set +x
  EPSTRING="DefaultEndpointsProtocol=https;AccountName=${ACCOUNTNAME};AccountKey=${ACCOUNTKEY}"
  docker run --restart=always -d 137.135.93.9/msdockeragentv3 http://${VMNAME}:2375 "${EPSTRING}"
  set -x
fi

##################
# Install Mesos
##################

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
CODENAME=$(lsb_release -cs)
echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | sudo tee /etc/apt/sources.list.d/mesosphere.list
# Mesos needs Marathon which asks for Oracle Java 8; we trick it with OpenJDK
time sudo add-apt-repository -y ppa:openjdk-r/ppa
time sudo apt-get -y update

# Actually install that Java
time sudo apt-get -y install openjdk-8-jre-headless

# Fix Mesos version to one that actually works with Toil
# We need to know the -ubuntuWhatever on the end of the package version we want.
FULL_MESOS_VERSION=`apt-cache policy mesos | grep "${TARGET_MESOS_VERSION}" | cut -d" " -f6`
sudo apt-get -y --force-yes install mesos=${FULL_MESOS_VERSION}
# Don't update it
sudo apt-mark hold mesos

if ismaster ; then
  # Masters also need some version of Mesosphere
  time sudo apt-get -y --force-yes install mesosphere
fi

#########################
# Configure ZooKeeper
#########################

zkmesosconfig=$(zkconfig "mesos")
echo $zkmesosconfig | sudo tee /etc/mesos/zk

if ismaster ; then
  echo $VMNUMBER | sudo tee /etc/zookeeper/conf/myid
  for i in `seq 1 $MASTERCOUNT` ;
  do
    IPADDR=`getent hosts ${MASTERPREFIX}${i} | awk '{ print $1 }'`
    echo "server.${i}=${IPADDR}:2888:3888" | sudo tee -a /etc/zookeeper/conf/zoo.cfg
    # due to mesos team experience ip addresses are chosen over dns names
    #echo "server.${i}=${MASTERPREFIX}${i}:2888:3888" | sudo tee -a /etc/zookeeper/conf/zoo.cfg
  done
fi

#########################################
# Configure Mesos Master and Frameworks
#########################################
if ismaster ; then
  quorum=`expr $MASTERCOUNT / 2 + 1`
  echo $quorum | sudo tee /etc/mesos-master/quorum
  hostname -I | sed 's/ /\n/' | grep "^10." | sudo tee /etc/mesos-master/ip
  hostname | sudo tee /etc/mesos-master/hostname
  echo 'Mesos Cluster on Microsoft Azure' | sudo tee /etc/mesos-master/cluster
fi

if ismaster  && [ "$MARATHONENABLED" == "true" ] ; then
  # setup marathon
  sudo mkdir -p /etc/marathon/conf
  sudo cp /etc/mesos-master/hostname /etc/marathon/conf
  sudo cp /etc/mesos/zk /etc/marathon/conf/master
  zkmarathonconfig=$(zkconfig "marathon")
  echo $zkmarathonconfig | sudo tee /etc/marathon/conf/zk
fi

#########################################
# Configure Mesos Master and Frameworks
#########################################
if ismaster ; then
  # Download and install mesos-dns
  sudo mkdir -p /usr/local/mesos-dns
  sudo wget https://github.com/mesosphere/mesos-dns/releases/download/v0.2.0/mesos-dns-v0.2.0-linux-amd64.tgz
  sudo tar zxvf mesos-dns-v0.2.0-linux-amd64.tgz
  sudo mv mesos-dns-v0.2.0-linux-amd64 /usr/local/mesos-dns/mesos-dns

  echo "
{
  \"zk\": \"zk://127.0.0.1:2181/mesos\",
  \"refreshSeconds\": 1,
  \"ttl\": 0,
  \"domain\": \"mesos\",
  \"port\": 53,
  \"timeout\": 1,
  \"listener\": \"0.0.0.0\",
  \"email\": \"root.mesos-dns.mesos\",
  \"externalon\": false
}
" > mesos-dns.json
  sudo mv mesos-dns.json /usr/local/mesos-dns/mesos-dns.json

  echo "
description \"mesos dns\"

# Start just after the System-V jobs (rc) to ensure networking and zookeeper
# are started. This is as simple as possible to ensure compatibility with
# Ubuntu, Debian, CentOS, and RHEL distros. See:
# http://upstart.ubuntu.com/cookbook/#standard-idioms
start on stopped rc RUNLEVEL=[2345]
respawn

exec /usr/local/mesos-dns/mesos-dns -config /usr/local/mesos-dns/mesos-dns.json" > mesos-dns.conf
  sudo mv mesos-dns.conf /etc/init
  sudo service mesos-dns start
fi


#########################
# Configure Mesos Agent
#########################
if isagent ; then
  # Add docker containerizer
  echo "docker,mesos" | sudo tee /etc/mesos-slave/containerizers
  # Add resources configuration
  if ismaster ; then
    echo "ports:[1-21,23-4399,4401-5049,5052-8079,8081-32000]" | sudo tee /etc/mesos-slave/resources
  else
    echo "ports:[1-21,23-5050,5052-32000]" | sudo tee /etc/mesos-slave/resources
  fi
  # Our hostname may not resolve yet, so we look at our IPs and find the 10.
  # address instead
  hostname -I | sed 's/ /\n/' | grep "^10." | sudo tee /etc/mesos-slave/ip
  hostname | sudo tee /etc/mesos-slave/hostname
  
  # Mark the node as non-preemptable so Toil won't complain that it doesn't know
  # whether the node is preemptable or not
  echo "preemptable:False" | sudo tee /etc/mesos-slave/attributes

  # Set up the Mesos salve work directory in the ephemeral /mnt
  echo "/mnt" | sudo tee /etc/mesos-slave/work_dir
    
  # Set the root reserved fraction of that device to 0 to work around
  # <https://github.com/BD2KGenomics/toil/issues/1650> and
  # <https://issues.apache.org/jira/browse/MESOS-7420>
  sudo tune2fs -m 0 `findmnt --target /mnt -n -o SOURCE`

  # Add mesos-dns IP addresses at the top of resolv.conf
  RESOLV_TMP=resolv.conf.temp
  rm -f $RESOLV_TMP
  for i in `seq $MASTERCOUNT` ; do
      echo nameserver `getent hosts ${MASTERPREFIX}${i} | awk '{ print $1 }'` >> $RESOLV_TMP
  done

  cat /etc/resolv.conf >> $RESOLV_TMP
  mv $RESOLV_TMP /etc/resolv.conf
fi

##############################################
# configure init rules restart all processes
##############################################

echo "(re)starting mesos and framework processes"
if ismaster ; then
  sudo service zookeeper restart
  sudo service mesos-master start
  if [ "$MARATHONENABLED" == "true" ] ; then
    sudo service marathon start
  fi
  if [ "$CHRONOSENABLED" == "true" ] ; then
    sudo service chronos start
  fi
else
  echo manual | sudo tee /etc/init/zookeeper.override
  sudo service zookeeper stop
  echo manual | sudo tee /etc/init/mesos-master.override
  sudo service mesos-master stop
fi

if isagent ; then
  echo "starting mesos-slave"
  sudo service mesos-slave start
  echo "completed starting mesos-slave with code $?"
else
  echo manual | sudo tee /etc/init/mesos-slave.override
  sudo service mesos-slave stop
fi

echo "processes after restarting mesos"
ps ax

# Run swarm manager container on port 2376 (no auth)
if [ ismaster ] && [ "$SWARMENABLED" == "true" ] ; then
  echo "starting docker swarm"
  echo "sleep to give master time to come up"
  sleep 10
  echo sudo docker run -d -e SWARM_MESOS_USER=root \
      --restart=always \
      -p 2376:2375 -p 3375:3375 swarm manage \
      -c mesos-experimental \
      --cluster-opt mesos.address=0.0.0.0 \
      --cluster-opt mesos.port=3375 $zkmesosconfig
  sudo docker run -d -e SWARM_MESOS_USER=root \
      --restart=always \
      -p 2376:2375 -p 3375:3375 swarm manage \
      -c mesos-experimental \
      --cluster-opt mesos.address=0.0.0.0 \
      --cluster-opt mesos.port=3375 $zkmesosconfig
  sudo docker ps
  echo "completed starting docker swarm"
fi
echo "processes at end of script"
ps ax
echo "Finished installing and configuring docker and swarm"

###############################################
# Install Toil
###############################################

if [ "$TOILENABLED" == "true" ] ; then
  # Upgrade Python to 2.7.latest
  sudo apt-add-repository -y ppa:fkrull/deadsnakes-python2.7
  sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
  time sudo apt-get -y update
  # Install Toil dependencies (and setuptools for easy_install)
  time sudo apt-get -y --force-yes install python2.7 python2.7-dev python2.7-dbg python-setuptools build-essential git gcc-4.9 gdb
  
  # Get a reasonably new pip
  time sudo easy_install pip
  
  # Upgrade setuptools
  time sudo pip install setuptools --upgrade
  
  # Install Toil from Git, retrieving the correct version. If you want a release
  # you might be able to use a tag here instead.
  echo "Installing branch ${GITHUB_BRANCH} of ${GITHUB_SOURCE} for Toil."
  time sudo pip install --pre "git+https://github.com/${GITHUB_SOURCE}@${GITHUB_BRANCH}#egg=toil[mesos,azure]"
  
  # Toil no longer attempts to actually install Mesos's Python bindings itself,
  # so we have to do it. First we need the Mesos dependencies.
  time sudo pip install protobuf==2.6.1
  
  # Install the right bindings for the Mesos we installed
  UBUNTU_VERSION=`lsb_release -rs`
  sudo easy_install https://pypi.python.org/packages/source/m/mesos.interface/mesos.interface-${BINDINGS_MESOS_VERSION}.tar.gz
  # Easy-install doesn't like this server's ssl for some reason.
  sudo wget https://downloads.mesosphere.io/master/ubuntu/${UBUNTU_VERSION}/mesos-${BINDINGS_MESOS_VERSION}-py2.7-linux-x86_64.egg
  sudo easy_install mesos-${BINDINGS_MESOS_VERSION}-py2.7-linux-x86_64.egg
  sudo rm mesos-${BINDINGS_MESOS_VERSION}-py2.7-linux-x86_64.egg
fi

if [ "x${PYTHON_PACKAGES}" != "x" ] ; then
    # Install additional Python packages
    time sudo pip install --upgrade ${PYTHON_PACKAGES}
fi

date
echo "completed mesos cluster configuration"
