.. highlight:: console

.. _ansibleInstallation-ref:

Ansible Installation
====================
This document describes how to prepare for and install the software required to leverage Ansible for provisioning cloud resources.

Install Ansible
---------------
1. Start your virtual environment::

   $ source ~/venv/bin/activate

2. Install Ansible using ``pip``::

   $ pip install ansible


3. Verify the Ansible version is 2.0 or greater::

   $ ansible --version



Enable and configure SSH on your laptop
---------------------------------------
1. If you are on a Mac, then following the instructions here_ to enable SSH.

.. _here: https://support.apple.com/kb/PH25252?viewlocale=en_US&locale=en_US

2. Generate an RSA key pair::

   $ ssh-keygen -t rsa

   NOTE: Accept the default file location (~/.ssh/id_rsa) and use an empty pass phrase (just hit carriage return twice).

3. Set permissions on the ~/.ssh directory::

   $ chmod 755 ~/.ssh

4. Create an authorized keys file::

   $ touch ~/.ssh/authorized_keys && chmod 644 ~/.ssh/authorized_keys

5. Install the ssh-copy-id command::

   $ brew install ssh-copy-id

6. Determine your login username::

   $ whoami

7. Populate the authorized keys file with your login account::

   $ ssh-copy-id <username>@127.0.0.1

   e.g.::

   $ ssh-copy-id jcasaletto@127.0.0.1

8. You were prompted for a password the ``ssh-copy-id`` invocation, but not for subsequent SSH attempts (that's why we put the key in the authorized keys file)::

   $ ssh <username>@127.0.0.1

Run a simple test of Ansible
----------------------------
Run the following ansible command to echo a string to your terminal::

   $ ansible all -i "localhost," -c local -m shell -a 'echo hello world'


Your output should be similar to the following::

   localhost | SUCCESS | rc=0 >>
   hello world

Use an Ansible playbook to create a VM in Azure
-----------------------------------------------

#. Download the :download:`create-azure-vm.yml playbook <../../src/toil/test/provisioners/azure/create-azure-vm.yml>`.


#. Run the playbook using the Azure cloud resources created earlier in the Azure installation document (TODO reference here)::

      (venv) $ ansible-playbook -v -c local --extra-vars "vmname=mytest2 resgrp=myresourcegroup vnet=myVnet subnet=mySubnet" create-azure-vm.yml

Use an Ansible playbook to delete a VM in Azure
-----------------------------------------------

#. Download the :download:`delete-azure-vm.yml playbook <../../src/toil/test/provisioners/azure/delete-azure-vm.yml>`.


#. Run the playbook::

      (venv) $ ansible-playbook -v -c local --extra-vars "vmname=mytest2 resgrp=myresourcegroup" delete-azure-vm.yml
