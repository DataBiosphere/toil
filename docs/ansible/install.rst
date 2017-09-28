.. highlight:: console

.. _ansibleInstallation-ref:

Ansible Installation
====================
This document describes how to prepare for and install the software required to leverage Ansible for provisioning cloud resources.

1. Start your virtual environment::

   $ source venv/bin/activate

2. Install Ansible using ``pip``::

   $ pip install ansible


3. Verify the Ansible version is 2.0 or greater::

   $ ansible --version

4. Run a simple test of Ansible::

   $ ansible all -i "localhost," -c local -m shell -a 'echo hello world'
