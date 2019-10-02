# Copyright (C) 2015-2016 Regents of the University of California
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
from __future__ import absolute_import

import importlib
import logging
logger = logging.getLogger(__name__)


def getClassFromFQN(cls):
    modules, className = cls.rsplit('.', 1)
    module = importlib.import_module(modules)
    return getattr(module, className)


def clusterFactory(provisioner, clusterName=None, zone=None, nodeStorage=50, **kwargs):
    """
    :param clusterName: The name of the cluster.
    :param provisioner: The cloud type of the cluster.
    :param zone: The cloud zone
    :return: A cluster object for the the cloud type.
    """
    known_provisionners = {'aws': ('toil.provisioners.aws.awsProvisioner.AWSProvisioner',
                                   'The aws extra must be installed to use this provisioner'),
                           'gce': ('toil.provisioners.gceProvisioner.GCEProvisioner',
                                   'The google extra must be installed to use this provisioner'),
                           'azure': ('toil.provisioners.azure.azureProvisioner.AzureProvisioner',
                                     'The azure extra must be installed to use this provisioner')}

    _provisioner = provisioner
    _err_msg = "Invalid provisioner '%s'" % provisioner

    known_provisionner = known_provisionners.get(provisioner)
    if known_provisionner:
        _provisioner = known_provisionner[0]
        _err_msg = known_provisionner[1]

    try:
        clazz = getClassFromFQN(_provisioner)
        return clazz(clusterName=clusterName, zone=zone, nodeStorage=nodeStorage, **kwargs)
    except ImportError:
        logger.error(_err_msg)
        raise
    except (ValueError, AttributeError):
        raise RuntimeError(_err_msg)


class NoSuchClusterException(Exception):
    """Indicates that the specified cluster does not exist."""
    def __init__(self, clusterName):
        super(NoSuchClusterException, self).__init__("The cluster '%s' could not be found" % clusterName)
