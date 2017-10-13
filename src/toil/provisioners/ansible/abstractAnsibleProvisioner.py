from abc import ABCMeta, abstractmethod


class AbstractAnsibleProvisioner(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def launchCluster(self, args, ):
        raise NotImplementedError("launchCluster abstract method not implemented")

    @abstractmethod
    def destroyCluster(self, args):
        raise NotImplementedError("destroyCluster abstract method not implemented")
