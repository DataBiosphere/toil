.. Toil documentation master file, created by
   sphinx-quickstart on Tue Aug 25 12:37:16 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Scripting Quick Start
=====================

See README for installation. Toil's Job class contains the Toil API, documented below.
To begin, consider this short toil script:

.. code:: python

   from toil.job import Job

   class HelloWorld(Job):
       def __init__(self):
           Job.__init__(self,  memory=100000, cores=2, disk=20000)
       def run(self, fileStore):
           fileId = getEmptyFileStoreID()
           self.wrapJobFn(childFn, fileId,
                          cores=1, memory="1M", disk="10M")
           self.addChild(FollowOn(fileId),

   def childFn(target, fileID):
       with target.fileStore.updateGlobalFileStream(fileID) as file:
           file.write("Hello, World!")

   class FollowOn(Job):
       def __init__(self,fileId):
           Job.__init__(self)
           self.fileId=fileId
       def run(self, fileStore):
           tempDir = self.getLocalTempDir()
           tempFilePath = "/".join(tempDir,"LocalCopy")
           with readGlobalFileStream(fileId) as globalFile:
               with open(tempFilePath, w) as localFile:
                  localFile.write(globalFile.read())

   def main():
       parser = OptionParser()
       Job.Runner.addToilOptions(parser)
       options, args = parser.parse_args( args )
       Job.Runner.startToil(HelloWorld(),  options )

   if __name__=="__main__":
       main()

Toil Docs
=========

Contents:

.. toctree::
   :maxdepth: 2

   code

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

