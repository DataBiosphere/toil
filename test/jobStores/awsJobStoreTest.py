import uuid
from xml.etree.cElementTree import Element

from jobStores.awsJobStore import AWSJobStore
from test import JobTreeTest

testRegion = "us-west-2"


class AWSJobStoreTest( JobTreeTest ):
    def _dummyConfig( self ):
        config = Element( "config" )
        config.attrib[ "try_count" ] = "1"
        return config

    def testCreation( self ):
        config = self._dummyConfig( )
        namePrefix = uuid.uuid4( )
        store = AWSJobStore( "%s:%s" % (testRegion, namePrefix), config )
