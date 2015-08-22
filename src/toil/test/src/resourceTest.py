# Copyright (C) 2015 UCSC Computational Genomics Lab
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

import sys

from mock import MagicMock, patch

from toil.resource import ModuleDescriptor, Resource
from toil.test import ToilTest


class ResourceTest( ToilTest ):
    def test( self ):
        # Create a ModuleDescriptor for the module containing ModuleDescriptor, i.e. toil.resource
        module_name = ModuleDescriptor.__module__
        self.assertEquals( module_name, 'toil.resource' )
        module = ModuleDescriptor.forModule( module_name )

        # Assert basic attributes and properties
        self.assertTrue( module.belongsToToil )
        self.assertEquals( module.name, module_name )
        self.assertEquals( module.filePath, sys.modules[ module_name ].__file__ )
        self.assertTrue( module.filePath.startswith( module.dirPath ) )
        self.assertTrue( module.dirPath.endswith( '/src' ) )

        # Before the module is saved as a resource, localize() and globalize() are identity
        # methods. This should log warnings.
        self.assertIs( module.localize( ), module )
        self.assertIs( module.globalize( ), module )

        # Create a mock job store ...
        jobStore = MagicMock( )
        # ... to generate a fake URL for the resource ...
        url = 'file://foo.zip'
        jobStore.getSharedPublicUrl.return_value = url
        # ... and save the resource to it.
        resource = module.saveAsResourceTo( jobStore )
        # Ensure that the URL generation method is actually called, ...
        jobStore.getSharedPublicUrl.assert_called_once_with( resource.pathHash )
        # ... and that ensure that writeSharedFileStream is called.
        jobStore.writeSharedFileStream.assert_called_once_with( resource.pathHash,
                                                                isProtected=False )

        # Now it gets a bit complicated: Ensure that the context manager returned by the
        # jobStore's writeSharedFileStream() method is entered and that the file handle yielded
        # by the context manager is written to once with the zipped source tree from which
        # 'toil.resource' was orginally imported. Keep the zipped tree around such that we can
        # mock the download later.
        file_handle = jobStore.writeSharedFileStream.return_value.__enter__.return_value
        # The first 0 index selects the first call of write(), the second 0 selects positional
        # instead of keyword arguments, and the third 0 selects the first positional, i.e. the
        # contents. This is a bit brittle since it assumes that all the data is written in a
        # single call to write(). If more calls are made we can easily concatenate them.
        zipFile = file_handle.write.call_args_list[ 0 ][ 0 ][ 0 ]
        self.assertTrue( zipFile.startswith( 'PK' ) ) # the magic header for ZIP files
        self.assertEquals( resource.url, url )

        # Now we're on the worker. Prepare the storage for localized resources
        Resource.prepareSystem( )
        # Register the resource for subsequent lookup.
        resource.register( )
        # Lookup the resource and ensure that the result is equal to but not the same as the
        # original resource. Lookup will also be used when we localize the module that was
        # originally used to create the resource.
        localResource = Resource.lookup( module._resourcePath )
        self.assertEquals( resource, localResource )
        self.assertIsNot( resource, localResource )

        # Now show that we can localize the module using the registered resource. Set up a mock
        # urlopen() that yields the zipped tree ...
        mock_urlopen = MagicMock( )
        mock_urlopen.return_value.read.return_value = zipFile
        with patch( 'toil.resource.urlopen', mock_urlopen ):
            # ... and use it to download and unpack the resource
            localModule = module.localize( )
        # Name and extension should be equal between original and localized resource ...
        self.assertEquals( module.name, localModule.name )
        self.assertEquals( module.extension, localModule.extension )
        # ... but the directory should be different.
        self.assertNotEquals( module.dirPath, localModule.dirPath )

        # Show that we can 'undo' localization. This is necessary when the user script's jobs are
        # invoked on the worker where they generate more child jobs.
        self.assertEquals( localModule.globalize( ), module )
