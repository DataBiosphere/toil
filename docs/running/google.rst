.. _runningGoogle:

Running in Google Cloud Platform (GCP)
======================================
After setting up Toil on :ref:`installation-ref`, Toil scripts
can be run just by designating a job store location as shown in
:ref:`quickstart`.

Toil supports using `Google Cloud Platform`_'s storage for job stores.
Setting this up is easy! just

#. make sure that the ``google`` extra (:ref:`extras`) is installed and

#. follow `Google's Instructions`_ to download credentials and set the
   ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.

Using the Google job store is also easy! When invoking a Toil script specify
a google job store name as in the following example::

    $ python HelloWorld.py google:projectID:jobStore

The ``projectID`` component of the job store argument refers your Google
Cloud Project ID, and will be visible in the Google Cloud Console banner
at the top of the window.

The ``jobStore`` component is a name of your choosing that you will use to
refer to this job store. It will need to be unique because GCP buckets are
in a global namespace.


.. _Google's Instructions: https://developers.google.com/identity/protocols/application-default-credentials#howtheywork

.. _Google Cloud Platform: https://cloud.google.com/storage/