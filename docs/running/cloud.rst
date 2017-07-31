.. _CloudRunning:

Running in the Cloud
====================



For long-running jobs on remote systems, we recommended
using a terminal multiplexer such as `screen`_ or `tmux`_.

For details on including dependencies in your distributed workflows have a
look at :ref:`hotDeploying`.


Screen
------

Screen allows you to run toil workflows in the cloud without the risk of a bad
connection forcing the workflow to fail.

Simply type ``screen`` to open a new ``screen``
session. Later, type ``ctrl-a`` and then ``d`` to disconnect from it, and run
``screen -r`` to reconnect to it. Commands running under ``screen`` will
continue running even when you are disconnected, allowing you to unplug your
laptop and take it home without ending your Toil jobs. See :ref:`sshCluster`
for complications that can occur when using screen within the Toil Appliance.

.. _screen: https://www.gnu.org/software/screen/
.. _tmux: https://tmux.github.io/



