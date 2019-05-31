.. _minAwsPermissions:

Minimum AWS IAM permissions
---------------------------

Toil requires at least the following permissions in an IAM role to operate on a cluster.
These are added by default when launching a cluster. However, ensure that they are present
if creating a custom IAM role when :ref:`launching a cluster <launchAwsClusterDetails>`
with the ``--awsEc2ProfileArn`` parameter.

::

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "ec2:*",
                    "s3:*",
                    "sdb:*",
                    "iam:PassRole"
                ],
                "Resource": "*"
            }
        ]
    }
