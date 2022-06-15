
import logging
import boto3
from toil.lib.aws import zone_to_region
from toil.provisioners.aws import get_best_aws_zone
from functools import lru_cache
from typing import Any, List, Dict

from toil.lib.aws.session import AWSConnectionManager


logger = logging.getLogger(__name__)

_CLUSTER_LAUNCHING_PERMISSIONS = {"iam:CreateRole",
                                  "iam:CreateInstanceProfile",
                                  "iam:TagInstanceProfile",
                                  "iam:DeleteRole",
                                  "iam:DeleteRoleProfile",
                                  "iam:ListAttatchedRolePolicies",
                                  "iam:ListPolicies",
                                  "iam:ListRoleTags",
                                  "iam:PutRolePolicy",
                                  "iam:RemoveRoleFromInstanceProfile",
                                  "iam:TagRole"
                                  }


def check_policy_warnings(allowed_actions: Dict[str, List[str]] = {'*': []}, launching_perms : set[str] = _CLUSTER_LAUNCHING_PERMISSIONS) -> None:
    """
    Check whether necessary permissions are permitted for AWS

    :param allowed_actions: Dictionary containing actions allowed by resource
    :param launching_perms: Set of required actions to launch a cluster on AWS
    """
    permissions = [x for x in launching_perms if helper_permission_check(x, allowed_actions["*"])]

    if not launching_perms.issubset(set(permissions)):
        raise RuntimeError("Missing permissions", permissions)

    return None


def helper_permission_check(perm : str, list_perms : List[str]) -> bool:
    """
    Takes a permission and checks whether it's allowed against a list of allowed permissions

    :param perm: Permission to check in string form
    :param list_perms: Permission list to check against
    """
    flag = False
    for allowed in list_perms:
        if allowed[0] == "*":
            if perm.endswith(allowed[1:]):
                flag = True

        if allowed[0] == "*" and allowed[-1] == "*":
            if allowed[1:-1] in perm:
                flag = True

        if allowed[-1] == "*":
            if perm.startswith(allowed[:-1]):
                flag = True

        if allowed == perm:
            flag = True
    if not flag:

        return False
    else:
        return True



def test_dummy_perms() -> bool:
    """
    Test for success of check policy warning against dummy permissions
    """
    launch_tester = {'*': ['ec2:*', 'iam:*', 's3:*', 'sdb:*']}

    check_policy_warnings(allowed_actions=launch_tester)
    print("Success")
    return True


def get_allowed_actions() -> Dict[str, List[str]]:
    """
    Returns a list of all allowed actions in a dictionary which is keyed by resource permissions
    are allowed upon.
    """
    aws = AWSConnectionManager()

    region = zone_to_region(get_best_aws_zone() or "us-west-2a" )

    iam = aws.client(region, 'iam')

    response = iam.get_instance_profile(InstanceProfileName="fakename_toil")

    role_name = response['InstanceProfile']['Roles'][0]['RoleName']

    list_policies = iam.list_role_policies(RoleName=role_name)

    account_num = boto3.client('sts').get_caller_identity().get('Account')

    str_arn = f"arn:aws:iam::{account_num}:role/{role_name}"

    role_name = response['InstanceProfile']['Roles'][0]['RoleName']

    list_policies = iam.list_role_policies(RoleName=role_name)

    account_num = boto3.client('sts').get_caller_identity().get('Account')

    allowed_actions = {}

    for policy_name in list_policies['PolicyNames']:
        policy_arn = f"arn:aws:iam::{account_num}:policy/{policy_name}"

        response = iam.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_name
        )

        if response["PolicyDocument"]["Statement"][0]["Effect"] == "Allow":
            if response["PolicyDocument"]["Statement"][0]["Resource"] not in allowed_actions.keys():
                allowed_actions[response["PolicyDocument"]["Statement"][0]["Resource"]] = []

            allowed_actions[response["PolicyDocument"]["Statement"][0]["Resource"]].append(
                response["PolicyDocument"]["Statement"][0]["Action"])

    check_policy_warnings(allowed_actions)
    return allowed_actions

@lru_cache
def get_aws_account_num() -> Any:
    """
    Returns AWS account num
    """
    return boto3.client('sts').get_caller_identity().get('Account')