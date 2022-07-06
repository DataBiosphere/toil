
import logging
import boto3
from toil.lib.aws import zone_to_region
from toil.provisioners.aws import get_best_aws_zone
from functools import lru_cache
from typing import Any, List, Dict, Set, cast

from mypy_boto3_iam import IAMClient
from mypy_boto3_iam.type_defs import GetRolePolicyResponseTypeDef
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


def check_policy_warnings(allowed_actions: Dict[str, List[str]] = {'*': []}, launching_perms: Set[str] = _CLUSTER_LAUNCHING_PERMISSIONS) -> bool:
    """
    Check whether necessary permissions are permitted for AWS

    :param allowed_actions: Dictionary containing actions allowed by resource
    :param launching_perms: Set of required actions to launch a cluster on AWS
    """
    permissions = [x for x in launching_perms if check_permission_allowed(x, allowed_actions["*"])]

    if not launching_perms.issubset(set(permissions)):
        for perm in permissions:
            logger.warning('Permission %s is missing', perm)
        return False

    return True


def check_permission_allowed(perm: str, list_perms: List[str]) -> bool:
    """
    Takes a permission and checks whether it's contained within a list of given permissions

    :param perm: Permission to check in string form
    :param list_perms: Permission list to check against
    """

    #Two wildcards, ? any single character, * match zero or more characters
    for allowed in list_perms:

        # Allowed permission is just a wildcard, automatically matches any permission we would test
        if allowed == "*":
            return True

        # Check if the exact permission is in the list of allowed actions
        elif allowed == perm:
            return True

        # Attempt to match permission character by character
        else:
            perm_index = 0
            allowed_index = 0
            while perm_index != len(perm) and allowed_index != len(allowed):

                #Matching character
                if perm[perm_index] == allowed[allowed_index]:
                    perm_index += 1
                    allowed_index += 1

                #Matched single character wildcard
                elif perm[perm_index] == '?' or allowed[allowed_index] == '?':
                    perm_index += 1
                    allowed_index += 1

                #This is weird, case doesn't happen w/ current use case
                elif perm[perm_index] == "*":
                    break

                #Allowed action has a wildcard
                elif allowed[allowed_index] == "*":
                    #Check if the string ends with a wildcard
                    if allowed_index + 1 == len(allowed):
                        return True
                    #Assumes that multiple wildcards sequentially eg ec2:** is not possible
                    while perm_index < len(perm) and (allowed[allowed_index + 1] != perm[perm_index] or perm[perm_index] == '?'):
                        perm_index += 1
                    allowed_index += 1

                #Unmatched characters
                else:
                    break

    return False



def test_dummy_perms() -> bool:
    """
    Test the function check_policy_warnings() against a known good list of permissions
    """
    launch_tester = {'*': ['ec2:*', 'iam:*', 's3:*', 'sdb:*']}

    check_policy_warnings(allowed_actions=launch_tester)
    print("Success")
    return True


def get_allowed_actions(zone: str) -> Dict[str, List[str]]:
    """
    Returns a list of all allowed actions in a dictionary which is keyed by resource permissions
    are allowed upon.
    """

    aws = AWSConnectionManager()
    region = zone
    iam: IAMClient = cast(IAMClient, aws.client(region, 'iam'))
    response = iam.get_instance_profile(InstanceProfileName="fakename_toil")
    role_name = response['InstanceProfile']['Roles'][0]['RoleName']
    list_policies = iam.list_role_policies(RoleName=role_name)
    account_num = get_aws_account_num()
    str_arn = f"arn:aws:iam::{account_num}:role/{role_name}"
    role_name = response['InstanceProfile']['Roles'][0]['RoleName']
    list_policies = iam.list_role_policies(RoleName=role_name)
    account_num = boto3.client('sts').get_caller_identity().get('Account')
    allowed_actions: Dict[Any, Any] = {}

    for policy_name in list_policies['PolicyNames']:
        policy_arn = f"arn:aws:iam::{account_num}:policy/{policy_name}"

        role_policy: Dict[Any, Any] = dict(iam.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_name
        ))

        for statement in role_policy["PolicyDocument"]["Statement"]:

            if statement["effect"] == "Allow":

                for resource in statement["Resource"]:
                    if resource not in allowed_actions.keys():
                        allowed_actions[resource] = []

                    allowed_actions[resource].append(role_policy[statement["Action"]])

    return allowed_actions

@lru_cache()
def get_aws_account_num() -> Any:
    """
    Returns AWS account num
    """
    return boto3.client('sts').get_caller_identity().get('Account')