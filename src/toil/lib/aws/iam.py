
import logging
import boto3
import fnmatch
from toil.lib.aws import zone_to_region
from toil.provisioners.aws import get_best_aws_zone
from functools import lru_cache
from typing import Any, List, Dict, Set, cast

from mypy_boto3_iam import IAMClient
from mypy_boto3_sts import STSClient
from mypy_boto3_iam.type_defs import GetRolePolicyResponseTypeDef
from toil.lib.aws.session import client as get_client
from collections import defaultdict

logger = logging.getLogger(__name__)

#TODO Make this comprehensive
CLUSTER_LAUNCHING_PERMISSIONS = {"iam:CreateRole",
                                  "iam:CreateInstanceProfile",
                                  "iam:TagInstanceProfile",
                                  "iam:DeleteRole",
                                  "iam:DeleteRoleProfile",
                                  "iam:ListAttatchedRolePolicies",
                                  "iam:ListPolicies",
                                  "iam:ListRoleTags",
                                  "iam:PassRole",
                                  "iam:PutRolePolicy",
                                  "iam:RemoveRoleFromInstanceProfile",
                                  "iam:TagRole",
                                  "ec2:AuthorizeSecurityGroupIngress",
                                  "ec2:CancelSpotInstanceRequests",
                                  "ec2:CreateSecurityGroup",
                                  "ec2:CreateTags",
                                  "ec2:DeleteSecurityGroup",
                                  "ec2:DescribeAvailabilityZones",
                                  "ec2:DescribeImages",
                                  "ec2:DescribeInstances",
                                  "ec2:DescribeInstanceStatus",
                                  "ec2:DescribeKeyPairs",
                                  "ec2:DescribeSecurityGroups",
                                  "ec2:DescribeSpotInstanceRequests",
                                  "ec2:DescribeSpotPriceHistory",
                                  "ec2:DescribeVolumes",
                                  "ec2:ModifyInstanceAttribute",
                                  "ec2:RequestSpotInstances",
                                  "ec2:RunInstances",
                                  "ec2:StartInstances",
                                  "ec2:StopInstances",
                                  "ec2:TerminateInstances",
                                  }


def policy_permissions_allow(required_permissions: Dict[str, List[str]] = {'*': []}, given_permissions: Set[str] = {}) -> bool:
    """
    Check whether given set of actions are a subset of another given set of actions, returns true if they are
    otherwise false.

    :param required_permissions: Dictionary containing actions required, keyed by resource
    :param given_permissions: Set of actions that are granted to a user or role
    """

    # We only check actions explicitly allowed on all resources here,
    #TODO: Add a resource parameter to check for actions allowed by resource
    #Permissions
    allowed_perms = [x for x in given_permissions if check_permission_allowed(x, required_permissions["*"])]
    if not given_permissions.issubset(set(allowed_perms)):
        #Any disallowed permission will not be in the list of permissions that is generated
        missing_perms = given_permissions.difference(set(allowed_perms))
        for perm in missing_perms:
            logger.warning('Permission %s is missing', perm)
        return False

    return True


def check_permission_allowed(perm: str, list_perms: List[str]) -> bool:
    """
    Takes a permission and checks whether it's contained within a list of given permissions
    Returns True if it is otherwise False

    :param perm: Permission to check in string form
    :param list_perms: Permission list to check against
    """

    for allowed in list_perms:
        if fnmatch.fnmatch(allowed, perm):
            return True
    return False

def allowed_actions_roles(iam: IAMClient, policy_names: List[str], role_name: str) -> Dict[str, List[str]]:
    """
    Returns a dictionary containing a list of all aws actions allowed for a given role.
    This dictionary is keyed by resource and gives a list of policies allowed on that resource.

    :param iam: IAM client to use
    :param policy_names: Name of policy document associated with a role
    :param role_name: Name of role to get associated policies
    """
    allowed_actions: Dict[str, List[str]] = defaultdict(list)

    for policy_name in policy_names:
        role_policy: Dict[str, List[str]] = dict(iam.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_name
        ))

        for statement in role_policy["PolicyDocument"]["Statement"]:

            if statement["effect"] == "Allow":

                for resource in statement["Resource"]:
                    allowed_actions[resource].append(role_policy[statement["Action"]])

    return allowed_actions

def allowed_actions_users(iam: IAMClient, policy_names: List[str], user_name: str) -> Dict[str, List[str]]:
    """
    Gets all allowed actions for a user given by user_name, returns a dictionary, keyed by resource,
    with a list of permissions allowed for each given resource.

    :param iam: IAM client to use
    :param policy_names: Name of policy document associated with a user
    :param user_name: Name of user to get associated policies
    """
    allowed_actions: Dict[str, List[str]] = defaultdict(list)

    for policy_name in policy_names:
        user_policy: Dict[Any, Any] = dict(iam.get_user_policy(
            UserName=user_name,
            PolicyName=policy_name
        ))

        for statement in user_policy["PolicyDocument"]["Statement"]:
            if statement["effect"] == "Allow":
                for resource in statement["Resource"]:
                    allowed_actions[resource].append(user_policy[statement["Action"]])

    return allowed_actions

def get_allowed_actions(zone: str) -> Dict[str, List[str]]:
    """
    Returns a list of all allowed actions in a dictionary which is keyed by resource permissions
    are allowed upon. Requires AWS credentials to be associated with a user or assumed role.

    :param zone: AWS zone to connect to
    """

    iam: IAMClient = cast(IAMClient, get_client(zone, 'iam'))
    sts: STSClient = cast(STSClient, get_client(zone, 'sts'))
    allowed_actions: Dict[str, List[str]] = {}
    try:
        # If successful then we assume we are operating as a user, and grab the associated permissions
        user = iam.get_user()
        list_policies = iam.list_user_policies(UserName=user['User']['UserName'])
        allowed_actions = allowed_actions_users(iam, list_policies['PolicyNames'], user['User']['UserName'])

    except:
        # If not successful, we check the role associated with an instance profile
        # and grab the role's associated permissions
        role = sts.get_caller_identity()
        # Splits a role arn of format 'arn:aws:sts::123456789012:assumed-role/my-role-name/my-role-session-name'
        # on "/" and takes the second element to get the role name to list policies
        try:
            role_name = role["Arn"].split("/")[1]
            list_policies = iam.list_role_policies(RoleName=role_name)
            allowed_actions = allowed_actions_roles(iam, list_policies['PolicyNames'], role_name)
        except:
            logger.exception("Exception when trying to get role policies")

    return allowed_actions

@lru_cache()
def get_aws_account_num() -> str:
    """
    Returns AWS account num
    """
    return boto3.client('sts').get_caller_identity().get('Account')