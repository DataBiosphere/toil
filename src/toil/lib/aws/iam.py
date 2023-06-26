import fnmatch
import json
import logging
from collections import defaultdict
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, cast, Union, Sequence

import boto3
from mypy_boto3_iam import IAMClient
from mypy_boto3_iam.type_defs import AttachedPolicyTypeDef
from mypy_boto3_sts import STSClient

from toil.lib.aws import zone_to_region
from toil.lib.aws.session import client as get_client
from toil.provisioners.aws import get_best_aws_zone

logger = logging.getLogger(__name__)

#TODO Make this comprehensive
CLUSTER_LAUNCHING_PERMISSIONS = ["iam:CreateRole",
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
                                  ]

AllowedActionCollection = Dict[str, Dict[str, List[str]]]

def init_action_collection() -> AllowedActionCollection:
    '''
    Initialization of an action collection, an action collection contains allowed Actions and NotActions
    by resource, these are patterns containing wildcards, an Action explicitly allows a matched pattern,
    eg ec2:* will explicitly allow all ec2 permissions

    A NotAction will explicitly allow all actions that don't match a specific pattern
    eg iam:* allows all non iam actions
    '''
    return defaultdict(lambda: {'Action': [], 'NotAction': []})

def add_to_action_collection(a: AllowedActionCollection, b: AllowedActionCollection) -> AllowedActionCollection:
    '''
    Combines two action collections
    '''
    to_return = init_action_collection()
    for key in a.keys():
        to_return[key]['Action'] += a[key]['Action']
        to_return[key]['NotAction'] += a[key]['NotAction']

    for key in b.keys():
        to_return[key]['Action'] += b[key]['Action']
        to_return[key]['NotAction'] += b[key]['NotAction']

    return to_return


def policy_permissions_allow(given_permissions: AllowedActionCollection, required_permissions: List[str] = []) -> bool:
    """
    Check whether given set of actions are a subset of another given set of actions, returns true if they are
    otherwise false and prints a warning.

    :param required_permissions: Dictionary containing actions required, keyed by resource
    :param given_permissions: Set of actions that are granted to a user or role
    """

    # We only check actions explicitly allowed on all resources here,
    #TODO: Add a resource parameter to check for actions allowed by resource
    resource = "*"

    missing_perms = []

    for permission in required_permissions:
        if not permission_matches_any(permission, given_permissions[resource]['Action']):
            if given_permissions[resource]['NotAction'] == [] or permission_matches_any(permission, given_permissions[resource]["NotAction"]):
                missing_perms.append(permission)

    if missing_perms:
        logger.warning('You appear to lack the folowing AWS permissions: %s', ', '.join(missing_perms))
        return False

    return True


def permission_matches_any(perm: str, list_perms: List[str]) -> bool:
    """
    Takes a permission and checks whether it's contained within a list of given permissions
    Returns True if it is otherwise False

    :param perm: Permission to check in string form
    :param list_perms: Permission list to check against
    """

    for allowed in list_perms:
        if fnmatch.fnmatch(perm, allowed):
            return True
    return False

def get_actions_from_policy_document(policy_doc: Dict[str, Any]) -> AllowedActionCollection:
    '''
    Given a policy document, go through each statement and create an AllowedActionCollection representing the
    permissions granted in the policy document.

    :param policy_doc: A policy document to examine
    '''
    allowed_actions: AllowedActionCollection = init_action_collection()
    # Policy document structured like so https://boto3.amazonaws.com/v1/documentation/api/latest/guide/iam-example-policies.html#example
    logger.debug(policy_doc)
    for statement in policy_doc["Statement"]:

        if statement["Effect"] == "Allow":

            for resource in statement["Resource"]:
                for key in ["Action", "NotAction"]:
                    if key in statement.keys():
                        if isinstance(statement[key], list):
                            allowed_actions[resource][key] += statement[key]
                        else:
                            #Assumes that if it isn't a list it's probably a string
                            allowed_actions[resource][key].append(statement[key])

    return allowed_actions
def allowed_actions_attached(iam: IAMClient, attached_policies: List[AttachedPolicyTypeDef]) -> AllowedActionCollection:
    """
    Go through all attached policy documents and create an AllowedActionCollection representing granted permissions.

    :param iam: IAM client to use
    :param attached_policies: Attached policies
    """

    allowed_actions: AllowedActionCollection = init_action_collection()
    for policy in attached_policies:
        policy_desc = iam.get_policy(PolicyArn=policy['PolicyArn'])
        policy_ver = iam.get_policy_version(PolicyArn=policy_desc['Policy']['Arn'], VersionId=policy_desc['Policy']['DefaultVersionId'])
        policy_document = policy_ver['PolicyVersion']['Document']
        #TODO whenever boto fixes the typing, stop ignoring this line in typecheck
        allowed_actions = add_to_action_collection(allowed_actions, get_actions_from_policy_document(policy_document)) # type: ignore

    return allowed_actions


def allowed_actions_roles(iam: IAMClient, policy_names: List[str], role_name: str) -> AllowedActionCollection:
    """
    Returns a dictionary containing a list of all aws actions allowed for a given role.
    This dictionary is keyed by resource and gives a list of policies allowed on that resource.

    :param iam: IAM client to use
    :param policy_names: Name of policy document associated with a role
    :param role_name: Name of role to get associated policies
    """
    allowed_actions: AllowedActionCollection = init_action_collection()

    for policy_name in policy_names:
        role_policy = iam.get_role_policy(
            RoleName=role_name,
            PolicyName=policy_name
        )
        logger.debug("Checking role policy")
        policy_document = json.loads(role_policy["PolicyDocument"])

        allowed_actions = add_to_action_collection(allowed_actions, get_actions_from_policy_document(policy_document))

    return allowed_actions


def collect_policy_actions(policy_documents: Sequence[Union[str, Dict[str, Any]]]) -> AllowedActionCollection:
    """
    Collect all of the actions allowed by the given policy documents into one AllowedActionCollection.
    """
    allowed_actions: AllowedActionCollection = init_action_collection()
    for policy_str in policy_documents:
        # sometimes a string is returned from the api, so convert to a dictionary
        if isinstance(policy_str, dict):
            policy_dict = policy_str
        else:
            policy_dict = json.loads(policy_str)
        allowed_actions = add_to_action_collection(allowed_actions, get_actions_from_policy_document(policy_dict))
    return allowed_actions


def allowed_actions_user(iam: IAMClient, policy_names: List[str], user_name: str) -> AllowedActionCollection:
    """
    Gets all allowed actions for a user given by user_name, returns a dictionary, keyed by resource,
    with a list of permissions allowed for each given resource.

    :param iam: IAM client to use
    :param policy_names: Name of policy document associated with a user
    :param user_name: Name of user to get associated policies
    """
    user_policies = [
        iam.get_user_policy(
            UserName=user_name,
            PolicyName=policy_name
        )["PolicyDocument"]
        for policy_name in policy_names
    ]
    return collect_policy_actions(user_policies)


def allowed_actions_group(iam: IAMClient, policy_names: List[str], group_name: str) -> AllowedActionCollection:
    """
    Gets all allowed actions for a group given by group_name, returns a dictionary, keyed by resource,
    with a list of permissions allowed for each given resource.

    :param iam: IAM client to use
    :param policy_names: Name of policy document associated with a user
    :param group_name: Name of group to get associated policies
    """
    group_policies = [
        iam.get_group_policy(
            GroupName=group_name,
            PolicyName=policy_name
        )["PolicyDocument"]
        for policy_name in policy_names
    ]
    return collect_policy_actions(group_policies)


def get_policy_permissions(region: str) -> AllowedActionCollection:
    """
    Returns an action collection containing lists of all permission grant patterns keyed by resource
    that they are allowed upon. Requires AWS credentials to be associated with a user or assumed role.

    :param zone: AWS zone to connect to
    """

    iam: IAMClient = cast(IAMClient, get_client('iam', region))
    sts: STSClient = cast(STSClient, get_client('sts', region))
    #TODO Condider effect: deny at some point
    allowed_actions: AllowedActionCollection = defaultdict(lambda: {'Action': [], 'NotAction': []})
    try:
        # If successful then we assume we are operating as a user, and grab the associated permissions
        user = iam.get_user()
        list_policies = iam.list_user_policies(UserName=user['User']['UserName'])
        attached_policies = iam.list_attached_user_policies(UserName=user['User']['UserName'])
        user_attached_policies = allowed_actions_attached(iam, attached_policies['AttachedPolicies'])
        allowed_actions = add_to_action_collection(allowed_actions, user_attached_policies)
        user_inline_policies = allowed_actions_user(iam, list_policies['PolicyNames'], user['User']['UserName'])
        allowed_actions = add_to_action_collection(allowed_actions, user_inline_policies)

        # grab group policies associated with the user
        groups = iam.list_groups_for_user(UserName=user['User']['UserName'])
        for group in groups["Groups"]:
            list_policies = iam.list_group_policies(GroupName=group['GroupName'])
            attached_policies = iam.list_attached_group_policies(GroupName=group['GroupName'])
            group_attached_policies = allowed_actions_attached(iam, attached_policies['AttachedPolicies'])
            allowed_actions = add_to_action_collection(allowed_actions, group_attached_policies)
            group_inline_policies = allowed_actions_group(iam, list_policies['PolicyNames'], group['GroupName'])
            allowed_actions = add_to_action_collection(allowed_actions, group_inline_policies)

    except:
        # If not successful, we check the role associated with an instance profile
        # and grab the role's associated permissions
        role = sts.get_caller_identity()
        # Splits a role arn of format 'arn:aws:sts::123456789012:assumed-role/my-role-name/my-role-session-name'
        # on "/" and takes the second element to get the role name to list policies
        try:
            role_name = role["Arn"].split("/")[1]
            list_policies = iam.list_role_policies(RoleName=role_name)
            attached_policies = iam.list_attached_role_policies(RoleName=role_name)
            role_attached_policies = allowed_actions_attached(iam, attached_policies['AttachedPolicies'])
            allowed_actions = add_to_action_collection(allowed_actions, role_attached_policies)
            role_inline_policies = allowed_actions_roles(iam, list_policies['PolicyNames'], role_name)
            allowed_actions = add_to_action_collection(allowed_actions, role_inline_policies)

        except:
            logger.exception("Exception when trying to get role policies")
    logger.debug("Allowed actions: %s", allowed_actions)
    return allowed_actions

@lru_cache()
def get_aws_account_num() -> Optional[str]:
    """
    Returns AWS account num
    """
    return boto3.client('sts').get_caller_identity().get('Account')
