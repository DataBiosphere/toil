import boto3
import logging
from typing import Generator, Optional, Dict, Any

logger = logging.getLogger(__name__)

db = boto3.resource('dynamodb', region_name='us-west-2')


class DynamoDBItemNotFound(Exception):
    pass


def _format_item(hash_key: str, sort_key: Optional[str], value: Optional[str], ttl: Optional[int] = None) -> dict:
    item = {'hash_key': {'S': hash_key}}
    if value:
        item['body'] = {'S': value}
    if sort_key:
        item['sort_key'] = {'S': sort_key}
    if ttl:
        item['ttl'] = {'N': str(ttl)}
    return item


def _format_ddb_response(ddb_response_object: dict) -> Dict:
    formatted_object: Dict[Any, Any] = {}
    for k, v in ddb_response_object.items():  # strips out ddb typing info
        if [*v.keys()][0] == 'L':
            formatted_object[k] = []
            for nested_dictionary in v['L']:
                formatted_object[k].append([*nested_dictionary.values()][0])
        else:
            formatted_object[k] = [*v.values()][0]
    return formatted_object


def put_item(*, table: str, hash_key: str, sort_key: Optional[str] = None, value: str,
             dont_overwrite: Optional[str] = None, ttl: Optional[int] = None) -> None:
    """
    Put an item into a dynamoDB table.

    Will determine the type of db this is being called on by the number of keys provided (omit
    sort_key to PUT into a db with only 1 primary key).

    :param table: Name of the table in AWS.
    :param str value: Value stored by the two primary keys.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
                         Note: If not specified, this will PUT only 1 key (hash_key) and 1 value.
    :param str dont_overwrite: Don't overwrite if this parameter exists.  For example, setting this
                               to 'sort_key' won't overwrite if that sort_key already exists in the table.
    :param int ttl: Time to Live for the item.  Only works if enabled for that specific table.
    :return: None
    """
    query = {'TableName': table,
             'Item': _format_item(hash_key=hash_key, sort_key=sort_key, value=value, ttl=ttl)}
    if dont_overwrite:
        query['ConditionExpression'] = f'attribute_not_exists({dont_overwrite})'
    db.put_item(**query)


def get_item(*, table: str, hash_key: str, sort_key: Optional[str] = None) -> Dict:
    """
    Get associated value for a given set of keys from a dynamoDB table.
    :param table: Name of the table in AWS.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
    :return: item object from ddb (dict), with attribute types omitted
    """
    query = {'TableName': table,
             'Key': _format_item(hash_key=hash_key, sort_key=sort_key, value=None)}
    try:
        item = db.get_item(**query).get('Item')
    except db.exceptions.ValidationException as ex:
        logger.error(ex)
        raise
    if item is None:
        raise DynamoDBItemNotFound(f'Query failed to fetch item from database: {query}')
    return _format_ddb_response(item)


def get_primary_key_items(*, table: str, key: str, return_key: str = 'body') -> Generator[str, None, None]:
    """
    Get associated value for a given set of keys from a dynamoDB table.
    :param table: Name of the table in AWS.
    :param str key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str return_key: Either "body" (to return all values) or "sort_key" (to return all 2nd primary keys).
    :return: Iterable (str)
    """
    paginator = db.get_paginator('query')
    for db_resp in paginator.paginate(TableName=table,
                                      ScanIndexForward=False,  # True = ascending, False = descending
                                      KeyConditionExpression="#hash_key=:key",
                                      ExpressionAttributeNames={'#hash_key': "hash_key"},
                                      ExpressionAttributeValues={':key': {'S': key}}):
        for item in db_resp.get('Items', []):
            yield item[return_key]['S']


def get_primary_key_count(*, table: str, key: str) -> int:
    """
    Returns the number of values associated with a primary key from a dynamoDB table.
    :param table: Name of the table in AWS.
    :param str key: 1st primary key that can be used to fetch associated sort_keys and values.
    :return: Int
    """
    res = db.query(TableName=table,
                   KeyConditionExpression="#hash_key=:key",
                   ExpressionAttributeNames={'#hash_key': "hash_key"},
                   ExpressionAttributeValues={':key': {'S': key}},
                   Select='COUNT')
    return res['Count']


def get_all_table_items(*, table: str, both_keys: bool = False) -> Generator[str, None, None]:
    """
    Return all items from a dynamoDB table.

    :param table: Name of the table in AWS.
    :param str return_key: Either "body" (to return all values) or "sort_key" (to return all 2nd primary keys).
    :return: Iterable (str)
    """
    paginator = db.get_paginator('scan')
    for db_resp in paginator.paginate(TableName=table):
        for item in db_resp.get('Items', []):
            if both_keys:
                yield item['hash_key']['S'], item['sort_key']['S']
            else:
                yield item['body']['S']


def delete_item(*, table: str, hash_key: str, sort_key: Optional[str] = None) -> None:
    """
    Delete an item from a dynamoDB table.

    Will determine the type of db this is being called on by the number of keys provided (omit
    sort_key to DELETE from a db with only 1 primary key).

    Unless you specify conditions, DeleteItem is an idempotent operation; running it multiple times
    on the same item or attribute does not result in an error response:
    https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html

    :param table: Name of the table in AWS.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
                         Note: If not specified, this will DELETE only 1 key (hash_key) and 1 value.
    """
    query = {'TableName': table,
             'Key': _format_item(hash_key=hash_key, sort_key=sort_key, value=None)}
    db.delete_item(**query)


def update_item(*, table: str, hash_key: str, sort_key: Optional[str] = None, update_expression: Optional[str],
                expression_attribute_values: Dict, return_values: str = 'ALL_NEW') -> Dict:
    """
    Update an item from a dynamoDB table.
    Will determine the type of db this is being called on by the number of keys provided (omit
    sort_key to UPDATE from a db with only 1 primary key).

    https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html

    :param table: Name of the table in AWS.
    :param str hash_key: 1st primary key that can be used to fetch associated sort_keys and values.
    :param str sort_key: 2nd primary key, used with hash_key to fetch a specific value.
                         Note: If not specified, this will DELETE only 1 key (hash_key) and 1 value.
    :param str update_expression: Expression used to update value, needs action to be performed and new value
    :param str expression_attribute_values: attribute values to use from the expression
    :param str return_values: return values to get back from the dynamodb API, defaults to 'ALL_NEW'
                              which provides all item attributes after the update.
    """
    query = {'TableName': table,
             'Key': _format_item(hash_key=hash_key, sort_key=sort_key, value=None)}
    if update_expression:
        query['UpdateExpression'] = update_expression
        query['ExpressionAttributeValues'] = expression_attribute_values
        query['ReturnValues'] = return_values
    resp = db.update_item(**query)
    return _format_ddb_response(resp.get('Attributes'))


def create_table(table: str, region: Optional[str]):
    # TODO: Make sure that this is region appropriate
    return db.create_table(
        TableName=table,
        KeySchema=[
            {
                'AttributeName': 'filetype',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'id',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'filetype',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )


def delete_table(table: str, region: Optional[str]):
    # TODO: Make sure that this is region appropriate
    return db.Table(table).delete()


def table_exists(table: str) -> bool:
    try:
        table = db.Table(table)
        return table.table_status in ("CREATING", "UPDATING", "DELETING", "ACTIVE")
    except db.ClientError:
        return False
