import boto3
from . import config


class DynamoDB:
    resource = None
    table = None
    table_name = ''

    def __init__(self, table_name=''):
        self.table_name = table_name
        self.resource = boto3.resource('dynamodb',
                                       aws_access_key_id=config.accessKey(),
                                       aws_secret_access_key=config.secretKey(),
                                       region_name=config.region())

    def create_table(self, table_name='', keys=[]):
        if table_name != '':
            self.table_name = table_name

        key_schemas = []
        attribute_definitions = []
        for key in keys:
            print('key::: ', key)
            strategy = {}
            attribute = {}
            strategy['AttributeName'] = key['name']
            if key['key_type'] == 'hash':
                strategy['KeyType'] = 'HASH'
            elif key['key_type'] == 'range':
                strategy['KeyType'] = 'RANGE'

            attribute['AttributeName'] = key['name']
            if key['type'] == 'int':
                attribute['AttributeType'] = 'N'
            elif key['type'] == 'str':
                attribute['AttributeType'] = 'S'

            key_schemas.append(strategy)
            attribute_definitions.append(attribute)

        table = self.resource.create_table(
            TableName=table_name,
            KeySchema=key_schemas,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        return table

    def get_items(self):
        if self.table is None:
            self.table = self.resource.Table(self.table_name)
        items = self.table.scan()
        return items

    def put_item(self, item={}):
        self.wait()
        self.table.put_item(Item=item)

    def update_item(self,
                    keys=None,
                    expression_names=None,
                    expression_values=None,
                    update_expression='',
                    return_values=''):
        table = self.resource.Table(self.table_name)
        res = table.update_item(
            Key=keys,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values,
            UpdateExpression=update_expression,
            ReturnValues=return_values
        )
        return res

    def delete_item(self, key={}):
        if self.table is None:
            self.table = self.resource.Table(self.table_name)
        self.table.delete_item(Key=key)

    def delete_table(self):
        if self.table is None:
            self.table = self.resource.Table(self.table_name)
        # Wait until the table exists.
        self.table.delete()

    def wait(self):
        if self.table is None:
            self.table = self.resource.Table(self.table_name)
        # Wait until the table exists.
        self.table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
