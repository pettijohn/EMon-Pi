import boto3

dynamodb = boto3.resource('dynamodb')

# https://boto3.readthedocs.io/en/latest/guide/dynamodb.html#creating-a-new-table
# Create the DynamoDB table.

prefix = 'EnergyMonitor.'
buckets = ['Minute', 'Hour', 'Day', 'Month', 'Year']
tables = dynamodb.meta.client.list_tables()['TableNames']

for b in buckets:
    tableName = prefix + b # e.g. 'EnergyMonitor.Minute'
    if tableName not in tables:

        table = dynamodb.create_table(
            TableName=tableName,
            KeySchema=[
                {
                    'AttributeName': 'device_id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'bucket_id',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'device_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'bucket_id',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )


        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=tableName)

        # Print out some data about the table.
        print(table.item_count)

# class foo:
    

#     table = Table('EnergyMonitor.Minutes')
#     item = table.put_item(data={
#         'device_arn': 'foo',
#         'bucket_id': '2018-05-07T18:01Z',
#         'amps': 39.001,
#         'volts': 242.1,
#         'watt_hours': 39.001*242.1/60
#     })

#     table = Table('EnergyMonitor.Minutes')
#     item = table.get_item(
#         device_arn='foo',
#         bucket_id='2018-05-07T18:01Z'
#     )