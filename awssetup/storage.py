import boto3

dynamodb = boto3.resource('dynamodb')

# https://boto3.readthedocs.io/en/latest/guide/dynamodb.html#creating-a-new-table
# Create the DynamoDB table.


tableName = "EnergyMonitor.PowerReadings"
table = dynamodb.create_table(
    TableName=tableName,
    KeySchema=[
        {
            'AttributeName': 'device_grain',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'bucket_id',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'device_grain',
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

quit()

tableName = "EnergyMonitor.AutomobileDistance"
table = dynamodb.create_table(
    TableName=tableName,
    KeySchema=[
        {
            'AttributeName': 'vin',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'date',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'vin',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'date',
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





quit()

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

