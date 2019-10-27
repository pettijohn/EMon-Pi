import json
# import io
import boto3
import time

# Convert from separate tables (Minute, Hour, Day, Month, Year)
# into a single table with "Grain" column



client = boto3.client('dynamodb', region_name='us-east-1')
# tableFromGrain = ["Year", "Month", "Day", "Hour", "Minute"]
tableFromGrain = ["Month"]

for grain in tableFromGrain:
    paginator = client.get_paginator('scan')
    for page in paginator.paginate(
        TableName='EnergyMonitor.' + grain,
        ConsistentRead=True
        ):

    
        items = page['Items']

        for item in items:
            print(item['bucket_id']['S'])
            device_grain = '{0}|{1}'.format(item['device_id']['S'], grain)
            item['grain'] = {'S': grain}
            item['device_grain'] = {'S': device_grain}
            
            client.put_item(
                TableName="EnergyMonitor.PowerReadings",
                Item=item)
            
            time.sleep(0.1)
        print(".")



        