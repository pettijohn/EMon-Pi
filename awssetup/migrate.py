import json
# import io
import boto3
import time

# Convert from separate tables (Minute, Hour, Day, Month, Year)
# into a single table with "Grain" column



dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
tableFromGrain = "Month"
tableFrom = dynamodb.Table('EnergyMonitor.' + tableFromGrain)
tableTo   = dynamodb.Table('EnergyMonitor.PowerReadings')

lastEvaluted = None
while True:
    if lastEvaluted is None:
        response = tableFrom.scan()
    else:
        response = tableFrom.scan(
            ExclusiveStartKey=lastEvaluted
        )

    items = response['Items']

    for item in items:
        bucketToReAgg = item['bucket_id']
        print(bucketToReAgg)
        item['grain'] = tableFromGrain
        item['device_grain'] = '{0}|{1}'.format(item['device_id'], tableFromGrain)
        
        tableTo.put_item(Item=item)
        
        time.sleep(0.1)
    print(".")
    if "LastEvaluatedKey" in response:
        # Paginate dynamo's results
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html
        lastEvaluted = response["LastEvaluatedKey"]
    else:
        quit()



    