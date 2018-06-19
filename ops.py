import os, sys
#sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import awslambda.aggregate as aggregate
from datetime import datetime, timedelta, timezone
import time
from decimal import Decimal
import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
from hardware import connect

arn = "arn:aws:iot:us-east-1:422446087002:thing/EMonPi"
firstBucket = "2018-05-13T03:58+0000"
firstTime = datetime(2018,5,13,3,58,0, tzinfo=timezone.utc)
endTime = datetime.utcnow().replace(tzinfo=timezone.utc)

if sys.argv[1] == "aggtest":
    values = { 
        'device_id': arn,
        "amps": Decimal(0),
        "volts": Decimal(0),
        "watt_hours": Decimal(0),
        "cost_usd": Decimal(0)
    }

    #time = datetime.utcnow() - timedelta(hours=1)
    time = datetime(2018,5,24,8,0,0, tzinfo=timezone.utc)
    minute = aggregate.MinuteBucket(time, values)
    aggd = minute.ProcessEvent()
    #hour = aggregate.HourBucket(time, minute, values)
    #aggd = hour.ProcessEvent()
    #items = hour.GetChildren()

if sys.argv[1] == "reagg":
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('EnergyMonitor.Minute')

    while True:
        response = table.scan(
            FilterExpression=Attr("bucket_id").contains("Z"),
            ProjectionExpression="bucket_id",
            ConsistentRead=True 
            )

        items = response['Items']
        if len(items) < 1:
            quit()

        for item in items:
            bucketToReAgg = item['bucket_id']
            print(bucketToReAgg)
            eventTime = datetime.strptime(bucketToReAgg, "%Y-%m-%dT%H:%MZ")
            values = { 
                'device_id': arn,
                'bucket_id': bucketToReAgg
            }

            minute = aggregate.MinuteBucket(eventTime, values)
            aggd = minute.ProcessEvent(True, False)

if sys.argv[1] == "reaggiot":
    print("Connecting IoT client")
    client = connect.Client(arn)

    startTime = datetime(2018,5,22,17,6,0, tzinfo=timezone.utc)
    firstMinute = aggregate.MinuteBucket(startTime, {})
    minute = firstMinute
    while minute.BucketStartTime() < datetime(2018,6,16,23,0,0, tzinfo=timezone.utc):
        
        payload = { 
            "device_id": arn,
            "bucket_id": minute.BucketID(),
            "action": "reagg"
        }
        print(minute.BucketID())
        strPayload = json.dumps(payload)
        client.publish("EnergyReading/Minute", strPayload, 1)

        # Advance the minute and repeat 
        minute = aggregate.MinuteBucket(minute.NextBucketStart(), {})
        time.sleep(1)
        # Capacity Read Write
        # Min 10 4
        # Hour 5 5 
        # Day 4 5
        # Month 3 3
        # Year 2 2
        
    client.disconnect()    
        
if sys.argv[1] == "fix":
    pass
    # identify all minutes containing 'Z' in bucket ID
    # or where current <> null
    # Reagg/migrate them
    # Then, for all hours, reagg
        

if sys.argv[1] == "missingdata":
    #2018-05-28T03:14Z
	
    #cost 0.01657178643891232
    #amps 30.98572685934019
    #volts 242
    #WH 124.97576499933876
	
	
    #2018-05-28T03:26Z
    pass
    


if sys.argv[1] == "fix":
    quit()
    lastInvalid = "2018-05-13T04:10Z"

    rate = Decimal('0.1326')/Decimal(1000) # 13 cents per KWh


    print("Fixing < " + lastInvalid)
    table = boto3.resource('dynamodb').Table("EnergyMonitor.Minute")
    items = table.query(
        KeyConditionExpression=Key('device_id').eq(arn) & Key('bucket_id').lt(lastInvalid)
    )['Items']
    for i in items:
        current = i['amps']
        assert type(current) == Decimal
        voltage = Decimal('242.0')
        wattHours = current*voltage/60
        costUsd = current*voltage/60*rate

        table.update_item(
            Key={
                'device_id': arn,
                'bucket_id': i['bucket_id']
            },
            UpdateExpression="set amps=:a, volts=:v, watt_hours=:w, cost_usd=:u",
            ExpressionAttributeValues={
                ':a': current,
                ':v': voltage,
                ':w': wattHours,
                ':u': costUsd
            },
            # ExpressionAttributeNames={
            #     "#c":"current"
            # },
            ReturnValues="UPDATED_NEW"
        )
        print(i)
    
