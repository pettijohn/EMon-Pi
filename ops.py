import os, sys
#sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import awslambda.aggregate as aggregate
from datetime import datetime, timedelta, timezone
import time
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key, Attr

arn = "arn:aws:iot:us-east-1:422446087002:thing/EMonPi"
firstBucket = "2018-05-13T03:58Z"
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
    time = datetime(2018,6,16,0,0,0, tzinfo=timezone.utc)
    minute = aggregate.MinuteBucket(time, values)
    aggd = minute.ProcessEvent()
    #hour = aggregate.HourBucket(time, minute, values)
    #aggd = hour.ProcessEvent()
    #items = hour.GetChildren()

if sys.argv[1] == "reagg":
    values = {"device_id": arn}
    startTime = firstTime
    firstMinute = aggregate.MinuteBucket(startTime, values)
    
    minute = firstMinute
    prevHour = None
    while minute.BucketStartTime() < endTime:
        # This will also migrate to the new bucket ID format
        minute.ProcessEvent()
        # Advance the minute and repeat 
        minute = aggregate.MinuteBucket(minute.NextBucketStart(), values)
        time.sleep(0.75)
        
        

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
    