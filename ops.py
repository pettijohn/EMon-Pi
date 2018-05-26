import os, sys
#sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import awslambda.aggregate as aggregate
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key, Attr

arn = "arn:aws:iot:us-east-1:422446087002:thing/EMonPi"

if sys.argv[1] == "agg":
    values = { 
        'device_id': arn,
        "current": Decimal(0),
        "volts": Decimal(0),
        "watt_hours": Decimal(0),
        "cost_usd": Decimal(0)
    }

    #time = datetime.utcnow() - timedelta(hours=1)
    time = datetime(2018,5,22,0,0,0, tzinfo=timezone.utc)
    minute = aggregate.MinuteBucket(time, values)
    hour = aggregate.HourBucket(time, minute, values)
    aggd = hour.ProcessEvent()
    #items = hour.GetChildren()

if sys.argv[1] == "fix":
    lastInvalid = "2018-05-13T04:10Z"

    rate = Decimal('0.1326')/Decimal(1000) # 13 cents per KWh


    print("Fixing < " + lastInvalid)
    table = boto3.resource('dynamodb').Table("EnergyMonitor.Minute")
    items = table.query(
        KeyConditionExpression=Key('device_id').eq(arn) & Key('bucket_id').lt(lastInvalid)
    )['Items']
    for i in items:
        current = i['current']
        assert type(current) == Decimal
        voltage = Decimal('242.0')
        wattHours = current*voltage/60
        costUsd = current*voltage/60*rate

        table.update_item(
            Key={
                'device_id': arn,
                'bucket_id': i['bucket_id']
            },
            UpdateExpression="set #c=:c, volts=:v, watt_hours=:w, cost_usd=:u",
            ExpressionAttributeValues={
                ':c': current,
                ':v': voltage,
                ':w': wattHours,
                ':u': costUsd
            },
            ExpressionAttributeNames={
                "#c":"current"
            },
            ReturnValues="UPDATED_NEW"
        )
        print(i)
    