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
    values = { 
        'device_id': arn
    }
    increment = sys.argv[2]

    startTime =   datetime(2018,6,22,6,0,0, tzinfo=timezone.utc)
    #now =        datetime(2018,6,23,8,0,0, tzinfo=timezone.utc)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    mb = aggregate.MinuteBucket(startTime, values)
    hb = aggregate.HourBucket(startTime, mb, values)
    db = aggregate.DayBucket(startTime, hb, values)

    if increment == "day":
        while db.EventTime < now:
            print("Processing " + db.BucketID())
            db.ProcessEvent()
            nextEvent = db.NextBucketStart()

            mb = aggregate.MinuteBucket(nextEvent, values)
            hb = aggregate.HourBucket(nextEvent, mb, values)
            db = aggregate.DayBucket(nextEvent, hb, values)

    if increment == "hour":
        while hb.EventTime < now:
            print("Processing " + hb.BucketID())
            hb.ProcessEvent()
            nextEvent = hb.NextBucketStart()

            mb = aggregate.MinuteBucket(nextEvent, values)
            hb = aggregate.HourBucket(nextEvent, mb, values)




if sys.argv[1] == "migrateminutes":
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('EnergyMonitor.Minute')

    lastEvaluted = None
    while True:
        if lastEvaluted is None:
            response = table.scan(
                FilterExpression=Attr("bucket_id").contains("Z"),
                ProjectionExpression="bucket_id"
            )
        else:
            response = table.scan(
                FilterExpression=Attr("bucket_id").contains("Z"),
                ProjectionExpression="bucket_id",
                ExclusiveStartKey=lastEvaluted
            )

        items = response['Items']

        for item in items:
            bucketToReAgg = item['bucket_id']
            print(bucketToReAgg)
            eventTime = datetime.strptime(bucketToReAgg, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
            values = { 
                'device_id': arn,
                #'bucket_id': bucketToReAgg
            }

            minute = aggregate.MinuteBucket(eventTime, values)
            aggd = minute.ProcessEvent(False, False)
            time.sleep(0.1)
        print(".")
        if "LastEvaluatedKey" in response:
            # Paginate dynamo's results
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html
            lastEvaluted = response["LastEvaluatedKey"]
        else:
            quit()

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

def tryParseDatetime(date_string: str):
    for format in ["%Y-%m-%dT%H:%M%z", "%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M", "%Y-%m-%d"]:
        try:
            val = datetime.strptime(date_string, format)
            if val.tzinfo == None:
                val = val.replace(tzinfo=timezone.utc)
            return val
        except ValueError:
            pass

if sys.argv[1] == "query":
    start = tryParseDatetime(sys.argv[2])
    end   = tryParseDatetime(sys.argv[3])

    results = aggregate.Query(start, end, {"device_id": arn})
    for r in results:
        print ("{0} : ${1:,.6f}".format(r['bucket_id'], r['cost_usd']))