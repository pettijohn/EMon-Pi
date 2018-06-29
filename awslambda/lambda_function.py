import boto3
from decimal import Decimal
import json
import aggregate
from datetime import datetime, timezone, timedelta
import pytz

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
        

bucketFormat = "%Y-%m-%dT%H:%M%z"

def tryParseDatetime(date_string: str):
    for format in ["%Y-%m-%dT%H:%M%z", "%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M", "%Y-%m-%d"]:
        try:
            val = datetime.strptime(date_string, format)
            if val.tzinfo == None:
                val = val.replace(tzinfo=timezone.utc)
            return val
        except ValueError:
            pass

def lambda_handler(event, context):
    event = json.loads(json.dumps(event, cls=DecimalEncoder), parse_float=Decimal)

    # Parse important fields and create bucket
    time = datetime.strptime(event['bucket_id'], bucketFormat).replace(tzinfo=timezone.utc)
    minute = aggregate.MinuteBucket(time, event)
    if 'action' in event and event['action'] == "reagg":
        minute.ProcessEvent(doInsert=False)
    elif 'action' in event and event['action'] == "query":
        start = tryParseDatetime(event['start'])
        end   = tryParseDatetime(event['end'])
        device_id = event['device_id']
        if start == None or end == None or device_id == None:
            return "ERROR - 'start', 'end', and 'device_id' are required"

        return aggregate.Query(start, end, {"device_id": device_id})
    else:
        # regular insert event
        minute.ProcessEvent()

if __name__ == "__main__":
    time = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(5) # work in the future 
    payload = { "device_id": "arn:aws:iot:us-east-1:422446087002:thing/EMonPi",
        "bucket_id": time.strftime("%Y-%m-%dT%H:%M%z"),
        "amps": Decimal('0'),
        "volts": Decimal('242.0'),
        "watt_hours": Decimal('60'),
        "cost_usd": Decimal('60')
    }
    lambda_handler(payload, None)