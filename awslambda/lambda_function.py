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
        
def lambda_handler(event, context):
    event = json.loads(json.dumps(event, cls=DecimalEncoder), parse_float=Decimal)

    # Parse important fields
    bucketFormat = "%Y-%m-%dT%H:%M%z"
    time = datetime.strptime(event['bucket_id'], bucketFormat).replace(tzinfo=timezone.utc)
    minute = aggregate.MinuteBucket(time, event)
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