import boto3
from decimal import Decimal
import json
import aggregate
from datetime import datetime, timezone, timedelta
import pytz
import sys

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
        
arn = "arn:aws:iot:us-east-1:422446087002:thing/EMonPi"
bucketFormat = "%Y-%m-%dT%H:%M%z"

def tryParseDatetime(date_string: str):
    for format in ["%Y-%m-%dT%H:%M%z", "%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M", "%Y-%m-%d"]:
        try:
            val = datetime.strptime(date_string, format)
            if val.tzinfo == None:
                # If timezone not present, default to Device Time Zone #FIXME
                val = pytz.timezone("America/Los_Angeles").localize(val)
            return val
        except ValueError:
            pass
    raise ValueError("Unable to parse input date/time string")

def lambda_handler(event, context):
    event = json.loads(json.dumps(event, cls=DecimalEncoder), parse_float=Decimal)

    # Check if this is an API Gateway request
    if 'path' in event and 'httpMethod' in event:
        if event['path'] != "/query":
            raise Exception("Unknown command")

        query = json.loads(event['body'], parse_float=Decimal)
        assert 'start' in query, "Parameter 'start' required"
        assert 'end' in query, "Parameter 'end' required"
        #assert 'format' in query, "Parameter 'format' required"
        #assert query['format'] in ['total', 'detail']
        start = tryParseDatetime(query['start'])
        end   = tryParseDatetime(query['end'])
        result = aggregate.Query(start, end, {"device_id": arn} )

        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps(result, cls=DecimalEncoder)
        }
    
    else:
        # AWS IoT event
        # Parse important fields and create bucket
        time = datetime.strptime(event['bucket_id'], bucketFormat).replace(tzinfo=timezone.utc)
        minute = aggregate.MinuteBucket(time, event)
        minute.ProcessEvent()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        time = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(5) # work in the future 
        payload = { "device_id": arn,
            "bucket_id": time.strftime("%Y-%m-%dT%H:%M%z"),
            "amps": Decimal('0'),
            "volts": Decimal('242.0'),
            "watt_hours": Decimal('60'),
            "cost_usd": Decimal('60')
        }
        lambda_handler(payload, None)
    else:
        jstr = """{
    "path": "/query",
    "httpMethod": "POST",
    "headers": {
        "Accept": "*/*",
        "Authorization": "eyJraWQiOiJLTzRVMWZs",
        "content-type": "application/json; charset=UTF-8"
    },
    "queryStringParameters": null,
    "pathParameters": null,
    "requestContext": {
        "authorizer": {
            "claims": {
                "cognito:username": "the_username"
            }
        }
    },
    "body": "{\\"start\\": \\"2018-5-22\\", \\"end\\": \\"2018-7-20\\"}"
}"""
        lambda_handler(json.loads(jstr), None)