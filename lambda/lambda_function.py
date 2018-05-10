import boto3
import copy
from decimal import Decimal
import json
from amazon.ion import simpleion
from io import StringIO
import aggregate

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
        
# def lambda_handler(event, context):
#     payloadValue = simpleion.load(StringIO(event))
#     c = copy.deepcopy(payloadValue)

#     table = boto3.resource('dynamodb').Table('EnergyMonitor.Minute')
#     return table.put_item(Item=payloadValue) 

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('EnergyMonitor.Minute')
    
    
    eventParsed = json.loads(json.dumps(event, cls=DecimalEncoder), parse_float=Decimal)
    return table.put_item(Item=eventParsed)

if __name__ == "__main__":
    arn = "TestMessages"
    current = Decimal(str(0.002))
    volts = Decimal(str(242.0))
    rate = Decimal(str(0.1326))
    payload = { "device_id": arn,
        "bucket_id": "2018-05-09T19:57Z",
        "current": (current),
        "volts": (volts),
        "watt_hours": (Decimal(str(current*volts/60))),
        "cost_usd": Decimal(str(current*volts/60*rate))
    }

    t = aggregate.Table('EnergyMonitor.Minute', aggregate.AllBuckets.MinuteBucket)
    t.ProcessRow(payload)
    

    lambda_handler(payload, None)




