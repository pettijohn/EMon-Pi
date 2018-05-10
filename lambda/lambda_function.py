import boto3
import copy
from decimal import Decimal
import json
from amazon.ion import simpleion
from io import StringIO

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
        
def myDeepCopy(values):
    z = {}
    for k in values.keys():
        t = type(k)
        z[k] = copy.deepcopy(t(values[k]))

def lambda_handler(event, context):
    payloadValue = simpleion.load(StringIO(event))
    z = myDeepCopy(payloadValue)
    c = copy.deepcopy(z)

    quit()
    table = boto3.resource('dynamodb').Table('EnergyMonitor.Minute')
    return table.put_item(Item=payloadValue) 

def bug():
    d = { "value": Decimal('1.1') }
    payload = simpleion.dumps(d)
    payloadValue = simpleion.load(StringIO(payload))
    c = copy.deepcopy(payloadValue)

if __name__ == "__main__":
    arn = "TestMessages"
    current = Decimal(str(0.002))
    volts = Decimal(str(242.0))
    rate = Decimal(str(0.1326))
    payload = { "device_id": arn,
        "bucket_id": "2018-05-08T20:10Z",
        "current": (current),
        "volts": (volts),
        "watt_hours": (Decimal(str(current*volts/60))),
        "cost_usd": Decimal(str(current*volts/60*rate))
    }
    payload = simpleion.dumps(payload)

    lambda_handler(payload, None)

def deadCode(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('EnergyMonitor.Minute')
    
    # below works - event is a dict - serialize it back to JSON and then parse back with Decimal handling. 
    print(type(event))
    
    eventParsed = json.loads(json.dumps(event, cls=DecimalEncoder), parse_float=decimal.Decimal)
    print(eventParsed)
    #eventWDecimal = json.dumps(eventParsed)
    return table.put_item(Item=eventParsed) 


