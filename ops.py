import os, sys
#sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import awslambda.aggregate as aggregate
from datetime import datetime, timedelta, timezone
from decimal import Decimal

arn = "arn:aws:iot:us-east-1:422446087002:thing/EMonPi"
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