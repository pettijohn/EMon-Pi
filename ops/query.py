import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import awslambda.aggregate as aggregate
from datetime import datetime


minute = aggregate.MinuteBucket(datetime.utcnow())
hour = aggregate.HourBucket(datetime.utcnow(), minute)
hour.GetChildren({})