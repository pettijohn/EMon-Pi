from datetime import datetime, timedelta
from pytz import timezone
import pytz
from collections import OrderedDict
from decimal import Decimal
from monthdelta import monthdelta
from typing import List
import boto3
from boto3.dynamodb.conditions import Key, Attr


class BucketRule:
    """ Defines a rule for bucketing by time windows, e.g. minute, day, year """

    """ Func to get a table. Dynamo by default. Could be swapped out for a mock table. """
    def GetTable(self):
        return boto3.resource('dynamodb', region_name='us-east-1').Table("EnergyMonitor.PowerReadings")
        
    def __init__(self, eventTime: datetime, aggedFrom, values: dict):
        self.Grain = None
        self.BucketFormat = None
        self.AggedFrom = aggedFrom
        self.AggTo = BucketRule # Callable class to chain for next level aggregation
        self.EventTime = eventTime
        self.Values = values
        self.TimeFormat = "%Y-%m-%dT%H:%M%z"

    def BucketID(self) -> str:
        """ ID used in underlying storage, the start of the bucket's time window """
        return self.EventTime.strftime(self.BucketFormat)

    def BucketStartTime(self) -> datetime:
        return datetime.strptime(self.BucketID(), self.BucketFormat).replace(tzinfo=pytz.utc)

    def BucketEndTime(self) -> datetime:
        """ Bucket end time, INCLUSIVE. Must go to end of child bucket. """ 
        # Subclass must implement because Month can't be string formatted
        pass

    def CountInBucket(self) -> int:
        """ How many of the previous bucket are required to make this bucket? """
        # Subclass must implement
        pass

    def NextBucketStart(self) -> datetime:
        """ Start of next bucket. """
        # Subclass must implement
        pass

    def BucketDuration(self) -> timedelta:
        # Subclass must implement
        pass

    def GetChildren(self):
        # Find the start and end bucket IDs for this bucket
        childStartTime = self.BucketStartTime()
        childEndTime = self.BucketEndTime()
        # By using the bucket ID logic from the agged-from class for each time
        childStartBucket = type(self.AggedFrom)(childStartTime).BucketID()
        childEndBucket = type(self.AggedFrom)(childEndTime).BucketID()

        return self.AggedFrom.GetRange(childStartBucket, childEndBucket)
    
    def GetRange(self, startBucketID, endBucketID):
        table = self.GetTable()
        device_grain = '{0}|{1}'.format(self.Values['device_id'], self.Grain)
        rangeItems = table.query(
            KeyConditionExpression=Key('device_grain').eq(device_grain) & Key('bucket_id').between(startBucketID, endBucketID),
            ConsistentRead=True
        )
        return rangeItems['Items']


    def ProcessEvent(self, chain=True):
        """ Take an event, aggregate it, and call the next bucket(s) """
        # All rules other than Minute follow a standard pattern:
        # - Get the existing row for the bucket, if present
        # - Get all of the rows from the previous bucket that aggregate into this bucket
        # - Aggregate
        # - Save to table
        
        item = { 
            'device_grain': '{0}|{1}'.format(self.Values['device_id'], self.Grain),
            'grain': self.Grain,
            'device_id': self.Values['device_id'],
            'bucket_id': self.BucketID(),
            "amps": Decimal(0),
            "volts": Decimal(0),
            "watt_hours": Decimal(0),
            "cost_usd": Decimal(0)
        }

        # Get all of the constituent rows
        childRows = self.GetChildren()
        if len(childRows) != self.CountInBucket() and self.Grain == "Hour":
            print("WARN: Bucket {0} {1} has {2} children, expected {3}".format(self.Grain, self.BucketID(), len(childRows), self.CountInBucket()))

        # Update by averaging or summing 
        # Recompute each one by selecting all of its children
        # Selecting and re-agg'ing is self-healing and better than trying to only incrementally update
        item['amps'] = sum(map(lambda i: i['amps'] if 'amps' in i else i['current'], childRows)) / self.CountInBucket()
        item['volts'] = sum(map(lambda i: i['volts'], childRows)) / self.CountInBucket()
        item['watt_hours'] = sum(map(lambda i: i['watt_hours'], childRows))
        item['cost_usd'] = sum(map(lambda i: i['cost_usd'], childRows))

        table = self.GetTable()
        results = None
        # Put will insert or overwrite
        results = table.put_item(Item=item)

        if chain and self.AggTo is not None:
            # Aggregate to the next level
            nextLevel = self.AggTo(self.EventTime, self, item)
            return nextLevel.ProcessEvent(chain)
        else:
            return results
        
    def GetItem(self, bucketID = None) -> dict:
        """ Returns the item from the this bucket table """
        if bucketID == None:
            bucketID = self.BucketID()
        table = self.GetTable()
        
        response = table.get_item(
            Key={
                'device_grain': '{0}|{1}'.format(self.Values['device_id'], self.Grain),
                'bucket_id': bucketID
            },
            ConsistentRead=True
        )
        if 'Item' in response:
            return response['Item']
        else:
            return None

class EndViaFormatBucket():
    def BucketEndTime(self, bucketStartTime=None) -> datetime:
        """ For subclasses that can compute the last bucket with format (i.e. everything but Month) """
        if bucketStartTime is None:
            bucketStartTime = self.EventTime
        # EventTime is UTC, so LocalTimeFormat parses with +0000
        return datetime.strptime(bucketStartTime.strftime(self.BucketEndFormat), self.TimeFormat)#.replace(tzinfo=pytz.utc)
        
class EndViaDuration():
    def BucketEndTime(self, bucketStartTime=None) -> datetime:
        return super().BucketStartTime() + self.BucketDuration() - self.AggedFrom.BucketDuration()

class MinuteBucket(EndViaFormatBucket, BucketRule):
    def __init__(self, eventTime: datetime, values: dict=None):
        super().__init__(eventTime, None, values)
        self.Grain = "Minute"
        self.BucketFormat = "%Y-%m-%dT%H:%M%z"
        self.LegacyBucketFormat = "%Y-%m-%dT%H:%MZ"
        self.AggTo = HourBucket

    def CountInBucket(self) -> int:
        return 1 

    def BucketDuration(self) -> timedelta:
        return timedelta(minutes=1)

    def NextBucketStart(self) -> timedelta:
        return self.BucketStartTime() + timedelta(minutes=1)

    def ProcessEvent(self, chain=True, doInsert=True):
        # See if there is an existing item 
        item = self.GetItem()
        table = self.GetTable()
        if item is not None:
            # should not exist, but noop
            print("Warning: Found entry for minute {0} / {1}".format(self.Values['device_id'], self.BucketID()))
            # For consistency, set Values to the retrieved item
            self.Values = item
        results = None
        if item is None and doInsert:
            # Insert. Wo don't insert when re-aggregating. 
            results = table.put_item(Item=self.Values)
            
        if chain and self.AggTo is not None and type(self.AggTo) != BucketRule:
            # Aggregate to the next level
            nextLevel = self.AggTo(self.EventTime, self, self.Values)
            return nextLevel.ProcessEvent(chain)
        else:
            return results

class HourBucket(EndViaFormatBucket, BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule=None, values: dict=None):
        super().__init__(eventTime, aggedFrom, values)
        self.Grain = "Hour"
        self.BucketFormat = "%Y-%m-%dT%H:00%z"
        self.BucketEndFormat = "%Y-%m-%dT%H:59%z"
        self.AggTo = DayBucket

    def BucketDuration(self) -> timedelta:
        return timedelta(hours=1)
    
    def NextBucketStart(self) -> timedelta:
        return self.BucketStartTime() + timedelta(hours=1)

    def CountInBucket(self) -> int:
        return int((self.BucketEndTime() - self.BucketStartTime()).total_seconds()/60 + 1)

class LocalBucketRule():
    def DeviceTimeZone(self):
        # TODO extract to a lookup table if I ever have more than one device
        return pytz.timezone("America/Los_Angeles") 

    """ A timezone-aware bucket rule. Currently hard-coded to America/Los_Angeles """
    def BucketStartStop(self):
        # Fancy time zone logic:
        # Minute and Hour are in UTC
        # Everything else is in local time. Days in particular benefit from local 
        # time for user comprehension. 
        
        # Find the local start & stop times. BucketID includes offset. 
        localStart = datetime.strptime(self.BucketID(), self.BucketFormat) 
              
        baseEnd = super().BucketEndTime(localStart).replace(tzinfo=None)
        # Fixup the time zone
        localEnd = self.DeviceTimeZone().localize(baseEnd)
        
        # Convert to UTC
        utcStart = localStart.astimezone(pytz.utc)
        utcEnd = localEnd.astimezone(pytz.utc)

        return utcStart, utcEnd

    def BucketStartTime(self) -> datetime:
        utcStart, utcEnd = self.BucketStartStop()
        return utcStart

    def BucketEndTime(self) -> datetime:
        utcStart, utcEnd = self.BucketStartStop()
        return utcEnd      

    def BucketID(self) -> str:
        """ ID used in underlying storage, the start of the bucket's time window """
        # Get the start time of the bucket for proper time zone
        # First, get the local time in case it moves to a different bucket (such as yesterday)
        localEvent = self.EventTime.astimezone(self.DeviceTimeZone())
        # Then chop off the time zone, find the bucket start, and format in the proper time zone
        bucketStartNaive = datetime.strptime(localEvent.strftime(self.BucketFormat), self.BucketFormat).replace(tzinfo=None)
        bucketStartLocal = self.DeviceTimeZone().localize(bucketStartNaive)
        return bucketStartLocal.strftime(self.BucketFormat)

class DayBucket(LocalBucketRule, EndViaFormatBucket, BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule=None, values: dict=None):
        super().__init__(eventTime, aggedFrom, values)
        self.Grain = "Day"
        self.BucketFormat = "%Y-%m-%dT00:00%z"
        self.BucketEndFormat = "%Y-%m-%dT23:00%z"
        self.AggTo = MonthBucket
    
    def NextBucketStart(self) -> timedelta:
        return self.BucketEndTime() + self.AggedFrom.BucketDuration()

    def CountInBucket(self) -> int:
        return int((self.BucketEndTime() - self.BucketStartTime()).total_seconds()/3600 + 1)

    def BucketDuration(self) -> timedelta:
        return timedelta(days=1)

class MonthBucket(LocalBucketRule, EndViaDuration, BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule=None, values: dict=None):
        super().__init__(eventTime, aggedFrom, values)
        self.TableSuffix = "Month"
        self.BucketFormat = "%Y-%m-01T00:00%z"
        self.AggTo = YearBucket

    def NextBucketStart(self) -> timedelta:
        #return self.BucketStartTime() + monthdelta(1)
        return self.BucketEndTime() + self.AggedFrom.BucketDuration()

    def CountInBucket(self) -> int:
        # Use seconds and rounding accont for 30.958 day month when daylight saving time starts and 30.052 when ends
        return int(round((self.BucketEndTime() - self.BucketStartTime()).total_seconds()/60/60/24)+1)

    def BucketDuration(self) -> timedelta:
        return monthdelta(1)

class YearBucket(LocalBucketRule, EndViaFormatBucket, BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule=None, values: dict=None):
        super().__init__(eventTime, aggedFrom, values)
        self.Grain = "Year"
        self.BucketFormat = "%Y-01-01T00:00%z"
        self.BucketEndFormat = "%Y-12-01T00:00%z"
        self.AggTo = None

    def NextBucketStart(self) -> timedelta:
        return self.BucketStartTime().replace(year=self.BucketStartTime().year+1)

    def CountInBucket(self) -> int:
        return 12

    def BucketDuration(self) -> timedelta:
        return monthdelta(12)

def ConstructHierarchy(eventTime: datetime, values: dict):
    """ Given an event time and values, chain together the minute,
    hour, day, month, and year buckets. Return a list of the same:
    [0] == MinuteBucket, ... [4] == YearBucket. """
    # TODO - should this return a dict keyed by type instead of an array? 
    minute = MinuteBucket(eventTime, values)
    hour = HourBucket(eventTime, minute, values)
    day = DayBucket(eventTime, hour, values)
    month = MonthBucket(eventTime, day, values)
    year = YearBucket(eventTime, month, values)
    return [minute, hour, day, month, year]
    
def Query(startTime: datetime, endTime: datetime, values: dict, grain="auto"):
    """ Return up to about 60 rows of data depending on start & end time. 
    Returns appropriate granularity (minute, hour, day, month, year) 
    to achieve that aim."""
    # Determine granularity
    grainMap = {"year": 4,
        "month": 3,
        "day": 2,
        "hour": 1,
        "minute": 0,
        "auto": -1
    }
    duration = endTime - startTime
    if grain == "auto":
        if duration >= timedelta(days=365*5):
            grain = 4 # YearBucket
        elif duration >= timedelta(days=60):
            # 60 days through 60 months - return months
            grain = 3 # MonthBucket
        elif duration >= timedelta(days=3):
            # 3 to 60 days  - return days
            grain = 2 # DayBucket
        elif duration > timedelta(seconds=60*61):
            # 61 minutes to 72 hours return hours
            grain = 1 # HourBucket
        else:
            grain = 0 # MinuteBucket
    else:
        grain = grainMap[grain]

    # Construct the aggreation hierarchy
    starts = ConstructHierarchy(startTime, values)
    ends = ConstructHierarchy(endTime, values)

    # Then use the grain to construct start & end buckets for query
    queryStart, queryEnd = starts[grain], ends[grain]
    return queryStart.GetRange(queryStart.BucketID(), queryEnd.BucketID())