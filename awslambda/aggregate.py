from datetime import datetime, timedelta
from pytz import timezone
import pytz
from collections import OrderedDict
from decimal import Decimal
from monthdelta import monthdelta
from typing import List
import boto3
from boto3.dynamodb.conditions import Key, Attr

class MockTable:
    def __init__(self, tableName):
        self.TableName = tableName

    """ Mocks boto3's dynamodb table. """
    def get_item(self, **kwargs):
        return {}

    def put_item(self, **kwargs):
        print(kwargs)

class BucketRule:
    """ Defines a rule for bucketing by time windows, e.g. minute, day, year """

    """ Func to get a table. Dynamo by default. Can be swapped out for mock table. """
    def GetTable(self, tableSuffix):
        return boto3.resource('dynamodb').Table("EnergyMonitor." + tableSuffix)
        
    def __init__(self, eventTime: datetime, aggedFrom, values: dict):
        """ Table is EnergyMonitor.<TableSuffix> """
        self.TableSuffix = None
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

        # The child bucket
        childTable = self.GetTable(self.AggedFrom.TableSuffix)
        children = childTable.query(
            KeyConditionExpression=Key('device_id').eq(self.Values['device_id']) & Key('bucket_id').between(childStartBucket, childEndBucket)
        )
        return children['Items']
        #return children

    def ProcessEvent(self, chain=True):
        """ Take an event, aggregate it, and call the next bucket(s) """
        # All rules other than Minute follow a standard pattern:
        # - Get the existing row for the bucket, if present
        # - Get all of the rows from the previous bucket that aggregate into this bucket
        # - Aggregate
        # - Save to table
        item = self.GetItem()
        insert = False
        if item is None:
            # Initialize empty item
            insert = True
            item = { 
                'device_id': self.Values['device_id'],
                'bucket_id': self.BucketID(),
                "amps": Decimal(0),
                "volts": Decimal(0),
                "watt_hours": Decimal(0),
                "cost_usd": Decimal(0)
            }

        # Get all of the constituent rows
        childRows = self.GetChildren()
        if len(childRows) != self.CountInBucket() and self.TableSuffix == "Hour":
            print("WARN: Bucket {0} {1} has {2} children, expected {3}".format(self.TableSuffix, self.BucketID(), len(childRows), self.CountInBucket()))

        # Update by averaging or summing 
        # Recompute each one by selecting all of its children
        # Selecting and re-agg'ing is self-healing and better than trying to only incrementally update
        # FIXME amps and current both until reagg completes
        # item['amps'] = sum(map(lambda i: i['amps'], childRows)) / self.CountInBucket()
        item['amps'] = sum(map(lambda i: i['amps'] if 'amps' in i else i['current'], childRows)) / self.CountInBucket()
        item['volts'] = sum(map(lambda i: i['volts'], childRows)) / self.CountInBucket()
        item['watt_hours'] = sum(map(lambda i: i['watt_hours'], childRows))
        item['cost_usd'] = sum(map(lambda i: i['cost_usd'], childRows))

        table = self.GetTable(self.TableSuffix)
        results = None
        if(insert):
             results = table.put_item(Item=item)
        else:
            results = table.update_item(
                Key={
                    'device_id': item['device_id'],
                    'bucket_id': item['bucket_id']
                },
                UpdateExpression="set amps=:a, volts=:v, watt_hours=:w, cost_usd=:u",
                ExpressionAttributeValues={
                    ':a': item['amps'],
                    ':v': item['volts'],
                    ':w': item['watt_hours'],
                    ':u': item['cost_usd']
                },
                ReturnValues="UPDATED_NEW"
            )

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
        table = self.GetTable(self.TableSuffix)
        
        response = table.get_item(
            Key={
                'device_id': self.Values['device_id'],
                'bucket_id': bucketID
            },
            ConsistentRead=True #2018-05-28T00
        )
        if 'Item' in response:
            return response['Item']
        else:
            return None

class EndViaFormatBucket():
    def BucketEndTime(self) -> datetime:
        """ For subclasses that can compute the last bucket with format (i.e. everything but Month) """
        # EventTime is UTC, so LocalTimeFormat parses with +0000
        return datetime.strptime(self.EventTime.strftime(self.BucketEndFormat), self.TimeFormat).replace(tzinfo=pytz.utc)
        
class EndViaDuration():
    def BucketEndTime(self) -> datetime:
        return super().BucketStartTime() + self.BucketDuration() - self.AggedFrom.BucketDuration()

class MinuteBucket(EndViaFormatBucket, BucketRule):
    def __init__(self, eventTime: datetime, values: dict=None):
        super().__init__(eventTime, None, values)
        self.TableSuffix = "Minute"
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
        table = self.GetTable(self.TableSuffix)
        # FIXME - get rid of legacy bucket ID check & migration
        if item is None:
            # Check for legacy bucket ID
            legacyID = self.EventTime.strftime(self.LegacyBucketFormat)
            item = self.GetItem(legacyID)
            if item is not None:
                # Found legacy - migrate it
                # We use a new bucket ID format and use 'amps' instead of 
                # 'current' because the latter is a reserved word in dynamodb
                item['bucket_id'] = self.BucketID()
                item['amps'] = item.pop('current')
                table.put_item(Item=item)
                table.delete_item(Key={
                        'device_id': item['device_id'],
                        'bucket_id': legacyID
                    })

        if item is None and doInsert:
            # If still none, insert. Wo don't insert when re-aggregating. 
            results = table.put_item(Item=self.Values)
            
        if chain and self.AggTo is not None and type(self.AggTo) != BucketRule:
            # Aggregate to the next level
            nextLevel = self.AggTo(self.EventTime, self, item)
            return nextLevel.ProcessEvent(chain)
        else:
            return results

class HourBucket(EndViaFormatBucket, BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule=None, values: dict=None):
        super().__init__(eventTime, aggedFrom, values)
        self.TableSuffix = "Hour"
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
        baseEnd = super().BucketEndTime().replace(tzinfo=None)
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
        self.TableSuffix = "Day"
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
        self.TableSuffix = "Year"
        self.BucketFormat = "%Y-01-01T00:00%z"
        self.BucketEndFormat = "%Y-12-01T00:00%z"
        self.AggTo = None

    def NextBucketStart(self) -> timedelta:
        return self.BucketStartTime().replace(year=self.BucketStartTime().year+1)

    def CountInBucket(self) -> int:
        return 12

    def BucketDuration(self) -> timedelta:
        return monthdelta(12)
