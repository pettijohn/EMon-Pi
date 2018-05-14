from datetime import datetime, timedelta, timezone
from decimal import Decimal
from monthdelta import monthdelta
from typing import List
import boto3

class MockTable:
    def __init__(self, tableName):
        self.TableName = tableName

    """ Mocks boto3's dynamodb table. """
    def get_item(self, **kwargs):
        assert "Key" in kwargs
        Key = kwargs["Key"]
        assert ['device_id']
        assert ['bucket_id']

    def put_item(self, **kwargs):
        pass


class BucketRule:
    """ Defines a rule for bucketing by time windows, e.g. minute, day, year """

    """ Func to get a table. Dynamo by default. Can be swapped out for mock table. """
    def GetTable(self, tableName):
        return boto3.resource('dynamodb').Table(tableName)

    def __init__(self, tableSuffix: str, bucketFormat: str, aggedFrom: type, *aggedTo):
        self.TableSuffix = tableSuffix
        self.BucketFormat = bucketFormat
        self.AggedFrom = aggedFrom
        self.AggedTo = aggedTo
    def BucketID(self, time: datetime) -> str:
        """ ID used in underlying storage """
        return datetime.strftime(self.BucketFormat)
    def BucketStartTime(self, time:datetime) -> datetime:
        return datetime.strptime(self.BucketID(time), self.BucketFormat).replace(tzinfo=timezone.utc)
    def BucketEndTime(self, time: datetime) -> datetime:
        pass
    def CountInBucket(self, time: datetime) -> int:
        """ How many of the previous bucket are required to make this bucket? """
        pass
    def ProcessEvent(self, prevBucket: BucketRule, eventTime: datetime, values: dict):
        """ Take an event, aggregate it, and call the next bucket(s) """
        # All rules other than Minute follow a standard pattern:
        # - Get the existing row for the bucket, if present
        # - If an average rule, divide this share and add
        # - Else if a sum rule, just add
        # - Save back to table
        item = self.GetItem(eventTime, values)
        insert = False
        if item is None:
            # Set everything to zero and insert
            insert = True
            item = { 
                'device_id': values['device_id'],
                'bucket_id': self.BucketID(eventTime),
                "current": Decimal(0),
                "volts": Decimal(0),
                "watt_hours": Decimal(0),
                "cost_usd": Decimal(0)
            }

        # Update by averaging or summing 
        # FIXME this pattern has bugs. If I insert to the minute bucket and then sum to the hour bucket,
        #  the next minute bucket will re-add the aggregated values.
        # Should I add the minute bucket to each bucket?
        # Or recompute each one by selecting everything each time?
        # Selecting and re-agg'ing seems more reliable, but may incure higher DynamoDB costs
        item['current'] = item['current'] + (values['current'] / Decimal(self.CountInBucket))
        item['volts'] = item['volts'] + (values['volts'] / Decimal(self.CountInBucket))
        item['watt_hours'] = item['watt_hours'] + values['watt_hours']
        item['cost_usd'] = item['cost_usd'] + values['cost_usd']

        table = self.GetTable("EnergyMonitor." + self.TableSuffix)
        if(insert):
            table.put_item(Item=values)
        else:
            response = table.update_item(
                Key={
                    'device_id': item['device_id'],
                    'bucket_id': item['bucket_id']
                },
                UpdateExpression="set current=:c, volts=:v, watt_hours=:w, cost_usd=:u",
                ExpressionAttributeValues={
                    ':c': item['current'],
                    ':v': item['volts'],
                    ':w': item['watt_hours'],
                    ':u': item['cost_usd']
                },
                ReturnValues="UPDATED_NEW"
            )

        # Chain and call the next level of buckets in the hierarchy
        for to in self.AggedTo:
            nextBucket = to()
            # FIXME - passing item to the next level is wrong
            nextBucket.ProcessEvent(self, eventTime, item)
        
    def GetItem(self, eventTime: datetime, values: dict) -> dict:
        """ Returns the item from the this bucket table """
        table = self.GetTable("EnergyMonitor." + self.TableSuffix)
        
        values['bucket_id'] = self.BucketID(eventTime)
        response = table.get_item(
            Key={
                'device_id': values['device_id'],
                'bucket_id': values['bucket_id']
            }
        )
        if 'Item' in response:
            return response['Item']
        else:
            return None

class MinuteBucket(BucketRule):
    def __init__(self):
        super().__init__("Minute", "%Y-%m-%dT%H:%MZ", None, HourBucket)

    def BucketEndTime(self, time:datetime) -> datetime:
        return self.BucketStartTime(time) + timedelta(minutes=1)
    
    def CountInBucket(self, time: datetime) -> int:
        return 1 #(self.BucketEndTime(time) - self.BucketStartTime(time)).total_seconds()

    def ProcessEvent(self, prevBucket: BucketRule, eventTime: datetime, values: dict):
        item = self.GetItem(values)
        if item is not None:
            # Got a matching row
            # This is an error for Minute bucket, but let's just log and ignore
            print("Error - found row that shouldn't exist with values {0} ...... Skiping writing {1}".format(str(response['Item']), str(values)))
        else:
            table = self.GetTable("EnergyMonitor." + self.TableSuffix)
            table.put_item(Item=values)
        
        # Chain and call the next level of buckets in the hierarchy
        for to in self.AggedTo:
            nextBucket = to()
            nextBucket.ProcessEvent(self, eventTime, values)

class HourBucket(BucketRule):
    def __init__(self):
        super().__init__("%Y-%m-%dT%H:00Z", MinuteBucket, DayBucket)

    def BucketEndTime(self, time:datetime) -> datetime:
        return self.BucketStartTime(time) + timedelta(hours=1)
    
    def CountInBucket(self, time: datetime) -> int:
        return (self.BucketEndTime(time) - self.BucketStartTime(time)).total_seconds()/60

    def ProcessEvent(self, prevBucket: BucketRule, eventTime: datetime, values: dict):
        pass


class DayBucket(BucketRule):
    def __init__(self):
        super().__init__("%Y-%m-%dT00:00Z", HourBucket)

    def BucketEndTime(self, time:datetime) -> datetime:
        return self.BucketStartTime(time) + timedelta(days=1)
    
    def CountInBucket(self, time: datetime) -> int:
        return (self.BucketEndTime(time) - self.BucketStartTime(time)).total_seconds()/3600

class MonthBucket(BucketRule):
    def __init__(self):
        super().__init__("%Y-%m-01T00:00Z", DayBucket)

    def BucketEndTime(self, time:datetime) -> datetime:
        return self.BucketStartTime(time) + monthdelta(1)
    
    def CountInBucket(self, time: datetime) -> int:
        return (self.BucketEndTime(time) - self.BucketStartTime(time)).days

class YearBucket(BucketRule):
    def __init__(self):
        super().__init__("%Y-01-01T00:00Z", DayBucket)

    def BucketEndTime(self, time:datetime) -> datetime:
        return self.BucketStartTime(time).replace(year=self.BucketStartTime(time).year+1)
    
    def CountInBucket(self, time: datetime) -> int:
        return (self.BucketEndTime(time) - self.BucketStartTime(time)).days

class AllBuckets:
    MinuteBucket = MinuteBucket()
    HourBucket = HourBucket()
    DayBucket = DayBucket()
    MonthBucket = MonthBucket()
    YearBucket = YearBucket()
    All = [MinuteBucket, HourBucket, DayBucket, MonthBucket, YearBucket]



class AllTables:
    Minute = Table('EnergyMonitor.Minute', MinuteBucket(), AggField("device_id", ))



# Tests
if (__name__ == "__main__"):
    baseCase     = datetime(2018,5,5,13,39,12, tzinfo=timezone.utc)
    leapyearCase = datetime(2016,2,29,13,39,12, tzinfo=timezone.utc)
    dstStartCase = datetime(2018,3,11,2,39,12, tzinfo=timezone.utc) # No effect, since UTC

    assert AllBuckets.MinuteBucket.BucketID(baseCase) == "2018-05-05T13:39Z"
    assert AllBuckets.MinuteBucket.BucketEndTime(baseCase) == datetime(2018,5,5,13,40, tzinfo=timezone.utc)
    assert AllBuckets.MinuteBucket.CountInBucket(baseCase) == 1

    assert AllBuckets.HourBucket.BucketID(baseCase) == "2018-05-05T13:00Z"
    assert AllBuckets.HourBucket.BucketEndTime(baseCase) == datetime(2018,5,5,14,0, tzinfo=timezone.utc)
    assert AllBuckets.HourBucket.CountInBucket(baseCase) == 60

    assert AllBuckets.DayBucket.BucketID(baseCase) == "2018-05-05T00:00Z"
    assert AllBuckets.DayBucket.BucketEndTime(baseCase) == datetime(2018,5,6,0,0, tzinfo=timezone.utc)
    assert AllBuckets.DayBucket.CountInBucket(baseCase) == 24

    assert AllBuckets.MonthBucket.BucketID(baseCase) == "2018-05-01T00:00Z"
    assert AllBuckets.MonthBucket.BucketEndTime(baseCase) == datetime(2018,6,1,0,0, tzinfo=timezone.utc)
    assert AllBuckets.MonthBucket.CountInBucket(baseCase) == 31

    assert AllBuckets.YearBucket.BucketID(baseCase) == "2018-01-01T00:00Z"
    assert AllBuckets.YearBucket.BucketEndTime(baseCase) == datetime(2019,1,1,0,0, tzinfo=timezone.utc)
    assert AllBuckets.YearBucket.CountInBucket(baseCase) == 365

    assert AllBuckets.MonthBucket.BucketID(leapyearCase) == "2016-02-01T00:00Z"
    assert AllBuckets.MonthBucket.BucketEndTime(leapyearCase) == datetime(2016,3,1,0,0, tzinfo=timezone.utc)
    assert AllBuckets.MonthBucket.CountInBucket(leapyearCase) == 29

    assert AllBuckets.YearBucket.BucketID(leapyearCase) == "2016-01-01T00:00Z"
    assert AllBuckets.YearBucket.BucketEndTime(leapyearCase) == datetime(2017,1,1,0,0, tzinfo=timezone.utc)
    assert AllBuckets.YearBucket.CountInBucket(leapyearCase) == 366

    # Override the dynamo table with mock for testing
    BucketRule.GetTable = lambda self, tableName: MockTable(tableName)