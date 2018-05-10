from datetime import datetime, timedelta, timezone
from monthdelta import monthdelta
from typing import List
import boto3

class BucketRule:
    """ Defines a rule for bucketing by time windows, e.g. minute, day, year """
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
    def ProcessEvent(self, time: datetime, values: dict):
        """ Take an event, aggregate it, and call the next bucket(s) """
        pass
    def GetItem(self, eventTime: datetime, values: dict) -> dict:
        """ Returns the item from the this bucket table """
        dynamodb = boto3.resource('dynamodb')
        dynamodb.Table("EnergyMonitor." + self.TableSuffix)
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

def foo():
    pass

class Table:
    def __init__(self, tableName, bucketRule):
        self.TableName = tableName
        self.BucketRule = bucketRule
        #self.Fields = fields

    def ProcessRow(self, values: dict):
        dynamodb = boto3.resource('dynamodb')
        dynamodb.Table(self.TableName)
        response = table.get_item(
            Key={
                'device_id': values['device_id'],
                'bucket_id': values['bucket_id']
            }
        )
        if 'Item' in response:
            # Got a matching row
        else:
            # Insert
        

class AggRule:
    def Agg(self, time, prev, increment):
        pass

class Unity(AggRule):
    """ Returns the increment; noop """
    def Agg(self, time, prev, increment):
        return increment

class Sum(AggRule):
    """ Returns prev + increment """
    def Agg(self, time, prev, increment):
        return prev + increment

class AllTables:
    Minute = Table('EnergyMonitor.Minute', MinuteBucket(), AggField("device_id", ))

class AggField:
    def __init__(self, fieldName, fAggRule):
        pass



class MinuteReading:
    def __init__(self, volts: float, amps: float):
        self.Volts = volts
        self.Amps = amps
        self.WattHours = volts * amps / 60.0

# class Foo:
#     def IncrementAvg(self, time, prev, increment):
#         bucketStart, bucketEnd, count = self.BucketRange(time)
#         return prev + (increment / count)

#     def IncrementSum(self, time, prev, increment):
#         #bucketStart, bucketEnd, count = self.BucketRange(time)
#         return prev + increment


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