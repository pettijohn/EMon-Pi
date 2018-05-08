from datetime import datetime, timedelta, timezone
from monthdelta import monthdelta
from typing import List

class BucketRule:
    """ Defines a rule for bucketing by time windows, e.g. minute, day, year """
    def __init__(self, bucketFormat: str, aggedFrom: type):
        self.BucketFormat = bucketFormat
        self.AggedFrom = aggedFrom
    def BucketID(self, time: datetime) -> str:
        """ ID used in underlying storage """
        return time.strftime(self.BucketFormat)
    def BucketStartTime(self, time:datetime) -> datetime:
        return datetime.strptime(self.BucketID(time), self.BucketFormat).replace(tzinfo=timezone.utc)
    def BucketEndTime(self, time: datetime) -> datetime:
        pass
    def CountInBucket(self, time: datetime) -> int:
        pass

class MinuteBucket(BucketRule):
    def __init__(self):
        super().__init__("%Y-%m-%dT%H:%MZ", MinuteBucket)

    def BucketEndTime(self, time:datetime) -> datetime:
        return self.BucketStartTime(time) + timedelta(minutes=1)
    
    def CountInBucket(self, time: datetime) -> int:
        return 1 #(self.BucketEndTime(time) - self.BucketStartTime(time)).total_seconds()

class HourBucket(BucketRule):
    def __init__(self):
        super().__init__("%Y-%m-%dT%H:00Z", MinuteBucket)

    def BucketEndTime(self, time:datetime) -> datetime:
        return self.BucketStartTime(time) + timedelta(hours=1)
    
    def CountInBucket(self, time: datetime) -> int:
        return (self.BucketEndTime(time) - self.BucketStartTime(time)).total_seconds()/60

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

class AggField:
    def __init__(self):
        pass
    def AggRule(self, time, ):
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