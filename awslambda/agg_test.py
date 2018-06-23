from aggregate import *
from datetime import datetime
import pytz

baseCase     = datetime(2018,5,5,13,39,12, tzinfo=pytz.utc)
leapyearCase = datetime(2016,2,29,13,39,12, tzinfo=pytz.utc)

mb = MinuteBucket(baseCase, {})
assert mb.BucketID() == "2018-05-05T13:39+0000" # UTC
#assert mb.BucketEndTime() == datetime(2018,5,5,13,39,59, tzinfo=pytz.utc)
assert mb.NextBucketStart() == datetime(2018,5,5,13,40,0, tzinfo=pytz.utc)
assert mb.CountInBucket() == 1
assert type(mb.CountInBucket()) == int

hb = HourBucket(baseCase, mb, {})
assert hb.BucketID() == "2018-05-05T13:00+0000" # UTC
assert hb.BucketEndTime() == datetime(2018,5,5,13,59, tzinfo=pytz.utc)
assert hb.NextBucketStart() == datetime(2018,5,5,14,00, tzinfo=pytz.utc)
assert hb.CountInBucket() == 60
assert type(hb.CountInBucket()) == int

db = DayBucket(baseCase, hb, {})
assert db.BucketID() == "2018-05-05T00:00-0700" # Local
assert db.BucketStartTime() == datetime(2018,5,5,7,0, tzinfo=pytz.utc)
assert db.BucketEndTime()   == datetime(2018,5,6,6,0, tzinfo=pytz.utc)
assert db.NextBucketStart() == datetime(2018,5,6,7,0, tzinfo=pytz.utc)
assert db.CountInBucket() == 24
assert type(db.CountInBucket()) == int

mt = MonthBucket(baseCase, db, {})
assert mt.BucketID() == "2018-05-01T00:00-0700"
assert mt.BucketStartTime() == datetime(2018,5,1,7,0, tzinfo=pytz.utc)
assert mt.BucketEndTime()   == datetime(2018,5,31,7,0, tzinfo=pytz.utc)
assert mt.NextBucketStart() == datetime(2018,6,1,7,0, tzinfo=pytz.utc)
assert mt.CountInBucket() == 31
assert type(mt.CountInBucket()) == int

yb = YearBucket(baseCase, mt, {})
assert yb.BucketID() == "2018-01-01T00:00-0800"
assert yb.BucketStartTime() == datetime(2018,1,1,8,0, tzinfo=pytz.utc)
assert yb.BucketEndTime()   == datetime(2018,12,1,8,0, tzinfo=pytz.utc)
assert yb.NextBucketStart() == datetime(2019,1,1,8,0, tzinfo=pytz.utc)
assert yb.CountInBucket() == 12
assert type(yb.CountInBucket()) == int

ml = MonthBucket(leapyearCase, DayBucket(leapyearCase, None, {}), {})
assert ml.BucketID() == "2016-02-01T00:00-0800"
assert ml.BucketStartTime() == datetime(2016,2,1,8,0, tzinfo=pytz.utc)
assert ml.BucketEndTime()   == datetime(2016,2,29,8,0, tzinfo=pytz.utc)
assert ml.NextBucketStart() == datetime(2016,3,1,8,0, tzinfo=pytz.utc)
assert ml.CountInBucket() == 29
assert type(ml.CountInBucket()) == int

yl = YearBucket(leapyearCase, ml, {})
assert yl.BucketID() == "2016-01-01T00:00-0800"
assert yl.BucketStartTime() == datetime(2016,1,1,8,0, tzinfo=pytz.utc)
assert yl.BucketEndTime()   == datetime(2016,12,1,8,0, tzinfo=pytz.utc)
assert yl.NextBucketStart() == datetime(2017,1,1,8,0, tzinfo=pytz.utc)
assert yl.CountInBucket() == 12
assert type(yl.CountInBucket()) == int

dstStartCase = datetime(2018,3,11,9,59,0, tzinfo=pytz.utc) # One minute before DST start
dst = DayBucket(dstStartCase, HourBucket(dstStartCase, None, {}), {})
assert dst.BucketID() == "2018-03-11T00:00-0800"
assert dst.BucketStartTime() == datetime(2018,3,11,8,0, tzinfo=pytz.utc)
assert dst.BucketEndTime()   == datetime(2018,3,12,6,0, tzinfo=pytz.utc)
assert dst.NextBucketStart() == datetime(2018,3,12,7,0, tzinfo=pytz.utc)
assert dst.CountInBucket() == 23
assert type(dst.CountInBucket()) == int

dstmo = MonthBucket(dstStartCase, dst, {})
assert dstmo.BucketID() == "2018-03-01T00:00-0800"
assert dstmo.BucketStartTime() == datetime(2018,3,1,8,0, tzinfo=pytz.utc)
assert dstmo.BucketEndTime()   == datetime(2018,3,31,7,0, tzinfo=pytz.utc)
assert dstmo.NextBucketStart() == datetime(2018,4,1,7,0, tzinfo=pytz.utc)
assert dstmo.CountInBucket() == 31
assert type(dstmo.CountInBucket()) == int

dstEndCase = datetime(2018,11,4,8,59,0, tzinfo=pytz.utc) # One minute before DST ends
dst = DayBucket(dstEndCase, HourBucket(dstStartCase, None, {}), {})
assert dst.BucketID() == "2018-11-04T00:00-0700"
assert dst.BucketStartTime() == datetime(2018,11,4,7,0, tzinfo=pytz.utc)
assert dst.BucketEndTime()   == datetime(2018,11,5,7,0, tzinfo=pytz.utc)
assert dst.NextBucketStart() == datetime(2018,11,5,8,0, tzinfo=pytz.utc)
assert dst.CountInBucket() == 25
assert type(dst.CountInBucket()) == int

dstmo = MonthBucket(dstEndCase, dst, {})
assert dstmo.BucketID() == "2018-11-01T00:00-0700"
assert dstmo.BucketStartTime() == datetime(2018,11,1,7,0, tzinfo=pytz.utc)
assert dstmo.BucketEndTime()   == datetime(2018,11,30,8,0, tzinfo=pytz.utc)
assert dstmo.NextBucketStart() == datetime(2018,12,1,8,0, tzinfo=pytz.utc)
assert dstmo.CountInBucket() == 30
assert type(dstmo.CountInBucket()) == int


localYesterday = datetime(2018,6,16,0,0,0, tzinfo=pytz.utc)

mb = MinuteBucket(localYesterday, {})
assert mb.BucketID() == "2018-06-16T00:00+0000" # UTC

hb = HourBucket(localYesterday, mb, {})
assert hb.BucketID() == "2018-06-16T00:00+0000" # UTC

db = DayBucket(localYesterday, hb, {})
assert db.BucketID() == "2018-06-15T00:00-0700" # Local
assert db.BucketStartTime() == datetime(2018,6,15,7,0,0, tzinfo=pytz.utc)
assert db.BucketEndTime() == datetime(2018,6,16,6,0,0, tzinfo=pytz.utc)

localLastMonth = datetime(2018,3,1,0,0,0, tzinfo=pytz.utc)

mb = MinuteBucket(localLastMonth, {})
assert mb.BucketID() == "2018-03-01T00:00+0000" # UTC

hb = HourBucket(localLastMonth, mb, {})
assert hb.BucketID() == "2018-03-01T00:00+0000" # UTC

db = DayBucket(localLastMonth, hb, {})
assert db.BucketID() == "2018-02-28T00:00-0800" # Local
assert db.BucketStartTime() == datetime(2018,2,28,8,0,0, tzinfo=pytz.utc)
assert db.BucketEndTime() == datetime(2018,3,1,7,0,0, tzinfo=pytz.utc)

mt = MonthBucket(localLastMonth, db, {})
assert mt.BucketID() == "2018-02-01T00:00-0800" # Local
assert mt.BucketStartTime() == datetime(2018,2,1,8,0,0, tzinfo=pytz.utc)
assert mt.BucketEndTime() == datetime(2018,2,28,8,0,0, tzinfo=pytz.utc)

localLastYear = datetime(2018,1,1,1,1,1, tzinfo=pytz.utc)#1:01:01am new year's day UTC, 5pm PDT NYE

mb = MinuteBucket(localLastYear, {})
assert mb.BucketID() == "2018-01-01T01:01+0000" # UTC

hb = HourBucket(localLastYear, mb, {})
assert hb.BucketID() == "2018-01-01T01:00+0000" # UTC

db = DayBucket(localLastYear, hb, {})
assert db.BucketID() == "2017-12-31T00:00-0800" # Local
assert db.BucketStartTime() == datetime(2017,12,31,8,0,0, tzinfo=pytz.utc)
assert db.BucketEndTime()   == datetime(2018, 1, 1,7,0,0, tzinfo=pytz.utc)

mt = MonthBucket(localLastYear, db, {})
assert mt.BucketID() == "2017-12-01T00:00-0800" # Local
assert mt.BucketStartTime() == datetime(2017,12, 1,8,0,0, tzinfo=pytz.utc)
assert mt.BucketEndTime()   == datetime(2017,12,31,8,0,0, tzinfo=pytz.utc)

yb = YearBucket(localLastYear, mt, {})
assert yb.BucketID() == "2017-01-01T00:00-0800" # Local
assert yb.BucketStartTime() == datetime(2017, 1, 1,8,0,0, tzinfo=pytz.utc)
assert yb.BucketEndTime()   == datetime(2017,12, 1,8,0,0, tzinfo=pytz.utc)


# TODO - add fall back case 

# Override the dynamo table with mock for testing
BucketRule.GetTable = lambda self, tableName: MockTable(tableName)
#MockTable.get_item = lambda self,  **kwargs: {} # Should not be a matching row

# mb.ProcessEvent(
#     { "device_id": "TestEvent",
#         "bucket_id": baseCase.strftime("%Y-%m-%dT%H:%MZ"),
#         "current": Decimal('1.1'),
#         "volts": Decimal('242.0'),
#         "watt_hours": Decimal('266.2')/60,
#         "cost_usd": Decimal('266.2')/60*Decimal('0.1326')/Decimal(1000)
#     }
# )