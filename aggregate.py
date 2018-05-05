from datetime import datetime, timedelta, timezone
from monthdelta import monthdelta

class Agg:
    def __init__(self, bucketFormatStr, fEndTime, fCount, cAggFrom):
        self.bucketFormatStr = bucketFormatStr
        self.fEndTime = fEndTime
        self.fCount = fCount
        self.cAggFrom = cAggFrom

    def BucketID(self, time):
        return time.strftime(self.bucketFormatStr)

    def BucketRange(self, time):
        """ Following the rules of the bucket, return the start (inclusive), end (exclusive), and count of subunits """
        bucketStart = datetime.strptime(self.BucketID(time), self.bucketFormatStr).replace(tzinfo=timezone.utc)
        bucketEnd = self.fEndTime(bucketStart)
        count = self.fCount(bucketEnd - bucketStart)
        return bucketStart, bucketEnd, count

    def IncrementAvg(self, time, prev, increment):
        bucketStart, bucketEnd, count = self.BucketRange(time)
        return prev + (increment / count)

    def IncrementSum(self, time, prev, increment):
        #bucketStart, bucketEnd, count = self.BucketRange(time)
        return prev + increment

# Define the rules for aggregation buckets.
minuteBucket = Agg("%Y-%m-%dT%H:%MZ", lambda t: t + timedelta(minutes=1), lambda d: d.total_seconds(), None)
hourBucket   = Agg("%Y-%m-%dT%H:00Z", lambda t: t + timedelta(hours=1), lambda d: d.total_seconds()/60, minuteBucket)
dayBucket    = Agg("%Y-%m-%dT00:00Z", lambda t: t + timedelta(days=1), lambda d: d.total_seconds()/3600, hourBucket)
# Note Day and Month go to 1, not zero
monthBucket  = Agg("%Y-%m-01T00:00Z", lambda t: t + monthdelta(1), lambda d: d.days, dayBucket)
# Year is 365 days -- not 12 months, since months are irregularly sized 
yearBucket   = Agg("%Y-01-01T00:00Z", lambda t: t.replace(year=t.year+1), lambda d: d.days, dayBucket)
# Put them all in an array--order matters! An update in the first cascades all the way up
#buckets = [minuteBucket, hourBucket, dayBucket, monthBucket, yearBucket]

# Unit tests
if (__name__ == "__main__"):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    print (now)
    
    assert minuteBucket.IncrementSum(now, 2, 2) == 4
    #assert minuteBucket.IncrementAvg(now, 1, 3) == 2

