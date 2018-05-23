from datetime import datetime, timedelta, timezone
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
    def GetTable(self, tableName):
        return boto3.resource('dynamodb').Table(tableName)
        
    def __init__(self, eventTime: datetime, aggedFrom):
        """ Table is EnergyMonitor.<TableSuffix> """
        self.TableSuffix = None
        self.BucketFormat = None
        self.AggedFrom = aggedFrom
        self.AggTo = None # Callable class to chain for next level aggregation
        self.EventTime = eventTime

    def BucketID(self) -> str:
        """ ID used in underlying storage, the start of the bucket's time window """
        return self.EventTime.strftime(self.BucketFormat)
    def BucketStartTime(self) -> datetime:
        return datetime.strptime(self.BucketID(), self.BucketFormat).replace(tzinfo=timezone.utc)
    def BucketEndTime(self) -> datetime:
        # Subclass must implement
        pass
    def CountInBucket(self) -> int:
        """ How many of the previous bucket are required to make this bucket? """
        # Subclass must implement
        pass

    def GetChildren(self, values: dict):
        # Find the start and end bucket IDs for this bucket
        childStart = self.BucketID()
        childEnd = self.BucketEndTime().strftime(self.BucketFormat)
        # The child bucket
        childTable = self.GetTable(self.AggedFrom.TableSuffix)
        children = childTable.query(
            KeyConditionExpression=Key('device_id').eq(values['device_id']) & Key('bucket_id').gte(childStart) & Key('bucket_id').lt(childEnd)
        )
        return children['Items']
    def ProcessEvent(self, values: dict):
        """ Take an event, aggregate it, and call the next bucket(s) """
        # All rules other than Minute follow a standard pattern:
        # - Get the existing row for the bucket, if present
        # - Get all of the rows from the previous bucket that aggregate into this bucket
        # - Aggregate
        # - Save to table
        item = self.GetItem(values)
        insert = False
        if item is None:
            insert = True
            item = { 
                'device_id': values['device_id'],
                'bucket_id': self.BucketID(),
                "current": Decimal(0),
                "volts": Decimal(0),
                "watt_hours": Decimal(0),
                "cost_usd": Decimal(0)
            }

        # Get all of the constituent rows
        childRows = self.GetChildren(values)

        # Update by averaging or summing 
        # FIXME this pattern has bugs. If I insert to the minute bucket and then sum to the hour bucket,
        #  the next minute bucket will re-add the aggregated values.
        # Should I add the minute bucket to each bucket?
        # Or recompute each one by selecting everything each time?
        # Selecting and re-agg'ing seems more reliable, but may incure higher DynamoDB costs
        item['current'] = item['current'] + (values['current'] / Decimal(self.CountInBucket()))
        item['volts'] = item['volts'] + (values['volts'] / Decimal(self.CountInBucket()))
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
        
    def GetItem(self, values: dict) -> dict:
        """ Returns the item from the this bucket table """
        table = self.GetTable("EnergyMonitor." + self.TableSuffix)
        
        values['bucket_id'] = self.BucketID()
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
    def __init__(self, eventTime: datetime):
        super().__init__(eventTime, None)
        self.TableSuffix = "Minute"
        self.BucketFormat = "%Y-%m-%dT%H:%MZ"
        self.AggTo = HourBucket

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime() + timedelta(minutes=1)
    
    def CountInBucket(self) -> int:
        return 1 

    def ProcessEvent(self, values: dict):
        item = self.GetItem(values)
        if item is not None:
            # Got a matching row
            # This is an error for Minute bucket, but let's just log and ignore
            print("Error - found row that shouldn't exist with values {0} ...... Skiping writing {1}".format(str(item), str(values)))
        else:
            table = self.GetTable("EnergyMonitor." + self.TableSuffix)
            table.put_item(Item=values)
        self.AggTo(self.EventTime, self)

class HourBucket(BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule):
        super().__init__(eventTime, aggedFrom)
        self.TableSuffix = "Hour"
        self.BucketFormat = "%Y-%m-%dT%H:00Z"
        self.AggTo = DayBucket

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime() + timedelta(hours=1)
    
    def CountInBucket(self) -> int:
        return (self.BucketEndTime() - self.BucketStartTime()).total_seconds()/60

class DayBucket(BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule):
        super().__init__(eventTime, aggedFrom)
        self.TableSuffix = "Day"
        self.BucketFormat = "%Y-%m-%dT00:00Z"
        self.AggTo = MonthBucket

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime() + timedelta(days=1)
    
    def CountInBucket(self) -> int:
        return (self.BucketEndTime() - self.BucketStartTime()).total_seconds()/3600

class MonthBucket(BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule):
        super().__init__(eventTime, aggedFrom)
        self.TableSuffix = "Month"
        self.BucketFormat = "%Y-%m-01T00:00Z"
        self.AggTo = YearBucket

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime() + monthdelta(1)
    
    def CountInBucket(self) -> int:
        return (self.BucketEndTime() - self.BucketStartTime()).days

class YearBucket(BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule):
        super().__init__(eventTime, aggedFrom)
        self.TableSuffix = "Year"
        self.BucketFormat = "%Y-01-01T00:00Z"

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime().replace(year=self.BucketStartTime().year+1)
    
    def CountInBucket(self) -> int:
        return (self.BucketEndTime() - self.BucketStartTime()).days


# Tests
if (__name__ == "__main__"):
    baseCase     = datetime(2018,5,5,13,39,12, tzinfo=timezone.utc)
    leapyearCase = datetime(2016,2,29,13,39,12, tzinfo=timezone.utc)
    dstStartCase = datetime(2018,3,11,2,39,12, tzinfo=timezone.utc) # No effect, since UTC
    
    
    mb = MinuteBucket(baseCase)
    assert mb.BucketID() == "2018-05-05T13:39Z"
    assert mb.BucketEndTime() == datetime(2018,5,5,13,40, tzinfo=timezone.utc)
    assert mb.CountInBucket() == 1

    hb = HourBucket(baseCase, mb)
    assert hb.BucketID() == "2018-05-05T13:00Z"
    assert hb.BucketEndTime() == datetime(2018,5,5,14,0, tzinfo=timezone.utc)
    assert hb.CountInBucket() == 60

    db = DayBucket(baseCase, hb)
    assert db.BucketID() == "2018-05-05T00:00Z"
    assert db.BucketEndTime() == datetime(2018,5,6,0,0, tzinfo=timezone.utc)
    assert db.CountInBucket() == 24

    mt = MonthBucket(baseCase, db)
    assert mt.BucketID() == "2018-05-01T00:00Z"
    assert mt.BucketEndTime() == datetime(2018,6,1,0,0, tzinfo=timezone.utc)
    assert mt.CountInBucket() == 31

    yb = YearBucket(baseCase, mb)
    assert yb.BucketID() == "2018-01-01T00:00Z"
    assert yb.BucketEndTime() == datetime(2019,1,1,0,0, tzinfo=timezone.utc)
    assert yb.CountInBucket() == 365

    ml = MonthBucket(leapyearCase, None)
    assert ml.BucketID() == "2016-02-01T00:00Z"
    assert ml.BucketEndTime() == datetime(2016,3,1,0,0, tzinfo=timezone.utc)
    assert ml.CountInBucket() == 29

    yl = YearBucket(leapyearCase, None)
    assert yl.BucketID() == "2016-01-01T00:00Z"
    assert yl.BucketEndTime() == datetime(2017,1,1,0,0, tzinfo=timezone.utc)
    assert yl.CountInBucket() == 366

    # Override the dynamo table with mock for testing
    BucketRule.GetTable = lambda self, tableName: MockTable(tableName)
    #MockTable.get_item = lambda self,  **kwargs: {} # Should not be a matching row

    mb.ProcessEvent(
        { "device_id": "TestEvent",
            "bucket_id": baseCase.strftime("%Y-%m-%dT%H:%MZ"),
            "current": Decimal('1.1'),
            "volts": Decimal('242.0'),
            "watt_hours": Decimal('266.2')/60,
            "cost_usd": Decimal('266.2')/60*Decimal('0.1326')/Decimal(1000)
        }
    )