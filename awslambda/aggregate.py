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

    def BucketID(self) -> str:
        """ ID used in underlying storage, the start of the bucket's time window """
        return self.EventTime.strftime(self.BucketFormat)
    def BucketStartTime(self) -> datetime:
        return datetime.strptime(self.BucketID(), self.BucketFormat).replace(tzinfo=timezone.utc)
    def BucketEndTime(self) -> datetime:
        """ Bucket end time, INCLUSIVE. Must go to end of child bucket. """ 
        # Subclass must implement
        pass
    def CountInBucket(self) -> int:
        """ How many of the previous bucket are required to make this bucket? """
        # Subclass must implement
        pass

    def NextBucketStart(self) -> datetime:
        """ Start of next bucket. """
        # Subclass must implement
        pass

    def GetChildren(self):
        # Find the start and end bucket IDs for this bucket
        childStart = self.BucketID()
        childEnd = self.BucketEndTime().strftime(self.AggedFrom.BucketFormat)
        # The child bucket
        childTable = self.GetTable(self.AggedFrom.TableSuffix)
        children = childTable.query(
            KeyConditionExpression=Key('device_id').eq(self.Values['device_id']) & Key('bucket_id').between(childStart, childEnd)
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
                "current": Decimal(0),
                "volts": Decimal(0),
                "watt_hours": Decimal(0),
                "cost_usd": Decimal(0)
            }

        # Get all of the constituent rows
        childRows = self.GetChildren()
        if len(childRows) != self.CountInBucket():
            print("WARN: Bucket {0} {1} has {2} children, expected {3}".format(self.TableSuffix, self.BucketID(), len(childRows), self.CountInBucket()))

        # Update by averaging or summing 
        # Recompute each one by selecting all of its children
        # Selecting and re-agg'ing is self-healing and better than trying to only incrementally update
        item['current'] = sum(map(lambda i: i['current'], childRows)) / self.CountInBucket()
        item['volts'] = sum(map(lambda i: i['volts'], childRows)) / self.CountInBucket()
        item['watt_hours'] = sum(map(lambda i: i['watt_hours'], childRows))
        item['cost_usd'] = sum(map(lambda i: i['cost_usd'], childRows))
        # if not chain:
        #     return item

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
                UpdateExpression="set #c=:c, volts=:v, watt_hours=:w, cost_usd=:u",
                ExpressionAttributeValues={
                    ':c': item['current'],
                    ':v': item['volts'],
                    ':w': item['watt_hours'],
                    ':u': item['cost_usd']
                },
                ExpressionAttributeNames={
                    # CURRENT is a reserved word in DynamoDB
                    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
                    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html 
                    "#c":"current"
                },
                ReturnValues="UPDATED_NEW"
            )

        if chain and self.AggTo is not None:
            # Aggregate to the next level
            nextLevel = self.AggTo(self.EventTime, self, item)
            return nextLevel.ProcessEvent(chain)
        else:
            return results
        
    def GetItem(self) -> dict:
        """ Returns the item from the this bucket table """
        table = self.GetTable(self.TableSuffix)
        
        self.Values['bucket_id'] = self.BucketID()
        response = table.get_item(
            Key={
                'device_id': self.Values['device_id'],
                'bucket_id': self.Values['bucket_id']
            }
        )
        if 'Item' in response:
            return response['Item']
        else:
            return None

class MinuteBucket(BucketRule):
    def __init__(self, eventTime: datetime, values: dict):
        super().__init__(eventTime, None, values)
        self.TableSuffix = "Minute"
        self.BucketFormat = "%Y-%m-%dT%H:%MZ"
        self.AggTo = HourBucket

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime() + timedelta(minutes=1) - timedelta(seconds=1)
    
    def CountInBucket(self) -> int:
        return 1 

    def NextBucketStart(self) -> timedelta:
        return self.BucketStartTime() + timedelta(minutes=1)

    def ProcessEvent(self, chain=True):
        item = self.GetItem()
        if item is not None:
            # Got a matching row
            # This is an error for Minute bucket, but let's just log and ignore
            print("Error - found row that shouldn't exist with values {0} ...... Skiping writing {1}".format(str(item), str(self.Values)))
        else:
            table = self.GetTable(self.TableSuffix)
            results = table.put_item(Item=self.Values)
            
        if chain and self.AggTo is not None and type(self.AggTo) != BucketRule:
            # Aggregate to the next level
            nextLevel = self.AggTo(self.EventTime, self, item)
            return nextLevel.ProcessEvent(chain)
        else:
            return results

class HourBucket(BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule, values: dict):
        super().__init__(eventTime, aggedFrom, values)
        self.TableSuffix = "Hour"
        self.BucketFormat = "%Y-%m-%dT%H:00Z"
        self.AggTo = DayBucket

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime() + timedelta(hours=1) - timedelta(minutes=1)
    
    def NextBucketStart(self) -> timedelta:
        return self.BucketStartTime() + timedelta(hours=1)

    def CountInBucket(self) -> int:
        return int((self.BucketEndTime() - self.BucketStartTime()).total_seconds()/60 + 1)

class DayBucket(BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule, values: dict):
        super().__init__(eventTime, aggedFrom, values)
        self.TableSuffix = "Day"
        self.BucketFormat = "%Y-%m-%dT00:00Z"
        self.AggTo = MonthBucket

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime() + timedelta(days=1) - timedelta(hours=1)
    
    def NextBucketStart(self) -> timedelta:
        return self.BucketStartTime() + timedelta(days=1)

    def CountInBucket(self) -> int:
        return int((self.BucketEndTime() - self.BucketStartTime()).total_seconds()/3600 + 1)

class MonthBucket(BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule, values: dict):
        super().__init__(eventTime, aggedFrom, values)
        self.TableSuffix = "Month"
        self.BucketFormat = "%Y-%m-01T00:00Z"
        self.AggTo = YearBucket

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime() + monthdelta(1) - timedelta(days=1)
    
    def NextBucketStart(self) -> timedelta:
        return self.BucketStartTime() + monthdelta(1)

    def CountInBucket(self) -> int:
        return int((self.BucketEndTime() - self.BucketStartTime()).days + 1)

class YearBucket(BucketRule):
    def __init__(self, eventTime: datetime, aggedFrom: BucketRule, values: dict):
        super().__init__(eventTime, aggedFrom, values)
        self.TableSuffix = "Year"
        self.BucketFormat = "%Y-01-01T00:00Z"
        self.AggTo = None

    def BucketEndTime(self) -> datetime:
        return self.BucketStartTime().replace(year=self.BucketStartTime().year+1) - timedelta(days=1)
    
    def NextBucketStart(self) -> timedelta:
        return self.BucketStartTime().replace(year=self.BucketStartTime().year+1)

    def CountInBucket(self) -> int:
        return 12


# Tests
if (__name__ == "__main__"):
    baseCase     = datetime(2018,5,5,13,39,12, tzinfo=timezone.utc)
    leapyearCase = datetime(2016,2,29,13,39,12, tzinfo=timezone.utc)
    dstStartCase = datetime(2018,3,11,2,39,12, tzinfo=timezone.utc) # No effect, since UTC
    
    
    mb = MinuteBucket(baseCase, {})
    assert mb.BucketID() == "2018-05-05T13:39Z"
    assert mb.BucketEndTime() == datetime(2018,5,5,13,39,59, tzinfo=timezone.utc)
    assert mb.NextBucketStart() == datetime(2018,5,5,13,40,0, tzinfo=timezone.utc)
    assert mb.CountInBucket() == 1
    assert type(mb.CountInBucket()) == int

    hb = HourBucket(baseCase, mb, {})
    assert hb.BucketID() == "2018-05-05T13:00Z"
    assert hb.BucketEndTime() == datetime(2018,5,5,13,59, tzinfo=timezone.utc)
    assert hb.NextBucketStart() == datetime(2018,5,5,14,00, tzinfo=timezone.utc)
    assert hb.CountInBucket() == 60
    assert type(hb.CountInBucket()) == int

    db = DayBucket(baseCase, hb, {})
    assert db.BucketID() == "2018-05-05T00:00Z"
    assert db.BucketEndTime() == datetime(2018,5,5,23,0, tzinfo=timezone.utc)
    assert db.NextBucketStart() == datetime(2018,5,6,0,0, tzinfo=timezone.utc)
    assert db.CountInBucket() == 24
    assert type(db.CountInBucket()) == int

    mt = MonthBucket(baseCase, db, {})
    assert mt.BucketID() == "2018-05-01T00:00Z"
    assert mt.BucketEndTime() == datetime(2018,5,31,0,0, tzinfo=timezone.utc)
    assert mt.NextBucketStart() == datetime(2018,6,1,0,0, tzinfo=timezone.utc)
    assert mt.CountInBucket() == 31
    assert type(mt.CountInBucket()) == int

    yb = YearBucket(baseCase, mb, {})
    assert yb.BucketID() == "2018-01-01T00:00Z"
    assert yb.BucketEndTime() == datetime(2018,12,31,0,0, tzinfo=timezone.utc)
    assert yb.NextBucketStart() == datetime(2019,1,1,0,0, tzinfo=timezone.utc)
    assert yb.CountInBucket() == 365
    assert type(yb.CountInBucket()) == int

    ml = MonthBucket(leapyearCase, None, {})
    assert ml.BucketID() == "2016-02-01T00:00Z"
    assert ml.BucketEndTime() == datetime(2016,2,29,0,0, tzinfo=timezone.utc)
    assert ml.NextBucketStart() == datetime(2016,3,1,0,0, tzinfo=timezone.utc)
    assert ml.CountInBucket() == 29
    assert type(ml.CountInBucket()) == int

    yl = YearBucket(leapyearCase, None, {})
    assert yl.BucketID() == "2016-01-01T00:00Z"
    assert yl.BucketEndTime() == datetime(2016,12,31,0,0, tzinfo=timezone.utc)
    assert yl.NextBucketStart() == datetime(2017,1,1,0,0, tzinfo=timezone.utc)
    assert yl.CountInBucket() == 366
    assert type(yl.CountInBucket()) == int

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
