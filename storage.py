from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.fields import RangeKey
from boto.dynamodb2.table import Table

# http://docs.pythonboto.org/en/latest/migrations/dynamodb_v1_to_v2.html
# http://docs.pythonboto.org/en/latest/dynamodb2_tut.html

table = Table.create('EnergyMonitor.Minutes', schema=[
    HashKey('device_arn'),
    RangeKey('bucket_id')
], throughput={
    'read': 1,
    'write': 1
})

table = Table('EnergyMonitor.Minutes')
item = table.put_item(data={
    'device_arn': 'foo',
    'bucket_id': '2018-05-07T18:01Z',
    'amps': 39.001,
    'volts': 242.1,
    'watt_hours': 39.001*242.1/60
})

table = Table('EnergyMonitor.Minutes')
item = table.get_item(
    device_arn='foo',
    bucket_id='2018-05-07T18:01Z'
)