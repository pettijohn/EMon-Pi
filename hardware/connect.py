from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import simplejson
from amazon.ion import simpleion
from decimal import Decimal

# https://s3.amazonaws.com/aws-iot-device-sdk-python-docs/sphinx/html/index.html#
# https://github.com/aws/aws-iot-device-sdk-python
# TODO - is there a way to discover this? Once we connect with the certificate, do we know somehow? 
arn = "arn:aws:iot:us-east-1:422446087002:thing/EMonPi"


current = Decimal(str(0.002))
volts = Decimal(str(242.0))
rate = Decimal(str(0.1326))
payload = { "device_id": arn,
    "bucket_id": "2018-05-08T20:18Z",
    "current": (current),
    "volts": (volts),
    "watt_hours": (Decimal(str(current*volts/60))),
    "cost_usd": Decimal(str(current*volts/60*rate))
}

print(simpleion.dumps(payload))

quit()

# For certificate based connection
# Hard-code the ARN for now

myMQTTClient = AWSIoTMQTTClient(arn)
# Configurations
# For TLS mutual authentication
myMQTTClient.configureEndpoint("a1saci22bpq1k0.iot.us-east-1.amazonaws.com", 8883)
#myMQTTClient.configureEndpoint("a1saci22bpq1k0.iot.us-east-1.amazonaws.com", 443)
myMQTTClient.configureCredentials("./hardware/certificates/root-CA.crt", "./hardware/certificates/EMonPi.private.key", "./hardware/certificates/EMonPi.cert.pem")

myMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myMQTTClient.configureDrainingFrequency(1)  # Draining: 2 Hz
myMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec


myMQTTClient.connect()


jsonStr = simplejson.dumps(payload, use_decimal=True)
jsonDict = simplejson.loads(jsonStr, use_decimal=True)

d = Decimal('30.40')
simplejson.dumps(d)
simplejson.loads(simplejson.dumps(d))
f = simplejson.loads(simplejson.dumps(d)) # f is float
f = simplejson.loads(simplejson.dumps(d), use_decimal=True) # f is decimal

d = simplejson.dumps(d, use_decimal=True) # d is string
f = simplejson.loads(d) # f is float
f = simplejson.loads(d, use_decimal=True) # f is decimal
z = simplejson.loads(simplejson.dumps(Decimal('30.40'), use_decimal=True), use_decimal=True) # round trip - decimal

foo = """
{
  "device_id": "arn:aws:iot:us-east-1:422446087002:thing/EMonPi",
  "bucket_id": "2018-05-08T20:18Z",
  "current": 0.002,
  "volts": 242,
  "watt_hours": 0.008066666666666666,
  "cost_usd": 0.00106964
}
"""
dz = simplejson.loads(foo, use_decimal=True)

#myMQTTClient.publish("EnergyReading/Test", jsonStr, 1)
myMQTTClient.publish("EnergyReading/Test", "This is a random string, not JSON", 1)
myMQTTClient.disconnect()