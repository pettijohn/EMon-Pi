from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json

# For certificate based connection
# Hard-code the ARN for now
# TODO - is there a way to discover this? Once we connect with the certificate, do we know somehow? 
arn = "arn:aws:iot:us-east-1:422446087002:thing/EMonPi"
myMQTTClient = AWSIoTMQTTClient(arn)
# Configurations
# For TLS mutual authentication
myMQTTClient.configureEndpoint("a1saci22bpq1k0.iot.us-east-1.amazonaws.com", 8883)
myMQTTClient.configureCredentials("./hardware/certificates/root-CA.crt", "./hardware/certificates/EMonPi.private.key", "./hardware/certificates/EMonPi.cert.pem")

myMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myMQTTClient.configureDrainingFrequency(1)  # Draining: 2 Hz
myMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec


myMQTTClient.connect()

current = 0.002
volts = 242.0
rate = 0.1326
payload = { "device_id": arn,
    "bucket_id": "2018-05-08T20:18Z",
    "current": current,
    "volts": volts,
    "watt_hours": current*volts/60,
    "cost_usd": current*volts/60*rate
}
myMQTTClient.publish("EnergyReading/Minute", json.dumps(payload), 1)
myMQTTClient.disconnect()