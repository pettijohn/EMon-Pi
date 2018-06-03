from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json
from decimal import Decimal


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def Client(arn):
    # For certificate based connection
    # Hard-code the ARN for now

    myMQTTClient = AWSIoTMQTTClient(arn)
    # Configurations
    # For TLS mutual authentication
    myMQTTClient.configureEndpoint("a1saci22bpq1k0.iot.us-east-1.amazonaws.com", 8883)
    #myMQTTClient.configureEndpoint("a1saci22bpq1k0.iot.us-east-1.amazonaws.com", 443)
    myMQTTClient.configureCredentials("./hardware/certificates/root-CA.crt", "./hardware/certificates/EMonPi.private.key", "./hardware/certificates/EMonPi.cert.pem")

    myMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
    myMQTTClient.configureDrainingFrequency(1)  # Draining: 1 Hz
    myMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
    myMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec


    myMQTTClient.connect()
    return myMQTTClient


if __name__ == "__main__":
    client = Client()
    myMQTTClient.publish("EnergyReading/Test", "This is a random string, not JSON", 1)
    myMQTTClient.disconnect()