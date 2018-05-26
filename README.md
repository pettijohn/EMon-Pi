# Energy monitoring on a Raspberry Pi.

Goal is to read current and voltage on a houshold AC circuit
via an ADS1115 I2C ADC, rendering a simple UI on a textual multi-line
display, finally logging data to an AWS lambda function for historical analysis plus a web UI.

# Hardware
Contains libraries for reading the sensors, updating the text display, and pushing to AWS IoT via MQTT.

Download certificates for the device to `hardware/certificates`

## References 
* ADS 1115 - I2C ADC    
  * Example https://learn.adafruit.com/raspberry-pi-analog-to-digital-converters/ads1015-slash-ads1115
  * Datasheet https://cdn-shop.adafruit.com/datasheets/ads1115.pdf
* I2C Multiline Textual Display 
  * Sample Code https://bitbucket.org/MattHawkinsUK/rpispy-misc/raw/master/python/lcd_i2c.py

# AWS-Setup
Contains code to create DynamoDB tables, configure IoT, and upload Lambda functions, etc. 

Run `aws configure` and enter the access key for an IAM user with the necessary permissions. 

# TODO
* 